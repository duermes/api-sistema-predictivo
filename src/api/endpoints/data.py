from fastapi import APIRouter, Query, Depends
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from src.exceptions import NotFound, BadRequest
import json


router = APIRouter()

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

def load_csv_data(filename: str) -> pd.DataFrame:
     file_path = DATA_DIR / filename
     if not file_path.exists():
         raise NotFound(f"Archivo no encontrado: {file_path}")
     return pd.read_csv(file_path)

def parse_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%d-%m-%Y")
    except ValueError:
        raise BadRequest(f"Formato de fecha inválido: {date_str}. Debe ser DD-MM-YYYY.")    

def date_to_annomes(date_obj: datetime) -> str:
    return date_obj.strftime("%Y%m")

@router.get("/summary")
async def get_summary(
    start_date: int = Query(..., description="Fecha de inicio (DD-MM-YYYY)"),
    end_date: int = Query(..., description="Fecha de fin (DD-MM-YYYY)"),
    product_type: Optional[List[str]] = Query(None, description="Tipos de producto (opcional, enviar ?product_type=A&product_type=B o ?product_type=A,B,C)"),
    strategy: Optional[List[str]] = Query(None, description="Estrategias de análisis (opcional, enviar ?strategy=S1&strategy=S2 o ?strategy=S1,S2)"),
    real_time: bool = Query(False, description="Usar stock de mstockalm (STKSALDO) como STOCK_FIN")
):
    try:
        tformdet_orig = load_csv_data("tformdet.csv")
        mstockalm_orig = load_csv_data("mstockalm.csv")
        mproducto_orig = load_csv_data("mproducto.csv")
        
        # --- Procesamiento de product_type y strategy para múltiples valores ---
        product_type_list = []
        if product_type:
            for item in product_type: # FastAPI debería entregar una lista si hay múltiples query params con el mismo nombre
                product_type_list.extend([pt.strip() for pt in item.split(',') if pt.strip()])
            product_type_list = list(set(pt for pt in product_type_list if pt)) # Únicos y remover vacíos

        strategy_list = []
        if strategy:
            for item in strategy:
                strategy_list.extend([s.strip() for s in item.split(',') if s.strip()])
            strategy_list = list(set(s for s in strategy_list if s)) # Únicos y remover vacíos


        # --- 1. Prepare mproducto (Product Details) ---
        mproducto = mproducto_orig.copy()
        if product_type_list:
            mproducto = mproducto[mproducto["MEDTIP"].astype(str).isin(product_type_list)]
        if strategy_list:
            mproducto = mproducto[mproducto["MEDEST"].astype(str).isin(strategy_list)]
        
        mproducto_cols_to_select = ["MEDCOD", "MEDNOM", "MEDPRES", "MEDCNC", "MEDTIP", "MEDPET", "MEDFF", "MEDEST"]
        # Asegurarse que las columnas existan en mproducto antes de seleccionar
        mproducto_cols_existentes = [col for col in mproducto_cols_to_select if col in mproducto.columns]
        mproducto_unique = mproducto[mproducto_cols_existentes].drop_duplicates(subset=["MEDCOD"])

        # --- 2. Prepare tformdet (Transaction/Consumption Data) ---
        tformdet = tformdet_orig.copy()
        # Asegurar que ANNOMES sea numérico para la comparación
        if 'ANNOMES' not in tformdet.columns:
            raise HTTPException(status_code=400, detail="La columna 'ANNOMES' no existe en tformdet.csv")
        tformdet['ANNOMES'] = pd.to_numeric(tformdet['ANNOMES'], errors='coerce')
        tformdet = tformdet.dropna(subset=['ANNOMES']) # Eliminar filas donde ANNOMES no fue numérico
        tformdet['ANNOMES'] = tformdet['ANNOMES'].astype(int)

        tformdet = tformdet[(tformdet["ANNOMES"] >= start_date) & (tformdet["ANNOMES"] <= end_date)]

        num_unique_anomes = 0
        if not tformdet.empty:
            num_unique_anomes = len(tformdet["ANNOMES"].unique())
                
        if tformdet.empty:
            return {"count": 0, "data_count": 0, "anomes": 0, "months": [], "data": []}

        # Verificar columnas para TOTAL_CONSUMO
        consumo_cols = ["VENTA", "SIS", "INTERSAN"]
        for col in consumo_cols:
            if col not in tformdet.columns:
                raise HTTPException(status_code=400, detail=f"La columna '{col}' requerida para TOTAL_CONSUMO no existe en tformdet.csv")
            tformdet[col] = pd.to_numeric(tformdet[col], errors='coerce').fillna(0) # Convertir a numérico y rellenar NaN

        tformdet["TOTAL_CONSUMO"] = tformdet[consumo_cols].sum(axis=1)
        
        # --- 3. Filter tformdet based on selected products ---
        if product_type_list or strategy_list:
            if not mproducto_unique.empty and "MEDCOD" in mproducto_unique.columns:
                 tformdet = tformdet.merge(mproducto_unique[["MEDCOD"]], left_on="CODIGO_MED", right_on="MEDCOD", how="inner")
            else:
                 tformdet = pd.DataFrame(columns=tformdet.columns) 
            
            if tformdet.empty:
                return {"count": 0, "data_count": 0, "anomes": num_unique_anomes, "months": [], "data": []}

        # --- 4. Calculate Monthly Consumption Pivot Table ---
        if tformdet.empty or "CODIGO_MED" not in tformdet.columns: # Añadida verificación de CODIGO_MED
            consumo_pivot = pd.DataFrame() # Vacío, se manejará más adelante
        else:
            consumo_mensual = tformdet.groupby(["CODIGO_MED", "ANNOMES"])["TOTAL_CONSUMO"].sum().reset_index()
            if consumo_mensual.empty:
                consumo_pivot = pd.DataFrame()
            else:
                consumo_pivot = consumo_mensual.pivot(index="CODIGO_MED", columns="ANNOMES", values="TOTAL_CONSUMO").fillna(0)

        # --- 5. Calculate CPMA and CONSUMO_MEN ---
        # Las columnas de meses en consumo_pivot SON numéricas (ej. 202401, 202402)
        month_columns_numeric = [col for col in consumo_pivot.columns if isinstance(col, (int, np.integer, float, np.floating))]
        
        if consumo_pivot.empty or not month_columns_numeric: # Chequeo si consumo_pivot está vacío
            # Si consumo_pivot está vacío, creamos las columnas CPMA y CONSUMO_MEN en un DF vacío con índice CODIGO_MED
            if consumo_pivot.empty and "CODIGO_MED" in tformdet.columns: # Si hubo productos pero sin datos pivoteables
                 # Crear un índice con los CODIGO_MED que pasaron los filtros
                 unique_codigos_med = tformdet["CODIGO_MED"].unique()
                 consumo_pivot = pd.DataFrame(index=pd.Index(unique_codigos_med, name="CODIGO_MED"))

            consumo_pivot["CPMA"] = 0.0
            consumo_pivot["CONSUMO_MEN"] = 0
        else:
            consumo_pivot["CPMA"] = consumo_pivot[month_columns_numeric].mean(axis=1)
            consumo_pivot["CONSUMO_MEN"] = (consumo_pivot[month_columns_numeric] > 0).sum(axis=1)
        
        months_for_output = sorted([str(col) for col in month_columns_numeric]) # Para la salida JSON
        
        if "CODIGO_MED" not in consumo_pivot.index.names and "CODIGO_MED" not in consumo_pivot.columns:
            # Si CODIGO_MED no es índice ni columna (caso de consumo_pivot totalmente vacío)
            # y necesitamos un índice para los merges posteriores.
            # Si tformdet tenía CODIGO_MED, podríamos usar esos.
            # Por ahora, si es vacío, los merges fallarán o darán resultados vacíos, lo cual es manejable.
             consumo_pivot = consumo_pivot.reset_index(drop=True) # O manejar de otra forma si es totalmente vacío
        else:
            consumo_pivot = consumo_pivot.reset_index()


        # --- 6. Prepare and Merge STOCK_FIN ---
        if real_time:
            stock_df = mstockalm_orig.copy()
            if not stock_df.empty and "MEDCOD" in stock_df.columns and "STKSALDO" in stock_df.columns:
                stock_df['STKSALDO'] = pd.to_numeric(stock_df['STKSALDO'], errors='coerce').fillna(0)
                stock_to_use = stock_df.groupby("MEDCOD", as_index=False)["STKSALDO"].sum()
                stock_to_use = stock_to_use.rename(columns={"MEDCOD": "CODIGO_MED", "STKSALDO": "STOCK_FIN"})
                if "CODIGO_MED" in consumo_pivot.columns:
                     consumo_pivot = consumo_pivot.merge(stock_to_use, on="CODIGO_MED", how="left")
                else: # consumo_pivot podría estar vacío o sin CODIGO_MED
                     consumo_pivot["STOCK_FIN"] = pd.NA
            else:
                consumo_pivot["STOCK_FIN"] = pd.NA
        else:
            if not tformdet.empty and "CODIGO_MED" in tformdet.columns and "STOCK_FIN" in tformdet.columns:
                tformdet['STOCK_FIN'] = pd.to_numeric(tformdet['STOCK_FIN'], errors='coerce').fillna(0)
                latest_stock_fin_in_period = tformdet.sort_values("ANNOMES", ascending=False)\
                                                 .drop_duplicates(subset=["CODIGO_MED"], keep="first")\
                                                 [["CODIGO_MED", "STOCK_FIN"]]
                if "CODIGO_MED" in consumo_pivot.columns:
                    consumo_pivot = consumo_pivot.merge(latest_stock_fin_in_period, on="CODIGO_MED", how="left")
                else:
                    consumo_pivot["STOCK_FIN"] = pd.NA
            else:
                consumo_pivot["STOCK_FIN"] = pd.NA

        if "STOCK_FIN" not in consumo_pivot.columns:
            consumo_pivot["STOCK_FIN"] = pd.NA
        consumo_pivot["STOCK_FIN"] = pd.to_numeric(consumo_pivot["STOCK_FIN"], errors='coerce').fillna(0.0)

        # --- 7. Calculate NIVELES and SITUACION ---
        # Asegurar que CPMA exista y sea numérico
        if "CPMA" not in consumo_pivot.columns: consumo_pivot["CPMA"] = 0.0
        consumo_pivot["CPMA"] = pd.to_numeric(consumo_pivot["CPMA"], errors='coerce').fillna(0.0)

        consumo_pivot["NIVELES"] = consumo_pivot["STOCK_FIN"] / consumo_pivot["CPMA"]
        consumo_pivot["NIVELES"] = consumo_pivot["NIVELES"].replace([np.inf, -np.inf], np.nan)
        consumo_pivot["NIVELES"] = consumo_pivot["NIVELES"].fillna(0.0)

        def situacion(nivel, cpma, stock_fin):
            if pd.isna(nivel): return "Indeterminado" 
            if cpma == 0:
                return "Sobrestock (Sin Consumo)" if stock_fin > 0 else "Normostock (Sin Movimiento)" 
            if nivel > 7: return "Sobrestock"
            elif nivel < 1: return "Substock"
            else: return "Normostock"

        if not consumo_pivot.empty :
            consumo_pivot["SITUACION"] = consumo_pivot.apply(lambda row: situacion(row["NIVELES"], row["CPMA"], row["STOCK_FIN"]), axis=1)
        else:
            consumo_pivot["SITUACION"] = None # O lista vacía

        # --- 8. Merge Product Details ---
        if not consumo_pivot.empty and "CODIGO_MED" in consumo_pivot.columns and not mproducto_unique.empty and "MEDCOD" in mproducto_unique.columns:
            consumo_pivot = consumo_pivot.merge(mproducto_unique, left_on="CODIGO_MED", right_on="MEDCOD", how="left", suffixes=('', '_mprod'))
            if 'MEDCOD_mprod' in consumo_pivot.columns:
                consumo_pivot = consumo_pivot.drop(columns=['MEDCOD_mprod'])
            # Si MEDCOD (de mproducto_unique) se unió y es diferente de CODIGO_MED, puede que quieras conservarlo o renombrarlo.
            # Por ahora, asumimos que CODIGO_MED es la clave principal.
        else: # Si no se puede hacer el merge, asegurarse que las columnas de mproducto existan con Nones
            for col in mproducto_cols_to_select:
                if col != "MEDCOD" and col not in consumo_pivot.columns: # MEDCOD ya es CODIGO_MED
                    consumo_pivot[col] = None


        # --- 9. Finalize DataFrame for Output ---
        final_df_data = {}
        # Columnas base que siempre deben existir (aunque estén vacías si no hay datos)
        base_cols = ["CODIGO_MED", "CPMA", "CONSUMO_MEN", "STOCK_FIN", "NIVELES", "SITUACION"]
        
        if consumo_pivot.empty: # Si no hay datos de consumo en absoluto
            final_df = pd.DataFrame(columns=base_cols + mproducto_cols_to_select[1:] + months_for_output) # Crear con todas las columnas posibles vacías
        else:
            # Columnas de atributos de producto (ya limpias, sin MEDCOD duplicado)
            product_attr_cols_final = [col for col in mproducto_cols_to_select if col != "MEDCOD" and col in consumo_pivot.columns]

            # Reconstruir `ordered_columns` para el `final_df`
            # Las columnas de meses (ej. 202401) deben ser las numéricas para seleccionar de `consumo_pivot`
            # pero sus equivalentes string para las claves del JSON final.
            ordered_columns_for_selection = ["CODIGO_MED"] + \
                                    month_columns_numeric + \
                                    ["CPMA", "CONSUMO_MEN", "STOCK_FIN", "NIVELES", "SITUACION"] + \
                                    product_attr_cols_final
            
            # Crear `final_df` seleccionando columnas existentes y luego renombrando las de meses
            existing_cols_for_selection = [col for col in ordered_columns_for_selection if col in consumo_pivot.columns]
            final_df = consumo_pivot[existing_cols_for_selection].copy() # Usar .copy() para evitar SettingWithCopyWarning

            # Renombrar columnas numéricas de meses a string para la salida JSON
            rename_map = {num_col: str(num_col) for num_col in month_columns_numeric if num_col in final_df.columns}
            final_df.rename(columns=rename_map, inplace=True)
            
            # Asegurar que todas las columnas de `months_for_output` (strings) existan en final_df, rellenar con 0 si no
            for month_str in months_for_output:
                if month_str not in final_df.columns:
                    final_df[month_str] = 0.0 # O null, según prefieras para meses sin consumo

            # Reordenar final_df con los nombres de columna string para los meses
            final_ordered_cols_with_str_months = ["CODIGO_MED"] + \
                                   months_for_output + \
                                   ["CPMA", "CONSUMO_MEN", "STOCK_FIN", "NIVELES", "SITUACION"] + \
                                   product_attr_cols_final
            
            # Asegurar que todas las columnas ordenadas existan, añadir las que falten con None/0.0
            for col_name in final_ordered_cols_with_str_months:
                if col_name not in final_df.columns:
                    if col_name in base_cols or col_name in months_for_output: # Numéricas o meses
                        final_df[col_name] = 0.0
                    else: # Atributos de producto
                        final_df[col_name] = None 
            
            final_df = final_df[final_ordered_cols_with_str_months] # Aplicar el orden final


        # Rellenar NaN en columnas descriptivas
        descriptive_text_cols = ["MEDNOM", "MEDPRES", "MEDCNC", "MEDTIP", "MEDPET", "MEDFF", "MEDEST"]
        for col in descriptive_text_cols:
            if col in final_df.columns:
                final_df[col] = final_df[col].fillna("Desconocido")
        
        # Convertir NaN restantes a None para JSON (to_json lo hace, pero explicitamente es más claro)
        final_df = final_df.replace({np.nan: None})
        
        # Asegurar que las columnas de meses (que ahora son strings) no tengan None si deben ser numéricas (0.0)
        for month_str_col in months_for_output:
            if month_str_col in final_df.columns:
                final_df[month_str_col] = final_df[month_str_col].fillna(0.0)


        data_output = json.loads(final_df.to_json(orient="records", date_format="iso"))


        return {
            "count": len(final_df),
            "data_count": len(data_output), 
            "anomes": num_unique_anomes, 
            "months": months_for_output, 
            "data": data_output
        }
    except ValueError as e:
        raise BadRequest(detail=f"Error en formato de fecha: {str(e)}")
    except Exception as e:
        raise BadRequest(detail=f"Error al procesar datos: {str(e)}")
    except FileNotFoundError as e:
        raise NotFound(detail=f"Error: Archivo CSV no encontrado - {str(e)}")
    

# @router.get("/consumo")

# @router.get("/productos")

# @router.get("/stock")

# @router.get("/predict/disponibilidad")

# @router.get("/resumen-estadistico")