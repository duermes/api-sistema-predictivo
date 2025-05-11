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
    product_type: str = Query(None, description="Tipo de producto (opcional)"),
    strategy: str = Query(None, description="Estrategia de análisis (opcional)")
):
    try:
        tformdet_orig = load_csv_data("tformdet.csv")
        mstockalm_orig = load_csv_data("mstockalm.csv") # NOT using it
        mproducto_orig = load_csv_data("mproducto.csv")

        mproducto = mproducto_orig.copy()
        if product_type:
            mproducto = mproducto[mproducto["MEDTIP"] == product_type]
        if strategy:
            mproducto = mproducto[mproducto["MEDEST"] == strategy]
        
        mproducto_cols_to_select = ["MEDCOD", "MEDNOM", "MEDPRES", "MEDCNC", "MEDTIP", "MEDPET", "MEDFF", "MEDEST"]
        mproducto_unique = mproducto[mproducto_cols_to_select].drop_duplicates(subset=["MEDCOD"])
        print(f"mproducto_unique tiene {mproducto_unique.shape[0]} filas y {mproducto_unique.shape[1]} columnas.")

        tformdet = tformdet_orig.copy()
        tformdet = tformdet[(tformdet["ANNOMES"] >= start_date) & (tformdet["ANNOMES"] <= end_date)]

        anomes = len(tformdet["ANNOMES"].unique())
         
        if tformdet.empty:
            return {"count": 0, "data count": 0, "data": []}

        tformdet["TOTAL_CONSUMO"] = tformdet[["VENTA", "SIS", "INTERSAN"]].sum(axis=1)
        
        if product_type or strategy:
            tformdet = tformdet.merge(mproducto_unique[["MEDCOD"]], left_on="CODIGO_MED", right_on="MEDCOD", how="inner")
            if tformdet.empty:
                return {"count": 0, "data count": 0, "data": []}

        # Pivot
        if tformdet.empty: # Just a filter, shouldn''t happen but its better be careful no?
            consumo_pivot = pd.DataFrame(columns=["CODIGO_MED"]).set_index("CODIGO_MED")
        else:
            consumo_mensual = tformdet.groupby(["CODIGO_MED", "ANNOMES"])["TOTAL_CONSUMO"].sum().reset_index()
            if consumo_mensual.empty:
                consumo_pivot = pd.DataFrame(columns=["CODIGO_MED"]).set_index("CODIGO_MED")
            else:
                consumo_pivot = consumo_mensual.pivot(index="CODIGO_MED", columns="ANNOMES", values="TOTAL_CONSUMO").fillna(0)

        # CPMA
        month_columns = [col for col in consumo_pivot.columns if isinstance(col, (int, float))] 
        
        if not month_columns:
            consumo_pivot["CPMA"] = 0.0
            consumo_pivot["CONSUMO_MEN"] = 0
        else:
            consumo_pivot["CPMA"] = consumo_pivot[month_columns].mean(axis=1)
            consumo_pivot["CONSUMO_MEN"] = (consumo_pivot[month_columns] > 0).sum(axis=1)

        months = [str(col) for col in sorted(month_columns)]
        
        consumo_pivot = consumo_pivot.reset_index() 

        if not tformdet.empty:
            latest_stock_fin_in_period = tformdet.sort_values("ANNOMES", ascending=False)\
                                             .drop_duplicates(subset=["CODIGO_MED"], keep="first")\
                                             [["CODIGO_MED", "STOCK_FIN"]]
            consumo_pivot = consumo_pivot.merge(latest_stock_fin_in_period, on="CODIGO_MED", how="left")
        else:
            consumo_pivot["STOCK_FIN"] = pd.NA 

        if "STOCK_FIN" not in consumo_pivot.columns:
            consumo_pivot["STOCK_FIN"] = pd.NA
        consumo_pivot["STOCK_FIN"] = consumo_pivot["STOCK_FIN"].fillna(0) 

        consumo_pivot["NIVELES"] = consumo_pivot["STOCK_FIN"].astype(float) / consumo_pivot["CPMA"].astype(float)
        consumo_pivot["NIVELES"] = consumo_pivot["NIVELES"].replace([float('inf'), -float('inf')], float('nan'))
        consumo_pivot["NIVELES"] = consumo_pivot["NIVELES"].fillna(0) 

        def situacion(nivel, cpma):
            if pd.isna(nivel):
                 return "Indeterminado" 
            if cpma == 0:
                if nivel > 0 : 
                    return "Sobrestock (Sin Consumo)" 
                else: 
                    return "Normostock (Sin Movimiento)" 
            if nivel > 7:
                return "Sobrestock"
            elif nivel < 1: 
                return "Substock"
            else:
                return "Normostock"

        consumo_pivot["SITUACION"] = consumo_pivot.apply(lambda row: situacion(row["NIVELES"], row["CPMA"]), axis=1)
    
        consumo_pivot = consumo_pivot.merge(mproducto_unique, left_on="CODIGO_MED", right_on="MEDCOD", how="left")
        
        if 'MEDCOD_y' in consumo_pivot.columns:
             consumo_pivot = consumo_pivot.drop(columns=['MEDCOD_y'])
        if 'MEDCOD_x' in consumo_pivot.columns and 'CODIGO_MED' in consumo_pivot.columns: 
             pass 

        ordered_columns = ["CODIGO_MED"] + \
                          [col for col in month_columns if col in consumo_pivot.columns] + \
                          ["CPMA", "CONSUMO_MEN", "STOCK_FIN", "NIVELES", "SITUACION"] + \
                          [col for col in mproducto_cols_to_select if col != "MEDCOD" and col in consumo_pivot.columns] 

        for col in ordered_columns:
            if col not in consumo_pivot.columns:
                consumo_pivot[col] = None 

        consumo_pivot = consumo_pivot[ordered_columns]

        desc_cols = ["MEDNOM", "MEDPRES", "MEDCNC", "MEDTIP", "MEDPET", "MEDFF", "MEDEST"]
        for col in desc_cols:
            if col in consumo_pivot.columns:
                consumo_pivot[col] = consumo_pivot[col].fillna("Desconocido") 

        json_string = consumo_pivot.to_json(orient="records", date_format="iso", default_handler=str)
        data_output = json.loads(json_string)

        return {
            "count": len(consumo_pivot),
            "data count": len(data_output),
            "anomes": anomes,
            "months": months,
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