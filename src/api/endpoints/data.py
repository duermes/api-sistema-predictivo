from fastapi import APIRouter, Query, Depends
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from src.exceptions import NotFound, BadRequest


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
    start_date: str = Query(..., description="Fecha de inicio (DD-MM-YYYY)"),
    end_date: str = Query(..., description="Fecha de fin (DD-MM-YYYY)"),
    product_type: str = Query(None, description="Tipo de producto (opcional)"),
    strategy: str = Query(None, description="Estrategia de análisis (opcional)")
):
    try:
        start_date = parse_date(start_date)
        end_date = parse_date(end_date)
        start_ym = date_to_annomes(start_date)
        end_ym = date_to_annomes(end_date)
        
        tformdet = load_csv_data("tformdet.csv")
        mstockalm = load_csv_data("mstockalm.csv")
        mproducto = load_csv_data("mproducto.csv")

        if 'ANNOMES' in tformdet.columns:
            tformdet_filtered = tformdet[
                (tformdet['ANNOMES'] >= start_ym) & 
                (tformdet['ANNOMES'] <= end_ym)
            ]
        else:
            # Si no existe ANNOMES se usa todos los datos
            tformdet_filtered = tformdet
                    

        if product_type and 'TIPSUM' in tformdet.columns:
            tformdet_filtered = tformdet_filtered[tformdet_filtered['TIPSUM'] == product_type]
        elif product_type and 'TIPSUM' in mproducto.columns:
            # FALTA EDITAR EL NOMBRE DE LA COLUMNA QUE TIENE TIPO DEL PRODUCTO
            mproducto_filtered = mproducto[mproducto['TIPSUM'] == product_type]
            # TENGO QUE CONFIRMAR LOS CAMPOS NO OLVIDAR :sob: 
            tformdet_filtered = pd.merge(
                tformdet_filtered, 
                mproducto_filtered[['CODIGO_MED']], 
                on='CODIGO_MED', 
                how='inner'
            )
        
        # UNIENDO LA DATA
        if not tformdet_filtered.empty and not mstockalm.empty:
            # NECESITO AASEGURARME QUE LAS COLUMNAS EXISTEN :I
            if 'CODIGO_MED' in tformdet_filtered.columns and 'MEDCOD' in mstockalm.columns:
                merged_data = pd.merge(
                    tformdet_filtered, 
                    mstockalm, 
                    left_on='CODIGO_MED', 
                    right_on='MEDCOD', 
                    how='left'
                )
            else:
                merged_data = tformdet_filtered
        else:
            merged_data = tformdet_filtered
        
        if not merged_data.empty and not mproducto.empty:
            if 'CODIGO_MED' in merged_data.columns and 'MEDCOD' in mproducto.columns:
                merged_data = pd.merge(
                    merged_data, 
                    mproducto, 
                    left_on='CODIGO_MED', 
                    right_on='MEDCOD', 
                    how='left'
                )
        
        # COMO SI FUERA LA CONSULTA PIVOT PERO VERSION CSV
        if not merged_data.empty:
            # REEMPLAZAR POR LAS REALES COLUMNAS NO SE CUALES SON?
            group_cols = ['CODIGO_MED', 'ANNOMES']
            
            # VERIFICAR QUE ESTO SIQUIERA EXISTE
            agg_dict = {}
            if 'PRECIO' in merged_data.columns:
                agg_dict['PRECIO'] = 'mean'
            if 'VENTA' in merged_data.columns:
                agg_dict['VENTA'] = 'sum'
            if 'STOCK_FIN' in merged_data.columns:
                agg_dict['STOCK_FIN'] = 'sum'
            if 'SIS' in merged_data.columns:
                agg_dict['SIS'] = 'sum'
            if 'INTERSAN' in merged_data.columns:
                agg_dict['INTERSAN'] = 'sum'
            
            if all(col in merged_data.columns for col in ['VENTA', 'SIS', 'INTERSAN']):
                merged_data['CONSUMO_TOTAL'] = merged_data['VENTA'] + merged_data['SIS'] + merged_data['INTERSAN']
                agg_dict['CONSUMO_TOTAL'] = 'sum'
            
            summary = merged_data.groupby(group_cols).agg(agg_dict).reset_index()
            result = summary.to_dict(orient='records')
            
            return {
                "start_date": start_date,
                "end_date": end_date,
                "product_type": product_type,
                "strategy": strategy,
                "count": len(result),
                "data": result
            }
        else:
            return {
                "start_date": start_date,
                "end_date": end_date,
                "product_type": product_type,
                "strategy": strategy,
                "count": 0,
                "data": []
            }
            
    except ValueError as e:
        raise BadRequest(detail=f"Error en formato de fecha: {str(e)}")
    except Exception as e:
        raise BadRequest(detail=f"Error al procesar datos: {str(e)}")
    

# @router.get("/consumo")

# @router.get("/productos")

# @router.get("/stock")

# @router.get("/predict/disponibilidad")

# @router.get("/resumen-estadistico")