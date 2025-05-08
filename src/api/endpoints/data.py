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
        # NOthing of this is necesary anymore (i think)
        # start_date = parse_date(start_date)
        # end_date = parse_date(end_date)
        start_ym = date_to_annomes(start_date)
        end_ym = date_to_annomes(end_date)
        
        tformdet = load_csv_data("tformdet.csv")
        mstockalm = load_csv_data("mstockalm.csv")
        mproducto = load_csv_data("mproducto.csv")

        # Filtrar por rango de fechas
        if 'ANNOMES' in tformdet.columns:
            tformdet_filtered = tformdet[
                (tformdet['ANNOMES'] >= start_ym) & 
                (tformdet['ANNOMES'] <= end_ym)
            ]
        else:
            tformdet_filtered = tformdet
                    
        # Filtrar por tipo de producto
        if product_type and 'MEDTIP' in mproducto.columns:
            mproducto_filtered = mproducto[mproducto['MEDTIP'] == product_type]
            tformdet_filtered = pd.merge(
                tformdet_filtered, 
                mproducto_filtered[['MEDCOD']], 
                left_on='CODIGO_MED', 
                right_on='MEDCOD', 
                how='inner'
            )

        # Filtrar por estrategia
        if strategy and 'MEDEST' in mproducto.columns:
            mproducto_strategy = mproducto[mproducto['MEDEST'] == strategy]
            
            if not tformdet_filtered.empty and not mproducto_strategy.empty:
                tformdet_filtered = pd.merge(
                    tformdet_filtered,
                    mproducto_strategy[['MEDCOD']],
                    left_on='CODIGO_MED',
                    right_on='MEDCOD',
                    how='inner'
                )
            
        merged_data = tformdet_filtered.copy()
        
        # Unir con mstockalm
        if not tformdet_filtered.empty and not mstockalm.empty:
            if 'CODIGO_MED' in tformdet_filtered.columns and 'MEDCOD' in mstockalm.columns:
                merged_data = pd.merge(
                    tformdet_filtered, 
                    mstockalm, 
                    left_on='CODIGO_MED', 
                    right_on='MEDCOD', 
                    how='left'
                )
        
        # Unir con mproducto
        if not merged_data.empty and not mproducto.empty:
            if 'CODIGO_MED' in merged_data.columns and 'MEDCOD' in mproducto.columns:
                merged_data = pd.merge(
                    merged_data, 
                    mproducto, 
                    left_on='CODIGO_MED', 
                    right_on='MEDCOD', 
                    how='left'
                )
        
        if all(col in merged_data.columns for col in ['VENTA', 'SIS', 'INTERSAN']):
            merged_data['TOTAL_CONSUMO'] = merged_data['VENTA'] + merged_data['SIS'] + merged_data['INTERSAN']
        
        result = merged_data.to_dict(orient='records')
        
        return {
            "start_date": start_date,
            "end_date": end_date,
            "product_type": product_type,
            "strategy": strategy,
            "count": len(result),
            "data": result
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