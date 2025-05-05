import csv
from dbfread import DBF
from pathlib import Path

campos_tformdet = [
    'CODIGO_EJE', 'CODIGO_PRE', 'TIPSUM', 'ANNOMES', 'CODIGO_MED',
    'PRECIO', 'INGRE', 'VENTA', 'SIS', 'INTERSAN',
    'STOCK_FIN', 'FEC_EXP', 'MEDLOTE', 'MEDREGSAN'
]
campos_mstockalm = [
    'ALMCOD' ,'MEDCOD' ,'STKSALDO', 'STKPRECIO', 'STKFECHULT', 'FLG_SOCKET'
]


CURRENT_DIR = Path(__file__).resolve().parent

TFORMDET_DBF = CURRENT_DIR / 'dbf' / 'TFORMDET.DBF'
TFORMDET_CSV = CURRENT_DIR / 'tformdet.csv'
MUSUARIO_DBF = CURRENT_DIR / 'dbf' / 'MUSUARIO.DBF'
MUSUARIO_CSV = CURRENT_DIR / 'usuario.csv'
MPRODUCTO_DBF = CURRENT_DIR / 'dbf' / 'MPRODUCTO.DBF'
MPRODUCTO_CSV = CURRENT_DIR / 'mproducto.csv'
MSTOCK_DBF = CURRENT_DIR / 'dbf' / 'MSTOCKALM.DBF'
MSTOCK_CSV = CURRENT_DIR / 'mstockalm.csv'

def process_dbf_to_csv(dbf_path, csv_path, campos=None):
    print(f"Procesando {dbf_path} -> {csv_path}")
    
    dbf = DBF(dbf_path)
    dbf.load()
    if campos is None:
        campos = dbf.field_names
        print(f"Campos detectados automáticamente: {campos}")
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(campos) 
        
        for record in dbf:
            row = [record.get(campo, '') for campo in campos]
            writer.writerow(row)
    
    print(f"Archivo CSV generado: {csv_path}")

process_dbf_to_csv(TFORMDET_DBF, TFORMDET_CSV, campos_tformdet)
process_dbf_to_csv(MSTOCK_DBF, MSTOCK_CSV, campos_mstockalm)
process_dbf_to_csv(MPRODUCTO_DBF, MPRODUCTO_CSV)
process_dbf_to_csv(MUSUARIO_DBF, MUSUARIO_CSV)


# def main():
#     process_dbf_to_csv(TFORMDET_DBF, TFORMDET_CSV, campos_tformdet)
#     process_dbf_to_csv(MSTOCK_DBF, MSTOCK_CSV, campos_mstockalm)
#     process_dbf_to_csv(MPRODUCTO_DBF, MPRODUCTO_CSV)
#     process_dbf_to_csv(MUSUARIO_DBF, MUSUARIO_CSV)
#     pass

# if __name__ == "__main__":
#     main()