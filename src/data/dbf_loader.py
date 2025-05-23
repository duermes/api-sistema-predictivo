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


def multiple_dbf_to_csv(dbf_paths, csv_path, campos=None):
    all_rows = []
    for path in dbf_paths:
        dbf = DBF(path)
        dbf.load()
        if campos is None:
            campos = dbf.field_names
        for record in dbf:
            all_rows.append([record.get(campo, '') for campo in campos])

    with open(csv_path, 'w', newline='', encoding='latin1') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(campos)
        writer.writerows(all_rows)
    
    print(f"CSV combinado generado: {csv_path}")

def multiple_dbf_to_csv(dbf_paths, csv_path, output_campos=None, dbf_read_encoding=None, csv_write_encoding='utf-8'):
    all_rows = []
    actual_header_fields = None
    
    print(f"Starting combination for CSV: {csv_path}")
    if dbf_read_encoding:
        print(f"Attempting to read DBF files with encoding: {dbf_read_encoding}")

    for i, path_obj in enumerate(dbf_paths):
        path_str = str(path_obj) # dbfread expects string paths
        try:
            print(f"Processing DBF: {path_str}")
            # Specify the encoding for reading the DBF file and error handling
            dbf = DBF(path_str, encoding=dbf_read_encoding, char_decode_errors='ignore')
            
            if actual_header_fields is None:
                if output_campos:
                    actual_header_fields = list(output_campos)
                else:
                    actual_header_fields = list(dbf.field_names)
                print(f"Using header fields for CSV: {actual_header_fields}")

            for record in dbf: # Iterating is memory-efficient
                row = [record.get(campo, '') for campo in actual_header_fields]
                all_rows.append(row)
        
        except UnicodeDecodeError as e:
            print(f"ERROR: Could not decode {path_str} using encoding '{dbf_read_encoding if dbf_read_encoding else 'dbfread default'}'.")
            print(f"Specific error: {e}")
            if hasattr(e, 'object') and isinstance(e.object, bytes) and hasattr(e, 'start') and hasattr(e, 'end'):
                 print(f"Problematic byte sequence: {e.object[e.start:e.end]}")
            print(f"Consider trying other encodings like 'latin1', 'cp850', 'utf-8', or check the DBF file's origin.")
            print(f"Skipping file {path_str} due to decoding error.")
            continue 
        except Exception as e:
            print(f"An unexpected error occurred while processing {path_str}: {e}")
            # If actual_header_fields could not be set (e.g. first file failed), we should not proceed.
            if actual_header_fields is None and i == 0 :
                print("Failed to process the first DBF file, cannot determine headers. Aborting.")
                return
            continue

    if not actual_header_fields:
        print(f"Could not determine header fields (e.g., all DBF paths failed or were empty). No CSV file generated for {csv_path}.")
        return

    if not all_rows:
        print(f"Warning: No data rows were collected. The CSV file {csv_path} will contain only headers (if headers were determined).")

    try:
        with open(csv_path, 'w', newline='', encoding=csv_write_encoding) as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(actual_header_fields)
            writer.writerows(all_rows)
        print(f"CSV combinado generado: {csv_path} (con {len(all_rows)} filas de datos y codificación '{csv_write_encoding}')")
    except UnicodeEncodeError as e:
        print(f"ERROR: Could not write CSV file {csv_path} with encoding '{csv_write_encoding}'.")
        print(f"Data might contain characters not representable in '{csv_write_encoding}'.")
        print(f"Specific error: {e}")
        print(f"Consider using 'utf-8' as the csv_write_encoding for the multiple_dbf_to_csv function.")
    except Exception as e:
        print(f"An error occurred during CSV writing for {csv_path}: {e}")


'''multiple_dbf_to_csv(
    [CURRENT_DIR / 'dbf' / '2021' / 'TFORMDET.DBF', CURRENT_DIR / 'dbf' / '2022' / 'TFORMDET.DBF', 
     CURRENT_DIR / 'dbf' / '2023' / 'TFORMDET.DBF', CURRENT_DIR / 'dbf' / '2024' / 'TFORMDET.DBF'],
    CURRENT_DIR / 'tformdet.csv',
    campos_tformdet
)
multiple_dbf_to_csv(
    [CURRENT_DIR / 'dbf' / '2021' / 'MSTOCKALM.DBF', CURRENT_DIR / 'dbf' / '2022' / 'MSTOCKALM.DBF', 
     CURRENT_DIR / 'dbf' / '2023' / 'MSTOCKALM.DBF', CURRENT_DIR / 'dbf' / '2024' / 'MSTOCKALM.DBF'],
    CURRENT_DIR / 'mstockalm.csv',
    campos_mstockalm
)'''
multiple_dbf_to_csv(
    [CURRENT_DIR / 'dbf' / '2021' / 'MPRODUCTO.DBF', CURRENT_DIR / 'dbf' / '2022' / 'MPRODUCTO.DBF', 
     CURRENT_DIR / 'dbf' / '2023' / 'MPRODUCTO.DBF', CURRENT_DIR / 'dbf' / '2024' / 'MPRODUCTO.DBF'],
    CURRENT_DIR / 'mproducto.csv'
) 

# process_dbf_to_csv(TFORMDET_DBF, TFORMDET_CSV, campos_tformdet)
# process_dbf_to_csv(MSTOCK_DBF, MSTOCK_CSV, campos_mstockalm)
# process_dbf_to_csv(MPRODUCTO_DBF, MPRODUCTO_CSV)
# process_dbf_to_csv(MUSUARIO_DBF, MUSUARIO_CSV)

# def main():
#     process_dbf_to_csv(TFORMDET_DBF, TFORMDET_CSV, campos_tformdet)
#     process_dbf_to_csv(MSTOCK_DBF, MSTOCK_CSV, campos_mstockalm)
#     process_dbf_to_csv(MPRODUCTO_DBF, MPRODUCTO_CSV)
#     process_dbf_to_csv(MUSUARIO_DBF, MUSUARIO_CSV)
#     pass

# if __name__ == "__main__":
#     main()