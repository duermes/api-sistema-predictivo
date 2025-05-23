import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import csv


DIR_PATH = Path(__file__).resolve().parent.parent


dfproducto = pd.read_csv(DIR_PATH / 'data' / 'mproducto.csv')
dfstock = pd.read_csv(DIR_PATH / 'data' / 'mstockalm.csv')
dfformdet = pd.read_csv(DIR_PATH / 'data' / 'tformdet.csv')

# print(dfproducto.info())

# estoy impiando PRODUCTO
producto_columns = ["MEDCOD", "MEDTIP", "MEDPET", "MEDFF", "MEDEST"]

# primero dropeo columnas porque tenia 2 MEDCOD y dropee la data incorrecta
dfproducto.drop(columns=[dfproducto.columns[1], dfproducto.columns[10], dfproducto.columns[13], dfproducto.columns[22
], dfproducto.columns[34], dfproducto.columns[36], dfproducto.columns[37], dfproducto.columns[42]], inplace=True)


dfproducto = dfproducto[producto_columns]

# print(dfproducto.info())

dfproducto.fillna(0, inplace=True)

# print(dfproducto.duplicated().sum())
dfproducto.drop_duplicates(inplace=True)
# print(dfproducto.duplicated().sum())

# print(f'dfproducto: {dfproducto.info()}')

# AHORA LIMPIARE TFORMDET

campos_tformdet = [
    'TIPSUM', 'ANNOMES', 'CODIGO_MED',
    'PRECIO', 'VENTA', 'SIS', 'INTERSAN',
    'STOCK_FIN'
]

# print(f'raw data: {dfformdet.info()}')
# print(f'duplicados: {dfformdet.duplicated().sum()}')
dfformdet.drop_duplicates(inplace=True)
# print(f'duplicados after drop: {dfformdet.duplicated().sum()}')
# print(f'data restante: {dfformdet.info()}')
dfformdet.dropna(subset=['ANNOMES', 'CODIGO_MED', 'PRECIO'], inplace=True) 
dfformdet = dfformdet[campos_tformdet]

mean2= dfformdet['VENTA'].mean()
mean3 = dfformdet['SIS'].mean()
mean4 = dfformdet['INTERSAN'].mean()
# print(mean2, mean3, mean4)

dfformdet.fillna({"VENTA": round(mean2)}, inplace=True)
dfformdet.fillna({"SIS": round(mean3)}, inplace=True)
dfformdet.fillna({"INTERSAN": round(mean4)}, inplace=True)

dfformdet['TOTAL_CONSUMO'] = dfformdet[['VENTA', 'SIS', 'INTERSAN']].sum(axis=1)


dfformdet['ds'] = pd.to_datetime(dfformdet['ANNOMES'], format='%Y%m').dt.strftime('%Y-%m')

dfformdet.sort_values(by=['CODIGO_MED', 'ds'], inplace=True)
dfformdet = dfformdet.groupby(['CODIGO_MED', 'ds'], as_index=False).agg({
    'PRECIO': 'last',
    'TOTAL_CONSUMO': 'sum',
    'STOCK_FIN': 'last',
})

df = pd.merge(dfformdet, dfproducto, how='left', left_on='CODIGO_MED', right_on='MEDCOD')

df.drop(columns=['MEDCOD'], inplace=True)

# print(df.tail(10))
# print(df.info())
# print('Luego de dropear NA vaues ')
df.dropna(inplace=True)
# print(df.tail(10))
# print(df.info())


dfmodel = df.rename(columns={'TOTAL_CONSUMO': 'y'})
dfmodel = pd.get_dummies(dfmodel, columns=['MEDTIP', 'MEDPET', 'MEDFF', 'MEDEST'])
print(dfmodel.info())

# write my model in csv
# df.to_csv(DIR_PATH / 'data' / 'df.csv', index=False)

# numerical_df = df.select_dtypes(include=[np.number])
# print(numerical_df.corr())



