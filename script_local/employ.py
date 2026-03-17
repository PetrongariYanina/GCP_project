import pandas as pd

# 1. LECTURA CORRECTA
df = pd.read_csv(
    'D:\Yanina\Documents\Data\Proyectos\Turismo vs empleo España despues Covid\csv\EPA-Spain-Trim-2025-2020.csv',
    encoding='latin-1',   # caracteres españoles
    sep=';'               # separador punto y coma
)

# 2. RENOMBRAR COLUMNAS
df = df.rename(columns={
    'Provincias':        'provincia',
    'Sector económico':  'sector',
    'Periodo':           'periodo',
    'Total':             'total'
})

# 3. CONVERTIR COLUMNA NUMÉRICA
# Decimal europeo: coma → punto, luego a float
df['total'] = (
    df['total']
    .str.replace(',', '.', regex=False)
    .pipe(pd.to_numeric, errors='coerce')
)

# 4. CONVERTIR PERIODO A FECHA (trimestral)
trimestre_a_mes = {'T1': '01', 'T2': '04', 'T3': '07', 'T4': '10'}

def convertir_periodo(serie):
    partes = serie.str.extract(r'(?P<anyo>\d{4})(?P<codigo>[MT]\w+)')
    meses = partes['codigo'].map(
        lambda x: trimestre_a_mes[x] if x.startswith('T') else x[1:].zfill(2)
    )
    return pd.to_datetime(partes['anyo'] + '-' + meses + '-01', format='%Y-%m-%d')

df['periodo'] = convertir_periodo(df['periodo'])

# 5. LIMPIAR PREFIJOS NUMÉRICOS EN PROVINCIAS
# "02 Albacete" → "Albacete"  |  "Total Nacional" se queda igual
df['provincia'] = df['provincia'].str.replace(r'^\d+\s+', '', regex=True)

# 6. VERIFICACIÓN
print(df.shape)       # (2332, 4)
print(df.dtypes)
print(df.head(10))
print(df['provincia'].unique())
print(df['sector'].unique())

# Guarda el archivo 
df.to_csv(r'D:\Yanina\Documents\Data\Proyectos\Turismo vs empleo España despues Covid\csv\Empleo_servicio.csv', 
          index=False, 
          sep=',', 
          encoding='utf-8-sig')
