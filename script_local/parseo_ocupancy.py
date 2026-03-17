import pandas as pd

# ── FUNCIONES REUTILIZABLES ──────────────────────────────────────────────────

TRIMESTRE_A_MES = {'T1': '01', 'T2': '04', 'T3': '07', 'T4': '10'}
MES_A_TRIMESTRE = {1:1, 2:1, 3:1, 4:4, 5:4, 6:4, 7:7, 8:7, 9:7, 10:10, 11:10, 12:10}

# Nombres limpios para los tipos de alojamiento
TIPOS_ALOJAMIENTO = {
    'Encuesta de Ocupación Hotelera':                       'Hotelera',
    'Encuesta de Ocupación en Campings':                    'Campings',
    'Encuesta de Ocupación en Apartamentos Turísticos':     'Apartamentos',
    'Encuesta de Ocupación en Alojamientos de Turismo Rural': 'Rural'
}

def parsear_periodo(serie: pd.Series) -> pd.Series:
    partes = serie.str.extract(r'(?P<anyo>\d{4})(?P<codigo>[MT]\w+)')
    meses = partes['codigo'].map(
        lambda x: TRIMESTRE_A_MES[x] if x.startswith('T') else x[1:].zfill(2)
    )
    return pd.to_datetime(partes['anyo'] + '-' + meses + '-01', format='%Y-%m-%d')


def agrupar_trimestral(df: pd.DataFrame, col_valor: str, cols_grupo: list) -> pd.DataFrame:
    df = df.copy()
    df[col_valor] = df[col_valor].astype('float64')
    df['periodo'] = df['periodo'].apply(
        lambda d: d.replace(month=MES_A_TRIMESTRE[d.month], day=1)
    )
    return (
        df
        .groupby(cols_grupo + ['periodo'], as_index=False)
        .agg(**{col_valor: (col_valor, 'sum')})
    )


# ── PARSEO DEL CSV ───────────────────────────────────────────────────────────

df = pd.read_csv(
    r'D:\Yanina\Documents\Data\Proyectos\Turismo vs empleo España despues Covid\csv\Ocupacion-Hotelera-Spain-XMonth-2026-2020.csv',
    encoding='latin-1',
    sep='\t'
)

df = df.drop(columns=['Residencia: Nivel 2', 'Total Nacional'])


df = df.rename(columns={
    'Operación':                        'tipo_alojamiento',
    'Comunidades y Ciudades Autónomas': 'comunidad',
    'Residencia: Nivel 1':              'residencia',
    'Viajeros y pernoctaciones':        'tipo_metrica',
    'Periodo':                          'periodo',
    'Total':                            'total'
})

# Simplificar nombres de tipo_alojamiento
df['tipo_alojamiento'] = df['tipo_alojamiento'].map(TIPOS_ALOJAMIENTO)

df['total'] = (
    df['total']
    .str.replace('.', '', regex=False)
    .replace({'\\.': None}, regex=True)     # '.' solo = valor nulo en el CSV
    .pipe(pd.to_numeric, errors='coerce')
    .astype('float64')
)

df['periodo'] = parsear_periodo(df['periodo'])

df['comunidad'] = df['comunidad'].fillna('Total Nacional')
df['comunidad'] = df['comunidad'].str.replace(r'^\d+\s+', '', regex=True)

# ── AGRUPACIÓN TRIMESTRAL ────────────────────────────────────────────────────

df_trim = agrupar_trimestral(
    df,
    col_valor  = 'total',
    cols_grupo = ['tipo_alojamiento', 'comunidad', 'residencia', 'tipo_metrica']
)


print(df_trim.head(10))
# ── VERIFICACIÓN ─────────────────────────────────────────────────────────────
mask = (
    (df_trim['comunidad'] == 'Total Nacional') &
    (df_trim['periodo'].dt.year == 2025) &
    (df_trim['periodo'] == '2025-10-01')
)
print(df_trim[mask][['tipo_alojamiento', 'comunidad', 'periodo', 'total']])

# Guarda el archivo 
df.to_csv(r'D:\Yanina\Documents\Data\Proyectos\Turismo vs empleo España despues Covid\csv\Ocupancy_parseado.csv', 
          index=False, 
          sep=',', 
          encoding='utf-8-sig')
