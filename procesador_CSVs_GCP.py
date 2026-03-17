import functions_framework
import requests
from google.cloud import storage
import pandas as pd
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── CONSTANTES ────────────────────────────────────────────────────────────────

BUCKET_NAME = 'turismo-espana-raw'

TRIMESTRE_A_MES  = {'T1': '01', 'T2': '04', 'T3': '07', 'T4': '10'}
MES_A_TRIMESTRE  = {1:1,2:1,3:1,4:4,5:4,6:4,7:7,8:7,9:7,10:10,11:10,12:10}

TIPOS_ALOJAMIENTO = {
            'Encuesta de Ocupación Hotelera':                         'Hotelera',
            'Encuesta de Ocupación en Campings':                      'Campings',
            'Encuesta de Ocupación en Apartamentos Turísticos':       'Apartamentos',
            'Encuesta de Ocupación en Alojamientos de Turismo Rural': 'Rural'
        }


# ── UTILIDADES COMPARTIDAS ────────────────────────────────────────────────────

def descargar_csv(url: str) -> str:
    '''Descarga un CSV desde una URL con manejo de errores y detección de codificación'''
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    response.encoding = response.apparent_encoding
    return response.text

def subir_a_gcs(bucket, ruta: str, contenido: str):
    '''Sube un string a Google Cloud Storage como un archivo CSV'''
    bucket.blob(ruta).upload_from_string(contenido, content_type='text/csv')

def leer_csv_con_deteccion(csv_crudo: str, nombre_dataset: str = "") -> pd.DataFrame:
    """Lee CSV probando separadores comunes (; o \\t)"""
    try:
        df = pd.read_csv(StringIO(csv_crudo), encoding='utf-8', sep=';')
        if df.shape[1] < 3:
            raise ValueError("Separador incorrecto, probando tabulador")
        logger.info(f"CSV {nombre_dataset} leído con separador ';' (UTF-8)")
        return df
    except Exception as e:
        logger.warning(f"Separador ';' no funcionó: {e}, intentando '\\t'")
        df = pd.read_csv(StringIO(csv_crudo), encoding='latin-1', sep='\t')
        logger.info(f"CSV {nombre_dataset} leído con separador '\\t' (latin-1)")
        return df

def limpiar_numeros(serie: pd.Series) -> pd.Series:
    """Convierte números con formato europeo (1.234,56) a decimal (1234.56)"""
    return (
        serie
        .astype(str)
        .str.replace('.', '', regex=False)      # Quita miles (.)
        .str.replace(',', '.', regex=False)     # Cambia decimales (, -> .)
        .replace({'': None, 'nan': None, '': None})
        .pipe(pd.to_numeric, errors='coerce')
    )

def parsear_periodo(serie: pd.Series) -> pd.Series:
    '''Convierte periodos en formato "2023T1" o "2023M01" a datetime (primer día del mes/trimestre)'''
    partes = serie.str.extract(r'(?P<anyo>\d{4})(?P<codigo>[MT]\w+)')
    meses  = partes['codigo'].map(
        lambda x: TRIMESTRE_A_MES[x] if x.startswith('T') else x[1:].zfill(2)
    )
    return pd.to_datetime(partes['anyo'] + '-' + meses + '-01', format='%Y-%m-%d')

def agrupar_trimestral(df: pd.DataFrame, col_valor: str, cols_grupo: list) -> pd.DataFrame:
            '''Agrupa un DataFrame por trimestre sumando el valor de col_valor, 
            manteniendo las columnas de cols_grupo'''
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

# ── LÓGICA DE CADA CSV ────────────────────────────────────────────────────────

def procesar_ocupacion(bucket):
    '''Procesa el CSV de ocupación hotelera, limpiando y transformando los datos'''
    url = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/49366.csv"
    csv_crudo = descargar_csv(url)
    subir_a_gcs(bucket, 'raw/ocupacion_raw.csv', csv_crudo)

    # Lectura robusta 
    df = leer_csv_con_deteccion(csv_crudo, "ocupacion")
    logger.info(f"Columnas: {df.columns.tolist()}")
    
    # Limpiar columnas (eliminar las que no necesitamos)
    cols_drop = [c for c in ['Residencia: Nivel 2', 'Total Nacional'] if c in df.columns]
    df = df.drop(columns=cols_drop)
    
    # Renombrar columnas
    df = df.rename(columns={
        'Operación':                        'tipo_alojamiento',
        'Comunidades y Ciudades Autónomas': 'comunidad',
        'Residencia: Nivel 1':              'residencia',
        'Viajeros y pernoctaciones':        'tipo_metrica',
        'Periodo':                          'periodo',
        'Total':                            'total'
    })

    if 'tipo_alojamiento' not in df.columns:
        raise ValueError(f"Columna 'tipo_alojamiento' no encontrada. Columnas: {df.columns.tolist()}")
    
    # Log y mapeo de tipos alojamiento
    valores_unicos = df['tipo_alojamiento'].unique().tolist()
    logger.info(f"Valores únicos: {valores_unicos}")
    df['tipo_alojamiento'] = df['tipo_alojamiento'].map(TIPOS_ALOJAMIENTO)
    
    no_mapeados = df['tipo_alojamiento'].isna().sum()
    if no_mapeados > 0:
        logger.warning(f"⚠️ {no_mapeados} filas sin mapeo")
        df['tipo_alojamiento'] = df['tipo_alojamiento'].fillna('Desconocido')
    
    # Limpiar valores y parsear fechas
    df['total'] = limpiar_numeros(df['total']).astype('float64')
    df['periodo'] = parsear_periodo(df['periodo'])
    df['comunidad'] = df['comunidad'].fillna('Total Nacional')
    df['comunidad'] = df['comunidad'].str.replace(r'^\d+\s+', '', regex=True)

    # Agrupar por trimestre
    df_trim = agrupar_trimestral(
        df, 
        col_valor='total', 
        cols_grupo=['tipo_alojamiento', 'comunidad', 'residencia', 'tipo_metrica']
    )
    
    subir_a_gcs(bucket, 'processed/ocupacion_procesado.csv', df_trim.to_csv(index=False))
    logger.info(f"Ocupación procesada: {len(df_trim)} filas")
    return len(df_trim)

def procesar_paro(bucket):
    '''Procesa el CSV de paro registrado, limpiando y transformando los datos'''
    url = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/75804.csv"
    csv_crudo = descargar_csv(url)
    subir_a_gcs(bucket, 'raw/paro_raw.csv', csv_crudo)

    # Lectura robusta con detección de codificación y separador
    df = leer_csv_con_deteccion(csv_crudo, "paro")
    logger.info(f"Columnas: {df.columns.tolist()}")
    
    # Validar columnas requeridas
    columnas_requeridas = ['Provincias', 'Sector económico', 'Periodo', 'Total']
    columnas_faltantes = [c for c in columnas_requeridas if c not in df.columns]
    if columnas_faltantes:
        raise ValueError(f"Columnas faltantes: {columnas_faltantes}")
    
    # Renombrar columnas
    df = df.rename(columns={
        'Provincias':       'provincia',
        'Sector económico': 'sector',
        'Periodo':          'periodo',
        'Total':            'total'
    })
    
    # Limpiar valores
    df['total'] = limpiar_numeros(df['total'])
    df['periodo'] = parsear_periodo(df['periodo'])
    df['provincia'] = df['provincia'].str.replace(r'^\d+\s+', '', regex=True)

    subir_a_gcs(bucket, 'processed/paro_procesado.csv', df.to_csv(index=False))
    logger.info(f"Paro procesado: {len(df)} filas")
    return len(df)

# ── ROUTER PRINCIPAL ──────────────────────────────────────────────────────────

@functions_framework.http
def procesar_datos_mensuales(request):
    """Procesa datos de ocupación y/o paro desde Google Cloud Function"""
    bucket = storage.Client().bucket(BUCKET_NAME)
    data = request.get_json(silent=True) or {}
    proceso = data.get('proceso', 'todos')

    try:
        logger.info(f"Iniciando: {proceso}")
        
        # Ejecutar procesos 
        resultados = {}
        if proceso in ['ocupacion', 'todos']:
            resultados['ocupacion'] = procesar_ocupacion(bucket)
        if proceso in ['paro', 'todos']:
            resultados['paro'] = procesar_paro(bucket)
        
        if not resultados:
            return f'Proceso "{proceso}" inválido (usa: ocupacion, paro, todos)', 400
        
        # Respuesta exitosa
        msg = ' | '.join([f'{k}: {v} filas' for k, v in resultados.items()])
        logger.info(f"Completado: {msg}")
        return msg, 200

    except requests.exceptions.RequestException as e:
        error_msg = f"Error de red: {str(e)}"
        logger.error(error_msg)
        return error_msg, 500
        
    except (KeyError, ValueError) as e:
        error_msg = f"Error de validación: {str(e)}"
        logger.error(error_msg)
        return error_msg, 500
        
    except Exception as e:
        error_msg = f"Error inesperado: {type(e).__name__}: {str(e)}"
        logger.exception(f"Excepción: {e}")
        return error_msg, 500