import functions_framework
import requests
from google.cloud import storage
import pandas as pd
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def procesar_datos_mensuales(request):
    try:
        storage_client = storage.Client()
        bucket_name = 'turismo-espana-raw'
        bucket = storage_client.bucket(bucket_name)

        # ✅ URL corregida — endpoint directo de descarga del INE
        url = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/49366.csv?nocab=1"
        
        logger.info(f"Descargando datos desde: {url}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()  # Lanza excepción si HTTP != 200
        
        # ✅ Detectar encoding real de la respuesta
        response.encoding = response.apparent_encoding
        csv_crudo = response.text
        
        logger.info(f"CSV descargado: {len(csv_crudo)} caracteres")
        logger.info(f"Primeras líneas:\n{csv_crudo[:500]}")  # Para debug

        # Guardar raw
        blob_raw = bucket.blob('raw/datos_mes_actual.csv')
        blob_raw.upload_from_string(csv_crudo, content_type='text/csv; charset=utf-8')

        # ── CONSTANTES ────────────────────────────────────────────────────────────
        TRIMESTRE_A_MES = {'T1': '01', 'T2': '04', 'T3': '07', 'T4': '10'}
        MES_A_TRIMESTRE = {1:1, 2:1, 3:1, 4:4, 5:4, 6:4, 7:7, 8:7, 9:7, 10:10, 11:10, 12:10}

        TIPOS_ALOJAMIENTO = {
            'Encuesta de Ocupación Hotelera':                         'Hotelera',
            'Encuesta de Ocupación en Campings':                      'Campings',
            'Encuesta de Ocupación en Apartamentos Turísticos':       'Apartamentos',
            'Encuesta de Ocupación en Alojamientos de Turismo Rural': 'Rural'
        }

        # ── FUNCIONES ─────────────────────────────────────────────────────────────
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

        # ── PARSEO ────────────────────────────────────────────────────────────────
        # ✅ Probar separadores comunes del INE (;  o \t)
        try:
            df = pd.read_csv(StringIO(csv_crudo), encoding='utf-8', sep=';')
            if df.shape[1] < 3:  # Si solo hay 1-2 columnas, sep incorrecto
                raise ValueError("Separador incorrecto, probando tabulador")
        except Exception:
            df = pd.read_csv(StringIO(csv_crudo), encoding='latin-1', sep='\t')

        logger.info(f"Columnas detectadas: {df.columns.tolist()}")
        logger.info(f"Shape inicial: {df.shape}")

        # ✅ Eliminar solo columnas que existan
        cols_a_eliminar = [c for c in ['Residencia: Nivel 2', 'Total Nacional'] if c in df.columns]
        df = df.drop(columns=cols_a_eliminar)

        df = df.rename(columns={
            'Operación':                        'tipo_alojamiento',
            'Comunidades y Ciudades Autónomas': 'comunidad',
            'Residencia: Nivel 1':              'residencia',
            'Viajeros y pernoctaciones':        'tipo_metrica',
            'Periodo':                          'periodo',
            'Total':                            'total'
        })

        df['tipo_alojamiento'] = df['tipo_alojamiento'].map(TIPOS_ALOJAMIENTO)

        df['total'] = (
            df['total']
            .astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)  # ✅ Decimales europeos
            .replace({'': None, 'nan': None, '.': None})
            .pipe(pd.to_numeric, errors='coerce')
            .astype('float64')
        )

        df['periodo'] = parsear_periodo(df['periodo'])
        df['comunidad'] = df['comunidad'].fillna('Total Nacional')
        df['comunidad'] = df['comunidad'].str.replace(r'^\d+\s+', '', regex=True)

        # ── AGRUPACIÓN ────────────────────────────────────────────────────────────
        df_trim = agrupar_trimestral(
            df,
            col_valor  = 'total',
            cols_grupo = ['tipo_alojamiento', 'comunidad', 'residencia', 'tipo_metrica']
        )

        logger.info(f"Shape procesado: {df_trim.shape}")

        # Subir procesado
        csv_procesado = df_trim.to_csv(index=False)
        blob_processed = bucket.blob('processed/datos_mes_actual_procesados.csv')
        blob_processed.upload_from_string(csv_procesado, content_type='text/csv')

        return f'Proceso completado. Filas procesadas: {len(df_trim)}', 200

    except requests.exceptions.RequestException as e:
        logger.error(f"Error descargando datos: {e}")
        return f'Error al descargar datos del INE: {str(e)}', 500

    except Exception as e:
        logger.exception(f"Error inesperado: {e}")
        return f'Error interno: {str(e)}', 500