import requests
import pandas as pd
import time
import os
import unicodedata
import re
import logging
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import warnings

# --- IMPORTS DE DB Y SQLALCHEMY ---
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import Integer, BigInteger, Text, Boolean, DateTime, Float, Numeric

load_dotenv()

# --- CONFIGURACIÓN ---
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

OBJECT_TYPE = "leads"
DB_SCHEMA = "hubspot_etl"      
TABLE_NAME = "leads"   

OUTPUT_FOLDER = "exports"
LOG_FILE = "etl_errors.log"

# Nombre dinámico del reporte
report_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
REPORT_FILE = os.path.join(OUTPUT_FOLDER, f"etl_health_report_{report_timestamp}.txt")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

def ensure_exports_folder():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

# --- CLASE MONITOR (Sin cambios mayores) ---
class ETLMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.metrics = {
            'api_calls': 0, 'retries_429': 0, 'retries_5xx': 0,
            'connection_errors': 0, 'records_fetched': 0,
            'records_processed_ok': 0, 'records_failed': 0,
            'columns_truncated': 0, 'associations_found': 0,
            'associations_missing': 0, 'db_upserts': 0,
            'schema_changes': 0,
            'db_execution_time': 0.0,
            'db_insert_errors': 0
        }
        self.null_stats = {}

    def increment(self, metric, count=1):
        if metric in self.metrics:
            self.metrics[metric] += count
    
    def set_metric(self, metric, value):
        if metric in self.metrics:
            self.metrics[metric] = value

    def record_null_stats(self, df):
        # Acumulativo simple para el reporte final basado en el último lote
        # (Idealmente se debería sumar, pero para monitoreo básico esto sirve por lote)
        total_rows = len(df)
        if total_rows == 0: return
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                pct = (count / total_rows) * 100
                # Guardamos el peor caso encontrado
                if col not in self.null_stats or pct > self.null_stats[col][1]:
                    self.null_stats[col] = (count, pct)

    def generate_report(self):
        duration = time.time() - self.start_time
        duration_str = str(timedelta(seconds=int(duration)))
        m = self.metrics
        
        nulls_report = ""
        if self.null_stats:
            sorted_nulls = sorted(self.null_stats.items(), key=lambda x: x[1][1], reverse=True)[:10]
            nulls_report = "\n   [Top Columnas con Valores Vacíos (Peor Lote Detectado)]\n"
            for col, (count, pct) in sorted_nulls:
                alert = "⚠️" if pct > 10 else " "
                nulls_report += f"   - {col[:30]:<30} : ({pct:>5.1f}%) {alert}\n"
        else:
            nulls_report = "   - No se detectaron valores nulos significativos.\n"

        report = f"""
==================================================
          DATA HEALTH REPORT - POSTGRES LOAD
==================================================
Fecha de Ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duración Total    : {duration_str}
Object Type       : {OBJECT_TYPE} (Method: GET List)
Estado General    : {'🟢 SALUDABLE' if m['records_failed'] == 0 and m['db_insert_errors'] == 0 else '⚠️ CON ERRORES'}

1. CONEXIÓN API & VOLUMEN
-------------------------
   - Llamadas API          : {m['api_calls']}
   - Registros Fetched     : {m['records_fetched']}
   - Procesados OK         : {m['records_processed_ok']}
   - Fallidos (ETL)        : {m['records_failed']}

2. DESEMPEÑO BASE DE DATOS
---------------------------------------
   - Registros Upserted    : {m['db_upserts']}
   - Tiempo DB (aprox)     : {m['db_execution_time']:.2f} s
   - Errores DB            : {m['db_insert_errors']}

3. INTEGRIDAD & CALIDAD
-----------------------------------------------
   - Schema Changes        : {m['schema_changes']}
   - Cols Truncadas        : {m['columns_truncated']}
   {nulls_report}
==================================================
"""
        print(report)
        try:
            with open(REPORT_FILE, "w", encoding="utf-8") as f:
                f.write(report)
            print(f"📄 Reporte guardado en: {REPORT_FILE}")
        except Exception as e:
            logging.error(f"Error guardando reporte: {e}")

monitor = ETLMonitor()

# --- FUNCIONES DE DB ---
def get_db_engine():
    db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

def upsert_on_conflict(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data: return
    
    stmt = insert(table.table).values(data)
    update_cols = {c.name: c for c in stmt.excluded if c.name != 'hs_object_id'}
    
    if not update_cols: 
        # Si no hay columnas para actualizar, hacemos un "do nothing"
        on_conflict_stmt = stmt.on_conflict_do_nothing(index_elements=['hs_object_id'])
    else:
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['hs_object_id'],
            set_=update_cols
        )
    conn.execute(on_conflict_stmt)

def sync_db_schema(engine, df, table_name, schema, prop_types=None, column_mapping=None):
    """
    Sincroniza el esquema de la BD con las columnas del DataFrame.
    
    Args:
        column_mapping: dict que mapea nombre_final_columna -> nombre_original_hubspot
    """
    inspector = inspect(engine)
    if not inspector.has_table(table_name, schema=schema): return

    existing_cols = [c['name'] for c in inspector.get_columns(table_name, schema=schema)]
    new_cols = set(df.columns) - set(existing_cols)
    
    if new_cols:
        print(f"⚠️ Schema Evolution: Creando {len(new_cols)} columnas nuevas...")
        with engine.begin() as conn:
            for col in new_cols:
                dtype = df[col].dtype
                
                # Intentar obtener el nombre original de HubSpot usando el mapeo
                original_col_name = column_mapping.get(col) if column_mapping else None
                
                # Intentar usar el tipo de HubSpot si está disponible
                if prop_types and original_col_name and original_col_name in prop_types:
                    hubspot_type = prop_types[original_col_name]
                    pg_type = get_postgres_type_from_hubspot(hubspot_type, dtype)
                    logging.info(f"Columna '{col}' (original: '{original_col_name}'): tipo HubSpot '{hubspot_type}' → PostgreSQL '{pg_type}'")
                else:
                    if not column_mapping:
                        logging.warning(f"No column mapping provided for '{col}'")
                    elif not original_col_name:
                        logging.warning(f"No original name found in mapping for '{col}'")
                    else:
                        logging.warning(f"No hubspot type found for original column '{original_col_name}'")
                    
                    logging.warning(f"Using pandas fallback for column: {col}")
                    # Fallback: inferir desde pandas (comportamiento original)
                    pg_type = "TEXT"
                    if pd.api.types.is_integer_dtype(dtype): pg_type = "BIGINT"
                    elif pd.api.types.is_float_dtype(dtype): pg_type = "NUMERIC"
                    elif pd.api.types.is_bool_dtype(dtype): pg_type = "BOOLEAN"
                    elif pd.api.types.is_datetime64_any_dtype(dtype): pg_type = "TIMESTAMP"
                
                # Evitamos error si la columna ya existe por concurrencia
                try:
                    conn.execute(text(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{col}" {pg_type}'))
                    monitor.increment('schema_changes')
                    logging.info(f"Columna creada: {col} ({pg_type})")
                except Exception as e:
                    logging.warning(f"Error al crear columna {col} (puede que ya exista): {e}")

def initialize_db_schema(engine):
    """
    Crea el Schema (si no existe) y la Tabla inicial con campos core.
    """
    create_schema_sql = text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    
    # Tabla base mínima necesaria
    ddl_query = text(f"""
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{TABLE_NAME} (
        "hs_object_id" BIGINT PRIMARY KEY,
        "hs_lastmodifieddate" TIMESTAMP,
        "fivetran_synced" TIMESTAMP,
        "fivetran_deleted" BOOLEAN
    );
    """)

    try:
        with engine.begin() as conn:
            conn.execute(create_schema_sql) 
            conn.execute(ddl_query)         
    except Exception as e:
        print(f"❌ Error inicializando BD: {e}")
        raise e


# --- FUNCIONES AUXILIARES ETL ---
def safe_request(method, url, **kwargs):
    max_retries = 3; backoff = 5
    monitor.increment('api_calls')
    for attempt in range(1, max_retries + 1):
        try:
            # Importante: para GET usamos params, para POST usamos json
            res = requests.request(method, url, headers=headers, **kwargs)
            
            if res.status_code == 200: 
                return res
            
            # Manejo específico de errores con detalles
            elif res.status_code == 400:
                error_detail = ""
                try:
                    error_json = res.json()
                    error_detail = json.dumps(error_json, indent=2)
                except:
                    error_detail = res.text
                
                # Log detallado del error
                logging.error(f"❌ ERROR 400 Bad Request en {url}")
                logging.error(f"   Método: {method}")
                logging.error(f"   Parámetros enviados: {kwargs}")
                logging.error(f"   Respuesta de HubSpot:\n{error_detail}")
                
                raise Exception(
                    f"Bad Request (400) en {url}\n"
                    f"Detalles: {error_detail[:500]}..."
                )
            
            elif res.status_code == 414:
                raise Exception("URL Too Long (414). Demasiadas propiedades solicitadas en GET.")
            
            elif res.status_code == 429:
                monitor.increment('retries_429')
                time.sleep(10)
            
            elif 500 <= res.status_code < 600:
                monitor.increment('retries_5xx')
                time.sleep(backoff * attempt)
            
            else: 
                res.raise_for_status()
                
        except requests.exceptions.ConnectionError:
            monitor.increment('connection_errors')
            time.sleep(backoff * attempt)
        except Exception as e: 
            raise e
            
    raise Exception(f"Fallo crítico en {url}")

def normalize_name(text):
    if not isinstance(text, str): return str(text) if text is not None else ""
    text = text.lower()
    text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
    text = text.replace(" ", "_").replace("-", "_")
    text = re.sub(r'[^a-z0-9_]', '', text)
    return re.sub(r'_{2,}', '_', text).strip('_')

def sanitize_columns_for_postgres(df):
    new_cols = []; seen = {}
    for col in df.columns:
        sanitized = col[:63] # Cortar a 63 chars
        if len(col) > 63:
            monitor.increment('columns_truncated')
        
        if sanitized in seen:
            seen[sanitized] += 1
            suffix = f"_{seen[sanitized]}"
            sanitized = f"{sanitized[:63-len(suffix)]}{suffix}"
        else: seen[sanitized] = 1
        new_cols.append(sanitized)
    df.columns = new_cols
    return df

def get_smart_mapping(all_props):
    # Intentamos obtener pipelines para mapeo inteligente de stages
    try:
        url = f"https://api.hubapi.com/crm/v3/pipelines/{OBJECT_TYPE}"
        res = safe_request('GET', url)
        pipelines = res.json().get('results', [])
    except Exception: return {}

    mapping = {}
    prefixes = ["hs_v2_latest_time_in", "hs_v2_date_entered", "hs_v2_date_exited", "hs_v2_cumulative_time_in", "hs_time_in", "hs_date_exited", "hs_date_entered"]
    
    for pipe in pipelines:
        p_lbl = pipe['label']
        for stage in pipe.get('stages', []):
            s_id = stage['id']  # ← Mantener el ID original con guiones
            s_id_normalized = s_id.replace("-", "_")  # ← Version con underscores
            s_lbl = stage['label']
            
            for prop in all_props:
                for pre in prefixes:
                    # Buscar tanto con guiones como con underscores
                    if prop.startswith(pre) and (s_id in prop or s_id_normalized in prop):
                        # Mapeamos IDs técnicos a Labels legibles
                        mapping[prop] = normalize_name(f"{pre}_{s_lbl}_{p_lbl}")
    return mapping

def get_assocs():
    # Obtiene asociaciones disponibles
    url = f"https://api.hubapi.com/crm/v3/schemas/{OBJECT_TYPE}"
    res = safe_request('GET', url)
    all_assocs = [a['toObjectTypeId'] for a in res.json().get('associations', [])]
    
    # Retornar todas las asociaciones (se procesarán en chunks después)
    if len(all_assocs) > 30:
        logging.info(f"ℹ️ Objeto tiene {len(all_assocs)} asociaciones. Se procesarán en chunks de 30.")
        print(f"ℹ️ Se encontraron {len(all_assocs)} asociaciones. Se procesarán en múltiples llamadas (límite API: 30)")
    
    return all_assocs

# --- NUEVAS FUNCIONES PARA GET / LIST ---

def get_properties_with_types():
    """
    Obtiene todas las propiedades con sus tipos de dato desde HubSpot.
    Retorna: (lista de nombres, diccionario de tipos)
    """
    url = f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}"
    res = safe_request('GET', url)
    properties_data = res.json()['results']
    
    # Extraer nombres y crear mapeo de tipos
    prop_names = []
    prop_types = {}
    
    for prop in properties_data:
        name = prop['name']
        hubspot_type = prop.get('type', 'string')  # Default a string si no viene
        
        prop_names.append(name)
        prop_types[name] = hubspot_type
    
    logging.info(f"Tipos de datos capturados para: {prop_types} propiedades")
    return prop_names, prop_types

def convert_hubspot_types_to_pandas(df, prop_types):
    """
    Convierte columnas del DataFrame según los tipos declarados por HubSpot.
    Esto asegura que los datos tengan el tipo correcto antes de subirlos a PostgreSQL.
    """
    conversions_applied = 0
    conversions_failed = 0
    
    for col in df.columns:
        # Saltar columnas de metadata que agregamos nosotros
        if col in ['hs_object_id', 'fivetran_synced', 'fivetran_deleted']:
            continue
        
        # Saltar columnas de asociaciones
        if col.startswith('asoc_'):
            continue
        
        # Buscar el tipo en el diccionario (puede no estar si es columna generada)
        hubspot_type = prop_types.get(col)
        if not hubspot_type:
            logging.info(f"No hubspot type found for column: {col}")
            continue
        
        try:
            # Mapeo de tipos HubSpot -> Pandas
            if hubspot_type == 'number':
                # Intentar convertir a numérico
                df[col] = pd.to_numeric(df[col], errors='coerce')
                conversions_applied += 1
                
            elif hubspot_type in ['date', 'datetime']:
                # Convertir a datetime
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
                conversions_applied += 1
                
            elif hubspot_type == 'bool':
                # Convertir a booleano
                # HubSpot envía "true"/"false" como strings
                df[col] = df[col].map({'true': True, 'false': False, '': None, None: None})
                conversions_applied += 1
                
            # Los tipos 'string', 'enumeration', 'phone_number' se quedan como object (texto)
            # No necesitan conversión explícita
            
        except Exception as e:
            conversions_failed += 1
            logging.warning(f"No se pudo convertir columna '{col}' de tipo '{hubspot_type}': {e}")
    
    if conversions_applied > 0:
        logging.info(f"Conversiones de tipo aplicadas: {conversions_applied} columnas (Fallos: {conversions_failed})")
    
    return df

def get_postgres_type_from_hubspot(hubspot_type, pandas_dtype=None):
    """
    Mapea un tipo de HubSpot a un tipo de PostgreSQL.
    Puede usar el dtype de pandas como fallback para mayor precisión.
    """
    # Mapeo directo de tipos conocidos de HubSpot
    type_mapping = {
        'string': 'TEXT',
        'number': 'NUMERIC',
        'date': 'TIMESTAMP',
        'datetime': 'TIMESTAMP',
        'bool': 'BOOLEAN',
        'enumeration': 'TEXT',
        'phone_number': 'TEXT',
    }
    
    pg_type = type_mapping.get(hubspot_type)
    
    # Si no hay mapeo directo, usar inferencia de pandas
    if not pg_type and pandas_dtype:
        logging.warning(f"No postgres type found for column: {pandas_dtype}, using fallback")
        if pd.api.types.is_integer_dtype(pandas_dtype): 
            pg_type = "BIGINT"
        elif pd.api.types.is_float_dtype(pandas_dtype): 
            pg_type = "NUMERIC"
        elif pd.api.types.is_bool_dtype(pandas_dtype): 
            pg_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype): 
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"
    
    return pg_type or "TEXT"

def fetch_all_records_with_chunked_assocs(properties, associations):
    """
    Descarga todos los registros manejando el límite de 30 asociaciones por request.
    Si hay más de 30 asociaciones, hace múltiples llamadas y combina los resultados.
    """
    chunk_size = 30
    
    # Si hay 30 o menos asociaciones, usar el flujo normal
    if len(associations) <= chunk_size:
        for batch in fetch_hubspot_records_generator(properties, associations):
            yield batch
        return
    
    # Dividir asociaciones en chunks
    assoc_chunks = [associations[i:i + chunk_size] for i in range(0, len(associations), chunk_size)]
    
    print(f"\n🔄 Dividiendo {len(associations)} asociaciones en {len(assoc_chunks)} llamadas API")
    logging.info(f"Procesando asociaciones en {len(assoc_chunks)} chunks de máximo {chunk_size}")
    
    # Diccionario para almacenar y combinar registros por ID
    all_records_dict = {}
    
    for chunk_idx, assoc_chunk in enumerate(assoc_chunks):
        print(f"\n📡 Procesando chunk {chunk_idx + 1}/{len(assoc_chunks)} de asociaciones...")
        print(f"   Asociaciones en este chunk: {len(assoc_chunk)}")
        
        # Descargar registros con este chunk de asociaciones
        for batch in fetch_hubspot_records_generator(properties, assoc_chunk):
            for record in batch:
                record_id = record['id']
                
                # Si es la primera vez que vemos este registro
                if record_id not in all_records_dict:
                    all_records_dict[record_id] = record
                else:
                    # Combinar asociaciones de múltiples llamadas
                    existing_record = all_records_dict[record_id]
                    existing_assocs = existing_record.get('associations', {})
                    new_assocs = record.get('associations', {})
                    
                    # Merge de asociaciones
                    for assoc_type, assoc_data in new_assocs.items():
                        if assoc_type not in existing_assocs:
                            existing_assocs[assoc_type] = assoc_data
                        else:
                            # Combinar results evitando duplicados
                            existing_results = existing_assocs[assoc_type].get('results', [])
                            new_results = assoc_data.get('results', [])
                            
                            # Crear set de IDs existentes para evitar duplicados
                            existing_ids = {r['id'] for r in existing_results if 'id' in r}
                            
                            # Agregar solo los nuevos que no existen
                            for new_result in new_results:
                                if 'id' in new_result and new_result['id'] not in existing_ids:
                                    existing_results.append(new_result)
                            
                            existing_assocs[assoc_type]['results'] = existing_results
                    
                    existing_record['associations'] = existing_assocs
        
        print(f"   ✓ Chunk {chunk_idx + 1} completado. Total registros únicos: {len(all_records_dict)}")
    
    # Convertir diccionario a lista y entregar en batches de 100
    print(f"\n✅ Todas las asociaciones procesadas. Entregando {len(all_records_dict)} registros únicos...")
    
    all_records_list = list(all_records_dict.values())
    batch_size = 100
    
    for i in range(0, len(all_records_list), batch_size):
        batch = all_records_list[i:i + batch_size]
        yield batch

def fetch_hubspot_records_generator(properties, associations):
    """
    Generador que usa el endpoint LIST (GET) y devuelve lotes de resultados.
    Maneja la paginación automáticamente.
    """
    url = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}"
    
    # Preparamos los parámetros base
    properties_str = ",".join(properties)
    associations_str = ",".join(associations) if associations else ""
    
    # 🔍 DEBUG: Validación detallada de parámetros
    print(f"\n{'='*60}")
    print(f"🔍 DEBUG - Parámetros de Request")
    print(f"{'='*60}")
    print(f"Object Type      : {OBJECT_TYPE}")
    print(f"URL Base         : {url}")
    print(f"Total Properties : {len(properties)}")
    print(f"Properties Length: {len(properties_str)} chars")
    print(f"Total Assocs     : {len(associations)}")
    print(f"Assocs Length    : {len(associations_str)} chars")
    
    # Mostrar primeras propiedades como muestra
    if properties:
        print(f"\nPrimeras 5 propiedades:")
        for prop in properties[:5]:
            print(f"  - {prop}")
        if len(properties) > 5:
            print(f"  ... y {len(properties) - 5} más")
    
    # Mostrar asociaciones
    if associations:
        print(f"\nAsociaciones solicitadas:")
        for assoc in associations:
            print(f"  - {assoc}")
    else:
        print(f"\n⚠️ No se solicitaron asociaciones")
    
    # Validación de longitud total
    estimated_url_length = len(url) + len(properties_str) + len(associations_str) + 100
    print(f"\nLongitud URL estimada: {estimated_url_length} chars")
    
    if estimated_url_length > 8000:
        warning_msg = (
            f"⚠️ URL demasiado larga ({estimated_url_length} chars).\n"
            f"   Límite recomendado: 8000 chars\n"
            f"   Propiedades: {len(properties)} ({len(properties_str)} chars)\n"
            f"   Considera reducir propiedades o usar POST/Search endpoint."
        )
        logging.warning(warning_msg)
        print(warning_msg)
    elif estimated_url_length > 6000:
        warning_msg = f"⚠️ Advertencia: URL larga ({estimated_url_length} chars). Podría fallar."
        logging.warning(warning_msg)
        print(warning_msg)
    else:
        print(f"✓ Longitud URL aceptable")
    
    print(f"{'='*60}\n")
    
    params = {
        "limit": 100,
        "properties": properties_str,
        "associations": associations_str,
        "archived": "false"
    }
    
    print(f"📡 Iniciando descarga con GET...")
    
    after = None
    while True:
        if after:
            params['after'] = after
        
        try:
            res = safe_request('GET', url, params=params)
            data = res.json()
            results = data.get('results', [])
            
            if not results:
                break
                
            yield results
            
            paging = data.get('paging')
            if paging and 'next' in paging:
                after = paging['next']['after']
            else:
                break
                
        except Exception as e:
            # 🔍 DEBUG: Capturar y mostrar error detallado
            logging.error(f"❌ Error en fetch_hubspot_records_generator")
            logging.error(f"   After token: {after}")
            logging.error(f"   Params keys: {list(params.keys())}")
            raise e

def process_batch(batch_records, col_map, prop_types=None):
    """
    Transforma una lista de diccionarios de HubSpot en un DataFrame limpio.
    Retorna: (DataFrame, dict de mapeo nombre_final -> nombre_original)
    """
    data_list = []
    synced_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    
    for record in batch_records:
        try:
            row = {"hs_object_id": int(record["id"])}
            
            # Aplanar propiedades
            row.update(record.get("properties", {}))
            
            # Metadata
            archived = record.get("archived", False)
            row["fivetran_synced"] = synced_at
            row["fivetran_deleted"] = archived
            
            # Procesar Asociaciones (Flatten to String)
            raw_assoc = record.get("associations")
            if raw_assoc and isinstance(raw_assoc, dict):
                for atype, adata in raw_assoc.items():
                    if isinstance(adata, dict):
                        # Nota: la estructura en GET puede variar ligeramente vs Search
                        ids = [str(a["id"]) for a in adata.get("results", []) if "id" in a]
                        if ids:
                            col_name = normalize_name(f"asoc_{atype}_ids")
                            row[col_name] = ",".join(ids)
                            monitor.increment('associations_found')
            
            data_list.append(row)
            monitor.increment('records_processed_ok')
            
        except Exception as e:
            monitor.increment('records_failed')
            logging.error(f"Error procesando registro {record.get('id')}: {e}")

    if not data_list:
        logging.warning(f"Lote saltado: 0 registros procesados de {len(batch_records)} recibidos.")
        return pd.DataFrame(), {}

    # Creación y limpieza de DataFrame
    df = pd.DataFrame(data_list)
    
    # Rastrear mapeo de nombres: final → original
    # Esto permite que sync_db_schema encuentre los tipos correctos
    column_name_mapping = {}
    original_columns = df.columns.tolist()

    # 1. ⭐ Convertir tipos según HubSpot (con nombres originales)
    if prop_types:
        df = convert_hubspot_types_to_pandas(df, prop_types)

    # 2. Renombrar columnas inteligentes (stage names)
    df.rename(columns=col_map, inplace=True)
    
    # Rastrear el mapeo después del primer cambio
    for i, orig_col in enumerate(original_columns):
        new_col = df.columns[i]
        column_name_mapping[new_col] = orig_col

    # 3. Normalizar nombres (snake_case, sin acentos)
    normalized_cols = [normalize_name(c) for c in df.columns]
    
    # Actualizar mapeo: el nombre normalizado apunta al original
    updated_mapping = {}
    for i, norm_col in enumerate(normalized_cols):
        old_col = df.columns[i]
        original = column_name_mapping.get(old_col, old_col)
        updated_mapping[norm_col] = original
    
    df.columns = normalized_cols
    column_name_mapping = updated_mapping

    # 4. Sanitizar para Postgres (longitud y duplicados)
    df_before_sanitize = df.copy()
    df = sanitize_columns_for_postgres(df)
    
    # Actualizar mapeo después de sanitizar (truncado/duplicados)
    final_mapping = {}
    for i, final_col in enumerate(df.columns):
        original_from_before = column_name_mapping.get(df_before_sanitize.columns[i])
        if original_from_before:
            final_mapping[final_col] = original_from_before
    
    column_name_mapping = final_mapping

    # 5. Convertir dicts/lists a JSON string para evitar error en DB
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    return df, column_name_mapping
    

# --- PROCESO PRINCIPAL ---
def run_postgres_etl():
    logging.info("\n" + "="*80 + "\n" + f"🚀 INICIANDO NUEVA CORRIDA ETL - OBJETO: {OBJECT_TYPE}" + "\n" + "="*80)
    try:
        ensure_exports_folder()
        engine = get_db_engine()
        print("✅ Motor de BD iniciado.")

        # 1. Preparar BD (Tabla inicial mínima)
        initialize_db_schema(engine) 

        # 2. Obtener Metadatos (Propiedades y Asociaciones)
        print("Obteniendo definición de propiedades...")
        all_props, prop_types = get_properties_with_types()
        
        print(f"✓ Propiedades obtenidas: {len(all_props)}")
        print(f"✓ Tipos de datos mapeados: {len(prop_types)}")
        
        print("Obteniendo asociaciones disponibles...")
        assocs = get_assocs()
        print(f"✓ Asociaciones disponibles: {len(assocs)}")
        
        # Validar que el objeto soporte asociaciones
        if not assocs:
            print(f"⚠️ Advertencia: Objeto '{OBJECT_TYPE}' no tiene asociaciones disponibles")
        
        col_map = get_smart_mapping(all_props)
        
        # 3. Flujo por Lotes (Generator -> Transform -> Load)
        print("🚀 Iniciando proceso ETL por lotes...")
        
        batch_count = 0
        total_records_processed = 0
        
        # Usar el generador con manejo de chunks de asociaciones
        for batch in fetch_all_records_with_chunked_assocs(all_props, assocs):
            monitor.metrics['records_fetched'] += len(batch)
            
            # A. Transformar (con tipos de HubSpot) - ahora retorna también el mapeo
            df_batch, column_mapping = process_batch(batch, col_map, prop_types)
            
            if df_batch.empty:
                continue

            # B. Evolución de Esquema (Checkea columnas nuevas en este lote, usando tipos y mapeo)
            sync_db_schema(engine, df_batch, TABLE_NAME, DB_SCHEMA, prop_types, column_mapping)
            
            # C. Carga a BD
            db_start_time = time.time()
            try:
                with engine.begin() as conn:
                    df_batch.to_sql(
                        TABLE_NAME, 
                        con=conn, 
                        schema=DB_SCHEMA,  
                        if_exists='append', 
                        index=False, 
                        method=upsert_on_conflict
                    )
                monitor.metrics['db_upserts'] += len(df_batch)
                
            except Exception as e:
                monitor.metrics['db_insert_errors'] += len(df_batch)
                
                # Identificar tipo específico de error
                error_type = type(e).__name__
                error_msg = str(e)
                
                # 1. Errores de Tipo de Dato (DatetimeFieldOverflow, DataError, etc.)
                if 'DatetimeFieldOverflow' in error_type or 'date/time field value out of range' in error_msg:
                    # Extraer el valor problemático del mensaje de error
                    value_match = re.search(r'"(\d+)"', error_msg)
                    problematic_value = value_match.group(1) if value_match else "desconocido"
                    
                    user_friendly_msg = (
                        f"⚠️ ERROR DE TIPO DE DATO - Lote {batch_count}\n"
                        f"   Problema: Intentando insertar valor '{problematic_value}' en columna con tipo incompatible"
                    )
                    
                    print(user_friendly_msg)
                    logging.error(user_friendly_msg)
                    logging.error(f"   Detalle técnico completo: {e}")
                
                # 2. Errores de Integridad (Duplicados, FKs, etc.)
                elif 'IntegrityError' in error_type or 'duplicate key' in error_msg.lower():
                    user_friendly_msg = (
                        f"⚠️ ERROR DE INTEGRIDAD - Lote {batch_count}\n"
                        f"   Problema: Violación de constraint de base de datos"
                    )
                    print(user_friendly_msg)
                    logging.error(user_friendly_msg)
                    logging.error(f"   Detalle técnico completo: {e}")
                
                # 3. Errores de Columna (nombre inválido, no existe, etc.)
                elif 'ProgrammingError' in error_type or 'column' in error_msg.lower():
                    user_friendly_msg = (
                        f"⚠️ ERROR DE ESQUEMA - Lote {batch_count}\n"
                        f"   Problema: Error en definición de columna o tabla"
                    )
                    print(user_friendly_msg)
                    logging.error(user_friendly_msg)
                    logging.error(f"   Detalle técnico completo: {e}")
                
                # 4. Error Genérico (mantener para otros casos)
                else:
                    user_friendly_msg = (
                        f"❌ ERROR DESCONOCIDO - Lote {batch_count}\n"
                        f"   Tipo: {error_type}"
                    )
                    print(user_friendly_msg)
                    logging.error(user_friendly_msg)
                    logging.error(f"   Detalle técnico completo: {e}")
            
            monitor.metrics['db_execution_time'] += (time.time() - db_start_time)
            monitor.record_null_stats(df_batch) # Stats por lote
            
            total_records_processed += len(df_batch)
            batch_count += 1
            print(f"   -> Lote {batch_count} procesado ({total_records_processed} total)...")

        print("✅ Proceso finalizado.")
        monitor.generate_report()
        logging.info(f"✅ CORRIDA FINALIZADA EXITOSAMENTE - OBJETO: {OBJECT_TYPE}\n" + "-"*80 + "\n")

    except Exception as e:
        print(f"\n💀 ERROR FATAL: {e.message}, error completo en logs")
        logging.critical(f"❌ CORRIDA DETENIDA POR ERROR FATAL - OBJETO: {OBJECT_TYPE}")
        logging.critical(f"   Detalle: {e}")
        logging.critical("\n" + "-"*80 + "\n")

if __name__ == "__main__":
    run_postgres_etl()