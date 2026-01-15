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

OBJECT_TYPE = "services"
DB_SCHEMA = "hubspot_etl"      
TABLE_NAME = "services"   

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

def sync_db_schema(engine, df, table_name, schema):
    inspector = inspect(engine)
    if not inspector.has_table(table_name, schema=schema): return

    existing_cols = [c['name'] for c in inspector.get_columns(table_name, schema=schema)]
    new_cols = set(df.columns) - set(existing_cols)
    
    if new_cols:
        print(f"⚠️ Schema Evolution: Creando {len(new_cols)} columnas nuevas...")
        with engine.begin() as conn:
            for col in new_cols:
                dtype = df[col].dtype
                pg_type = "TEXT"
                # Lógica simple de tipos, por defecto TEXT para seguridad
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

def clean_dates(df):
    # Keywords que indican fechas REALES (no duración)
    date_keywords = ['date', 'synced', 'timestamp', 'createdate', 'modifieddate']
    
    # Excepciones: columnas que contienen "time" pero NO son fechas (son duraciones en ms)
    duration_keywords = ['cumulative_time', 'latest_time_in', 'time_in_stage', 'duration']
    
    for col in df.columns:
        col_lower = col.lower()
        
        # Si es una columna de duración, NO la toques
        if any(k in col_lower for k in duration_keywords):
            continue
        
        # Si contiene keywords de fecha, intenta convertir
        if any(k in col_lower for k in date_keywords):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception:
                pass
    
    return df

# --- FUNCIONES AUXILIARES ETL ---
def safe_request(method, url, **kwargs):
    max_retries = 3; backoff = 5
    monitor.increment('api_calls')
    for attempt in range(1, max_retries + 1):
        try:
            # Importante: para GET usamos params, para POST usamos json
            res = requests.request(method, url, headers=headers, **kwargs)
            
            if res.status_code == 200: return res
            elif res.status_code == 414:
                raise Exception("URL Too Long (414). Demasiadas propiedades solicitadas en GET.")
            elif res.status_code == 429:
                monitor.increment('retries_429'); time.sleep(10)
            elif 500 <= res.status_code < 600:
                monitor.increment('retries_5xx'); time.sleep(backoff * attempt)
            else: res.raise_for_status()
        except requests.exceptions.ConnectionError:
            monitor.increment('connection_errors'); time.sleep(backoff * attempt)
        except Exception as e: raise e
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
    prefixes = ["hs_v2_latest_time_in", "hs_v2_date_entered", "hs_v2_date_exited", "hs_v2_cumulative_time_in"]
    
    for pipe in pipelines:
        for stage in pipe.get('stages', []):
            s_id = stage['id']  # ← Mantener el ID original con guiones
            s_id_normalized = s_id.replace("-", "_")  # ← Version con underscores
            s_lbl = stage['label']
            
            for prop in all_props:
                for pre in prefixes:
                    # Buscar tanto con guiones como con underscores
                    if prop.startswith(pre) and (s_id in prop or s_id_normalized in prop):
                        # Mapeamos IDs técnicos a Labels legibles
                        mapping[prop] = normalize_name(f"{pre}_{s_lbl}")
    return mapping

def get_assocs():
    # Obtiene asociaciones disponibles
    url = f"https://api.hubapi.com/crm/v3/schemas/{OBJECT_TYPE}"
    res = safe_request('GET', url)
    return [a['toObjectTypeId'] for a in res.json().get('associations', [])]

# --- NUEVAS FUNCIONES PARA GET / LIST ---

def fetch_hubspot_records_generator(properties, associations):
    """
    Generador que usa el endpoint LIST (GET) y devuelve lotes de resultados.
    Maneja la paginación automáticamente.
    """
    url = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}"
    
    # Preparamos los parámetros base
    # NOTA: Si properties_str es > 2000 chars, esto podría fallar con 414.
    properties_str = ",".join(properties)
    associations_str = ",".join(associations)
    
    params = {
        "limit": 100, # Max permitido por página en GET
        "properties": properties_str,
        "associations": associations_str,
        "archived": "false"
    }
    
    print(f"📡 Iniciando descarga con GET (Params length: {len(properties_str)} chars)...")
    
    after = None
    while True:
        if after:
            params['after'] = after
            
        res = safe_request('GET', url, params=params)
        data = res.json()
        results = data.get('results', [])
        
        if not results:
            break
            
        yield results # Entregamos el lote actual para procesar
        
        paging = data.get('paging')
        if paging and 'next' in paging:
            after = paging['next']['after']
        else:
            break

def process_batch(batch_records, col_map):
    """
    Transforma una lista de diccionarios de HubSpot en un DataFrame limpio.
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
        return pd.DataFrame()

    # Creación y limpieza de DataFrame
    df = pd.DataFrame(data_list)
    
    # 1. Renombrar columnas inteligentes (stage names)
    df.rename(columns=col_map, inplace=True)
    
    # 2. Normalizar nombres (snake_case, sin acentos)
    df.columns = [normalize_name(c) for c in df.columns]
    
    # 3. Sanitizar para Postgres (longitud y duplicados)
    df = sanitize_columns_for_postgres(df)
    
    # 4. Limpiar fechas
    df = clean_dates(df)
    
    # 5. Convertir dicts/lists a JSON string para evitar error en DB
    for col in df.columns:
        # Verificación rápida de tipo object
        if df[col].dtype == 'object':
            # Aplicamos solo si el valor es lista o dict
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
            
    return df

# --- PROCESO PRINCIPAL ---
def run_postgres_etl():
    try:
        ensure_exports_folder()
        engine = get_db_engine()
        print("✅ Motor de BD iniciado.")

        # 1. Preparar BD (Tabla inicial mínima)
        initialize_db_schema(engine) 

        # 2. Obtener Metadatos (Propiedades y Asociaciones)
        print("Obteniendo definición de propiedades...")
        props_res = safe_request('GET', f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}")
        all_props = [p['name'] for p in props_res.json()['results']]
        
        # Validar longitud de URL
        url_simulated_len = len(",".join(all_props))
        if url_simulated_len > 6000:
            logging.warning(f"⚠️ URL muy larga ({url_simulated_len} chars). Podría fallar con 414. Considera filtrar columnas.")
        
        col_map = get_smart_mapping(all_props)
        assocs = get_assocs()
        
        # 3. Flujo por Lotes (Generator -> Transform -> Load)
        print("🚀 Iniciando proceso ETL por lotes...")
        
        batch_count = 0
        total_records_processed = 0
        
        # Iteramos sobre el generador (trae 100 en 100)
        for batch in fetch_hubspot_records_generator(all_props, assocs):
            monitor.metrics['records_fetched'] += len(batch)
            
            # A. Transformar
            df_batch = process_batch(batch, col_map)
            
            if df_batch.empty:
                continue

            # B. Evolución de Esquema (Checkea columnas nuevas en este lote)
            sync_db_schema(engine, df_batch, TABLE_NAME, DB_SCHEMA)
            
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

    except Exception as e:
        print(f"\n💀 ERROR FATAL: {e}")
        logging.critical(f"Error Fatal: {e}")

if __name__ == "__main__":
    run_postgres_etl()