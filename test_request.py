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
# --- CAMBIOS SOLICITADOS ---
DB_SCHEMA = "public"      
TABLE_NAME = "services"   
# ---------------------------

OUTPUT_FOLDER = "exports"
LOG_FILE = "etl_errors.log"

# Nombre dinámico del reporte con fecha y hora
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

# --- CLASE MONITOR ---
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
        total_rows = len(df)
        if total_rows == 0: return
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                pct = (count / total_rows) * 100
                self.null_stats[col] = (count, pct)

    def generate_report(self):
        duration = time.time() - self.start_time
        duration_str = str(timedelta(seconds=int(duration)))
        m = self.metrics
        
        nulls_report = ""
        if self.null_stats:
            sorted_nulls = sorted(self.null_stats.items(), key=lambda x: x[1][1], reverse=True)[:10]
            nulls_report = "\n   [Top Columnas con Valores Vacíos]\n"
            for col, (count, pct) in sorted_nulls:
                alert = "⚠️" if pct > 10 else " "
                nulls_report += f"   - {col[:30]:<30} : {count:>4} vacíos ({pct:>5.1f}%) {alert}\n"
        else:
            nulls_report = "   - No se detectaron valores nulos significativos.\n"

        report = f"""
==================================================
          DATA HEALTH REPORT - POSTGRES LOAD
==================================================
Fecha de Ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duración Total    : {duration_str}
Schema Destino    : {DB_SCHEMA}
Tabla Destino     : {TABLE_NAME}
Estado General    : {'🟢 SALUDABLE' if m['records_failed'] == 0 and m['db_insert_errors'] == 0 else '⚠️ CON ERRORES'}

1. CONEXIÓN API & VOLUMEN
-------------------------
   - Llamadas API          : {m['api_calls']} (Reintentos: {m['retries_429'] + m['retries_5xx']})
   - Registros Fetched     : {m['records_fetched']}
   - Procesados OK         : {m['records_processed_ok']}
   - Fallidos (ETL)        : {m['records_failed']}

2. DESEMPEÑO BASE DE DATOS (LOAD PHASE)
---------------------------------------
   - Registros Upserted    : {m['db_upserts']}
   - Tiempo Ejecución DB   : {m['db_execution_time']:.2f} segundos
   - Rechazos/Errores DB   : {m['db_insert_errors']} filas no cargadas

3. INTEGRIDAD & CALIDAD DE DATOS (DATA QUALITY)
-----------------------------------------------
   - Schema Changes        : {m['schema_changes']} nuevas columnas
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
    stmt = insert(table.table).values(data)
    update_cols = {c.name: c for c in stmt.excluded if c.name != 'hs_object_id'}
    if not update_cols: return
    
    on_conflict_stmt = stmt.on_conflict_do_update(
        index_elements=['hs_object_id'],
        set_=update_cols
    )
    conn.execute(on_conflict_stmt)

def sync_db_schema(engine, df, table_name, schema):
    print("Verificando consistencia del esquema...")
    inspector = inspect(engine)
    
    if not inspector.has_table(table_name, schema=schema): 
        return

    existing_cols = [c['name'] for c in inspector.get_columns(table_name, schema=schema)]
    new_cols = set(df.columns) - set(existing_cols)
    
    if new_cols:
        print(f"⚠️ Detectadas {len(new_cols)} columnas nuevas. Actualizando esquema...")
        with engine.begin() as conn:
            for col in new_cols:
                dtype = df[col].dtype
                pg_type = "TEXT"
                if pd.api.types.is_integer_dtype(dtype): pg_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype): pg_type = "NUMERIC"
                elif pd.api.types.is_bool_dtype(dtype): pg_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype): pg_type = "TIMESTAMP"
                
                conn.execute(text(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{col}" {pg_type}'))
                monitor.increment('schema_changes')
                logging.warning(f"Schema Evolution: Agregada columna '{col}' ({pg_type})")

def initialize_db_schema(engine):
    """
    Crea el Schema (si no existe) y la Tabla inicial.
    """
    print(f"Verificando Schema '{DB_SCHEMA}' y Tabla '{TABLE_NAME}'...")
    
    create_schema_sql = text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")

    ddl_query = text(f"""
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{TABLE_NAME} (
        "hs_object_id" BIGINT PRIMARY KEY,
        "company_id" BIGINT,
        "hs_created_by_user_id" BIGINT,
        "hs_updated_by_user_id" BIGINT,
        "hubspot_owner_id" BIGINT,
        "hubspot_team_id" BIGINT,
        
        "bia_profit" NUMERIC,
        "hs_amount_paid" NUMERIC,
        "hs_amount_remaining" NUMERIC,
        "hs_total_cost" NUMERIC,
        "investment_value" NUMERIC,
        "offer_value" NUMERIC,
        "revenue" NUMERIC,
        
        "hs_createdate" TIMESTAMP,
        "hs_lastmodifieddate" TIMESTAMP,
        "hs_close_date" TIMESTAMP,
        "hs_next_activity_date" TIMESTAMP,
        "hubspot_owner_assigneddate" TIMESTAMP,
        "_fivetran_synced" TIMESTAMP, 
        
        "hs_v2_cumulative_time_in_envio_de_oferta" BIGINT,
        "hs_v2_cumulative_time_in_decision_del_cliente" BIGINT,
        "hs_v2_cumulative_time_in_completado" BIGINT,
        "hs_v2_cumulative_time_in_visita_previa" BIGINT,
        "hs_v2_cumulative_time_in_oportunidad" BIGINT,
        
        "hs_v2_date_entered_envio_de_oferta" TIMESTAMP,
        "hs_v2_date_entered_decision_del_cliente" TIMESTAMP,
        "hs_v2_date_entered_completado" TIMESTAMP,
        "hs_v2_date_entered_visita_previa" TIMESTAMP,
        "hs_v2_date_entered_oportunidad" TIMESTAMP,
        
        "hs_was_imported" BOOLEAN,
        "_fivetran_deleted" BOOLEAN,
        "is_deleted" BOOLEAN,

        "hs_name" TEXT,
        "hs_pipeline" TEXT,
        "hs_pipeline_stage" TEXT,
        "bia_code" TEXT,
        "business_model" TEXT,
        "hs_category" TEXT,
        "hs_description" TEXT,
        "hs_object_source" TEXT,
        "hs_object_source_label" TEXT,
        "offer_url" TEXT,
        "operations_status" TEXT,
        "reason_for_objection" TEXT,
        "requesting_area" TEXT,
        
        "asoc_contacts_ids" TEXT,
        "asoc_companies_ids" TEXT,
        
        "hs_all_accessible_team_ids" TEXT,
        "hs_all_owner_ids" TEXT,
        "hs_merged_object_ids" TEXT,
        "hs_unique_creation_key" TEXT,
        "hs_user_ids_of_all_owners" TEXT
    );
    """)

    try:
        with engine.begin() as conn:
            conn.execute(create_schema_sql) 
            conn.execute(ddl_query)         
        print("✅ Esquema y Tabla verificados correctamente.")
    except Exception as e:
        print(f"❌ Error inicializando BD: {e}")
        raise e

def clean_dates(df):
    date_keywords = ['date', 'time', 'synced', 'timestamp']
    import warnings
    for col in df.columns:
        if any(k in col.lower() for k in date_keywords) or df[col].dtype == 'object':
            if any(k in col.lower() for k in date_keywords):
                try:
                    df[col] = pd.to_datetime(df[col], errors='raise', format='ISO8601')
                except (ValueError, TypeError, Exception):
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

# --- FUNCIONES AUXILIARES ETL ---
def safe_request(method, url, **kwargs):
    max_retries = 3; backoff = 5
    monitor.increment('api_calls')
    for attempt in range(1, max_retries + 1):
        try:
            res = requests.request(method, url, headers=headers, **kwargs)
            if res.status_code == 200: return res
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
        sanitized = col[:63]
        if len(col) > 63:
            monitor.increment('columns_truncated')
            logging.warning(f"Truncado: {col} -> {sanitized}")
        
        if sanitized in seen:
            seen[sanitized] += 1
            suffix = f"_{seen[sanitized]}"
            sanitized = f"{sanitized[:63-len(suffix)]}{suffix}"
        else: seen[sanitized] = 1
        new_cols.append(sanitized)
    df.columns = new_cols
    return df

def get_smart_mapping(all_props):
    print("Generando mapeo...")
    try:
        url = f"https://api.hubapi.com/crm/v3/pipelines/{OBJECT_TYPE}"
        res = safe_request('GET', url)
        pipelines = res.json().get('results', [])
    except Exception: return {}

    mapping = {}
    prefixes = ["hs_v2_latest_time_in", "hs_v2_date_entered", "hs_v2_date_exited", "hs_v2_cumulative_time_in"]
    
    for pipe in pipelines:
        for stage in pipe.get('stages', []):
            s_id = stage['id'].replace("-", "_")
            s_lbl = stage['label']
            for prop in all_props:
                for pre in prefixes:
                    if prop.startswith(pre) and s_id in prop:
                        mapping[prop] = normalize_name(f"{pre}_{s_lbl}")
    return mapping

def get_assocs():
    url = f"https://api.hubapi.com/crm/v3/schemas/{OBJECT_TYPE}"
    res = safe_request('GET', url)
    return [a['toObjectTypeId'] for a in res.json().get('associations', [])]

# --- PROCESO PRINCIPAL ---
def run_postgres_etl():
    try:
        ensure_exports_folder()
        engine = get_db_engine()
        print("✅ Motor de BD iniciado.")

        # 1. Preparar BD 
        initialize_db_schema(engine) 

        # 2. Descarga de HubSpot
        print("Obteniendo propiedades...")
        props_res = safe_request('GET', f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}")
        all_props = [p['name'] for p in props_res.json()['results']]
        
        col_map = get_smart_mapping(all_props)
        assocs = get_assocs()
        
        search_url = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}/search"
        all_records = []; after = None
        
        print("Iniciando descarga...")
        while True:
            payload = {
                "properties": all_props,
                "associations": assocs,
                "limit": 100,
                "filterGroups": []
            }
            if after: payload["after"] = after
            
            res = safe_request('POST', search_url, json=payload)
            data = res.json()
            results = data.get('results', [])
            
            monitor.metrics['records_fetched'] += len(results)
            all_records.extend(results)
            print(f"   -> {len(all_records)} recuperados...")
            
            paging = data.get('paging')
            if paging and 'next' in paging: after = paging['next']['after']
            else: break

        # 3. Procesamiento en Memoria
        print(f"Procesando {len(all_records)} registros...")
        data_list = []
        synced_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        
        for record in all_records:
            try:
                row = {"hs_object_id": int(record["id"])}
                row.update(record.get("properties", {}))
                
                archived = record.get("archived", False)
                row["_fivetran_synced"] = synced_at
                row["_fivetran_deleted"] = archived
                row["is_deleted"] = archived
                
                raw_assoc = record.get("associations")
                if raw_assoc and isinstance(raw_assoc, dict):
                    for atype, adata in raw_assoc.items():
                        if isinstance(adata, dict):
                            ids = [str(a["id"]) for a in adata.get("results", []) if "id" in a]
                            if ids: row[normalize_name(f"asoc_{atype}_ids")] = ",".join(ids)
                            monitor.increment('associations_found' if ids else 'associations_missing')
                else:
                    monitor.increment('associations_missing')
                
                data_list.append(row)
                monitor.increment('records_processed_ok')
                
            except Exception as e:
                monitor.increment('records_failed')
                logging.error(f"Error registro {record.get('id')}: {e}")

        if not data_list: return

        # 4. Transformación DataFrame
        df = pd.DataFrame(data_list)
        df.rename(columns=col_map, inplace=True)
        df.columns = [normalize_name(c) for c in df.columns]
        df = sanitize_columns_for_postgres(df)
        df = clean_dates(df)
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

        print("Analizando calidad de datos (Valores Nulos)...")
        monitor.record_null_stats(df)

        # 5. Sincronización de Schema 
        sync_db_schema(engine, df, TABLE_NAME, DB_SCHEMA)

        # 6. Carga a BD 
        print(f"Subiendo a PostgreSQL ({DB_SCHEMA}.{TABLE_NAME})...")
        
        db_start_time = time.time()
        
        try:
            with engine.begin() as conn:
                df.to_sql(
                    TABLE_NAME, 
                    con=conn, 
                    schema=DB_SCHEMA,  
                    if_exists='append', 
                    index=False, 
                    method=upsert_on_conflict, 
                    chunksize=500 
                )
            
            monitor.metrics['db_upserts'] = len(df)
        
        except Exception as e:
            monitor.metrics['db_insert_errors'] = len(df)
            logging.critical(f"Fallo masivo en carga DB: {e}")
            print(f"❌ Error crítico en base de datos: {e}")
        
        finally:
            db_duration = time.time() - db_start_time
            monitor.set_metric('db_execution_time', db_duration)

        print("✅ Proceso finalizado.")
        monitor.generate_report()

    except Exception as e:
        print(f"\n💀 ERROR FATAL: {e}")
        logging.critical(f"Error Fatal: {e}")

if __name__ == "__main__":
    run_postgres_etl()