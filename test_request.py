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

# --- NUEVOS IMPORTS PARA BASE DE DATOS ---
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

load_dotenv()

# --- CONFIGURACIÓN ---
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
# Credenciales de BD
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

OBJECT_TYPE = "services"
TABLE_NAME = "services_etl" # Nombre de la tabla en Postgres
LOG_FILE = "etl_errors.log"

# Configuración de Logging
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

# --- CLASE MONITOR ---
class ETLMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.metrics = {
            'api_calls': 0, 'retries_429': 0, 'retries_5xx': 0,
            'connection_errors': 0, 'records_fetched': 0,
            'records_processed_ok': 0, 'records_failed': 0,
            'columns_truncated': 0, 'associations_found': 0,
            'associations_missing': 0, 'db_upserts': 0
        }

    def increment(self, metric, count=1):
        if metric in self.metrics:
            self.metrics[metric] += count

    def generate_report(self):
        duration = time.time() - self.start_time
        duration_str = str(timedelta(seconds=int(duration)))
        m = self.metrics
        
        report = f"""
==================================================
          DATA HEALTH REPORT - POSTGRES LOAD
==================================================
Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duración: {duration_str}
Estado: {'🟢 SALUDABLE' if m['records_failed'] == 0 else '⚠️ CON ERRORES'}

1. CONEXIÓN API
   - Llamadas: {m['api_calls']} | Reintentos: {m['retries_429'] + m['retries_5xx']}

2. DATOS
   - Fetched: {m['records_fetched']} | Procesados OK: {m['records_processed_ok']}
   - Fallidos: {m['records_failed']} | Upserts DB: {m['db_upserts']}
   - Con Asoc: {m['associations_found']} | Sin Asoc: {m['associations_missing']}

3. SCHEMA
   - Cols Truncadas: {m['columns_truncated']}
==================================================
"""
        print(report)

monitor = ETLMonitor()

# --- FUNCIONES DE DB ---
def get_db_engine():
    """Crea la conexión a PostgreSQL"""
    db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

def upsert_on_conflict(table, conn, keys, data_iter):
    """
    Función personalizada para manejar el UPSERT en Postgres.
    Si el ID ya existe, actualiza los campos; si no, inserta.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    
    # Preparamos la sentencia INSERT de Postgres
    stmt = insert(table.table).values(data)
    
    # Definimos qué columnas actualizar en caso de conflicto (todas menos el ID)
    # 'excluded' contiene los nuevos valores que intentábamos insertar
    update_cols = {c.name: c for c in stmt.excluded if c.name != 'hs_object_id'}
    
    if not update_cols:
        # Si no hay columnas para actualizar (solo ID), no hacemos nada
        return

    # Sentencia final: ON CONFLICT (hs_object_id) DO UPDATE ...
    on_conflict_stmt = stmt.on_conflict_do_update(
        index_elements=['hs_object_id'], # Asume que esta es la Primary Key
        set_=update_cols
    )
    
    conn.execute(on_conflict_stmt)

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
        # 1. Configurar DB
        engine = get_db_engine()
        print("✅ Conexión a BD establecida.")

        # 2. Metadatos
        print("Obteniendo propiedades...")
        props_res = safe_request('GET', f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}")
        all_props = [p['name'] for p in props_res.json()['results']]
        
        col_map = get_smart_mapping(all_props)
        assocs = get_assocs()
        
        # 3. Descarga
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

        # 4. Procesamiento
        print(f"Procesando {len(all_records)} registros...")
        data_list = []
        synced_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        
        for record in all_records:
            try:
                row = {"hs_object_id": int(record["id"])} # Asegurar int para PK
                row.update(record.get("properties", {}))
                
                # Campos Fivetran
                archived = record.get("archived", False)
                row["_fivetran_synced"] = synced_at
                row["_fivetran_deleted"] = archived
                row["is_deleted"] = archived
                
                # Asociaciones
                has_assoc = False
                raw_assoc = record.get("associations")
                if raw_assoc and isinstance(raw_assoc, dict):
                    for atype, adata in raw_assoc.items():
                        if isinstance(adata, dict):
                            ids = [str(a["id"]) for a in adata.get("results", []) if "id" in a]
                            if ids:
                                row[normalize_name(f"asoc_{atype}_ids")] = ",".join(ids)
                                has_assoc = True
                
                if has_assoc: monitor.increment('associations_found')
                else: monitor.increment('associations_missing')
                
                data_list.append(row)
                monitor.increment('records_processed_ok')
                
            except Exception as e:
                monitor.increment('records_failed')
                logging.error(f"Error registro {record.get('id')}: {e}")

        if not data_list: return

        # 5. DataFrame y Limpieza
        df = pd.DataFrame(data_list)
        df.rename(columns=col_map, inplace=True)
        df.columns = [normalize_name(c) for c in df.columns]
        df = sanitize_columns_for_postgres(df)
        
        # Limpieza de tipos para Postgres
        # Postgres no acepta dicts o listas en columnas TEXT directamente desde Pandas
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

        # 6. Carga a BD (UPSERT)
        print("Subiendo a PostgreSQL (Upsert)...")
        # Usamos chunksize para no saturar la memoria en cargas grandes
        df.to_sql(
            TABLE_NAME, 
            engine, 
            if_exists='append', 
            index=False, 
            method=upsert_on_conflict, # <--- Aquí ocurre la magia
            chunksize=500 
        )
        
        monitor.metrics['db_upserts'] = len(df)
        print("✅ Carga completada exitosamente.")
        monitor.generate_report()

    except Exception as e:
        print(f"\n💀 ERROR FATAL: {e}")
        logging.critical(f"Error Fatal: {e}")

if __name__ == "__main__":
    run_postgres_etl()