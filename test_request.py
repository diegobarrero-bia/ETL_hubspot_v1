import requests
import pandas as pd
import time
import os
import unicodedata
import re
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
OBJECT_TYPE = "services" 
OUTPUT_FOLDER = "exports"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "hubspot_etl_postgres_final.xlsx")
REPORT_FILE = os.path.join(OUTPUT_FOLDER, "etl_health_report.txt")
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

# --- CLASE NUEVA: MONITOR DE SALUD ---
class ETLMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.metrics = {
            'api_calls': 0,
            'retries_429': 0,
            'retries_5xx': 0,
            'connection_errors': 0,
            'records_fetched': 0,
            'records_processed_ok': 0,
            'records_failed': 0,
            'columns_truncated': 0,
            'associations_found': 0,
            'associations_missing': 0
        }

    def increment(self, metric):
        if metric in self.metrics:
            self.metrics[metric] += 1

    def generate_report(self):
        duration = time.time() - self.start_time
        duration_str = str(timedelta(seconds=int(duration)))
        
        m = self.metrics
        total_records = m['records_fetched']
        success_rate = (m['records_processed_ok'] / total_records * 100) if total_records > 0 else 0
        
        report = f"""
==================================================
          DATA HEALTH REPORT - ETL RUN
==================================================
Fecha de Ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duración Total    : {duration_str}
Estado General    : {'🟢 SALUDABLE' if m['records_failed'] == 0 else '⚠️ CON ERRORES'}

1. SALUD DE CONEXIÓN (API PERFORMANCE)
--------------------------------------
   - Llamadas a API realizadas : {m['api_calls']}
   - Reintentos por Rate Limit : {m['retries_429']}
   - Reintentos por Servidor   : {m['retries_5xx']}
   - Fallos de Conexión        : {m['connection_errors']}

2. INTEGRIDAD DE DATOS (DATA INTEGRITY)
---------------------------------------
   - Total Registros HubSpot   : {total_records}
   - Procesados Exitosamente   : {m['records_processed_ok']} ({success_rate:.2f}%)
   - Registros Fallidos        : {m['records_failed']}
   - Registros con Asociaciones: {m['associations_found']}
   - Registros sin Asociaciones: {m['associations_missing']}

3. CALIDAD DE ESQUEMA (SCHEMA HEALTH)
-------------------------------------
   - Columnas Truncadas (>63c) : {m['columns_truncated']}
   
==================================================
"""
        print(report)
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"📄 Reporte guardado en: {REPORT_FILE}")

# --- Instancia global del monitor ---
monitor = ETLMonitor()

def safe_request(method, url, **kwargs):
    max_retries = 3
    backoff_factor = 5
    monitor.increment('api_calls')

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                monitor.increment('retries_429')
                wait_time = 10
                logging.warning(f"Rate Limit (429). Reintento {attempt}...")
                time.sleep(wait_time)
                continue
            elif 500 <= response.status_code < 600:
                monitor.increment('retries_5xx')
                wait_time = backoff_factor * attempt
                logging.warning(f"Error Servidor ({response.status_code}). Reintento {attempt}...")
                time.sleep(wait_time)
                continue
            else:
                response.raise_for_status()

        except requests.exceptions.ConnectionError:
            monitor.increment('connection_errors')
            wait_time = backoff_factor * attempt
            logging.warning(f"Fallo Conexión. Reintento {attempt}...")
            time.sleep(wait_time)
        except Exception as e:
            raise e

    raise Exception(f"Fallo crítico tras {max_retries} intentos en {url}")

def ensure_exports_folder():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

def normalize_name(text):
    if not isinstance(text, str):
        if text is not None: 
            logging.debug(f"Normalizando valor no-string: {text}")
        return str(text)
    
    text = text.lower()
    text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
    text = text.replace(" ", "_").replace("-", "_")
    text = re.sub(r'[^a-z0-9_]', '', text)
    return re.sub(r'_{2,}', '_', text).strip('_')

def sanitize_columns_for_postgres(df):
    print("Verificando compatibilidad con PostgreSQL (Límite 63 chars)...")
    new_columns = []
    seen_columns = {} 

    for col in df.columns:
        sanitized = col[:63]
        
        # Métrica de salud: Columnas truncadas
        if len(col) > 63:
            monitor.increment('columns_truncated')
            logging.warning(f"Columna truncada: '{col}' -> '{sanitized}'")

        if sanitized in seen_columns:
            count = seen_columns[sanitized]
            seen_columns[sanitized] += 1
            suffix = f"_{count}"
            trim_length = 63 - len(suffix)
            sanitized = f"{sanitized[:trim_length]}{suffix}"
            logging.warning(f"Colisión resuelta: '{col}' -> '{sanitized}'")
        else:
            seen_columns[sanitized] = 1
            
        new_columns.append(sanitized)

    df.columns = new_columns
    return df

def get_smart_column_mapping(object_type, all_properties):
    print(f"\n--- GENERANDO MAPEO Y NORMALIZACIÓN ---")
    url = f"https://api.hubapi.com/crm/v3/pipelines/{object_type}"
    try:
        res = safe_request('GET', url)
        pipelines = res.json().get('results', [])
    except Exception as e:
        logging.critical(f"Error pipelines: {e}")
        raise

    mapping = {}
    target_prefixes = ["hs_v2_latest_time_in", "hs_v2_date_entered", "hs_v2_date_exited", "hs_v2_cumulative_time_in"]

    for pipeline in pipelines:
        for stage in pipeline.get('stages', []):
            s_id_clean = stage['id'].replace("-", "_")
            s_label = stage['label']
            for prop_name in all_properties:
                for prefix in target_prefixes:
                    if prop_name.startswith(prefix) and s_id_clean in prop_name:
                        mapping[prop_name] = normalize_name(f"{prefix}_{s_label}")
    return mapping

def get_all_association_types(object_type):
    url = f"https://api.hubapi.com/crm/v3/schemas/{object_type}"
    try:
        res = safe_request('GET', url)
        return [assoc['toObjectTypeId'] for assoc in res.json().get('associations', [])]
    except Exception as e:
        raise

def export_hubspot_health():
    try:
        ensure_exports_folder()
        
        print("Obteniendo inventario de propiedades...")
        props_url = f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}"
        props_res = safe_request('GET', props_url)
        all_prop_names = [p['name'] for p in props_res.json()['results']]
        
        col_mapping = get_smart_column_mapping(OBJECT_TYPE, all_prop_names)
        dynamic_associations = get_all_association_types(OBJECT_TYPE)
        
        search_url = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}/search"
        all_records = []
        after = None
        
        print(f"Iniciando descarga monitoreada...")
        while True:
            payload = {
                "properties": all_prop_names,
                "associations": dynamic_associations,
                "limit": 100,
                "filterGroups": []
            }
            if after: payload["after"] = after

            response = safe_request('POST', search_url, json=payload)
            data = response.json()
            results = data.get('results', [])
            
            # Métrica: Registros Obtenidos
            monitor.metrics['records_fetched'] += len(results)
            all_records.extend(results)
            
            print(f"   -> {len(all_records)} registros recuperados...")

            paging = data.get('paging')
            if paging and 'next' in paging:
                after = paging['next']['after']
            else:
                break

        print(f"Procesando {len(all_records)} registros...")
        data_list = []
        
        for record in all_records:
            try:
                row = {"hs_object_id": record["id"]}
                row.update(record.get("properties", {}))
                
                # Procesamiento de asociaciones con métricas
                has_associations = False
                assoc_raw = record.get("associations")
                
                if assoc_raw and isinstance(assoc_raw, dict):
                    for obj_type, assoc_data in assoc_raw.items():
                        if isinstance(assoc_data, dict):
                            ids = [str(a["id"]) for a in assoc_data.get("results", []) if "id" in a]
                            if ids:
                                col_name = normalize_name(f"asoc_{obj_type}_ids")
                                row[col_name] = ",".join(ids)
                                has_associations = True
                
                # Métrica: Asociaciones
                if has_associations:
                    monitor.increment('associations_found')
                else:
                    monitor.increment('associations_missing')

                data_list.append(row)
                monitor.increment('records_processed_ok') # Éxito

            except Exception as e:
                monitor.increment('records_failed') # Fallo
                logging.error(f"Error registro {record.get('id')}: {e}")

        if not data_list:
            print("No hay datos válidos.")
            return

        df = pd.DataFrame(data_list)
        df.rename(columns=col_mapping, inplace=True)
        df.columns = [normalize_name(col) for col in df.columns]
        df = sanitize_columns_for_postgres(df)
        
        df.to_excel(OUTPUT_FILE, index=False)
        
        # --- GENERAR REPORTE FINAL ---
        monitor.generate_report()

    except Exception as e:
        print(f"\n💀 ERROR FATAL: {e}")
        logging.critical(f"Error Fatal: {e}")

if __name__ == "__main__":
    export_hubspot_health()