import requests
import pandas as pd
import time
import os
import unicodedata
import re
import logging
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
OBJECT_TYPE = "services" 
OUTPUT_FOLDER = "exports"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "hubspot_etl_integrity.xlsx") # Nombre actualizado
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

def safe_request(method, url, **kwargs):
    """
    Realiza una petición HTTP con lógica de reintentos automática.
    """
    max_retries = 3
    backoff_factor = 5

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                wait_time = 10
                msg = f"Rate Limit (429). Reintento {attempt}/{max_retries} en {wait_time}s..."
                print(f"⚠️ {msg}")
                logging.warning(msg)
                time.sleep(wait_time)
                continue
            elif 500 <= response.status_code < 600:
                wait_time = backoff_factor * attempt
                msg = f"Error Servidor HubSpot ({response.status_code}). Reintento {attempt}/{max_retries} en {wait_time}s..."
                print(f"🔥 {msg}")
                logging.warning(msg)
                time.sleep(wait_time)
                continue
            else:
                response.raise_for_status()

        except requests.exceptions.ConnectionError:
            wait_time = backoff_factor * attempt
            msg = f"Fallo de Conexión. Reintento {attempt}/{max_retries} en {wait_time}s..."
            print(f"🔌 {msg}")
            logging.warning(msg)
            time.sleep(wait_time)
        except Exception as e:
            raise e

    raise Exception(f"Fallo crítico tras {max_retries} intentos en {url}")

def ensure_exports_folder():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

# --- MEJORA B: Logging en Normalización ---
def normalize_name(text):
    if not isinstance(text, str):
        # Registramos si llega algo extraño que no es texto
        if text is not None: 
            logging.debug(f"Normalizando valor no-string: {text} (Tipo: {type(text)})")
        return str(text)
    
    text = text.lower()
    text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
    text = text.replace(" ", "_").replace("-", "_")
    text = re.sub(r'[^a-z0-9_]', '', text)
    return re.sub(r'_{2,}', '_', text).strip('_')

def get_smart_column_mapping(object_type, all_properties):
    print(f"\n--- GENERANDO MAPEO Y NORMALIZACIÓN ---")
    url = f"https://api.hubapi.com/crm/v3/pipelines/{object_type}"
    
    try:
        res = safe_request('GET', url)
        pipelines = res.json().get('results', [])
    except Exception as e:
        print(f"❌ Error crítico obteniendo Pipelines: {e}")
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
        print(f"❌ Error crítico obteniendo Esquema: {e}")
        raise

def export_hubspot_integrity():
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
        
        print(f"Iniciando descarga resiliente de registros...")
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
            all_records.extend(results)
            print(f"   -> {len(all_records)} registros recuperados...")

            paging = data.get('paging')
            if paging and 'next' in paging:
                after = paging['next']['after']
            else:
                break

        # Procesamiento
        data_list = []
        print(f"Procesando {len(all_records)} registros con validación de integridad...")
        
        for record in all_records:
            try:
                row = {"hs_object_id": record["id"]}
                row.update(record.get("properties", {}))
                
                # --- MEJORA A: Validación Defensiva de Asociaciones ---
                assoc_raw = record.get("associations")
                
                # 1. Validamos que 'associations' exista y sea un Diccionario
                if assoc_raw and isinstance(assoc_raw, dict):
                    
                    for obj_type, assoc_data in assoc_raw.items():
                        # 2. Validamos que los datos internos también sean diccionario
                        if isinstance(assoc_data, dict):
                            results_list = assoc_data.get("results", [])
                            
                            # 3. Extraemos IDs de forma segura (solo si tienen la clave 'id')
                            ids = [str(a["id"]) for a in results_list if isinstance(a, dict) and "id" in a]
                            
                            if ids:
                                col_name = normalize_name(f"asoc_{obj_type}_ids")
                                row[col_name] = ",".join(ids)
                        else:
                            logging.warning(f"Asociación mal formada en registro {record['id']}: {obj_type} no es dict.")

                data_list.append(row)

            except Exception as e:
                logging.error(f"Error procesando registro {record.get('id')}: {e}")

        if not data_list:
            print("No hay datos válidos.")
            return

        df = pd.DataFrame(data_list)
        df.rename(columns=col_mapping, inplace=True)
        df.columns = [normalize_name(col) for col in df.columns]
        
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"--- ¡ÉXITO! ---")
        print(f"Archivo: {OUTPUT_FILE}")
        print(f"Log de errores: {LOG_FILE}")

    except Exception as e:
        print(f"\n💀 ERROR FATAL: {e}")
        logging.critical(f"Error Fatal: {e}")

if __name__ == "__main__":
    export_hubspot_integrity()