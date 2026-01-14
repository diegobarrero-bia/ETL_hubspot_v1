import requests
import os
import json
import unicodedata
import re
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
OBJECT_TYPE = "services"

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

# --- FUNCIONES DE NORMALIZACIÓN (Idénticas al ETL) ---
def normalize_name(text):
    """
    Normaliza una cadena para que coincida con los nombres de columna en Postgres.
    """
    if not isinstance(text, str): return str(text) if text is not None else ""
    text = text.lower()
    text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
    text = text.replace(" ", "_").replace("-", "_")
    text = re.sub(r'[^a-z0-9_]', '', text)
    return re.sub(r'_{2,}', '_', text).strip('_')

def get_pipeline_map():
    """
    Obtiene los pipelines y crea un mapa de traducción: { 'stage_id_clean': 'stage_label' }
    """
    print("⏳ Obteniendo metadatos de Pipelines...")
    url = f"https://api.hubapi.com/crm/v3/pipelines/{OBJECT_TYPE}"
    try:
        response = requests.get(url, headers=headers)
        results = response.json().get('results', [])
        
        stage_map = {}
        for pipe in results:
            for stage in pipe.get('stages', []):
                # Limpiamos el ID tal como lo hace el ETL (guiones a guion bajo)
                s_id_clean = stage['id'].replace("-", "_")
                stage_map[s_id_clean] = stage['label']
        return stage_map
    except Exception as e:
        print(f"⚠️ No se pudieron cargar pipelines: {e}")
        return {}

def get_all_properties():
    print("⏳ Obteniendo inventario de propiedades...")
    url = f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}"
    try:
        response = requests.get(url, headers=headers)
        results = response.json().get('results', [])
        return [p['name'] for p in results]
    except Exception as e:
        print(f"❌ Error propiedades: {e}")
        return []

def get_association_types():
    url = f"https://api.hubapi.com/crm/v3/schemas/{OBJECT_TYPE}"
    try:
        response = requests.get(url, headers=headers)
        return [assoc['toObjectTypeId'] for assoc in response.json().get('associations', [])]
    except:
        return []

def inspect_specific_service(service_id):
    # 1. Obtener Metadatos
    all_props = get_all_properties()
    all_assocs = get_association_types()
    stage_map = get_pipeline_map() # <--- Mapa de traducción de etapas

    if not all_props:
        return

    # 2. Buscar el registro
    search_url = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}/search"
    payload = {
        "filterGroups": [{"filters": [{"propertyName": "hs_object_id", "operator": "EQ", "value": service_id}]}],
        "properties": all_props,
        "associations": all_assocs,
        "limit": 1
    }

    print(f"\n🔍 Buscando Service ID: {service_id} ...")
    
    try:
        response = requests.post(search_url, headers=headers, json=payload)
        data = response.json()
        results = data.get('results', [])
        
        if not results:
            print(f"⚠️ No se encontró ningún registro con el ID: {service_id}")
            return

        record = results[0]
        properties = record.get('properties', {})

        # 3. Imprimir RAW JSON (Para ver lo que llega crudo)
        print("\n" + "="*60)
        print(f" 1. DATOS CRUDOS (RAW JSON) - ID: {service_id}")
        print("="*60)
        # Filtramos propiedades vacías para no ensuciar la terminal
        props_with_values = {k: v for k, v in properties.items() if v is not None and v != ""}
        print(json.dumps(props_with_values, indent=4, ensure_ascii=False))

        # 4. SIMULACIÓN ETL (Para comparar con Postgres)
        print("\n" + "="*60)
        print(f" 2. SIMULACIÓN ETL (COMO SE VE EN POSTGRES)")
        print("="*60)
        print(f"{'COLUMNA POSTGRES (Normalizada)':<55} | {'VALOR'}")
        print("-" * 80)

        # Prefijos técnicos de pipelines que queremos traducir
        prefixes = ["hs_v2_latest_time_in", "hs_v2_date_entered", "hs_v2_date_exited", "hs_v2_cumulative_time_in"]
        
        # Ordenamos las llaves para facilitar la lectura
        for key in sorted(properties.keys()):
            val = properties[key]
            if val is None or val == "": 
                continue # Saltamos vacíos para facilitar lectura

            # --- Lógica de Normalización ---
            normalized_key = normalize_name(key) # Normalización base
            
            # Revisamos si es una columna de Pipeline para renombrarla inteligentemente
            # Ejemplo: hs_v2_date_entered_12345 -> hs_v2_date_entered_visita_previa
            for prefix in prefixes:
                if key.startswith(prefix):
                    # Intentamos encontrar el ID de la etapa dentro del nombre crudo
                    for s_id, s_label in stage_map.items():
                        if s_id in key:
                            # Construimos el nombre legible
                            readable_name = f"{prefix}_{s_label}"
                            normalized_key = normalize_name(readable_name)
                            break
            
            # Imprimir fila alineada
            # Recortamos el nombre si es muy largo para que quepa en pantalla
            print(f"{normalized_key:<55} | {val}")

        print("="*60)
        print("TIP: Las columnas de arriba deben coincidir EXACTAMENTE con las de DBeaver.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    target_id = input("Introduce el ID del Service: ").strip()
    if target_id:
        inspect_specific_service(target_id)