import requests
import pandas as pd
import time
import os
import unicodedata
import re
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
OBJECT_TYPE = "services" 
OUTPUT_FOLDER = "exports"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "hubspot_etl_normalized.xlsx")

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

def ensure_exports_folder():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

def normalize_name(text):
    """
    Normaliza una cadena para que sea segura como nombre de columna en BD.
    Ej: "Tiempo en: Visita Previa" -> "tiempo_en_visita_previa"
    """
    if not isinstance(text, str):
        return str(text)
    
    # 1. Convertir a minúsculas
    text = text.lower()
    
    # 2. Eliminar tildes (Normalización NFD separa caracteres de sus acentos)
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore').decode("utf-8")
    
    # 3. Reemplazar espacios y guiones por guion bajo
    text = text.replace(" ", "_").replace("-", "_")
    
    # 4. Eliminar cualquier caracter que no sea letra, número o guion bajo
    text = re.sub(r'[^a-z0-9_]', '', text)
    
    # 5. Evitar guiones bajos dobles resultantes
    text = re.sub(r'_{2,}', '_', text)
    
    return text.strip('_')

def get_smart_column_mapping(object_type, all_properties):
    """
    Genera un mapeo buscando el ID de la etapa y NORMALIZA el nombre resultante.
    """
    print(f"\n--- GENERANDO MAPEO Y NORMALIZACIÓN ---")
    
    url = f"https://api.hubapi.com/crm/v3/pipelines/{object_type}"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    pipelines = res.json().get('results', [])

    mapping = {}
    
    target_prefixes = [
        "hs_v2_latest_time_in",
        "hs_v2_date_entered",
        "hs_v2_date_exited",
        "hs_v2_cumulative_time_in"
    ]

    for pipeline in pipelines:
        for stage in pipeline.get('stages', []):
            s_id = stage['id']
            s_label = stage['label']
            s_id_clean = s_id.replace("-", "_")
            
            # Buscamos coincidencias
            for prop_name in all_properties:
                for prefix in target_prefixes:
                    # Si la propiedad técnica contiene el ID de la etapa
                    if prop_name.startswith(prefix) and s_id_clean in prop_name:
                        
                        # Construimos el nombre legible
                        raw_readable_name = f"{prefix}_{s_label}"
                        
                        # APLICAMOS NORMALIZACIÓN
                        # Ej: hs_v2_date_entered_Visita previa -> hs_v2_date_entered_visita_previa
                        normalized_name = normalize_name(raw_readable_name)
                        
                        mapping[prop_name] = normalized_name
                        print(f"  [Norm] {prop_name} -> {normalized_name}")

    print(f"--- Mapeo finalizado: {len(mapping)} columnas preparadas ---\n")
    return mapping

def get_all_association_types(object_type):
    url = f"https://api.hubapi.com/crm/v3/schemas/{object_type}"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    return [assoc['toObjectTypeId'] for assoc in res.json().get('associations', [])]

def export_hubspot_normalized():
    ensure_exports_folder()
    
    # 1. Obtener lista de todas las propiedades
    print("Obteniendo inventario de propiedades...")
    props_url = f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}"
    props_res = requests.get(props_url, headers=headers)
    all_props_data = props_res.json()['results']
    all_prop_names = [p['name'] for p in all_props_data]
    
    # 2. Generar mapeo normalizado
    col_mapping = get_smart_column_mapping(OBJECT_TYPE, all_prop_names)
    dynamic_associations = get_all_association_types(OBJECT_TYPE)
    
    # 3. Descarga de datos
    search_url = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}/search"
    all_records = []
    after = None
    
    print(f"Iniciando descarga de registros...")
    while True:
        payload = {
            "properties": all_prop_names,
            "associations": dynamic_associations,
            "limit": 100,
            "filterGroups": []
        }
        if after: payload["after"] = after

        response = requests.post(search_url, headers=headers, json=payload)
        if response.status_code == 429:
            time.sleep(10)
            continue
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        all_records.extend(results)
        print(f"   -> {len(all_records)} registros recuperados...")

        paging = data.get('paging')
        if paging and 'next' in paging:
            after = paging['next']['after']
        else:
            break

    # 4. Procesar datos
    data_list = []
    for record in all_records:
        row = {"hs_object_id": record["id"]}
        row.update(record.get("properties", {}))
        
        if "associations" in record:
            for obj_type, assoc_data in record["associations"].items():
                ids = [a["id"] for a in assoc_data.get("results", [])]
                # Normalizamos también el nombre de la columna de asociación
                assoc_col_name = normalize_name(f"asoc_{obj_type}_ids")
                row[assoc_col_name] = ",".join(ids)
        data_list.append(row)

    df = pd.DataFrame(data_list)
    
    # 5. Aplicar el mapeo normalizado a las columnas de pipeline
    df.rename(columns=col_mapping, inplace=True)
    
    # OPCIONAL: Normalizar TAMBIÉN las columnas que no entraron en el mapeo (ej. propiedades estándar)
    # Esto asegura que "hs_created_by_user_id" o "custom Property" queden uniformes.
    df.columns = [normalize_name(col) for col in df.columns]
    
    # 6. Guardar
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"--- EXPORTACIÓN FINALIZADA ---")
    print(f"Archivo guardado: {OUTPUT_FILE}")

if __name__ == "__main__":
    export_hubspot_normalized()