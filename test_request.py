import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
OBJECT_TYPE = "services" 
OUTPUT_FILE = "hubspot_dynamic_etl.xlsx"

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

def get_all_association_types(object_type):
    """Obtiene dinámicamente todos los objetos asociados permitidos para el tipo de objeto."""
    print(f"Buscando asociaciones permitidas para {object_type}...")
    url = f"https://api.hubapi.com/crm/v3/schemas/{object_type}"
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    schema_data = response.json()
    # Extraemos el 'toObjectTypeId' de cada asociación definida
    associations = [assoc['toObjectTypeId'] for assoc in schema_data.get('associations', [])]
    
    print(f"   -> Asociaciones encontradas: {associations}")
    return associations

def export_hubspot_with_pagination():
    # 1. Obtener nombres de propiedades
    props_url = f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}"
    props_res = requests.get(props_url, headers=headers)
    props_res.raise_for_status()
    all_props = [p['name'] for p in props_res.json()['results']]
    
    # 2. OBTENER ASOCIACIONES DINÁMICAMENTE
    dynamic_associations = get_all_association_types(OBJECT_TYPE)
    
    # 3. Configuración de búsqueda
    search_url = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}/search"
    all_records = []
    after = None
    
    print(f"Iniciando descarga de registros...")

    while True:
        payload = {
            "properties": all_props,
            "associations": dynamic_associations, # <--- Se pasan las asociaciones dinámicas
            "limit": 100,
            "filterGroups": []
        }
        
        if after:
            payload["after"] = after

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
        
        # Aplanar asociaciones dinámicamente
        if "associations" in record:
            for obj_type, assoc_data in record["associations"].items():
                ids = [a["id"] for a in assoc_data.get("results", [])]
                row[f"associated_{obj_type}_ids"] = ",".join(ids)

        data_list.append(row)

    df = pd.DataFrame(data_list)
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"--- ¡ETL FINALIZADA! ---")
    print(f"Archivo: {OUTPUT_FILE} | Registros: {len(df)}")

if __name__ == "__main__":
    try:
        export_hubspot_with_pagination()
    except Exception as e:
        print(f"Error: {e}")