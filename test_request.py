import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# --- CONFIGURACIÓN ---
OBJECT_TYPE = "services" # Puede ser companies, deals, tickets, etc.
OUTPUT_FILE = "hubspot_export2.xlsx"


headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

def export_hubspot_with_pagination():
    # 1. Obtener los nombres de todas las propiedades
    print(f"1. Obteniendo definiciones de propiedades para {OBJECT_TYPE}...")
    props_url = f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}"
    props_res = requests.get(props_url, headers=headers)
    props_res.raise_for_status()
    all_props = [p['name'] for p in props_res.json()['results']]
    
    # 2. Configuración para el bucle de paginación
    search_url = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}/search"
    all_records = []
    after = None # Aquí guardaremos el token de la siguiente página
    
    print(f"2. Iniciando descarga de registros (esto puede tardar)...")

    while True:
        payload = {
            "properties": all_props,
            "limit": 100,
            "filterGroups": []
        }
        
        # Si tenemos un token 'after', lo añadimos al body
        if after:
            payload["after"] = after

        response = requests.post(search_url, headers=headers, json=payload)
        
        # Manejo de Rate Limiting (Opcional pero recomendado)
        if response.status_code == 429:
            print("Límite de velocidad alcanzado. Esperando 10 segundos...")
            time.sleep(10)
            continue
            
        response.raise_for_status()
        data = response.json()
        
        results = data.get('results', [])
        all_records.extend(results)
        
        print(f"   -> Descargados {len(all_records)} registros...")

        # Verificamos si hay una página siguiente
        paging = data.get('paging')
        if paging and 'next' in paging:
            after = paging['next']['after']
        else:
            # Si no hay objeto 'next', hemos terminado
            break

    if not all_records:
        print("No se encontraron registros para exportar.")
        return

    # 3. Procesar datos para Excel
    print(f"3. Procesando {len(all_records)} registros para Excel...")
    data_list = []
    for record in all_records:
        row = {"hs_object_id": record["id"]}
        row.update(record["properties"])
        data_list.append(row)

    df = pd.DataFrame(data_list)
    
    # Reordenar columnas para que el ID sea la primera
    cols = ['hs_object_id'] + [c for c in df.columns if c != 'hs_object_id']
    df = df[cols]

    # 4. Guardar archivo
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"--- ¡ÉXITO! ---")
    print(f"Archivo guardado: {OUTPUT_FILE}")
    print(f"Total registros exportados: {len(df)}")

if __name__ == "__main__":
    try:
        export_hubspot_with_pagination()
    except Exception as e:
        print(f"Error durante la ejecución: {e}")