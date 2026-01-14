import requests
import os
import unicodedata
import re
import json
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
OBJECT_TYPE = "services"

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

# --- LISTA DE COLUMNAS DEFINIDAS MANUALMENTE EN TU SQL ---
# Estas son las que TIENES en tu initialize_db_schema.
# El script verificará si los nombres generados coinciden con estas.
EXPECTED_COLUMNS = [
    "hs_v2_cumulative_time_in_envio_de_oferta",
    "hs_v2_cumulative_time_in_decision_del_cliente",
    "hs_v2_cumulative_time_in_completado",
    "hs_v2_cumulative_time_in_visita_previa",
    "hs_v2_cumulative_time_in_oportunidad",
    "hs_v2_date_entered_envio_de_oferta",
    "hs_v2_date_entered_decision_del_cliente",
    "hs_v2_date_entered_completado",
    "hs_v2_date_entered_visita_previa",
    "hs_v2_date_entered_oportunidad",
    "hs_v2_date_exited_envio_de_oferta",
    "hs_v2_date_exited_decision_del_cliente",
    "hs_v2_date_exited_completado",
    "hs_v2_date_exited_visita_previa",
    "hs_v2_date_exited_oportunidad"
]

def normalize_name(text):
    """Tu función de normalización exacta"""
    if not isinstance(text, str): return str(text) if text is not None else ""
    text = text.lower()
    text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
    text = text.replace(" ", "_").replace("-", "_")
    text = re.sub(r'[^a-z0-9_]', '', text)
    return re.sub(r'_{2,}', '_', text).strip('_')

def safe_request(method, url, **kwargs):
    try:
        res = requests.request(method, url, headers=headers, **kwargs)
        if res.status_code == 200: return res
        res.raise_for_status()
    except Exception as e:
        print(f"❌ Error API: {e}")
        return None

def debug_mapping_logic():
    print("--- INICIANDO DEBUG DE NOMBRES DE COLUMNA ---\n")

    # 1. Obtener todas las propiedades
    print("1. Obteniendo propiedades crudas de Services...")
    props_res = safe_request('GET', f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}")
    if not props_res: return
    all_props = [p['name'] for p in props_res.json()['results']]
    print(f"   -> {len(all_props)} propiedades encontradas.")

    # 2. Obtener Pipelines
    print("\n2. Obteniendo Pipelines y Stages...")
    pipe_res = safe_request('GET', f"https://api.hubapi.com/crm/v3/pipelines/{OBJECT_TYPE}")
    if not pipe_res: return
    pipelines = pipe_res.json().get('results', [])
    
    # 3. Simular Mapeo
    print("\n3. Simulando Generación de Nombres...")
    
    mapping = {}
    prefixes = ["hs_v2_latest_time_in", "hs_v2_date_entered", "hs_v2_date_exited", "hs_v2_cumulative_time_in"]
    
    # Contadores para el reporte
    matches = 0
    mismatches = 0

    print(f"\n{'NOMBRE CRUDO (HubSpot)':<45} | {'NOMBRE GENERADO (Python)':<45} | {'ESTADO'}")
    print("-" * 110)

    for pipe in pipelines:
        for stage in pipe.get('stages', []):
            s_id = stage['id'].replace("-", "_")
            s_lbl = stage['label']
            
            # --- DEBUG ESPECÍFICO DE ETIQUETAS ---
            # Esto nos dirá si HubSpot tiene un espacio oculto, ej: "Visita previa "
            print(f"[DEBUG STAGE] ID: {s_id:<15} | Label: '{s_lbl}'") 

            for prop in all_props:
                for pre in prefixes:
                    if prop.startswith(pre) and s_id in prop:
                        # Esta es la lógica EXACTA de tu ETL
                        generated_name = normalize_name(f"{pre}_{s_lbl}")
                        
                        # Verificamos si coincide con lo que escribimos en SQL
                        status = "✅ OK"
                        if generated_name in EXPECTED_COLUMNS:
                            matches += 1
                        else:
                            status = "⚠️ NUEVA/DIFERENTE"
                            mismatches += 1
                        
                        print(f"{prop:<45} | {generated_name:<45} | {status}")
                        mapping[prop] = generated_name

    print("\n" + "="*50)
    print(" RESUMEN DEL DEBUG")
    print("="*50)
    print(f"Total Columnas Mapeadas : {len(mapping)}")
    print(f"Coincidencias (Van a tu tabla manual) : {matches}")
    print(f"Diferencias (Crean columnas nuevas)   : {mismatches}")
    
    if mismatches > 0:
        print("\n⚠️ CONCLUSIÓN:")
        print("El código está generando nombres que NO coinciden con tu 'initialize_db_schema'.")
        print("Por favor revisa la lista de arriba marcada con ⚠️ y actualiza tu código SQL")
        print("para que use exactamente el 'NOMBRE GENERADO'.")

if __name__ == "__main__":
    debug_mapping_logic()