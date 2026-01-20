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

# --- VALIDACIÓN DE VARIABLES DE ENTORNO ---
def validate_env():
    """
    Valida que todas las variables de entorno críticas estén configuradas.
    Falla rápido si falta alguna, evitando errores tardíos en el proceso.
    """
    required_vars = {
        "ACCESS_TOKEN": "Token de acceso a la API de HubSpot",
        "DB_HOST": "Host de la base de datos PostgreSQL",
        "DB_NAME": "Nombre de la base de datos",
        "DB_USER": "Usuario de la base de datos",
        "DB_PASS": "Contraseña de la base de datos"
    }
    
    missing = []
    for var, description in required_vars.items():
        value = os.getenv(var)
        if not value or value.strip() == "":
            missing.append(f"{var} ({description})")
    
    if missing:
        error_msg = "❌ Variables de entorno faltantes o vacías:\n"
        for var in missing:
            error_msg += f"   • {var}\n"
        error_msg += "\n💡 Verifica tu archivo .env"
        raise EnvironmentError(error_msg)
    
    print("✅ Variables de entorno validadas")

# --- CONFIGURACIÓN ---
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# --- CONFIGURACIÓN DE TIMEZONE ---
import pytz
BOGOTA_TZ = pytz.timezone('America/Bogota')  # UTC-5

OBJECT_TYPE = "services"
DB_SCHEMA = "hubspot_etl"      
TABLE_NAME = "services"   

OUTPUT_FOLDER = "exports"
LOG_FILE = "etl_errors.log"

# Nombre dinámico del reporte
report_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
REPORT_FILE = os.path.join(OUTPUT_FOLDER, f"etl_health_report_{report_timestamp}.txt")

# --- CONFIGURACIÓN DE LOGGING POR NIVELES ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
VALID_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Validar nivel de log
if LOG_LEVEL not in VALID_LEVELS:
    print(f"⚠️ LOG_LEVEL inválido '{LOG_LEVEL}', usando 'INFO'")
    LOG_LEVEL = "INFO"

# Convertir string a constante de logging
NUMERIC_LEVEL = getattr(logging, LOG_LEVEL, logging.INFO)

# Configurar logging a archivo
logging.basicConfig(
    filename=LOG_FILE,
    level=NUMERIC_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)

# Agregar handler para consola (opcional, solo para DEBUG)
if LOG_LEVEL == "DEBUG":
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    logging.getLogger().addHandler(console_handler)
    print(f"🔍 Modo DEBUG activado - Logs detallados en consola y archivo")
else:
    print(f"📊 Nivel de log configurado: {LOG_LEVEL}")

headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

# --- FUNCIONES AUXILIARES DE LOGGING ---
def log_debug(message):
    """Log solo en modo DEBUG"""
    if LOG_LEVEL == "DEBUG":
        logging.debug(message)

def log_info(message):
    """Log en modo INFO y superior"""
    logging.info(message)

def log_warning(message):
    """Log siempre (WARNING y superior)"""
    logging.warning(message)

def log_error(message):
    """Log siempre (ERROR y superior)"""
    logging.error(message)

def log_critical(message):
    """Log siempre (CRITICAL)"""
    logging.critical(message)

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
            'db_insert_errors': 0,
            'pipelines_loaded': 0,
            'stages_loaded': 0,
            'association_tables_created': 0
        }
        self.null_stats = {}
        self.association_tables = []  # Lista de nombres de tablas de asociaciones creadas
        self.truncated_columns = []   # Lista de (original, truncado) para columnas truncadas

    def increment(self, metric, count=1):
        if metric in self.metrics:
            self.metrics[metric] += count
    
    def set_metric(self, metric, value):
        if metric in self.metrics:
            self.metrics[metric] = value
    
    def add_association_table(self, table_name):
        """Registra una tabla de asociaciones creada"""
        if table_name not in self.association_tables:
            self.association_tables.append(table_name)
            self.metrics['association_tables_created'] += 1
    
    def add_truncated_column(self, original, truncated):
        """Registra una columna que fue truncada"""
        # Evitar duplicados (puede ocurrir en múltiples lotes)
        truncation = (original, truncated)
        if truncation not in self.truncated_columns:
            self.truncated_columns.append(truncation)
    
    def _format_association_tables(self):
        """Formatea la lista de tablas de asociaciones para el reporte"""
        if not self.association_tables:
            return "   - No se crearon tablas de asociaciones\n"
        
        tables_str = "   Tablas:\n"
        for table in sorted(self.association_tables):
            tables_str += f"      • {table}\n"
        return tables_str
    
    def _format_truncated_columns(self, max_display=10):
        """Formatea la lista de columnas truncadas para el reporte"""
        if not self.truncated_columns:
            return ""
        
        total = len(self.truncated_columns)
        display_count = min(total, max_display)
        
        # Ordenar por nombre original para facilitar búsqueda
        sorted_truncations = sorted(self.truncated_columns, key=lambda x: x[0])
        
        truncations_str = f"\n   [Columnas Truncadas - Mostrando {display_count} de {total}]\n"
        for original, truncated in sorted_truncations[:display_count]:
            # Mostrar solo los primeros 70 chars del original
            orig_display = original if len(original) <= 70 else original[:67] + "..."
            truncations_str += f"   • {orig_display}\n"
            truncations_str += f"     → {truncated}\n"
        
        if total > max_display:
            truncations_str += f"   ... y {total - max_display} más (ver logs para lista completa)\n"
        
        return truncations_str

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

3. METADATA DE PIPELINES
---------------------------------------
   - Pipelines Cargados    : {m['pipelines_loaded']}
   - Stages Cargados       : {m['stages_loaded']}

4. METADATA DE ASOCIACIONES
---------------------------------------
   - Tablas Creadas        : {m['association_tables_created']}
   - Asociaciones Totales  : {m['associations_found']}
{self._format_association_tables()}
5. INTEGRIDAD & CALIDAD
-----------------------------------------------
   - Schema Changes        : {m['schema_changes']}
   - Cols Truncadas        : {m['columns_truncated']}
{self._format_truncated_columns()}
   {nulls_report}
==================================================
"""
        print(report)
        try:
            with open(REPORT_FILE, "w", encoding="utf-8") as f:
                f.write(report)
            print(f"📄 Reporte guardado en: {REPORT_FILE}")
            
            # Guardar lista completa de columnas truncadas en log si hay muchas
            if len(self.truncated_columns) > 10:
                log_info(f"Lista completa de {len(self.truncated_columns)} columnas truncadas:")
                for original, truncated in sorted(self.truncated_columns):
                    log_info(f"  '{original}' → '{truncated}'")
                    
        except Exception as e:
            log_error(f"Error guardando reporte: {e}")

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

def sync_db_schema(engine, df, table_name, schema, prop_types=None, column_mapping=None):
    """
    Sincroniza el esquema de la BD con las columnas del DataFrame.
    
    Args:
        column_mapping: dict que mapea nombre_final_columna -> nombre_original_hubspot
    """
    inspector = inspect(engine)
    if not inspector.has_table(table_name, schema=schema): return

    existing_cols = [c['name'] for c in inspector.get_columns(table_name, schema=schema)]
    new_cols = set(df.columns) - set(existing_cols)
    
    if new_cols:
        print(f"⚠️ Schema Evolution: Creando {len(new_cols)} columnas nuevas...")
        with engine.begin() as conn:
            for col in new_cols:
                dtype = df[col].dtype
                
                # Intentar obtener el nombre original de HubSpot usando el mapeo
                original_col_name = column_mapping.get(col) if column_mapping else None
                
                # Intentar usar el tipo de HubSpot si está disponible
                if prop_types and original_col_name and original_col_name in prop_types:
                    hubspot_type = prop_types[original_col_name]
                    pg_type = get_postgres_type_from_hubspot(hubspot_type, dtype)
                    log_debug(f"Columna '{col}' (original: '{original_col_name}'): tipo HubSpot '{hubspot_type}' → PostgreSQL '{pg_type}'")
                else:
                    if not column_mapping:
                        log_warning(f"No column mapping provided for '{col}'")
                    elif not original_col_name:
                        log_warning(f"No original name found in mapping for '{col}'")
                    else:
                        log_warning(f"No hubspot type found for original column '{original_col_name}'")
                    
                    log_warning(f"Using pandas fallback for column: {col}")
                    # Fallback: inferir desde pandas (comportamiento original)
                    pg_type = "TEXT"
                    if pd.api.types.is_integer_dtype(dtype): pg_type = "BIGINT"
                    elif pd.api.types.is_float_dtype(dtype): pg_type = "NUMERIC"
                    elif pd.api.types.is_bool_dtype(dtype): pg_type = "BOOLEAN"
                    elif pd.api.types.is_datetime64_any_dtype(dtype): pg_type = "TIMESTAMPTZ"
                
                # Evitamos error si la columna ya existe por concurrencia
                try:
                    conn.execute(text(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{col}" {pg_type}'))
                    monitor.increment('schema_changes')
                    log_info(f"Columna creada: {col} ({pg_type})")
                except Exception as e:
                    log_warning(f"Error al crear columna {col} (puede que ya exista): {e}")

def initialize_db_schema(engine):
    """
    Crea el Schema (si no existe) y la Tabla inicial con campos core.
    """
    create_schema_sql = text(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    
    # Tabla base mínima necesaria
    ddl_query = text(f"""
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{TABLE_NAME} (
        "hs_object_id" BIGINT PRIMARY KEY,
        "hs_lastmodifieddate" TIMESTAMPTZ,
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


# --- FUNCIONES AUXILIARES ETL ---
def safe_request(method, url, **kwargs):
    max_retries = 3; backoff = 5
    monitor.increment('api_calls')
    for attempt in range(1, max_retries + 1):
        try:
            # Importante: para GET usamos params, para POST usamos json
            res = requests.request(method, url, headers=headers, **kwargs)
            
            if res.status_code == 200: 
                return res
            
            # Manejo específico de errores con detalles
            elif res.status_code == 400:
                error_detail = ""
                try:
                    error_json = res.json()
                    error_detail = json.dumps(error_json, indent=2)
                except:
                    error_detail = res.text
                
                # Log detallado del error
                logging.error(f"❌ ERROR 400 Bad Request en {url}")
                logging.error(f"   Método: {method}")
                logging.error(f"   Parámetros enviados: {kwargs}")
                logging.error(f"   Respuesta de HubSpot:\n{error_detail}")
                
                raise Exception(
                    f"Bad Request (400) en {url}\n"
                    f"Detalles: {error_detail[:500]}..."
                )
            
            elif res.status_code == 414:
                raise Exception("URL Too Long (414). Demasiadas propiedades solicitadas en GET.")
            
            elif res.status_code == 429:
                monitor.increment('retries_429')
                time.sleep(10)
            
            elif 500 <= res.status_code < 600:
                monitor.increment('retries_5xx')
                time.sleep(backoff * attempt)
            
            else: 
                res.raise_for_status()
                
        except requests.exceptions.ConnectionError:
            monitor.increment('connection_errors')
            time.sleep(backoff * attempt)
        except Exception as e: 
            raise e
            
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
            # Registrar qué columna fue truncada
            monitor.add_truncated_column(col, sanitized)
        
        if sanitized in seen:
            seen[sanitized] += 1
            suffix = f"_{seen[sanitized]}"
            sanitized = f"{sanitized[:63-len(suffix)]}{suffix}"
        else: seen[sanitized] = 1
        new_cols.append(sanitized)
    df.columns = new_cols
    return df

def get_initials(text):
    """
    Obtener iniciales de un texto, se usa para reducir el largo de los nombres de las columnas
    de pipelines.
    """
    if not text:
        return ""
    # Primero normalizamos para quitar acentos y caracteres raros
    normalized = normalize_name(text)
    # Dividimos por guiones bajos
    parts = [p for p in normalized.split("_") if p]
    # Tomamos la primera letra de cada palabra y las unimos con '_'
    initials = "_".join([p[0] for p in parts])
    return initials

def get_smart_mapping(all_props):
    """
    Obtiene el mapeo de las columnas de las propiedades de los pipelines.
    Se usa para reducir el largo de los nombres de las columnas de las propiedades de los pipelines.
    """
    try:
        url = f"https://api.hubapi.com/crm/v3/pipelines/{OBJECT_TYPE}"
        res = safe_request('GET', url)
        pipelines = res.json().get('results', [])
    except Exception: return {}

    mapping = {}
    # Prefijos acortados sin _v2, para ganar más espacio
    prefix_map = {
        "hs_v2_cumulative_time_in": "hs_cumulative_time_in",
        "hs_v2_date_entered": "hs_date_entered",
        "hs_v2_date_exited": "hs_date_exited",
        "hs_v2_latest_time_in": "hs_latest_time_in",
        "hs_time_in": "hs_time_in",
        "hs_date_exited": "hs_date_exited",
        "hs_date_entered": "hs_date_entered"
    }
    
    for pipe in pipelines:
        #Generamos el acrónimo del Pipeline
        p_initials = get_initials(pipe['label']) 

        for stage in pipe.get('stages', []):
            s_id = stage['id']
            s_id_normalized = s_id.replace("-", "_")
            s_lbl = stage['label'] 
            
            for prop in all_props:
                for pre, short_pre in prefix_map.items():
                    if prop.startswith(pre) and (s_id in prop or s_id_normalized in prop):
                        new_name = f"{short_pre}_{s_lbl}_in_{p_initials}_pipeline"
                        mapping[prop] = normalize_name(new_name)
                        
    return mapping

"""
def get_smart_mapping(all_props):
    # Intentamos obtener pipelines para mapeo inteligente de stages
    try:
        url = f"https://api.hubapi.com/crm/v3/pipelines/{OBJECT_TYPE}"
        res = safe_request('GET', url)
        pipelines = res.json().get('results', [])
    except Exception: return {}

    mapping = {}
    prefixes = ["hs_v2_latest_time_in", "hs_v2_date_entered", "hs_v2_date_exited", "hs_v2_cumulative_time_in", "hs_time_in", "hs_date_exited", "hs_date_entered"]
    
    for pipe in pipelines:
        p_lbl = pipe['label']
        for stage in pipe.get('stages', []):
            s_id = stage['id']  # ← Mantener el ID original con guiones
            s_id_normalized = s_id.replace("-", "_")  # ← Version con underscores
            s_lbl = stage['label']
            
            for prop in all_props:
                for pre in prefixes:
                    # Buscar tanto con guiones como con underscores
                    if prop.startswith(pre) and (s_id in prop or s_id_normalized in prop):
                        # Mapeamos IDs técnicos a Labels legibles
                        mapping[prop] = normalize_name(f"{pre}_{s_lbl}_{p_lbl}")
    return mapping
"""

def get_assocs():
    # Obtiene asociaciones disponibles
    url = f"https://api.hubapi.com/crm/v3/schemas/{OBJECT_TYPE}"
    res = safe_request('GET', url)
    all_assocs = [a['toObjectTypeId'] for a in res.json().get('associations', [])]
    
    # Retornar todas las asociaciones (se procesarán en chunks después)
    if len(all_assocs) > 30:
        log_info(f"ℹ️ Objeto tiene {len(all_assocs)} asociaciones. Se procesarán en chunks de 30.")
        print(f"ℹ️ Se encontraron {len(all_assocs)} asociaciones. Se procesarán en múltiples llamadas (límite API: 30)")
    
    return all_assocs

def fetch_and_transform_pipelines():
    """
    Extrae la información de pipelines con sus stages y la convierte en DataFrames.
    Retorna: (df_pipelines, df_stages) o (None, None) si no hay pipelines disponibles.
    
    Referencia: https://developers.hubspot.com/docs/api-reference/crm-pipelines-v3/guide
    """
    try:
        url = f"https://api.hubapi.com/crm/v3/pipelines/{OBJECT_TYPE}"
        res = safe_request('GET', url)
        pipelines = res.json().get('results', [])
        
        if not pipelines:
            log_info(f"No hay pipelines para el objeto '{OBJECT_TYPE}'")
            return None, None
        
        # Transformar pipelines a lista de diccionarios
        pipeline_data = []
        stage_data = []
        synced_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        
        for pipe in pipelines:
            # Información del pipeline
            pipeline_row = {
                'pipeline_id': pipe.get('id'),
                'label': pipe.get('label'),
                'display_order': pipe.get('displayOrder', 1),
                'created_at': pipe.get('createdAt'),
                'updated_at': pipe.get('updatedAt'),
                'archived': pipe.get('archived', False),
                'fivetran_deleted': False,
                'fivetran_synced': synced_at
            }
            pipeline_data.append(pipeline_row)
            
            # Información de los stages de este pipeline
            stages = pipe.get('stages', [])
            for stage in stages:
                stage_row = {
                    'stage_id': stage.get('id'),
                    'pipeline_id': pipe.get('id'),
                    'label': stage.get('label'),
                    'display_order': stage.get('displayOrder', 0),
                    'created_at': stage.get('createdAt'),
                    'updated_at': stage.get('updatedAt'),
                    'archived': stage.get('archived', False),
                    'fivetran_deleted': False,
                    'fivetran_synced': synced_at
                }
                
                # Metadata puede variar según el tipo de objeto
                # Extraer 'state' o 'leadState' si existe
                metadata = stage.get('metadata', {})
                if metadata:
                    # Buscar state o leadState en el metadata
                    state_value = metadata.get('state') or metadata.get('leadState')
                    if state_value:
                        stage_row['state'] = state_value
                
                stage_data.append(stage_row)
        
        # Crear DataFrames
        df_pipelines = pd.DataFrame(pipeline_data)
        df_stages = pd.DataFrame(stage_data)
        
        # Convertir fechas de pipelines a timezone Bogotá
        for df in [df_pipelines, df_stages]:
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
                df['created_at'] = df['created_at'].dt.tz_convert(BOGOTA_TZ)
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce', utc=True)
                df['updated_at'] = df['updated_at'].dt.tz_convert(BOGOTA_TZ)
        
        log_info(f"Pipelines extraídos: {len(df_pipelines)} pipelines, {len(df_stages)} stages")
        return df_pipelines, df_stages
        
    except Exception as e:
        log_warning(f"No se pudieron obtener pipelines para '{OBJECT_TYPE}': {e}")
        return None, None

def load_pipelines_to_db(engine, df_pipelines, df_stages):
    """
    Carga la información de pipelines y stages a tablas separadas.
    Usa estrategia TRUNCATE + INSERT para mantener sincronizado.
    
    Esto asegura que:
    - Si se elimina un pipeline/stage en HubSpot, desaparecerá de la tabla
    - No hay conflictos de duplicados
    - La metadata siempre está actualizada
    """
    if df_pipelines is None or df_pipelines.empty:
        log_info("No hay pipelines para cargar a BD")
        return
    
    pipeline_table_name = f"{TABLE_NAME}_pipelines"
    stages_table_name = f"{TABLE_NAME}_pipeline_stages"
    
    try:
        with engine.begin() as conn:
            # 1. Crear tabla de pipelines si no existe
            create_pipeline_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{pipeline_table_name} (
                "pipeline_id" TEXT PRIMARY KEY,
                "label" TEXT,
                "display_order" INTEGER,
                "created_at" TIMESTAMPTZ,
                "updated_at" TIMESTAMPTZ,
                "archived" BOOLEAN,
                "fivetran_deleted" BOOLEAN,
                "fivetran_synced" TIMESTAMP
            );
            """)
            conn.execute(create_pipeline_table_sql)
            
            # 2. Crear tabla de stages si no existe
            create_stages_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{stages_table_name} (
                "stage_id" TEXT PRIMARY KEY,
                "pipeline_id" TEXT,
                "label" TEXT,
                "display_order" INTEGER,
                "created_at" TIMESTAMPTZ,
                "updated_at" TIMESTAMPTZ,
                "archived" BOOLEAN,
                "state" TEXT,
                "fivetran_deleted" BOOLEAN,
                "fivetran_synced" TIMESTAMP,
                FOREIGN KEY ("pipeline_id") REFERENCES {DB_SCHEMA}.{pipeline_table_name}("pipeline_id")
            );
            """)
            conn.execute(create_stages_table_sql)
            
            # 3. Truncar tablas (eliminar registros viejos)
            # Orden importante: primero stages (tiene FK), luego pipelines
            truncate_stages_sql = text(f'TRUNCATE TABLE {DB_SCHEMA}.{stages_table_name} CASCADE')
            truncate_pipeline_sql = text(f'TRUNCATE TABLE {DB_SCHEMA}.{pipeline_table_name} CASCADE')
            conn.execute(truncate_stages_sql)
            conn.execute(truncate_pipeline_sql)
            
            # 4. Insertar pipelines
            df_pipelines.to_sql(
                pipeline_table_name,
                con=conn,
                schema=DB_SCHEMA,
                if_exists='append',
                index=False
            )
            monitor.set_metric('pipelines_loaded', len(df_pipelines))
            
            # 5. Insertar stages
            if df_stages is not None and not df_stages.empty:
                df_stages.to_sql(
                    stages_table_name,
                    con=conn,
                    schema=DB_SCHEMA,
                    if_exists='append',
                    index=False
                )
                monitor.set_metric('stages_loaded', len(df_stages))
            
        print(f"✅ Tabla '{pipeline_table_name}' actualizada con {len(df_pipelines)} pipelines")
        if df_stages is not None and not df_stages.empty:
            print(f"✅ Tabla '{stages_table_name}' actualizada con {len(df_stages)} stages")
        
        log_info(f"Pipelines cargados a '{DB_SCHEMA}.{pipeline_table_name}': {len(df_pipelines)} registros")
        if df_stages is not None:
            log_info(f"Stages cargados a '{DB_SCHEMA}.{stages_table_name}': {len(df_stages)} registros")
        
    except Exception as e:
        log_error(f"Error cargando pipelines/stages a BD: {e}")
        print(f"⚠️ No se pudieron cargar pipelines/stages a BD: {e}")
        raise e

def extract_normalized_associations(batch_records):
    """
    Extrae las asociaciones de los registros y las normaliza.
    Retorna un diccionario: {to_object_type: [lista de filas]}
    
    Solo incluye tipos de asociación que tienen datos reales.
    
    Ejemplo de retorno:
    {
        'companies': [
            {'services_id': 123, 'companies_id': 456, 'type_id': 5, 'category': 'HUBSPOT_DEFINED'},
            {'services_id': 123, 'companies_id': 789, 'type_id': 341, 'category': 'USER_DEFINED'},
        ],
        'contacts': [...]
    }
    """
    synced_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    associations_by_type = {}
    
    for record in batch_records:
        from_id = int(record['id'])
        raw_assoc = record.get('associations', {})
        
        if not raw_assoc or not isinstance(raw_assoc, dict):
            continue
        
        # raw_assoc tiene estructura: {'companies': {'results': [...]}, 'contacts': {...}}
        for to_object_type, assoc_data in raw_assoc.items():
            if not isinstance(assoc_data, dict):
                continue
            
            results = assoc_data.get('results', [])
            
            for assoc_item in results:
                if 'id' not in assoc_item:
                    continue
                
                # Extraer información de la asociación
                # Usar nombres en plural para consistencia
                row = {
                    f'{TABLE_NAME}_id': from_id,  # ej: 'services_id', 'deals_id'
                    f'{to_object_type}_id': int(assoc_item['id']),  # ej: 'companies_id', 'contacts_id'
                    'type_id': assoc_item.get('type'),  # El typeId numérico
                    'category': assoc_item.get('category', 'HUBSPOT_DEFINED'),
                    'fivetran_synced': synced_at
                }
                
                # Agregar a la lista correspondiente
                if to_object_type not in associations_by_type:
                    associations_by_type[to_object_type] = []
                
                associations_by_type[to_object_type].append(row)
                monitor.increment('associations_found')
    
    # Convertir a DataFrames solo si hay datos
    dataframes = {}
    for to_type, rows in associations_by_type.items():
        if rows:  # Solo crear DataFrame si hay asociaciones reales
            df = pd.DataFrame(rows)
            dataframes[to_type] = df
            log_info(f"Asociaciones extraídas hacia '{to_type}': {len(df)} registros")
    
    return dataframes

def load_associations_to_db(engine, associations_dataframes):
    """
    Carga asociaciones normalizadas en tablas relacionales.
    Cada tipo de asociación (ej: companies, contacts) va a su propia tabla.
    
    Solo crea tablas para asociaciones que realmente existen (tienen datos).
    Usa estrategia TRUNCATE + INSERT para mantener sincronizado.
    """
    if not associations_dataframes:
        log_info("No hay asociaciones para cargar")
        return
    
    from_object = TABLE_NAME  # ej: 'deals', 'services' (ya en plural)
    
    try:
        with engine.begin() as conn:
            for to_object_type, df_assoc in associations_dataframes.items():
                if df_assoc.empty:
                    continue
                
                # Nombre de tabla: {from_objeto}_{to_objeto} (ambos en plural)
                # Ejemplo: services_companies, deals_contacts
                assoc_table_name = f"{from_object}_{to_object_type}"
                
                # Definir nombres de columnas dinámicamente (en plural)
                from_col = f"{from_object}_id"
                to_col = f"{to_object_type}_id"
                
                # Crear tabla si no existe
                create_table_sql = text(f"""
                CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{assoc_table_name} (
                    "{from_col}" BIGINT NOT NULL,
                    "{to_col}" BIGINT NOT NULL,
                    "type_id" TEXT,
                    "category" TEXT,
                    "fivetran_synced" TIMESTAMP,
                    PRIMARY KEY ("{from_col}", "{to_col}", "type_id")
                );
                """)
                conn.execute(create_table_sql)
                
                # Truncar tabla (eliminar registros viejos)
                truncate_sql = text(f'TRUNCATE TABLE {DB_SCHEMA}.{assoc_table_name}')
                conn.execute(truncate_sql)
                
                # Insertar nuevos datos
                df_assoc.to_sql(
                    assoc_table_name,
                    con=conn,
                    schema=DB_SCHEMA,
                    if_exists='append',
                    index=False
                )
                
                # Registrar tabla en el monitor
                monitor.add_association_table(assoc_table_name)
                
                print(f"✅ Tabla '{assoc_table_name}' actualizada con {len(df_assoc)} asociaciones")
                log_info(f"Asociaciones cargadas a '{DB_SCHEMA}.{assoc_table_name}': {len(df_assoc)} registros")
        
    except Exception as e:
        log_error(f"Error cargando asociaciones a BD: {e}")
        print(f"⚠️ Error cargando asociaciones: {e}")
        raise e

# --- NUEVAS FUNCIONES PARA GET / LIST ---

def get_properties_with_types():
    """
    Obtiene todas las propiedades con sus tipos de dato desde HubSpot.
    Retorna: (lista de nombres, diccionario de tipos)
    """
    url = f"https://api.hubapi.com/crm/v3/properties/{OBJECT_TYPE}"
    res = safe_request('GET', url)
    properties_data = res.json()['results']
    
    # Extraer nombres y crear mapeo de tipos
    prop_names = []
    prop_types = {}
    
    for prop in properties_data:
        name = prop['name']
        hubspot_type = prop.get('type', 'string')  # Default a string si no viene
        
        prop_names.append(name)
        prop_types[name] = hubspot_type
    
    log_debug(f"Tipos de datos capturados para {len(prop_types)} propiedades")
    return prop_names, prop_types

def convert_hubspot_types_to_pandas(df, prop_types):
    """
    Convierte columnas del DataFrame según los tipos declarados por HubSpot.
    Esto asegura que los datos tengan el tipo correcto antes de subirlos a PostgreSQL.
    """
    conversions_applied = 0
    conversions_failed = 0
    
    for col in df.columns:
        # Saltar columnas de metadata que agregamos nosotros
        if col in ['hs_object_id', 'fivetran_synced', 'fivetran_deleted']:
            continue
        
        # Saltar columnas de asociaciones
        if col.startswith('asoc_'):
            continue
        
        # Buscar el tipo en el diccionario (puede no estar si es columna generada)
        hubspot_type = prop_types.get(col)
        if not hubspot_type:
            log_debug(f"No hubspot type found for column: {col}")
            continue
        
        try:
            # Mapeo de tipos HubSpot -> Pandas
            if hubspot_type == 'number':
                # Intentar convertir a numérico
                df[col] = pd.to_numeric(df[col], errors='coerce')
                conversions_applied += 1
                
            elif hubspot_type in ['date', 'datetime']:
                # Convertir a datetime con timezone de Bogotá
                # Primero a UTC, luego convertir a Bogotá
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
                df[col] = df[col].dt.tz_convert(BOGOTA_TZ)
                conversions_applied += 1
                
            elif hubspot_type == 'bool':
                # Convertir a booleano
                # HubSpot envía "true"/"false" como strings
                df[col] = df[col].map({'true': True, 'false': False, '': None, None: None})
                conversions_applied += 1
                
            # Los tipos 'string', 'enumeration', 'phone_number' se quedan como object (texto)
            # No necesitan conversión explícita
            
        except Exception as e:
            conversions_failed += 1
            log_warning(f"No se pudo convertir columna '{col}' de tipo '{hubspot_type}': {e}")
    
    if conversions_applied > 0:
        log_info(f"Conversiones de tipo aplicadas: {conversions_applied} columnas (Fallos: {conversions_failed})")
    
    return df

def get_postgres_type_from_hubspot(hubspot_type, pandas_dtype=None):
    """
    Mapea un tipo de HubSpot a un tipo de PostgreSQL.
    Puede usar el dtype de pandas como fallback para mayor precisión.
    """
    # Mapeo directo de tipos conocidos de HubSpot
    type_mapping = {
        'string': 'TEXT',
        'number': 'NUMERIC',
        'date': 'TIMESTAMPTZ',
        'datetime': 'TIMESTAMPTZ',
        'bool': 'BOOLEAN',
        'enumeration': 'TEXT',
        'phone_number': 'TEXT',
    }
    
    pg_type = type_mapping.get(hubspot_type)
    
    # Si no hay mapeo directo, usar inferencia de pandas
    if not pg_type and pandas_dtype:
        log_warning(f"No postgres type found for hubspot type '{hubspot_type}', using fallback")
        if pd.api.types.is_integer_dtype(pandas_dtype): 
            pg_type = "BIGINT"
        elif pd.api.types.is_float_dtype(pandas_dtype): 
            pg_type = "NUMERIC"
        elif pd.api.types.is_bool_dtype(pandas_dtype): 
            pg_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype): 
            pg_type = "TIMESTAMPTZ"
        else:
            pg_type = "TEXT"
    
    return pg_type or "TEXT"

def fetch_all_records_with_chunked_assocs(properties, associations):
    """
    Descarga todos los registros manejando el límite de 30 asociaciones por request.
    Si hay más de 30 asociaciones, hace múltiples llamadas y combina los resultados.
    """
    chunk_size = 30
    
    # Si hay 30 o menos asociaciones, usar el flujo normal
    if len(associations) <= chunk_size:
        for batch in fetch_hubspot_records_generator(properties, associations):
            yield batch
        return
    
    # Dividir asociaciones en chunks
    assoc_chunks = [associations[i:i + chunk_size] for i in range(0, len(associations), chunk_size)]
    
    print(f"\n🔄 Dividiendo {len(associations)} asociaciones en {len(assoc_chunks)} llamadas API")
    log_info(f"Procesando asociaciones en {len(assoc_chunks)} chunks de máximo {chunk_size}")
    
    # Diccionario para almacenar y combinar registros por ID
    all_records_dict = {}
    
    for chunk_idx, assoc_chunk in enumerate(assoc_chunks):
        print(f"\n📡 Procesando chunk {chunk_idx + 1}/{len(assoc_chunks)} de asociaciones...")
        print(f"   Asociaciones en este chunk: {len(assoc_chunk)}")
        
        # Descargar registros con este chunk de asociaciones
        for batch in fetch_hubspot_records_generator(properties, assoc_chunk):
            for record in batch:
                record_id = record['id']
                
                # Si es la primera vez que vemos este registro
                if record_id not in all_records_dict:
                    all_records_dict[record_id] = record
                else:
                    # Combinar asociaciones de múltiples llamadas
                    existing_record = all_records_dict[record_id]
                    existing_assocs = existing_record.get('associations', {})
                    new_assocs = record.get('associations', {})
                    
                    # Merge de asociaciones
                    for assoc_type, assoc_data in new_assocs.items():
                        if assoc_type not in existing_assocs:
                            existing_assocs[assoc_type] = assoc_data
                        else:
                            # Combinar results evitando duplicados
                            existing_results = existing_assocs[assoc_type].get('results', [])
                            new_results = assoc_data.get('results', [])
                            
                            # Crear set de IDs existentes para evitar duplicados
                            existing_ids = {r['id'] for r in existing_results if 'id' in r}
                            
                            # Agregar solo los nuevos que no existen
                            for new_result in new_results:
                                if 'id' in new_result and new_result['id'] not in existing_ids:
                                    existing_results.append(new_result)
                            
                            existing_assocs[assoc_type]['results'] = existing_results
                    
                    existing_record['associations'] = existing_assocs
        
        print(f"   ✓ Chunk {chunk_idx + 1} completado. Total registros únicos: {len(all_records_dict)}")
    
    # Convertir diccionario a lista y entregar en batches de 100
    print(f"\n✅ Todas las asociaciones procesadas. Entregando {len(all_records_dict)} registros únicos...")
    
    all_records_list = list(all_records_dict.values())
    batch_size = 100
    
    for i in range(0, len(all_records_list), batch_size):
        batch = all_records_list[i:i + batch_size]
        yield batch

def fetch_hubspot_records_generator(properties, associations):
    """
    Generador que usa el endpoint LIST (GET) y devuelve lotes de resultados.
    Maneja la paginación automáticamente.
    """
    url = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}"
    
    # Preparamos los parámetros base
    properties_str = ",".join(properties)
    associations_str = ",".join(associations) if associations else ""
    
    # 🔍 DEBUG: Validación detallada de parámetros
    print(f"\n{'='*60}")
    print(f"🔍 DEBUG - Parámetros de Request")
    print(f"{'='*60}")
    print(f"Object Type      : {OBJECT_TYPE}")
    print(f"URL Base         : {url}")
    print(f"Total Properties : {len(properties)}")
    print(f"Properties Length: {len(properties_str)} chars")
    print(f"Total Assocs     : {len(associations)}")
    print(f"Assocs Length    : {len(associations_str)} chars")
    
    # Mostrar primeras propiedades como muestra
    if properties:
        print(f"\nPrimeras 5 propiedades:")
        for prop in properties[:5]:
            print(f"  - {prop}")
        if len(properties) > 5:
            print(f"  ... y {len(properties) - 5} más")
    
    # Mostrar asociaciones
    if associations:
        print(f"\nAsociaciones solicitadas:")
        for assoc in associations:
            print(f"  - {assoc}")
    else:
        print(f"\n⚠️ No se solicitaron asociaciones")
    
    # Validación de longitud total
    estimated_url_length = len(url) + len(properties_str) + len(associations_str) + 100
    print(f"\nLongitud URL estimada: {estimated_url_length} chars")
    
    if estimated_url_length > 8000:
        warning_msg = (
            f"⚠️ URL demasiado larga ({estimated_url_length} chars).\n"
            f"   Límite recomendado: 8000 chars\n"
            f"   Propiedades: {len(properties)} ({len(properties_str)} chars)\n"
            f"   Considera reducir propiedades o usar POST/Search endpoint."
        )
        log_warning(warning_msg)
        print(warning_msg)
    elif estimated_url_length > 6000:
        warning_msg = f"⚠️ Advertencia: URL larga ({estimated_url_length} chars). Podría fallar."
        log_warning(warning_msg)
        print(warning_msg)
    else:
        print(f"✓ Longitud URL aceptable")
    
    print(f"{'='*60}\n")
    
    params = {
        "limit": 100,
        "properties": properties_str,
        "associations": associations_str,
        "archived": "false"
    }
    
    print(f"📡 Iniciando descarga con GET...")
    
    after = None
    while True:
        if after:
            params['after'] = after
        
        try:
            res = safe_request('GET', url, params=params)
            data = res.json()
            results = data.get('results', [])
            
            if not results:
                break
                
            yield results
            
            paging = data.get('paging')
            if paging and 'next' in paging:
                after = paging['next']['after']
            else:
                break
                
        except Exception as e:
            # 🔍 DEBUG: Capturar y mostrar error detallado
            log_error(f"❌ Error en fetch_hubspot_records_generator")
            log_error(f"   After token: {after}")
            log_error(f"   Params keys: {list(params.keys())}")
            raise e

def process_batch(batch_records, col_map, prop_types=None):
    """
    Transforma una lista de diccionarios de HubSpot en un DataFrame limpio.
    Retorna: (DataFrame, dict de mapeo nombre_final -> nombre_original)
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
            
            # Las asociaciones se procesarán por separado y se normalizarán
            # en tablas relacionales (NO las guardamos como strings aquí)
            
            data_list.append(row)
            monitor.increment('records_processed_ok')
            
        except Exception as e:
            monitor.increment('records_failed')
            log_error(f"Error procesando registro {record.get('id')}: {e}")

    if not data_list:
        log_warning(f"Lote saltado: 0 registros procesados de {len(batch_records)} recibidos.")
        return pd.DataFrame(), {}

    # Creación y limpieza de DataFrame
    df = pd.DataFrame(data_list)
    
    # Rastrear mapeo de nombres: final → original
    # Esto permite que sync_db_schema encuentre los tipos correctos
    column_name_mapping = {}
    original_columns = df.columns.tolist()

    # 1. ⭐ Convertir tipos según HubSpot (con nombres originales)
    if prop_types:
        df = convert_hubspot_types_to_pandas(df, prop_types)

    # 2. Renombrar columnas inteligentes (stage names)
    df.rename(columns=col_map, inplace=True)
    
    # Rastrear el mapeo después del primer cambio
    for i, orig_col in enumerate(original_columns):
        new_col = df.columns[i]
        column_name_mapping[new_col] = orig_col

    # 3. Normalizar nombres (snake_case, sin acentos)
    normalized_cols = [normalize_name(c) for c in df.columns]
    
    # Actualizar mapeo: el nombre normalizado apunta al original
    updated_mapping = {}
    for i, norm_col in enumerate(normalized_cols):
        old_col = df.columns[i]
        original = column_name_mapping.get(old_col, old_col)
        updated_mapping[norm_col] = original
    
    df.columns = normalized_cols
    column_name_mapping = updated_mapping

    # 4. Sanitizar para Postgres (longitud y duplicados)
    df_before_sanitize = df.copy()
    df = sanitize_columns_for_postgres(df)
    
    # Actualizar mapeo después de sanitizar (truncado/duplicados)
    final_mapping = {}
    for i, final_col in enumerate(df.columns):
        original_from_before = column_name_mapping.get(df_before_sanitize.columns[i])
        if original_from_before:
            final_mapping[final_col] = original_from_before
    
    column_name_mapping = final_mapping

    # 5. Convertir dicts/lists a JSON string para evitar error en DB
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    return df, column_name_mapping
    

# --- PROCESO PRINCIPAL ---
def run_postgres_etl():
    log_info("\n" + "="*80 + "\n" + f"🚀 INICIANDO NUEVA CORRIDA ETL - OBJETO: {OBJECT_TYPE}" + "\n" + "="*80)
    try:
        # Validar variables de entorno críticas
        validate_env()
        log_info("✅ Validación de variables de entorno exitosa")
        
        ensure_exports_folder()
        engine = get_db_engine()
        print("✅ Motor de BD iniciado.")

        # 1. Preparar BD (Tabla inicial mínima)
        initialize_db_schema(engine) 

        # 2. Obtener Metadatos (Propiedades y Asociaciones)
        print("Obteniendo definición de propiedades...")
        all_props, prop_types = get_properties_with_types()
        
        print(f"✓ Propiedades obtenidas: {len(all_props)}")
        print(f"✓ Tipos de datos mapeados: {len(prop_types)}")
        
        print("Obteniendo asociaciones disponibles...")
        assocs = get_assocs()
        print(f"✓ Asociaciones disponibles: {len(assocs)}")
        
        # Validar que el objeto soporte asociaciones
        if not assocs:
            print(f"⚠️ Advertencia: Objeto '{OBJECT_TYPE}' no tiene asociaciones disponibles")
        
        # 2.5 Obtener y cargar información de Pipelines y Stages
        print("\n📊 Obteniendo información de pipelines...")
        df_pipelines, df_stages = fetch_and_transform_pipelines()
        if df_pipelines is not None:
            print(f"✓ Pipelines encontrados: {len(df_pipelines)}")
            if df_stages is not None:
                print(f"✓ Stages encontrados: {len(df_stages)}")
            load_pipelines_to_db(engine, df_pipelines, df_stages)
        else:
            print(f"ℹ️ Objeto '{OBJECT_TYPE}' no tiene pipelines o no están disponibles")
            log_info(f"Objeto '{OBJECT_TYPE}' no soporta pipelines o no hay pipelines configurados")
        
        col_map = get_smart_mapping(all_props)
        
        # 3. Flujo por Lotes (Generator -> Transform -> Load)
        print("🚀 Iniciando proceso ETL por lotes...")
        
        batch_count = 0
        total_records_processed = 0
        
        # Usar el generador con manejo de chunks de asociaciones
        for batch in fetch_all_records_with_chunked_assocs(all_props, assocs):
            monitor.metrics['records_fetched'] += len(batch)
            
            # A. Transformar (con tipos de HubSpot) - ahora retorna también el mapeo
            df_batch, column_mapping = process_batch(batch, col_map, prop_types)
            
            if df_batch.empty:
                continue

            # B. Evolución de Esquema (Checkea columnas nuevas en este lote, usando tipos y mapeo)
            sync_db_schema(engine, df_batch, TABLE_NAME, DB_SCHEMA, prop_types, column_mapping)
            
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
                    log_error(user_friendly_msg)
                    log_error(f"   Detalle técnico completo: {e}")
                
                # 2. Errores de Integridad (Duplicados, FKs, etc.)
                elif 'IntegrityError' in error_type or 'duplicate key' in error_msg.lower():
                    user_friendly_msg = (
                        f"⚠️ ERROR DE INTEGRIDAD - Lote {batch_count}\n"
                        f"   Problema: Violación de constraint de base de datos"
                    )
                    print(user_friendly_msg)
                    log_error(user_friendly_msg)
                    log_error(f"   Detalle técnico completo: {e}")
                
                # 3. Errores de Columna (nombre inválido, no existe, etc.)
                elif 'ProgrammingError' in error_type or 'column' in error_msg.lower():
                    user_friendly_msg = (
                        f"⚠️ ERROR DE ESQUEMA - Lote {batch_count}\n"
                        f"   Problema: Error en definición de columna o tabla"
                    )
                    print(user_friendly_msg)
                    log_error(user_friendly_msg)
                    log_error(f"   Detalle técnico completo: {e}")
                
                # 4. Error Genérico (mantener para otros casos)
                else:
                    user_friendly_msg = (
                        f"❌ ERROR DESCONOCIDO - Lote {batch_count}\n"
                        f"   Tipo: {error_type}"
                    )
                    print(user_friendly_msg)
                    log_error(user_friendly_msg)
                    log_error(f"   Detalle técnico completo: {e}")
            
            # D. Extraer y cargar asociaciones normalizadas
            try:
                associations_dfs = extract_normalized_associations(batch)
                if associations_dfs:
                    load_associations_to_db(engine, associations_dfs)
            except Exception as e:
                log_error(f"Error procesando asociaciones del lote {batch_count}: {e}")
                print(f"⚠️ Error en asociaciones del lote {batch_count}, continuando...")
            
            monitor.metrics['db_execution_time'] += (time.time() - db_start_time)
            monitor.record_null_stats(df_batch) # Stats por lote
            
            total_records_processed += len(df_batch)
            batch_count += 1
            print(f"   -> Lote {batch_count} procesado ({total_records_processed} total)...")

        print("✅ Proceso finalizado.")
        monitor.generate_report()
        log_info(f"✅ CORRIDA FINALIZADA EXITOSAMENTE - OBJETO: {OBJECT_TYPE}\n" + "-"*80 + "\n")

    except Exception as e:
        error_msg = f"💥 ERROR FATAL: {type(e).__name__} - {str(e)}"
        print(f"\n{error_msg}")
        print(f"📄 Ver detalles completos en: {LOG_FILE}")
        
        log_critical(f"❌ CORRIDA DETENIDA POR ERROR FATAL - OBJETO: {OBJECT_TYPE}")
        log_critical(f"   Tipo: {type(e).__name__}")
        log_critical(f"   Detalle: {e}", exc_info=True)
        log_critical("\n" + "-"*80 + "\n")
        
        # Re-lanzar la excepción para que sea manejada por __main__
        raise

if __name__ == "__main__":
    import sys
    
    try:
        print("=" * 60)
        print("  🚀 ETL HUBSPOT → POSTGRESQL")
        print(f"  📦 Objeto: {OBJECT_TYPE}")
        print(f"  🗄️  Destino: {DB_SCHEMA}.{TABLE_NAME}")
        print("=" * 60 + "\n")
        
        run_postgres_etl()
        
        print("\n" + "=" * 60)
        print("  🎉 ETL COMPLETADA EXITOSAMENTE")
        print("=" * 60)
        
        log_info("✅ Proceso ETL finalizado con exit code 0")
        sys.exit(0)
        
    except EnvironmentError as e:
        # Error de configuración (variables faltantes)
        print(f"\n💥 ERROR DE CONFIGURACIÓN:\n{e}\n")
        log_critical(f"Error de configuración: {e}")
        sys.exit(2)  # Exit code 2 para errores de configuración
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Proceso interrumpido por el usuario (Ctrl+C)")
        log_warning("Proceso interrumpido manualmente")
        sys.exit(130)  # Exit code estándar para SIGINT
        
    except Exception as e:
        print(f"\n💥 ERROR FATAL EN ETL:")
        print(f"   Tipo: {type(e).__name__}")
        print(f"   Mensaje: {str(e)}")
        print(f"\n📄 Ver detalles completos en: {LOG_FILE}\n")
        
        log_critical("=" * 80)
        log_critical(f"❌ ETL DETENIDO POR ERROR FATAL - OBJETO: {OBJECT_TYPE}")
        log_critical(f"   Tipo de error: {type(e).__name__}")
        log_critical(f"   Detalle: {e}")
        log_critical(f"   Traceback completo:", exc_info=True)
        log_critical("=" * 80)
        
        sys.exit(1)  # Exit code 1 para errores de ejecución