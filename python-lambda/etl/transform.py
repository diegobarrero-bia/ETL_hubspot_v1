"""Transformaciones de datos para el ETL HubSpot → PostgreSQL."""
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone

import pandas as pd

from etl.config import BOGOTA_TZ
from etl.monitor import ETLMonitor

logger = logging.getLogger(__name__)


# =====================================================================
# Funciones de normalización de nombres
# =====================================================================

def normalize_name(text: str) -> str:
    """
    Normaliza un nombre a formato válido para PostgreSQL:
    - minúsculas
    - sin acentos ni caracteres especiales
    - snake_case
    """
    if not isinstance(text, str):
        return str(text) if text is not None else ""
    text = text.lower()
    text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
    text = text.replace(" ", "_").replace("-", "_")
    text = re.sub(r'[^a-z0-9_]', '', text)
    return re.sub(r'_{2,}', '_', text).strip('_')


def sanitize_columns_for_postgres(df: pd.DataFrame, monitor: ETLMonitor) -> pd.DataFrame:
    """
    Trunca columnas a 63 caracteres (límite PostgreSQL) y resuelve duplicados.
    """
    new_cols = []
    seen: dict[str, int] = {}

    for col in df.columns:
        sanitized = col[:63]
        if len(col) > 63:
            monitor.increment('columns_truncated')
            monitor.add_truncated_column(col, sanitized)

        if sanitized in seen:
            seen[sanitized] += 1
            suffix = f"_{seen[sanitized]}"
            sanitized = f"{sanitized[:63 - len(suffix)]}{suffix}"
        else:
            seen[sanitized] = 1
        new_cols.append(sanitized)

    df.columns = new_cols
    return df


# =====================================================================
# Conversiones de tipos
# =====================================================================

def convert_hubspot_types_to_pandas(
    df: pd.DataFrame, prop_types: dict[str, str]
) -> pd.DataFrame:
    """
    Convierte columnas del DataFrame según los tipos declarados por HubSpot.
    """
    conversions_applied = 0
    conversions_failed = 0

    for col in df.columns:
        # Saltar columnas de metadata
        if col in ('hs_object_id', 'fivetran_synced', 'fivetran_deleted'):
            continue
        if col.startswith('asoc_'):
            continue

        hubspot_type = prop_types.get(col)
        if not hubspot_type:
            continue

        try:
            if hubspot_type == 'number':
                df[col] = pd.to_numeric(df[col], errors='coerce')
                conversions_applied += 1

            elif hubspot_type in ('date', 'datetime'):
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
                df[col] = df[col].dt.tz_convert(BOGOTA_TZ)
                conversions_applied += 1

            elif hubspot_type == 'bool':
                df[col] = df[col].map({
                    'true': True, 'false': False, '': None, None: None,
                })
                conversions_applied += 1

        except Exception as e:
            conversions_failed += 1
            logger.warning("No se pudo convertir columna '%s' tipo '%s': %s", col, hubspot_type, e)

    if conversions_applied > 0:
        logger.info(
            "Conversiones de tipo: %d aplicadas, %d fallidas",
            conversions_applied, conversions_failed,
        )

    return df


def get_postgres_type_from_hubspot(
    hubspot_type: str, pandas_dtype=None
) -> str:
    """Mapea un tipo de HubSpot a un tipo de PostgreSQL."""
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

    if not pg_type and pandas_dtype:
        logger.warning(
            "Sin tipo PG para HubSpot '%s', usando fallback de pandas", hubspot_type
        )
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


# =====================================================================
# Transformación de lotes de registros
# =====================================================================

def process_batch(
    batch_records: list[dict],
    col_map: dict[str, str],
    prop_types: dict[str, str],
    monitor: ETLMonitor,
    table_name: str,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Transforma una lista de registros de HubSpot en un DataFrame limpio.

    Returns:
        (DataFrame, dict de mapeo nombre_final -> nombre_original)
    """
    data_list = []
    synced_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for record in batch_records:
        try:
            row = {"hs_object_id": int(record["id"])}
            row.update(record.get("properties", {}))

            archived = record.get("archived", False)
            row["fivetran_synced"] = synced_at
            row["fivetran_deleted"] = archived

            data_list.append(row)
            monitor.increment('records_processed_ok')

        except Exception as e:
            monitor.increment('records_failed')
            logger.error("Error procesando registro %s: %s", record.get('id'), e)

    if not data_list:
        logger.warning(
            "Lote saltado: 0 registros procesados de %d recibidos.",
            len(batch_records),
        )
        return pd.DataFrame(), {}

    df = pd.DataFrame(data_list)

    column_name_mapping: dict[str, str] = {}
    original_columns = df.columns.tolist()

    # 1. Convertir tipos según HubSpot (con nombres originales)
    if prop_types:
        df = convert_hubspot_types_to_pandas(df, prop_types)

    # 2. Renombrar columnas de pipeline/stages (quitar sufijo numérico)
    df.rename(columns=col_map, inplace=True)

    # Rastrear mapeo después del rename
    for i, orig_col in enumerate(original_columns):
        new_col = df.columns[i]
        column_name_mapping[new_col] = orig_col

    # 3. Normalizar nombres (snake_case, sin acentos)
    normalized_cols = [normalize_name(c) for c in df.columns]

    updated_mapping: dict[str, str] = {}
    for i, norm_col in enumerate(normalized_cols):
        old_col = df.columns[i]
        original = column_name_mapping.get(old_col, old_col)
        updated_mapping[norm_col] = original

    df.columns = normalized_cols
    column_name_mapping = updated_mapping

    # 4. Sanitizar para Postgres (longitud y duplicados)
    df_before_sanitize = df.copy()
    df = sanitize_columns_for_postgres(df, monitor)

    final_mapping: dict[str, str] = {}
    for i, final_col in enumerate(df.columns):
        original_from_before = column_name_mapping.get(df_before_sanitize.columns[i])
        if original_from_before:
            final_mapping[final_col] = original_from_before

    column_name_mapping = final_mapping

    # 5. Convertir dicts/lists a JSON string
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )

    return df, column_name_mapping


# =====================================================================
# Transformación de pipelines y stages
# =====================================================================

def transform_pipelines(pipelines: list[dict]) -> tuple:
    """
    Transforma la lista de pipelines en DataFrames de pipelines y stages.

    Returns:
        (df_pipelines, df_stages) o (None, None) si no hay datos.
    """
    if not pipelines:
        return None, None

    synced_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    pipeline_data = []
    stage_data = []

    for pipe in pipelines:
        pipeline_data.append({
            'pipeline_id': pipe.get('id'),
            'label': pipe.get('label'),
            'display_order': pipe.get('displayOrder', 1),
            'created_at': pipe.get('createdAt'),
            'updated_at': pipe.get('updatedAt'),
            'archived': pipe.get('archived', False),
            'fivetran_deleted': pipe.get('archived', False),
            'fivetran_synced': synced_at,
        })

        for stage in pipe.get('stages', []):
            stage_row = {
                'stage_id': stage.get('id'),
                'pipeline_id': pipe.get('id'),
                'label': stage.get('label'),
                'display_order': stage.get('displayOrder', 0),
                'created_at': stage.get('createdAt'),
                'updated_at': stage.get('updatedAt'),
                'archived': stage.get('archived', False),
                'fivetran_deleted': stage.get('archived', False),
                'fivetran_synced': synced_at,
            }

            metadata = stage.get('metadata', {})
            if metadata:
                state_value = metadata.get('state') or metadata.get('leadState')
                if state_value:
                    stage_row['state'] = state_value

            stage_data.append(stage_row)

    df_pipelines = pd.DataFrame(pipeline_data)
    df_stages = pd.DataFrame(stage_data)

    # Convertir fechas a timezone Bogotá
    for df in [df_pipelines, df_stages]:
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
            df['created_at'] = df['created_at'].dt.tz_convert(BOGOTA_TZ)
        if 'updated_at' in df.columns:
            df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce', utc=True)
            df['updated_at'] = df['updated_at'].dt.tz_convert(BOGOTA_TZ)

    logger.info("Pipelines: %d, Stages: %d", len(df_pipelines), len(df_stages))
    return df_pipelines, df_stages


# =====================================================================
# Extracción de asociaciones normalizadas
# =====================================================================

def extract_normalized_associations(
    batch_records: list[dict], table_name: str, monitor: ETLMonitor
) -> dict[str, pd.DataFrame]:
    """
    Extrae asociaciones de los registros y las normaliza en DataFrames.

    Returns:
        dict {to_object_type: DataFrame}
    """
    synced_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    associations_by_type: dict[str, list[dict]] = {}

    for record in batch_records:
        from_id = int(record['id'])
        raw_assoc = record.get('associations', {})

        if not raw_assoc or not isinstance(raw_assoc, dict):
            continue

        for to_object_type_raw, assoc_data in raw_assoc.items():
            to_object_type = normalize_name(to_object_type_raw)
            if not isinstance(assoc_data, dict):
                continue

            results = assoc_data.get('results', [])

            for assoc_item in results:
                if 'id' not in assoc_item:
                    continue

                # Self-reference detection
                if table_name == to_object_type:
                    from_col = f'from_{table_name}_id'
                    to_col = f'to_{to_object_type}_id'
                else:
                    from_col = f'{table_name}_id'
                    to_col = f'{to_object_type}_id'

                row = {
                    from_col: from_id,
                    to_col: int(assoc_item['id']),
                    'type_id': assoc_item.get('type'),
                    'category': assoc_item.get('category', 'HUBSPOT_DEFINED'),
                    'fivetran_synced': synced_at,
                }

                if to_object_type not in associations_by_type:
                    associations_by_type[to_object_type] = []

                associations_by_type[to_object_type].append(row)
                monitor.increment('associations_found')

    # Convertir a DataFrames
    dataframes: dict[str, pd.DataFrame] = {}
    for to_type, rows in associations_by_type.items():
        if rows:
            df = pd.DataFrame(rows)
            dataframes[to_type] = df
            logger.info("Asociaciones hacia '%s': %d registros", to_type, len(df))

    return dataframes
