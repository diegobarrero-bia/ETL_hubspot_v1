"""Operaciones de base de datos PostgreSQL para el ETL."""
import logging

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from etl.config import ETLConfig
from etl.monitor import ETLMonitor
from etl.transform import get_postgres_type_from_hubspot

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Maneja todas las operaciones contra PostgreSQL."""

    def __init__(self, config: ETLConfig, monitor: ETLMonitor):
        self.config = config
        self.monitor = monitor
        self.engine: Engine = self._create_engine()
        self._accumulated_associations: dict[str, list[pd.DataFrame]] = {}
        self._column_cache: set[str] | None = None
        self._loaded_ids: set[int] = set()

    def _create_engine(self) -> Engine:
        db_url = (
            f"postgresql+psycopg2://{self.config.db_user}:{self.config.db_pass}"
            f"@{self.config.db_host}:{self.config.db_port}/{self.config.db_name}"
        )
        return create_engine(db_url)

    # -----------------------------------------------------------------
    # Inicialización de esquema
    # -----------------------------------------------------------------

    def initialize_schema(self) -> None:
        """Crea el schema y la tabla base si no existen."""
        schema = self.config.db_schema
        table = self.config.table_name

        create_schema_sql = text(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        ddl_query = text(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "hs_object_id" BIGINT PRIMARY KEY,
            "hs_lastmodifieddate" TIMESTAMPTZ,
            "fivetran_synced" TIMESTAMP,
            "fivetran_deleted" BOOLEAN
        );
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(create_schema_sql)
                conn.execute(ddl_query)
        except Exception as e:
            logger.error("Error inicializando BD: %s", e)
            raise

    # -----------------------------------------------------------------
    # Metadata de sincronización (carga incremental)
    # -----------------------------------------------------------------

    def initialize_metadata_table(self) -> None:
        """Crea la tabla de metadata de sincronización si no existe."""
        schema = self.config.db_schema
        with self.engine.begin() as conn:
            conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.etl_sync_metadata (
                "object_type" TEXT PRIMARY KEY,
                "last_sync_timestamp" TIMESTAMPTZ,
                "records_synced" INTEGER,
                "sync_mode" TEXT,
                "updated_at" TIMESTAMPTZ DEFAULT NOW()
            );
            """))

    def get_last_sync_timestamp(self) -> str | None:
        """Obtiene el timestamp de la última sincronización exitosa."""
        schema = self.config.db_schema
        table_name = self.config.table_name
        with self.engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT "last_sync_timestamp"
                FROM {schema}.etl_sync_metadata
                WHERE "object_type" = :obj_type
            """), {"obj_type": table_name})
            row = result.fetchone()
            return row[0].isoformat() if row and row[0] else None

    def update_sync_metadata(
        self, timestamp: str, records_synced: int, mode: str
    ) -> None:
        """Actualiza la metadata de sincronización tras un run exitoso."""
        schema = self.config.db_schema
        table_name = self.config.table_name
        with self.engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {schema}.etl_sync_metadata
                    ("object_type", "last_sync_timestamp", "records_synced", "sync_mode", "updated_at")
                VALUES (:obj_type, :ts, :count, :mode, NOW())
                ON CONFLICT ("object_type") DO UPDATE SET
                    "last_sync_timestamp" = EXCLUDED."last_sync_timestamp",
                    "records_synced" = EXCLUDED."records_synced",
                    "sync_mode" = EXCLUDED."sync_mode",
                    "updated_at" = NOW()
            """), {"obj_type": table_name, "ts": timestamp, "count": records_synced, "mode": mode})

    # -----------------------------------------------------------------
    # Evolución de esquema
    # -----------------------------------------------------------------

    def sync_schema(
        self,
        df: pd.DataFrame,
        prop_types: dict[str, str] | None = None,
        column_mapping: dict[str, str] | None = None,
    ) -> None:
        """Detecta y crea columnas nuevas en la tabla. Usa cache para evitar queries repetidos."""
        schema = self.config.db_schema
        table = self.config.table_name

        # Poblar cache la primera vez consultando la BD
        if self._column_cache is None:
            inspector = inspect(self.engine)
            if not inspector.has_table(table, schema=schema):
                return
            self._column_cache = set(
                c['name'] for c in inspector.get_columns(table, schema=schema)
            )

        new_cols = set(df.columns) - self._column_cache

        if not new_cols:
            return

        logger.info("Schema Evolution: Creando %d columnas nuevas...", len(new_cols))

        with self.engine.begin() as conn:
            for col in new_cols:
                dtype = df[col].dtype
                original_col_name = column_mapping.get(col) if column_mapping else None

                if prop_types and original_col_name and original_col_name in prop_types:
                    hubspot_type = prop_types[original_col_name]
                    pg_type = get_postgres_type_from_hubspot(hubspot_type, dtype)
                    logger.debug(
                        "Columna '%s' (original: '%s'): HubSpot '%s' -> PG '%s'",
                        col, original_col_name, hubspot_type, pg_type,
                    )
                else:
                    logger.warning("Usando fallback de pandas para columna: %s", col)
                    pg_type = "TEXT"
                    if pd.api.types.is_integer_dtype(dtype):
                        pg_type = "BIGINT"
                    elif pd.api.types.is_float_dtype(dtype):
                        pg_type = "NUMERIC"
                    elif pd.api.types.is_bool_dtype(dtype):
                        pg_type = "BOOLEAN"
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        pg_type = "TIMESTAMPTZ"

                try:
                    conn.execute(
                        text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{col}" {pg_type}')
                    )
                    self._column_cache.add(col)
                    self.monitor.increment('schema_changes')
                    logger.info("Columna creada: %s (%s)", col, pg_type)
                except Exception as e:
                    # Columna puede ya existir (ej: concurrencia), agregar al cache igualmente
                    self._column_cache.add(col)
                    logger.warning("Error al crear columna %s (puede que ya exista): %s", col, e)

    def invalidate_column_cache(self) -> None:
        """Fuerza re-lectura de columnas desde BD en el próximo sync_schema."""
        self._column_cache = None

    # -----------------------------------------------------------------
    # Upsert de registros
    # -----------------------------------------------------------------

    def upsert_records(self, df: pd.DataFrame) -> None:
        """Inserta o actualiza registros con psycopg2 execute_values (bulk)."""
        if df.empty:
            return

        schema = self.config.db_schema
        table = self.config.table_name
        columns = df.columns.tolist()

        cols_quoted = ', '.join(f'"{c}"' for c in columns)
        update_cols = [c for c in columns if c != 'hs_object_id']
        set_clause = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

        sql = f"""
            INSERT INTO "{schema}"."{table}" ({cols_quoted})
            VALUES %s
            ON CONFLICT ("hs_object_id") DO UPDATE SET {set_clause}
        """

        # Convertir DataFrame a lista de tuplas, NaN/NaT -> None
        data = []
        for row in df.itertuples(index=False, name=None):
            data.append(tuple(
                None if pd.isna(v) else v for v in row
            ))

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                execute_values(cur, sql, data, page_size=500)
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()

        self._loaded_ids.update(df['hs_object_id'].astype(int).tolist())
        self.monitor.metrics['db_upserts'] += len(df)

    # -----------------------------------------------------------------
    # Detección de registros eliminados
    # -----------------------------------------------------------------

    def reconcile_deleted_records(self) -> int:
        """Marca como eliminados los registros en PG que no vinieron en el full load."""
        if not self._loaded_ids:
            return 0

        schema = self.config.db_schema
        table = self.config.table_name
        ids_array = list(self._loaded_ids)

        sql = f"""
            UPDATE "{schema}"."{table}"
            SET "fivetran_deleted" = true,
                "fivetran_synced" = NOW()
            WHERE "hs_object_id" != ALL(%s)
              AND ("fivetran_deleted" IS DISTINCT FROM true)
        """

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                cur.execute(sql, (ids_array,))
                count = cur.rowcount
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()

        if count > 0:
            self.monitor.increment('records_deleted', count)
        return count

    def mark_records_as_deleted(self, ids: list[int]) -> int:
        """Marca IDs específicos como eliminados (para archivados en incremental)."""
        if not ids:
            return 0

        schema = self.config.db_schema
        table = self.config.table_name

        sql = f"""
            UPDATE "{schema}"."{table}"
            SET "fivetran_deleted" = true,
                "fivetran_synced" = NOW()
            WHERE "hs_object_id" = ANY(%s)
              AND ("fivetran_deleted" IS DISTINCT FROM true)
        """

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                cur.execute(sql, (ids,))
                count = cur.rowcount
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()

        if count > 0:
            self.monitor.increment('records_deleted', count)
        return count

    # -----------------------------------------------------------------
    # Pipelines y Stages
    # -----------------------------------------------------------------

    def load_pipelines(
        self, df_pipelines: pd.DataFrame, df_stages: pd.DataFrame
    ) -> None:
        """Carga pipelines y stages con UPSERT + soft delete."""
        if df_pipelines is None or df_pipelines.empty:
            return

        schema = self.config.db_schema
        table = self.config.table_name
        pipeline_table = f"{table}_pipeline"
        stages_table = f"{table}_pipeline_stage"

        try:
            # Crear tablas si no existen (requiere SQLAlchemy conn)
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{pipeline_table} (
                    "pipeline_id" TEXT PRIMARY KEY,
                    "label" TEXT,
                    "display_order" INTEGER,
                    "created_at" TIMESTAMPTZ,
                    "updated_at" TIMESTAMPTZ,
                    "archived" BOOLEAN,
                    "fivetran_deleted" BOOLEAN,
                    "fivetran_synced" TIMESTAMP
                );
                """))
                conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{stages_table} (
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
                    FOREIGN KEY ("pipeline_id") REFERENCES {schema}.{pipeline_table}("pipeline_id")
                );
                """))

            # Upsert + soft delete con raw connection
            raw_conn = self.engine.raw_connection()
            try:
                with raw_conn.cursor() as cur:
                    # A. Upsert pipelines
                    self._upsert_dataframe(
                        cur, schema, pipeline_table,
                        df_pipelines, pk="pipeline_id",
                    )

                    # B. Upsert stages
                    if df_stages is not None and not df_stages.empty:
                        self._upsert_dataframe(
                            cur, schema, stages_table,
                            df_stages, pk="stage_id",
                        )

                    # C. Soft delete: marcar ausentes
                    synced_at = df_pipelines['fivetran_synced'].iloc[0]

                    active_pipeline_ids = df_pipelines['pipeline_id'].tolist()
                    self._soft_delete_missing(
                        cur, schema, pipeline_table,
                        "pipeline_id", active_pipeline_ids, synced_at,
                    )

                    if df_stages is not None and not df_stages.empty:
                        active_stage_ids = df_stages['stage_id'].tolist()
                    else:
                        active_stage_ids = []
                    self._soft_delete_missing(
                        cur, schema, stages_table,
                        "stage_id", active_stage_ids, synced_at,
                    )

                raw_conn.commit()
            except Exception:
                raw_conn.rollback()
                raise
            finally:
                raw_conn.close()

            self.monitor.set_metric('pipelines_loaded', len(df_pipelines))
            if df_stages is not None and not df_stages.empty:
                self.monitor.set_metric('stages_loaded', len(df_stages))

            logger.info(
                "Pipelines cargados: %d pipelines, %d stages",
                len(df_pipelines),
                len(df_stages) if df_stages is not None else 0,
            )

        except Exception as e:
            logger.error("Error cargando pipelines/stages: %s", e)
            raise

    def _upsert_dataframe(
        self, cursor, schema: str, table: str,
        df: pd.DataFrame, pk: str,
    ) -> None:
        """Upsert generico de un DataFrame usando ON CONFLICT."""
        columns = df.columns.tolist()
        cols_quoted = ', '.join(f'"{c}"' for c in columns)
        update_cols = [c for c in columns if c != pk]
        set_clause = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

        sql = f"""
            INSERT INTO "{schema}"."{table}" ({cols_quoted})
            VALUES %s
            ON CONFLICT ("{pk}") DO UPDATE SET {set_clause}
        """

        data = []
        for row in df.itertuples(index=False, name=None):
            data.append(tuple(
                None if isinstance(v, float) and pd.isna(v) else v
                for v in row
            ))

        execute_values(cursor, sql, data, page_size=500)

    def _soft_delete_missing(
        self, cursor, schema: str, table: str,
        pk_col: str, active_ids: list, synced_at: str,
    ) -> None:
        """Marca como fivetran_deleted=TRUE los registros ausentes."""
        if not active_ids:
            return

        placeholders = ', '.join(['%s'] * len(active_ids))
        sql = f"""
            UPDATE "{schema}"."{table}"
            SET "fivetran_deleted" = TRUE, "fivetran_synced" = %s
            WHERE "{pk_col}" NOT IN ({placeholders})
            AND "fivetran_deleted" = FALSE
        """
        cursor.execute(sql, [synced_at] + active_ids)

    # -----------------------------------------------------------------
    # Acumulación y carga de asociaciones
    # -----------------------------------------------------------------

    def accumulate_associations(self, associations_dfs: dict[str, pd.DataFrame]) -> None:
        """Acumula DataFrames de asociaciones en memoria para carga posterior."""
        if not associations_dfs:
            return

        for to_type, df_assoc in associations_dfs.items():
            if df_assoc.empty:
                continue
            if to_type not in self._accumulated_associations:
                self._accumulated_associations[to_type] = []
            self._accumulated_associations[to_type].append(df_assoc)

    def flush_associations(self, mode: str = "full") -> None:
        """
        Escribe todas las asociaciones acumuladas a BD.

        Args:
            mode: "full" = TRUNCATE + INSERT, "incremental" = INSERT ON CONFLICT DO UPDATE
        """
        if not self._accumulated_associations:
            return

        schema = self.config.db_schema

        try:
            with self.engine.begin() as conn:
                for assoc_table, df_list in self._accumulated_associations.items():
                    df_assoc = pd.concat(df_list, ignore_index=True)
                    if df_assoc.empty:
                        continue

                    # Extraer columnas de ID del DataFrame (ya en orden canonico)
                    id_cols = [c for c in df_assoc.columns
                               if c.endswith('_id') and c != 'type_id']
                    from_col, to_col = id_cols[0], id_cols[1]

                    # Deduplicar por clave compuesta
                    key_cols = [from_col, to_col, "type_id"]
                    existing_key_cols = [c for c in key_cols if c in df_assoc.columns]
                    if existing_key_cols:
                        df_assoc = df_assoc.drop_duplicates(
                            subset=existing_key_cols, keep="last"
                        )

                    # Crear tabla
                    conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{assoc_table} (
                        "{from_col}" BIGINT NOT NULL,
                        "{to_col}" BIGINT NOT NULL,
                        "type_id" TEXT,
                        "category" TEXT,
                        "fivetran_synced" TIMESTAMP,
                        PRIMARY KEY ("{from_col}", "{to_col}", "type_id")
                    );
                    """))

                    if mode == "full":
                        # Full load: TRUNCATE + INSERT
                        conn.execute(text(f'TRUNCATE TABLE {schema}.{assoc_table}'))
                        df_assoc.to_sql(
                            assoc_table, con=conn, schema=schema,
                            if_exists='append', index=False,
                        )
                    else:
                        # Incremental: INSERT ON CONFLICT DO UPDATE
                        cols = df_assoc.columns.tolist()
                        cols_quoted = ', '.join(f'"{c}"' for c in cols)
                        update_cols = [c for c in cols if c not in key_cols]
                        set_clause = ', '.join(
                            f'"{c}" = EXCLUDED."{c}"' for c in update_cols
                        )
                        pk_clause = ', '.join(f'"{c}"' for c in key_cols)

                        sql = f"""
                            INSERT INTO {schema}.{assoc_table} ({cols_quoted})
                            VALUES %s
                            ON CONFLICT ({pk_clause}) DO UPDATE SET {set_clause}
                        """
                        data = [
                            tuple(None if pd.isna(v) else v for v in row)
                            for row in df_assoc.itertuples(index=False, name=None)
                        ]
                        raw_conn = self.engine.raw_connection()
                        try:
                            with raw_conn.cursor() as cur:
                                execute_values(cur, sql, data, page_size=500)
                            raw_conn.commit()
                        except Exception:
                            raw_conn.rollback()
                            raise
                        finally:
                            raw_conn.close()

                    self.monitor.add_association_table(assoc_table)
                    logger.info(
                        "Asociaciones '%s': %d registros cargados (mode=%s)",
                        assoc_table, len(df_assoc), mode,
                    )

        except Exception as e:
            logger.error("Error cargando asociaciones: %s", e)
            raise
        finally:
            self._accumulated_associations.clear()

    def load_associations(self, associations_dfs: dict[str, pd.DataFrame]) -> None:
        """Wrapper de compatibilidad. Usa accumulate_associations + flush_associations."""
        self.accumulate_associations(associations_dfs)
