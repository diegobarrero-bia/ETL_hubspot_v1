"""Operaciones de base de datos PostgreSQL para el ETL."""
import logging

import pandas as pd
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
    # Evolución de esquema
    # -----------------------------------------------------------------

    def sync_schema(
        self,
        df: pd.DataFrame,
        prop_types: dict[str, str] | None = None,
        column_mapping: dict[str, str] | None = None,
    ) -> None:
        """Detecta y crea columnas nuevas en la tabla."""
        schema = self.config.db_schema
        table = self.config.table_name

        inspector = inspect(self.engine)
        if not inspector.has_table(table, schema=schema):
            return

        existing_cols = [c['name'] for c in inspector.get_columns(table, schema=schema)]
        new_cols = set(df.columns) - set(existing_cols)

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
                    self.monitor.increment('schema_changes')
                    logger.info("Columna creada: %s (%s)", col, pg_type)
                except Exception as e:
                    logger.warning("Error al crear columna %s (puede que ya exista): %s", col, e)

    # -----------------------------------------------------------------
    # Upsert de registros
    # -----------------------------------------------------------------

    @staticmethod
    def _upsert_on_conflict(table, conn, keys, data_iter):
        """Método callback para pandas to_sql: INSERT ON CONFLICT DO UPDATE."""
        data = [dict(zip(keys, row)) for row in data_iter]
        if not data:
            return

        stmt = insert(table.table).values(data)
        update_cols = {c.name: c for c in stmt.excluded if c.name != 'hs_object_id'}

        if not update_cols:
            on_conflict_stmt = stmt.on_conflict_do_nothing(
                index_elements=['hs_object_id']
            )
        else:
            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=['hs_object_id'],
                set_=update_cols,
            )
        conn.execute(on_conflict_stmt)

    def upsert_records(self, df: pd.DataFrame) -> None:
        """Inserta o actualiza registros en la tabla principal."""
        schema = self.config.db_schema
        table = self.config.table_name

        with self.engine.begin() as conn:
            df.to_sql(
                table,
                con=conn,
                schema=schema,
                if_exists='append',
                index=False,
                method=self._upsert_on_conflict,
            )
        self.monitor.metrics['db_upserts'] += len(df)

    # -----------------------------------------------------------------
    # Pipelines y Stages
    # -----------------------------------------------------------------

    def load_pipelines(
        self, df_pipelines: pd.DataFrame, df_stages: pd.DataFrame
    ) -> None:
        """Carga pipelines y stages con estrategia TRUNCATE + INSERT."""
        if df_pipelines is None or df_pipelines.empty:
            return

        schema = self.config.db_schema
        table = self.config.table_name
        pipeline_table = f"{table}_pipelines"
        stages_table = f"{table}_pipeline_stages"

        try:
            with self.engine.begin() as conn:
                # Crear tabla de pipelines
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

                # Crear tabla de stages
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

                # Truncar (stages primero por FK)
                conn.execute(text(f'TRUNCATE TABLE {schema}.{stages_table} CASCADE'))
                conn.execute(text(f'TRUNCATE TABLE {schema}.{pipeline_table} CASCADE'))

                # Insertar pipelines
                df_pipelines.to_sql(
                    pipeline_table, con=conn, schema=schema,
                    if_exists='append', index=False,
                )
                self.monitor.set_metric('pipelines_loaded', len(df_pipelines))

                # Insertar stages
                if df_stages is not None and not df_stages.empty:
                    df_stages.to_sql(
                        stages_table, con=conn, schema=schema,
                        if_exists='append', index=False,
                    )
                    self.monitor.set_metric('stages_loaded', len(df_stages))

            logger.info(
                "Pipelines cargados: %d pipelines, %d stages",
                len(df_pipelines),
                len(df_stages) if df_stages is not None else 0,
            )

        except Exception as e:
            logger.error("Error cargando pipelines/stages: %s", e)
            raise

    # -----------------------------------------------------------------
    # Asociaciones
    # -----------------------------------------------------------------

    def load_associations(self, associations_dfs: dict[str, pd.DataFrame]) -> None:
        """Carga asociaciones normalizadas en tablas relacionales."""
        if not associations_dfs:
            return

        schema = self.config.db_schema
        from_object = self.config.table_name

        try:
            with self.engine.begin() as conn:
                for to_object_type, df_assoc in associations_dfs.items():
                    if df_assoc.empty:
                        continue

                    assoc_table = f"{from_object}_{to_object_type}"

                    # Nombres de columnas
                    if from_object == to_object_type:
                        from_col = f"from_{from_object}_id"
                        to_col = f"to_{to_object_type}_id"
                    else:
                        from_col = f"{from_object}_id"
                        to_col = f"{to_object_type}_id"

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

                    # Truncar e insertar
                    conn.execute(text(f'TRUNCATE TABLE {schema}.{assoc_table}'))

                    df_assoc.to_sql(
                        assoc_table, con=conn, schema=schema,
                        if_exists='append', index=False,
                    )

                    self.monitor.add_association_table(assoc_table)
                    logger.info(
                        "Asociaciones '%s': %d registros cargados",
                        assoc_table, len(df_assoc),
                    )

        except Exception as e:
            logger.error("Error cargando asociaciones: %s", e)
            raise
