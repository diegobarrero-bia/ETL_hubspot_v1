"""Monitor de métricas y generación de reportes del ETL."""
import logging
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ETLMonitor:
    """Recolecta métricas durante la ejecución del ETL y genera reportes."""

    def __init__(self, object_type: str, db_schema: str, table_name: str):
        self.object_type = object_type
        self.db_schema = db_schema
        self.table_name = table_name
        self.start_time = time.time()
        self.metrics = {
            'api_calls': 0,
            'retries_429': 0,
            'retries_5xx': 0,
            'connection_errors': 0,
            'records_fetched': 0,
            'records_processed_ok': 0,
            'records_failed': 0,
            'columns_truncated': 0,
            'associations_found': 0,
            'associations_missing': 0,
            'db_upserts': 0,
            'schema_changes': 0,
            'db_execution_time': 0.0,
            'db_insert_errors': 0,
            'pipelines_loaded': 0,
            'stages_loaded': 0,
            'association_tables_created': 0,
        }
        self.null_stats: dict = {}
        self.association_tables: list[str] = []
        self.truncated_columns: list[tuple[str, str]] = []

    # -----------------------------------------------------------------
    # Registro de métricas
    # -----------------------------------------------------------------

    def increment(self, metric: str, count: int = 1) -> None:
        if metric in self.metrics:
            self.metrics[metric] += count

    def set_metric(self, metric: str, value) -> None:
        if metric in self.metrics:
            self.metrics[metric] = value

    def add_association_table(self, table_name: str) -> None:
        """Registra una tabla de asociaciones creada."""
        if table_name not in self.association_tables:
            self.association_tables.append(table_name)
            self.metrics['association_tables_created'] += 1

    def add_truncated_column(self, original: str, truncated: str) -> None:
        """Registra una columna que fue truncada."""
        truncation = (original, truncated)
        if truncation not in self.truncated_columns:
            self.truncated_columns.append(truncation)

    def record_null_stats(self, df) -> None:
        """Registra estadísticas de valores nulos por columna."""
        total_rows = len(df)
        if total_rows == 0:
            return
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                pct = (count / total_rows) * 100
                if col not in self.null_stats or pct > self.null_stats[col][1]:
                    self.null_stats[col] = (count, pct)

    # -----------------------------------------------------------------
    # Resumen y reporte
    # -----------------------------------------------------------------

    def get_summary(self) -> dict:
        """Retorna un diccionario resumen para respuesta Lambda / CLI."""
        duration = time.time() - self.start_time
        m = self.metrics
        return {
            "object_type": self.object_type,
            "status": "healthy" if m['records_failed'] == 0 and m['db_insert_errors'] == 0 else "with_errors",
            "duration_seconds": round(duration, 2),
            "records_fetched": m['records_fetched'],
            "records_processed_ok": m['records_processed_ok'],
            "records_failed": m['records_failed'],
            "db_upserts": m['db_upserts'],
            "db_insert_errors": m['db_insert_errors'],
            "api_calls": m['api_calls'],
            "pipelines_loaded": m['pipelines_loaded'],
            "stages_loaded": m['stages_loaded'],
            "association_tables_created": m['association_tables_created'],
            "associations_found": m['associations_found'],
            "schema_changes": m['schema_changes'],
            "columns_truncated": m['columns_truncated'],
        }

    def generate_report(self) -> str:
        """Genera un reporte de texto con el resumen del ETL."""
        duration = time.time() - self.start_time
        duration_str = str(timedelta(seconds=int(duration)))
        m = self.metrics

        # Sección de nulos
        nulls_report = ""
        if self.null_stats:
            sorted_nulls = sorted(
                self.null_stats.items(), key=lambda x: x[1][1], reverse=True
            )[:10]
            nulls_report = "\n   [Top Columnas con Valores Vacíos (Peor Lote Detectado)]\n"
            for col, (count, pct) in sorted_nulls:
                alert = "⚠️" if pct > 10 else " "
                nulls_report += f"   - {col[:30]:<30} : ({pct:>5.1f}%) {alert}\n"
        else:
            nulls_report = "   - No se detectaron valores nulos significativos.\n"

        # Sección de tablas de asociaciones
        if not self.association_tables:
            assoc_tables_str = "   - No se crearon tablas de asociaciones\n"
        else:
            assoc_tables_str = "   Tablas:\n"
            for table in sorted(self.association_tables):
                assoc_tables_str += f"      - {table}\n"

        # Sección de columnas truncadas
        truncations_str = ""
        if self.truncated_columns:
            total = len(self.truncated_columns)
            display_count = min(total, 10)
            sorted_truncations = sorted(self.truncated_columns, key=lambda x: x[0])
            truncations_str = f"\n   [Columnas Truncadas - {display_count} de {total}]\n"
            for original, truncated in sorted_truncations[:display_count]:
                orig_display = original if len(original) <= 70 else original[:67] + "..."
                truncations_str += f"   - {orig_display}\n"
                truncations_str += f"     -> {truncated}\n"
            if total > 10:
                truncations_str += f"   ... y {total - 10} más (ver logs)\n"

        has_errors = m['records_failed'] > 0 or m['db_insert_errors'] > 0
        status_str = "CON ERRORES" if has_errors else "SALUDABLE"

        report = f"""
==================================================
          DATA HEALTH REPORT - POSTGRES LOAD
==================================================
Fecha de Ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duración Total    : {duration_str}
Object Type       : {self.object_type} (Method: GET List)
Estado General    : {status_str}

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
{assoc_tables_str}
5. INTEGRIDAD & CALIDAD
-----------------------------------------------
   - Schema Changes        : {m['schema_changes']}
   - Cols Truncadas        : {m['columns_truncated']}
{truncations_str}
   {nulls_report}
==================================================
"""
        logger.info(report)
        return report
