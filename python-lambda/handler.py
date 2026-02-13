"""
ETL HubSpot → PostgreSQL.

Entry point dual: funciona como Lambda handler y como script CLI local.

Lambda Event:
    {
        "object_type": "contacts",       # Required
        "log_level": "INFO"              # Optional (default: INFO)
    }

CLI:
    python handler.py --object-type contacts
    python handler.py --object-type contacts --log-level DEBUG
"""
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone

logger = logging.getLogger()


# =====================================================================
# Core ETL
# =====================================================================

def run_etl(config) -> dict:
    """
    Ejecuta el proceso ETL completo para un tipo de objeto de HubSpot.

    Args:
        config: Instancia de ETLConfig con la configuración.

    Returns:
        Diccionario con métricas del proceso.
    """
    from etl.hubspot import HubSpotExtractor
    from etl.database import DatabaseLoader
    from etl.monitor import ETLMonitor
    from etl.transform import (
        process_batch,
        transform_pipelines,
        extract_normalized_associations,
    )

    monitor = ETLMonitor(
        object_type=config.object_type,
        db_schema=config.db_schema,
        table_name=config.table_name,
    )

    # 1. Inicializar componentes
    extractor = HubSpotExtractor(config, monitor)
    loader = DatabaseLoader(config, monitor)

    # 2. Obtener metadata (propiedades y asociaciones)
    all_props, prop_types = extractor.get_properties_with_types()
    logger.info("Propiedades obtenidas: %d", len(all_props))

    assocs = extractor.get_associations()
    logger.info("Asociaciones disponibles: %d", len(assocs))

    # 3. Resolver nombre del schema HubSpot como nombre base de tablas
    config.table_name = extractor.get_schema_name()
    monitor.table_name = config.table_name

    logger.info(
        "INICIANDO ETL - Objeto: %s, Destino: %s.%s",
        config.object_type, config.db_schema, config.table_name,
    )

    # 4. Preparar BD
    loader.initialize_schema()
    logger.info("Esquema de BD inicializado.")

    # 4. Pipelines y stages
    pipelines = extractor.get_pipelines()
    if pipelines:
        df_pipelines, df_stages = transform_pipelines(pipelines)
        if df_pipelines is not None:
            loader.load_pipelines(df_pipelines, df_stages)
            logger.info("Pipelines y stages cargados a BD.")
    else:
        logger.info("Objeto '%s' no tiene pipelines disponibles.", config.object_type)

    # 5. Mapeo de columnas de pipeline
    col_map = extractor.get_smart_mapping(all_props)

    # 5b. Determinar modo de sincronización (incremental vs full)
    loader.initialize_metadata_table()
    last_sync = loader.get_last_sync_timestamp()
    sync_start_time = datetime.now(timezone.utc).isoformat()
    sync_mode = "full"

    batch_count = 0
    total_records = 0

    if last_sync and not config.force_full_load:
        logger.info("Intentando carga incremental desde %s", last_sync)
        try:
            modified_records, exceeded = extractor.search_modified_records(all_props, last_sync)

            if not exceeded and not modified_records:
                sync_mode = "incremental"
                logger.info("Sin registros modificados desde la última sincronización.")
            elif not exceeded:
                sync_mode = "incremental"
                logger.info(
                    "Modo incremental: %d registros modificados encontrados",
                    len(modified_records),
                )
                # Procesar registros modificados en lotes de 100
                for i in range(0, len(modified_records), 100):
                    batch = modified_records[i:i + 100]
                    _process_batch_records(
                        batch, col_map, prop_types, monitor, config,
                        loader, extract_normalized_associations,
                        process_batch, batch_count,
                    )
                    total_records += len(batch)
                    batch_count += 1
                    logger.info(
                        "Lote incremental %d procesado (%d registros acumulados)",
                        batch_count, total_records,
                    )
            else:
                logger.info("Demasiados cambios (>10000). Cambiando a full load.")
        except Exception as e:
            logger.warning("Error en carga incremental, cambiando a full load: %s", e)

    if sync_mode == "full":
        # 6. Proceso por lotes (full load)
        if config.force_full_load:
            logger.info("Full load forzado por configuración.")
        else:
            logger.info("Ejecutando full load.")

        for batch in extractor.fetch_all_records_with_chunked_assocs(all_props, assocs):
            monitor.metrics['records_fetched'] += len(batch)

            _process_batch_records(
                batch, col_map, prop_types, monitor, config,
                loader, extract_normalized_associations,
                process_batch, batch_count,
            )

            total_records += len(batch)
            batch_count += 1
            logger.info("Lote %d procesado (%d registros acumulados)", batch_count, total_records)

    # 6b. Carga de todas las asociaciones acumuladas
    try:
        loader.flush_associations(mode=sync_mode)
        logger.info("Asociaciones volcadas a BD correctamente (mode=%s).", sync_mode)
    except Exception as e:
        logger.error("Error volcando asociaciones: %s", e)

    # 6c. Actualizar metadata de sincronización
    try:
        loader.update_sync_metadata(sync_start_time, total_records, sync_mode)
        logger.info("Metadata de sincronización actualizada.")
    except Exception as e:
        logger.error("Error actualizando metadata de sync: %s", e)

    # 7. Reporte
    monitor.set_metric('sync_mode', sync_mode)
    report = monitor.generate_report()
    summary = monitor.get_summary()
    summary['sync_mode'] = sync_mode

    logger.info("ETL FINALIZADO - Objeto: %s, Modo: %s", config.object_type, sync_mode)
    return summary


def _process_batch_records(
    batch, col_map, prop_types, monitor, config,
    loader, extract_normalized_associations_fn,
    process_batch_fn, batch_count,
):
    """Procesa un lote de registros: transform, schema sync, upsert, asociaciones."""
    monitor.metrics['records_fetched'] += len(batch)

    # A. Transformar
    df_batch, column_mapping = process_batch_fn(
        batch, col_map, prop_types, monitor, config.table_name,
    )

    if df_batch.empty:
        return

    # B. Evolución de esquema
    loader.sync_schema(df_batch, prop_types, column_mapping)

    # C. Carga a BD
    db_start = time.time()
    try:
        loader.upsert_records(df_batch)
    except Exception as e:
        monitor.metrics['db_insert_errors'] += len(df_batch)
        _log_db_error(e, batch_count)

    # D. Asociaciones (acumular para carga al final)
    try:
        associations_dfs = extract_normalized_associations_fn(
            batch, config.table_name, monitor,
        )
        if associations_dfs:
            loader.accumulate_associations(associations_dfs)
    except Exception as e:
        logger.error("Error en asociaciones del lote %d: %s", batch_count, e)

    monitor.metrics['db_execution_time'] += (time.time() - db_start)
    monitor.record_null_stats(df_batch)


def _log_db_error(error: Exception, batch_count: int) -> None:
    """Clasifica y registra errores de base de datos."""
    error_type = type(error).__name__
    error_msg = str(error)

    if 'DatetimeFieldOverflow' in error_type or 'date/time field value out of range' in error_msg:
        value_match = re.search(r'"(\d+)"', error_msg)
        problematic_value = value_match.group(1) if value_match else "desconocido"
        logger.error(
            "ERROR DE TIPO DE DATO - Lote %d: valor '%s' incompatible",
            batch_count, problematic_value,
        )
    elif 'IntegrityError' in error_type or 'duplicate key' in error_msg.lower():
        logger.error("ERROR DE INTEGRIDAD - Lote %d: violación de constraint", batch_count)
    elif 'ProgrammingError' in error_type or 'column' in error_msg.lower():
        logger.error("ERROR DE ESQUEMA - Lote %d: error en definición de columna", batch_count)
    else:
        logger.error("ERROR DB DESCONOCIDO - Lote %d: %s - %s", batch_count, error_type, error_msg)

    logger.error("Detalle técnico: %s", error)


# =====================================================================
# Lambda Handler
# =====================================================================

def handler(event: dict, context=None) -> dict:
    """
    AWS Lambda handler.

    Soporta invocación directa, EventBridge y test events.
    """
    from etl.config import ETLConfig

    # Configurar logging para Lambda (CloudWatch)
    _setup_logging(event.get("log_level", "INFO"))

    logger.info("Evento recibido: %s", json.dumps(event, default=str))

    try:
        config = ETLConfig.from_lambda_event(event)
        config.validate()

        summary = run_etl(config)

        status_code = 200 if summary.get("status") == "healthy" else 207

        return {
            "statusCode": status_code,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(summary, default=str),
        }

    except EnvironmentError as e:
        logger.error("Error de configuración: %s", e)
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "configuration_error", "detail": str(e)}),
        }

    except Exception as e:
        logger.exception("Error fatal en ETL")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": type(e).__name__, "detail": str(e)}),
        }


# =====================================================================
# CLI (modo local)
# =====================================================================

def _setup_logging(level: str = "INFO", log_file: str = None) -> None:
    """
    Configura logging según el entorno.
    
    Args:
        level: Nivel de logging (DEBUG, INFO, etc.)
        log_file: Ruta opcional para guardar logs en archivo (solo modo local)
    """
    level = level.upper()
    valid = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
    if level not in valid:
        level = "INFO"

    numeric_level = getattr(logging, level, logging.INFO)

    # Limpiar handlers previos
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    if not root_logger.handlers:
        # Handler para consola (siempre presente)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(numeric_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # Handler para archivo (solo si se especifica)
        if log_file:
            try:
                file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
                file_handler.setLevel(numeric_level)
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)
                print(f"📄 Logs guardándose en: {log_file}")
            except Exception as e:
                logger.warning("No se pudo crear archivo de log '%s': %s", log_file, e)


def main() -> None:
    """Entry point para ejecución local (CLI)."""
    import argparse
    from dotenv import load_dotenv
    from etl.config import ETLConfig

    load_dotenv()

    parser = argparse.ArgumentParser(
        description="ETL HubSpot → PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python handler.py --object-type contacts
  python handler.py --object-type deals --log-level DEBUG
  python handler.py --object-type services --log-file etl_errors.log
        """,
    )
    parser.add_argument(
        "--object-type",
        required=True,
        help="Tipo de objeto de HubSpot (contacts, deals, companies, services, etc.)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Nivel de logging (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        help="Ruta del archivo para guardar logs (opcional). Si se proporciona, los logs se verán en pantalla Y se guardarán en el archivo.",
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Forzar carga completa (ignorar sincronización incremental).",
    )

    args = parser.parse_args()

    _setup_logging(args.log_level, args.log_file)

    print("=" * 60)
    print("  ETL HUBSPOT -> POSTGRESQL")
    print(f"  Objeto: {args.object_type}")
    print("=" * 60 + "\n")

    try:
        config = ETLConfig.from_env()
        # Sobrescribir object_type desde CLI
        config.object_type = args.object_type
        config.table_name = args.object_type
        config.log_level = args.log_level
        config.force_full_load = args.full_load
        # Regenerar headers con token actualizado
        config.headers = {
            'Authorization': f'Bearer {config.access_token}',
            'Content-Type': 'application/json',
        }
        config.validate()

        summary = run_etl(config)

        print("\n" + "=" * 60)
        if summary.get("status") == "healthy":
            print("  ETL COMPLETADA EXITOSAMENTE")
        else:
            print("  ETL COMPLETADA CON ERRORES")
        print("=" * 60)
        print(json.dumps(summary, indent=2, default=str))

        sys.exit(0 if summary.get("status") == "healthy" else 1)

    except EnvironmentError as e:
        print(f"\nERROR DE CONFIGURACIÓN:\n{e}\n")
        sys.exit(2)

    except KeyboardInterrupt:
        print("\n\nProceso interrumpido por el usuario (Ctrl+C)")
        sys.exit(130)

    except Exception as e:
        print(f"\nERROR FATAL EN ETL:")
        print(f"   Tipo: {type(e).__name__}")
        print(f"   Mensaje: {str(e)}")
        logger.critical("ETL detenido por error fatal", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
