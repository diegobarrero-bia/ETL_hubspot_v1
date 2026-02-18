"""
ETL HubSpot → PostgreSQL.

Entry point dual: funciona como Lambda handler y como script CLI local.
Soporta uno o varios objetos por invocación.

Lambda Event:
    {"object_type": "contacts"}
    {"object_types": ["services", "p50445259_meters", "projects"]}

CLI:
    python handler.py --object-type contacts
    python handler.py --object-type services,p50445259_meters,projects
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
    col_map = extractor.get_smart_mapping(all_props, pipelines)

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

    # 6c. Detección de registros eliminados
    try:
        if sync_mode == "full":
            deleted = loader.reconcile_deleted_records()
            if deleted:
                logger.info("Registros marcados como eliminados: %d", deleted)
        elif sync_mode == "incremental":
            archived_ids = extractor.fetch_archived_record_ids(last_sync)
            if archived_ids:
                marked = loader.mark_records_as_deleted(archived_ids)
                logger.info("Registros archivados detectados: %d", marked)
    except Exception as e:
        logger.error("Error en detección de eliminados: %s", e)

    # 6d. Actualizar metadata de sincronización
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

    Soporta uno o varios objetos:
        {"object_type": "contacts"}
        {"object_types": ["services", "p50445259_meters", "projects"]}
    """
    import os
    from etl.config import ETLConfig

    _setup_logging(event.get("log_level", "INFO"))
    logger.info("Evento recibido: %s", json.dumps(event, default=str))

    # Determinar lista de objetos
    object_types = event.get("object_types")
    if not object_types:
        single = event.get("object_type", os.getenv("OBJECT_TYPE", "contacts"))
        object_types = [single]

    results = []
    errors = []

    try:
        for obj_type in object_types:
            obj_event = {**event, "object_type": obj_type}
            try:
                config = ETLConfig.from_lambda_event(obj_event)
                config.validate()
                summary = run_etl(config)
                results.append(summary)
            except Exception as e:
                logger.error("Error procesando objeto '%s': %s", obj_type, e, exc_info=True)
                errors.append({"object_type": obj_type, "error": type(e).__name__, "detail": str(e)})

    except EnvironmentError as e:
        logger.error("Error de configuración: %s", e)
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "configuration_error", "detail": str(e)}),
        }

    # Respuesta
    if len(object_types) == 1 and not errors:
        summary = results[0]
        status_code = 200 if summary.get("status") == "healthy" else 207
        return {
            "statusCode": status_code,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(summary, default=str),
        }

    all_healthy = all(r.get("status") == "healthy" for r in results) and not errors
    body = {"results": results, "errors": errors, "total_objects": len(object_types)}
    return {
        "statusCode": 200 if all_healthy else 207,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
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
  python handler.py --object-type services,p50445259_meters,projects
        """,
    )
    parser.add_argument(
        "--object-type",
        required=True,
        help="Tipo(s) de objeto de HubSpot, separados por coma (ej: services,p50445259_meters)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Nivel de logging (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        help="Ruta del archivo para guardar logs (opcional).",
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Forzar carga completa (ignorar sincronización incremental).",
    )

    args = parser.parse_args()
    _setup_logging(args.log_level, args.log_file)

    object_types = [t.strip() for t in args.object_type.split(",") if t.strip()]

    print("=" * 60)
    print("  ETL HUBSPOT -> POSTGRESQL")
    if len(object_types) == 1:
        print(f"  Objeto: {object_types[0]}")
    else:
        print(f"  Objetos: {', '.join(object_types)}")
    print("=" * 60 + "\n")

    all_results = []
    all_errors = []

    try:
        for i, obj_type in enumerate(object_types, 1):
            if len(object_types) > 1:
                print(f"\n{'─' * 60}")
                print(f"  [{i}/{len(object_types)}] Procesando: {obj_type}")
                print(f"{'─' * 60}\n")

            try:
                config = ETLConfig.from_env()
                config.object_type = obj_type
                config.log_level = args.log_level
                config.force_full_load = args.full_load

                config.validate()
                summary = run_etl(config)
                all_results.append(summary)

            except Exception as e:
                logger.error("Error procesando objeto '%s': %s", obj_type, e, exc_info=True)
                all_errors.append({"object_type": obj_type, "error": type(e).__name__, "detail": str(e)})

    except KeyboardInterrupt:
        print("\n\nProceso interrumpido por el usuario (Ctrl+C)")
        sys.exit(130)

    # Resumen final
    print("\n" + "=" * 60)
    if len(object_types) > 1:
        print(f"  RESUMEN MULTI-OBJETO ({len(object_types)} objetos)")
        print("=" * 60)
        for r in all_results:
            obj = r.get("object_type", "?")
            status = r.get("status", "?")
            records = r.get("records_processed_ok", 0)
            duration = r.get("duration_seconds", 0)
            print(f"  {obj:<25} : {status:<12} ({records} registros, {duration}s)")
        for e in all_errors:
            print(f"  {e['object_type']:<25} : ERROR - {e['detail'][:40]}")
    else:
        if all_results and all_results[0].get("status") == "healthy":
            print("  ETL COMPLETADA EXITOSAMENTE")
        else:
            print("  ETL COMPLETADA CON ERRORES")

    print("=" * 60)

    if len(all_results) == 1 and not all_errors:
        print(json.dumps(all_results[0], indent=2, default=str))
    else:
        print(json.dumps({"results": all_results, "errors": all_errors}, indent=2, default=str))

    all_healthy = all(r.get("status") == "healthy" for r in all_results) and not all_errors
    sys.exit(0 if all_healthy else 1)


if __name__ == "__main__":
    main()
