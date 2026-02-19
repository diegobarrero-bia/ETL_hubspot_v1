"""
Core ETL execution logic.

Extracted from handler.py. The run_etl() function and its helpers
are the business logic; everything else (HTTP, Lambda, CLI) is just a trigger.
"""
import logging
import re
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


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
            _process_batch_records(
                batch, col_map, prop_types, monitor, config,
                loader, extract_normalized_associations,
                process_batch, batch_count,
            )

            total_records += len(batch)
            batch_count += 1
            logger.info("Lote %d procesado (%d registros acumulados)", batch_count, total_records)

    # 6b. Carga de todas las asociaciones acumuladas
    associations_ok = True
    try:
        loader.flush_associations(mode=sync_mode)
        logger.info("Asociaciones volcadas a BD correctamente (mode=%s).", sync_mode)
    except Exception as e:
        associations_ok = False
        monitor.increment('association_flush_failed')
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

    # 6d. Actualizar metadata de sincronización (solo si asociaciones OK)
    if associations_ok:
        try:
            loader.update_sync_metadata(sync_start_time, total_records, sync_mode)
            logger.info("Metadata de sincronización actualizada.")
        except Exception as e:
            logger.error("Error actualizando metadata de sync: %s", e)
    else:
        logger.warning(
            "Metadata de sync NO actualizada porque las asociaciones fallaron. "
            "El siguiente run repetirá el proceso completo."
        )

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
