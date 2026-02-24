"""Procesador de eventos webhook (core business logic)."""
import logging

from etl.database import DatabaseLoader
from etl.hubspot import HubSpotExtractor
from etl.monitor import ETLMonitor
from etl.transform import process_batch

from core.config import WebhookConfig
from processor.batcher import EventBatch

logger = logging.getLogger(__name__)


class EventHandler:
    """Procesa batches de eventos webhook usando los módulos ETL existentes."""

    def __init__(self, config: WebhookConfig):
        self.config = config

    def process_batch(self, batch: EventBatch) -> dict:
        """
        Procesa un batch de eventos agrupándolos por tipo de objeto.

        Returns:
            dict con contadores: {"processed": N, "deleted": N, "errors": N}
        """
        summary = {"processed": 0, "deleted": 0, "errors": 0}

        groups = batch.group_by_object_type()

        for object_type, events in groups.items():
            try:
                self._process_object_type(object_type, events, summary)
            except Exception as e:
                logger.error(
                    "Error procesando %s (%d eventos): %s",
                    object_type, len(events), e, exc_info=True,
                )
                summary["errors"] += len(events)

        return summary

    def _process_object_type(self, object_type: str, events: list, summary: dict) -> None:
        """Procesa todos los eventos de un tipo de objeto."""
        etl_config = self.config.build_etl_config(object_type)
        monitor = ETLMonitor(object_type, etl_config.db_schema, etl_config.table_name)
        extractor = HubSpotExtractor(etl_config, monitor)
        loader = DatabaseLoader(etl_config, monitor)

        # Separar por tipo de cambio
        updates = [e for e in events if e.change_type not in ("deletion", "association")]
        deletions = [e for e in events if e.change_type == "deletion"]
        associations = [e for e in events if e.change_type == "association"]

        # Obtener propiedades y smart mapping una sola vez por object_type
        properties, prop_types = extractor.get_properties_with_types()
        col_map = extractor.get_smart_mapping(properties)

        if updates:
            self._process_updates(extractor, loader, monitor, etl_config, updates, summary, properties, prop_types, col_map)

        if deletions:
            self._process_deletions(loader, deletions, summary)

        if associations:
            self._process_associations(extractor, loader, monitor, etl_config, associations, summary, properties, prop_types, col_map)

    def _process_updates(self, extractor, loader, monitor, config, events, summary,
                         properties, prop_types, col_map) -> None:
        """Fetch, transform, y upsert records actualizados/creados."""
        object_ids = [e.object_id for e in events]
        url = f"{extractor.BASE_URL}/objects/{config.object_type}/batch/read"
        body = {
            "inputs": [{"id": str(oid)} for oid in object_ids],
            "properties": properties,
        }
        response = extractor.safe_request("POST", url, json=body)
        records = response.json()["results"]

        df, column_mapping = process_batch(
            records, col_map, prop_types, monitor, config.table_name,
        )

        loader.sync_schema(df, prop_types, column_mapping)
        loader.upsert_records(df)
        summary["processed"] += len(df)

    def _process_deletions(self, loader, events, summary) -> None:
        """Marca registros como eliminados (soft delete)."""
        ids = [e.object_id for e in events]
        deleted = loader.mark_records_as_deleted(ids)
        summary["deleted"] += deleted

    def _process_associations(self, extractor, loader, monitor, config, events, summary,
                              properties, prop_types, col_map) -> None:
        """
        Procesa cambios de asociación re-sincronizando los registros afectados.

        Cuando una asociación cambia entre A y B, re-fetch ambos registros
        con sus asociaciones actualizadas desde HubSpot.
        """
        # Recolectar IDs únicos de objetos afectados (from_object_id)
        # objectId es el "from" en association events
        affected_ids = set()
        for event in events:
            if event.object_id:
                affected_ids.add(event.object_id)

        if not affected_ids:
            logger.warning("Association events sin object_id válido: %d eventos", len(events))
            return

        logger.info(
            "Procesando %d eventos de asociación para %d %s únicos",
            len(events), len(affected_ids), config.object_type,
        )

        # Re-fetch los registros afectados (con asociaciones actualizadas)
        url = f"{extractor.BASE_URL}/objects/{config.object_type}/batch/read"

        # Incluir associations en el fetch
        associations_param = ",".join(config.associations) if config.associations else None
        body = {
            "inputs": [{"id": str(oid)} for oid in affected_ids],
            "properties": properties,
        }
        if associations_param:
            body["associations"] = associations_param.split(",")

        response = extractor.safe_request("POST", url, json=body)
        records = response.json()["results"]

        # Transform y upsert
        df, column_mapping = process_batch(
            records, col_map, prop_types, monitor, config.table_name,
        )

        loader.sync_schema(df, prop_types, column_mapping)
        loader.upsert_records(df)

        # Re-sync associations para estos registros
        if config.associations:
            loader.flush_associations(df, records)

        summary["processed"] += len(df)
        logger.info(
            "Association sync completado: %d registros actualizados",
            len(df),
        )
