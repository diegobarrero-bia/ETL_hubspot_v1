"""Extracción de datos desde la API de HubSpot."""
import json
import logging
import time

import requests

from etl.config import ETLConfig
from etl.monitor import ETLMonitor

logger = logging.getLogger(__name__)


class HubSpotExtractor:
    """Maneja todas las interacciones con la API de HubSpot."""

    BASE_URL = "https://api.hubapi.com/crm/v3"

    def __init__(self, config: ETLConfig, monitor: ETLMonitor):
        self.config = config
        self.monitor = monitor
        self.headers = config.headers

    # -----------------------------------------------------------------
    # Request con reintentos
    # -----------------------------------------------------------------

    def safe_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Ejecuta un request HTTP con reintentos y manejo de errores."""
        max_retries = 3
        backoff = 5
        self.monitor.increment('api_calls')

        for attempt in range(1, max_retries + 1):
            try:
                res = requests.request(method, url, headers=self.headers, **kwargs)

                if res.status_code == 200:
                    return res

                elif res.status_code == 400:
                    error_detail = ""
                    try:
                        error_detail = json.dumps(res.json(), indent=2)
                    except Exception:
                        error_detail = res.text

                    logger.error("ERROR 400 Bad Request en %s", url)
                    logger.error("   Método: %s", method)
                    logger.error("   Parámetros: %s", kwargs)
                    logger.error("   Respuesta:\n%s", error_detail)

                    raise Exception(
                        f"Bad Request (400) en {url}\n"
                        f"Detalles: {error_detail[:500]}..."
                    )

                elif res.status_code == 414:
                    raise Exception(
                        "URL Too Long (414). Demasiadas propiedades solicitadas en GET."
                    )

                elif res.status_code == 429:
                    self.monitor.increment('retries_429')
                    time.sleep(10)

                elif 500 <= res.status_code < 600:
                    self.monitor.increment('retries_5xx')
                    time.sleep(backoff * attempt)

                else:
                    res.raise_for_status()

            except requests.exceptions.ConnectionError:
                self.monitor.increment('connection_errors')
                time.sleep(backoff * attempt)
            except Exception:
                raise

        raise Exception(f"Fallo crítico en {url} después de {max_retries} intentos")

    # -----------------------------------------------------------------
    # Propiedades
    # -----------------------------------------------------------------

    def get_properties_with_types(self) -> tuple[list[str], dict[str, str]]:
        """
        Obtiene todas las propiedades con sus tipos de dato desde HubSpot.

        Returns:
            (lista de nombres, diccionario {nombre: tipo_hubspot})
        """
        url = f"{self.BASE_URL}/properties/{self.config.object_type}"
        res = self.safe_request('GET', url)
        properties_data = res.json()['results']

        prop_names = []
        prop_types = {}

        for prop in properties_data:
            name = prop['name']
            hubspot_type = prop.get('type', 'string')
            prop_names.append(name)
            prop_types[name] = hubspot_type

        logger.debug("Tipos capturados para %d propiedades", len(prop_types))
        return prop_names, prop_types

    # -----------------------------------------------------------------
    # Asociaciones
    # -----------------------------------------------------------------

    def get_associations(self) -> list[str]:
        """Obtiene las asociaciones disponibles para el tipo de objeto."""
        url = f"{self.BASE_URL}/schemas/{self.config.object_type}"
        res = self.safe_request('GET', url)
        all_assocs = [
            a['toObjectTypeId']
            for a in res.json().get('associations', [])
        ]

        if len(all_assocs) > 30:
            logger.info(
                "Objeto tiene %d asociaciones. Se procesarán en chunks de 30.",
                len(all_assocs),
            )

        return all_assocs

    # -----------------------------------------------------------------
    # Pipelines
    # -----------------------------------------------------------------

    def get_pipelines(self) -> list[dict]:
        """
        Obtiene los pipelines del tipo de objeto.

        Returns:
            Lista de pipelines (cada uno con sus stages), o lista vacía.
        """
        try:
            url = f"{self.BASE_URL}/pipelines/{self.config.object_type}"
            res = self.safe_request('GET', url)
            return res.json().get('results', [])
        except Exception as e:
            logger.warning(
                "No se pudieron obtener pipelines para '%s': %s",
                self.config.object_type, e,
            )
            return []

    # -----------------------------------------------------------------
    # Mapeo inteligente de columnas de pipelines
    # -----------------------------------------------------------------

    def get_smart_mapping(self, all_props: list[str]) -> dict[str, str]:
        """
        Simplifica nombres de columnas de propiedades de pipeline/stages
        eliminando el sufijo numérico que HubSpot agrega.

        Convierte:
          hs_v2_date_entered_600b692d_a3fe_4052_9cd7_278b134d7941_2005647967
        A:
          hs_v2_date_entered_600b692d_a3fe_4052_9cd7_278b134d7941
        """
        pipelines = self.get_pipelines()
        if not pipelines:
            return {}

        mapping: dict[str, str] = {}

        prefixes = [
            "hs_v2_cumulative_time_in",
            "hs_v2_date_entered",
            "hs_v2_date_exited",
            "hs_v2_latest_time_in",
            "hs_time_in",
            "hs_date_exited",
            "hs_date_entered",
        ]

        for pipe in pipelines:
            for stage in pipe.get('stages', []):
                s_id = stage['id']
                s_id_normalized = s_id.replace("-", "_")

                for prop in all_props:
                    for prefix in prefixes:
                        if prop.startswith(prefix) and (
                            s_id in prop or s_id_normalized in prop
                        ):
                            new_name = f"{prefix}_{s_id_normalized}"
                            mapping[prop] = new_name
                            break

        return mapping

    # -----------------------------------------------------------------
    # Descarga de registros
    # -----------------------------------------------------------------

    def fetch_records_generator(
        self, properties: list[str], associations: list[str]
    ):
        """
        Generador que descarga registros usando el endpoint LIST (GET).
        Maneja paginación automáticamente.
        """
        url = f"{self.BASE_URL}/objects/{self.config.object_type}"

        properties_str = ",".join(properties)
        associations_str = ",".join(associations) if associations else ""

        # Validar longitud URL
        estimated_url_length = len(url) + len(properties_str) + len(associations_str) + 100
        if estimated_url_length > 8000:
            logger.warning(
                "URL demasiado larga (%d chars). Podría fallar con error 414.",
                estimated_url_length,
            )

        params = {
            "limit": 100,
            "properties": properties_str,
            "associations": associations_str,
            "archived": "false",
        }

        logger.info(
            "Iniciando descarga GET - %s (props: %d, assocs: %d)",
            self.config.object_type, len(properties), len(associations),
        )

        after = None
        while True:
            if after:
                params['after'] = after

            try:
                res = self.safe_request('GET', url, params=params)
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
                logger.error("Error en fetch_records_generator, after=%s", after)
                raise e

    def fetch_all_records_with_chunked_assocs(
        self, properties: list[str], associations: list[str]
    ):
        """
        Descarga todos los registros manejando el límite de 30 asociaciones.
        Si hay más de 30, hace múltiples llamadas y combina resultados.
        """
        chunk_size = 30

        if len(associations) <= chunk_size:
            for batch in self.fetch_records_generator(properties, associations):
                yield batch
            return

        assoc_chunks = [
            associations[i:i + chunk_size]
            for i in range(0, len(associations), chunk_size)
        ]

        logger.info(
            "Dividiendo %d asociaciones en %d chunks",
            len(associations), len(assoc_chunks),
        )

        all_records_dict: dict = {}

        for chunk_idx, assoc_chunk in enumerate(assoc_chunks):
            logger.info(
                "Procesando chunk %d/%d de asociaciones (%d assocs)",
                chunk_idx + 1, len(assoc_chunks), len(assoc_chunk),
            )

            for batch in self.fetch_records_generator(properties, assoc_chunk):
                for record in batch:
                    record_id = record['id']

                    if record_id not in all_records_dict:
                        all_records_dict[record_id] = record
                    else:
                        # Combinar asociaciones
                        existing = all_records_dict[record_id]
                        existing_assocs = existing.get('associations', {})
                        new_assocs = record.get('associations', {})

                        for assoc_type, assoc_data in new_assocs.items():
                            if assoc_type not in existing_assocs:
                                existing_assocs[assoc_type] = assoc_data
                            else:
                                existing_results = existing_assocs[assoc_type].get('results', [])
                                new_results = assoc_data.get('results', [])
                                existing_ids = {
                                    r['id'] for r in existing_results if 'id' in r
                                }
                                for new_result in new_results:
                                    if 'id' in new_result and new_result['id'] not in existing_ids:
                                        existing_results.append(new_result)
                                existing_assocs[assoc_type]['results'] = existing_results

                        existing['associations'] = existing_assocs

        # Entregar en lotes de 100
        all_records_list = list(all_records_dict.values())
        batch_size = 100

        logger.info(
            "Asociaciones combinadas. Entregando %d registros únicos.",
            len(all_records_list),
        )

        for i in range(0, len(all_records_list), batch_size):
            yield all_records_list[i:i + batch_size]
