"""Extracción de datos desde la API de HubSpot."""
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self._assoc_name_map: dict[str, str] = {}
        self._schema_object_name: str | None = None

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

                if res.status_code in (200, 207):
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

        EXCLUDED_TYPES = {'object_coordinates'}

        prop_names = []
        prop_types = {}
        excluded_count = 0

        for prop in properties_data:
            name = prop['name']
            hubspot_type = prop.get('type', 'string')

            if hubspot_type in EXCLUDED_TYPES:
                excluded_count += 1
                logger.debug("Propiedad '%s' excluida (tipo: %s)", name, hubspot_type)
                continue

            prop_names.append(name)
            prop_types[name] = hubspot_type

        if excluded_count:
            logger.warning(
                "Propiedades excluidas por tipo no soportado (object_coordinates): %d de %d totales",
                excluded_count, len(properties_data),
            )

        logger.debug("Tipos capturados para %d propiedades", len(prop_types))
        return prop_names, prop_types

    # -----------------------------------------------------------------
    # Asociaciones
    # -----------------------------------------------------------------

    def get_associations(self) -> list[str]:
        """Obtiene las asociaciones disponibles (deduplicadas) y resuelve nombres."""
        url = f"{self.BASE_URL}/schemas/{self.config.object_type}"
        res = self.safe_request('GET', url)
        schema_data = res.json()

        all_assocs = list(set(
            a['toObjectTypeId']
            for a in schema_data.get('associations', [])
        ))

        if len(all_assocs) > 30:
            logger.info(
                "Objeto tiene %d asociaciones únicas.",
                len(all_assocs),
            )

        # Resolver IDs a nombres legibles
        self._resolve_assoc_names(schema_data, all_assocs)

        return all_assocs

    def _resolve_assoc_names(
        self, schema_data: dict, type_ids: list[str]
    ) -> None:
        """
        Resuelve objectTypeId → nombre legible usando 3 estrategias:
        1. Parseo del campo 'name' de las asociaciones del schema (0 API calls)
        2. GET /crm/v3/schemas para objetos custom (1 API call)
        3. GET /crm/v3/schemas/{id} individual para no resueltos (paralelo)
        """
        # Estrategia 1: Parsear nombres de asociación del schema
        our_type_id = schema_data.get('objectTypeId', '')
        our_name = schema_data.get('name', '').upper()

        # Guardar nombre del schema como nombre base para tablas
        if our_name:
            self._schema_object_name = our_name.lower()

        if our_name:
            prefix = f"{our_name}_TO_"
            suffix = f"_TO_{our_name}"

            for assoc in schema_data.get('associations', []):
                from_id = assoc.get('fromObjectTypeId', '')
                to_id = assoc.get('toObjectTypeId', '')
                assoc_name = assoc.get('name', '').upper()

                if from_id == our_type_id and assoc_name.startswith(prefix):
                    target_name = assoc_name[len(prefix):].lower()
                    if target_name:
                        self._assoc_name_map[to_id] = target_name
                elif to_id == our_type_id and assoc_name.endswith(suffix):
                    source_name = assoc_name[:-len(suffix)].lower()
                    if source_name:
                        self._assoc_name_map[from_id] = source_name

        # Estrategia 2: GET /crm/v3/schemas para custom objects
        remaining = [tid for tid in type_ids if tid not in self._assoc_name_map]
        if remaining:
            try:
                schemas_url = f"{self.BASE_URL}/schemas"
                schemas_res = self.safe_request('GET', schemas_url)
                for schema in schemas_res.json().get('results', []):
                    oid = schema.get('objectTypeId', '')
                    name = schema.get('name', '').lower()
                    if oid and name:
                        self._assoc_name_map[oid] = name
            except Exception as e:
                logger.warning("Error obteniendo schemas para nombres: %s", e)

        # Estrategia 3: Resolución individual para no resueltos
        still_remaining = [tid for tid in type_ids if tid not in self._assoc_name_map]
        if still_remaining:
            logger.info(
                "Resolviendo %d tipos de asociación por API individual",
                len(still_remaining),
            )

            def _resolve_one(type_id):
                try:
                    schema_url = f"{self.BASE_URL}/schemas/{type_id}"
                    res = self.safe_request('GET', schema_url)
                    return type_id, res.json().get('name', '').lower()
                except Exception:
                    return type_id, None

            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                futures = [executor.submit(_resolve_one, tid) for tid in still_remaining]
                for future in as_completed(futures):
                    tid, name = future.result()
                    if name:
                        self._assoc_name_map[tid] = name

        resolved = sum(1 for tid in type_ids if tid in self._assoc_name_map)
        logger.info(
            "Nombres de asociación resueltos: %d/%d",
            resolved, len(type_ids),
        )

    def get_assoc_name(self, type_id: str) -> str:
        """Retorna el nombre legible de un objectTypeId, o el ID normalizado como fallback."""
        return self._assoc_name_map.get(type_id, type_id.replace('-', '_'))

    def get_schema_name(self) -> str:
        """Retorna el nombre del schema HubSpot (e.g., 'service') o fallback a object_type."""
        return self._schema_object_name or self.config.object_type

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

    def get_smart_mapping(self, all_props: list[str], pipelines: list[dict] | None = None) -> dict[str, str]:
        """
        Simplifica nombres de columnas de propiedades de pipeline/stages
        eliminando el sufijo numérico que HubSpot agrega.

        Convierte:
          hs_v2_date_entered_600b692d_a3fe_4052_9cd7_278b134d7941_2005647967
        A:
          hs_v2_date_entered_600b692d_a3fe_4052_9cd7_278b134d7941

        Args:
            all_props: Lista de nombres de propiedades del objeto.
            pipelines: Lista de pipelines ya obtenida. Si es None, se consulta la API.
        """
        if pipelines is None:
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

        # Validar longitud URL
        estimated_url_length = len(url) + len(properties_str) + 100
        if estimated_url_length > 8000:
            logger.warning(
                "URL demasiado larga (%d chars). Podría fallar con error 414.",
                estimated_url_length,
            )

        params = {
            "limit": 100,
            "properties": properties_str,
            "archived": "false",
        }

        # Solo incluir associations si hay (enviar "" causa error 400 en HubSpot)
        if associations:
            params["associations"] = ",".join(associations)

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

    # -----------------------------------------------------------------
    # Asociaciones v4 (batch read)
    # -----------------------------------------------------------------

    def fetch_associations_batch(
        self, object_ids: list[str], from_type: str, to_type: str
    ) -> dict[str, list[dict]]:
        """
        Obtiene asociaciones usando el endpoint batch v4.

        POST /crm/v4/associations/{fromObjectType}/{toObjectType}/batch/read
        Máximo 1000 IDs por request.

        Returns:
            dict {from_id: [{"id": str, "type": str, "category": str}, ...]}
        """
        url = f"https://api.hubapi.com/crm/v4/associations/{from_type}/{to_type}/batch/read"
        result: dict[str, list[dict]] = {}

        chunk_size = 1000
        for i in range(0, len(object_ids), chunk_size):
            chunk = object_ids[i:i + chunk_size]
            body = {"inputs": [{"id": oid} for oid in chunk]}

            try:
                res = self.safe_request('POST', url, json=body)
                data = res.json()

                for item in data.get('results', []):
                    from_obj = item.get('from', {})
                    from_id = str(from_obj.get('id', ''))
                    to_list = item.get('to', [])

                    if not from_id:
                        continue

                    if from_id not in result:
                        result[from_id] = []

                    # Normalizar formato v4 a v3 para compatibilidad con
                    # extract_normalized_associations en transform.py
                    for to_item in to_list:
                        to_id = to_item.get('toObjectId')
                        for assoc_type_info in to_item.get('associationTypes', []):
                            normalized = {
                                'id': str(to_id),
                                'type': str(assoc_type_info.get('typeId', '')),
                                'category': assoc_type_info.get(
                                    'category', 'HUBSPOT_DEFINED'
                                ),
                            }
                            result[from_id].append(normalized)

            except Exception as e:
                logger.error(
                    "Error en batch associations %s -> %s (chunk %d): %s",
                    from_type, to_type, i // chunk_size + 1, e,
                )
                raise

        return result

    # -----------------------------------------------------------------
    # Estrategia optimizada: registros + asociaciones separadas
    # -----------------------------------------------------------------

    def fetch_records_then_associations(
        self, properties: list[str], associations: list[str]
    ):
        """
        Estrategia en 2 fases:
        1. Descarga registros UNA sola vez (solo propiedades, sin asociaciones).
        2. Para cada tipo de asociación, batch-fetch via API v4.
        3. Merge y yield en batches de 100.
        """
        # Fase 1: Registros sin asociaciones
        all_records: dict[str, dict] = {}
        for batch in self.fetch_records_generator(properties, associations=[]):
            for record in batch:
                all_records[record['id']] = record

        logger.info(
            "Fase 1 completada: %d registros descargados (solo propiedades)",
            len(all_records),
        )

        if not all_records:
            return

        # Fase 2: Asociaciones por tipo via API v4 (en paralelo)
        object_ids = list(all_records.keys())
        max_workers = min(self.config.max_workers, len(associations))

        def _fetch_one_assoc(assoc_type_id):
            return assoc_type_id, self.fetch_associations_batch(
                object_ids, self.config.object_type, assoc_type_id,
            )

        logger.info(
            "Obteniendo %d tipos de asociación con %d workers",
            len(associations), max_workers,
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_one_assoc, at): at
                for at in associations
            }
            for future in as_completed(futures):
                assoc_type_id = futures[future]
                try:
                    _, assoc_map = future.result()

                    # Merge en registros usando nombre legible como key
                    assoc_name = self.get_assoc_name(assoc_type_id)
                    for from_id, to_list in assoc_map.items():
                        if from_id in all_records:
                            record = all_records[from_id]
                            if 'associations' not in record:
                                record['associations'] = {}
                            if assoc_name not in record['associations']:
                                record['associations'][assoc_name] = {'results': []}
                            record['associations'][assoc_name]['results'].extend(to_list)

                except Exception as e:
                    logger.error(
                        "Error obteniendo asociaciones %s -> %s: %s",
                        self.config.object_type, assoc_type_id, e,
                    )

        # Fase 3: Yield en batches de 100
        records_list = list(all_records.values())
        batch_size = 100

        logger.info(
            "Registros con asociaciones combinados. Entregando %d registros.",
            len(records_list),
        )

        for i in range(0, len(records_list), batch_size):
            yield records_list[i:i + batch_size]

    def fetch_all_records_with_chunked_assocs(
        self, properties: list[str], associations: list[str]
    ):
        """
        Punto de entrada principal para descargar registros con asociaciones.
        Usa la estrategia optimizada de 2 fases (fetch separado).
        """
        if not associations:
            for batch in self.fetch_records_generator(properties, associations=[]):
                yield batch
            return

        yield from self.fetch_records_then_associations(properties, associations)

    # -----------------------------------------------------------------
    # Search API (carga incremental)
    # -----------------------------------------------------------------

    def search_modified_records(
        self, properties: list[str], since_timestamp: str
    ) -> tuple[list[dict], bool]:
        """
        Busca registros modificados desde un timestamp usando la Search API.

        POST /crm/v3/objects/{objectType}/search

        Args:
            properties: Lista de propiedades a incluir.
            since_timestamp: Timestamp ISO para filtrar (GTE).

        Returns:
            (lista_de_registros, exceeded_limit)
            exceeded_limit=True si hay >10000 resultados (límite de Search API).
        """
        url = f"{self.BASE_URL}/objects/{self.config.object_type}/search"

        # Convertir ISO timestamp a milisegundos (formato que espera HubSpot)
        from datetime import datetime, timezone
        try:
            dt = datetime.fromisoformat(since_timestamp.replace('+00:00', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts_ms = str(int(dt.timestamp() * 1000))
        except (ValueError, AttributeError):
            ts_ms = since_timestamp

        body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_lastmodifieddate",
                    "operator": "GTE",
                    "value": ts_ms,
                }]
            }],
            "properties": properties,
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"}],
            "limit": 200,
        }

        all_results = []
        after = 0

        while True:
            if after:
                body["after"] = after

            try:
                res = self.safe_request('POST', url, json=body)
                data = res.json()
                total = data.get('total', 0)

                if total > 10000:
                    logger.warning(
                        "Search API: %d resultados excede el límite de 10000. "
                        "Se requiere full load.",
                        total,
                    )
                    return [], True

                results = data.get('results', [])
                all_results.extend(results)

                paging = data.get('paging')
                if paging and 'next' in paging:
                    after = paging['next']['after']
                else:
                    break

            except Exception as e:
                logger.error("Error en Search API: %s", e)
                raise

        logger.info(
            "Search API: %d registros modificados desde %s",
            len(all_results), since_timestamp,
        )
        return all_results, False

    # -----------------------------------------------------------------
    # Detección de archivados (incremental)
    # -----------------------------------------------------------------

    def fetch_archived_record_ids(self, since_timestamp: str) -> list[int]:
        """
        Obtiene IDs de registros archivados desde un timestamp.

        GET /crm/v3/objects/{type}?archived=true&limit=100

        Nota: Custom objects pueden no soportar archived=true.
        En ese caso retorna lista vacía con warning.
        """
        from datetime import datetime, timezone

        try:
            dt = datetime.fromisoformat(since_timestamp.replace('+00:00', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            dt = None

        url = f"{self.BASE_URL}/objects/{self.config.object_type}"
        archived_ids: list[int] = []
        after = None

        try:
            while True:
                params: dict = {"archived": "true", "limit": 100}
                if after:
                    params["after"] = after

                res = self.safe_request('GET', url, params=params)
                data = res.json()
                results = data.get('results', [])

                if not results:
                    break

                for record in results:
                    archived_at_str = record.get('archivedAt')
                    if dt and archived_at_str:
                        try:
                            archived_at = datetime.fromisoformat(
                                archived_at_str.replace('Z', '+00:00')
                            )
                            if archived_at <= dt:
                                continue
                        except (ValueError, AttributeError):
                            pass

                    try:
                        archived_ids.append(int(record['id']))
                    except (KeyError, ValueError):
                        continue

                paging = data.get('paging')
                if paging and paging.get('next', {}).get('after'):
                    after = paging['next']['after']
                else:
                    break

        except Exception as e:
            error_msg = str(e).lower()
            if '400' in error_msg or 'bad request' in error_msg:
                logger.warning(
                    "Objeto '%s' no soporta archived=true (posible custom object): %s",
                    self.config.object_type, e,
                )
                return []
            logger.warning("Error obteniendo registros archivados: %s", e)
            return []

        if archived_ids:
            logger.info(
                "Registros archivados detectados desde %s: %d",
                since_timestamp, len(archived_ids),
            )
        return archived_ids
