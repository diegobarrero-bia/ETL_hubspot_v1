"""Tests para Fase 4: Paralelismo con ThreadPoolExecutor."""
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
import responses

from etl.hubspot import HubSpotExtractor
from etl.monitor import ETLMonitor


@pytest.fixture
def extractor(fake_config, monitor):
    fake_config.max_workers = 3
    return HubSpotExtractor(fake_config, monitor)


def _register_list_endpoint(object_type, records):
    """Mock GET /objects/{type}."""
    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}"
    responses.add(responses.GET, url, json={"results": records}, status=200)


def _register_v4_assoc_endpoint(from_type, to_type, assoc_map):
    """Mock POST v4 batch associations."""
    url = f"https://api.hubapi.com/crm/v4/associations/{from_type}/{to_type}/batch/read"

    def callback(request):
        body = json.loads(request.body)
        input_ids = [inp["id"] for inp in body.get("inputs", [])]
        results = []
        for from_id in input_ids:
            if from_id in assoc_map:
                to_entries = [{
                    "toObjectId": int(to_id),
                    "associationTypes": [{"category": "HUBSPOT_DEFINED", "typeId": int(tid)}],
                } for to_id, tid in assoc_map[from_id]]
                results.append({"from": {"id": from_id}, "to": to_entries})
        return (200, {}, json.dumps({"results": results}))

    responses.add_callback(responses.POST, url, callback=callback)


class TestParallelFetch:
    @responses.activate
    def test_parallel_results_match_sequential(self, fake_config, monitor):
        """Resultados con workers=1 y workers=3 deben ser idénticos."""
        records = [
            {"id": "101", "properties": {"firstname": "Juan"}, "archived": False},
            {"id": "102", "properties": {"firstname": "María"}, "archived": False},
        ]

        assoc_types = ["companies", "deals", "tickets"]
        assoc_data = {
            "companies": {"101": [("201", "1")], "102": [("202", "1")]},
            "deals": {"101": [("301", "3")]},
            "tickets": {"102": [("401", "5")]},
        }

        def _run_with_workers(n_workers):
            responses.reset()
            _register_list_endpoint("contacts", records)
            for atype in assoc_types:
                _register_v4_assoc_endpoint("contacts", atype, assoc_data.get(atype, {}))

            cfg = fake_config
            cfg.max_workers = n_workers
            mon = ETLMonitor("contacts", "hubspot_etl", "contacts")
            ext = HubSpotExtractor(cfg, mon)
            batches = list(ext.fetch_records_then_associations(
                properties=["firstname"], associations=assoc_types,
            ))
            # Extraer asociaciones de cada registro para comparación
            result = {}
            for batch in batches:
                for rec in batch:
                    assocs = {}
                    for k, v in rec.get("associations", {}).items():
                        ids = sorted([r["id"] for r in v.get("results", [])])
                        assocs[k] = ids
                    result[rec["id"]] = assocs
            return result

        sequential = _run_with_workers(1)
        parallel = _run_with_workers(3)

        assert sequential == parallel

    @responses.activate
    def test_all_association_types_fetched(self, extractor):
        """Todos los tipos de asociación se buscan incluso en paralelo."""
        records = [{"id": "101", "properties": {"name": "Test"}, "archived": False}]
        _register_list_endpoint("contacts", records)

        types = ["companies", "deals", "tickets", "calls", "emails"]
        for t in types:
            _register_v4_assoc_endpoint("contacts", t, {"101": [("201", "1")]})

        batches = list(extractor.fetch_records_then_associations(
            properties=["name"], associations=types,
        ))

        record = batches[0][0]
        for t in types:
            assert t in record.get("associations", {}), f"Missing association type: {t}"


class TestMonitorThreadSafety:
    def test_concurrent_increments(self, monitor):
        """100 incrementos paralelos deben dar exactamente 100."""
        def _increment(_):
            monitor.increment('api_calls')

        with ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(_increment, range(100)))

        assert monitor.metrics['api_calls'] == 100
