"""Tests para Fase 3: Desacoplar fetch de asociaciones con API v4."""
import json

import pytest
import responses

from etl.hubspot import HubSpotExtractor


@pytest.fixture
def extractor(fake_config, monitor):
    return HubSpotExtractor(fake_config, monitor)


def _register_list_endpoint(object_type, records, page_size=100):
    """Registra mock del endpoint GET /objects/{type} con paginación."""
    base_url = f"https://api.hubapi.com/crm/v3/objects/{object_type}"

    for i in range(0, len(records), page_size):
        batch = records[i:i + page_size]
        has_next = i + page_size < len(records)

        body = {"results": batch}
        if has_next:
            body["paging"] = {"next": {"after": str(i + page_size)}}

        responses.add(
            responses.GET,
            base_url,
            json=body,
            status=200,
        )


def _register_v4_assoc_endpoint(from_type, to_type, assoc_map):
    """Registra mock del endpoint POST v4 batch associations."""
    url = f"https://api.hubapi.com/crm/v4/associations/{from_type}/{to_type}/batch/read"

    def callback(request):
        body = json.loads(request.body)
        input_ids = [inp["id"] for inp in body.get("inputs", [])]

        results = []
        for from_id in input_ids:
            if from_id in assoc_map:
                to_entries = []
                for to_id, type_id in assoc_map[from_id]:
                    to_entries.append({
                        "toObjectId": int(to_id),
                        "associationTypes": [
                            {"category": "HUBSPOT_DEFINED", "typeId": int(type_id)}
                        ],
                    })
                results.append({"from": {"id": from_id}, "to": to_entries})

        return (200, {}, json.dumps({"results": results}))

    responses.add_callback(responses.POST, url, callback=callback)


class TestFetchAssociationsBatch:
    @responses.activate
    def test_chunks_at_1000(self, extractor):
        """2500 IDs deben generar 3 POSTs (1000 + 1000 + 500)."""
        url = "https://api.hubapi.com/crm/v4/associations/contacts/companies/batch/read"
        responses.add(responses.POST, url, json={"results": []}, status=200)

        ids = [str(i) for i in range(2500)]
        extractor.fetch_associations_batch(ids, "contacts", "companies")

        assert len(responses.calls) == 3

        # Verificar tamaños de cada chunk
        for i, call_obj in enumerate(responses.calls):
            body = json.loads(call_obj.request.body)
            if i < 2:
                assert len(body["inputs"]) == 1000
            else:
                assert len(body["inputs"]) == 500

    @responses.activate
    def test_normalizes_v4_to_v3_format(self, extractor):
        """Respuesta v4 debe normalizarse al formato {id, type, category}."""
        url = "https://api.hubapi.com/crm/v4/associations/contacts/companies/batch/read"

        v4_response = {
            "results": [
                {
                    "from": {"id": "101"},
                    "to": [
                        {
                            "toObjectId": 201,
                            "associationTypes": [
                                {"category": "HUBSPOT_DEFINED", "typeId": 1},
                                {"category": "USER_DEFINED", "typeId": 42},
                            ],
                        },
                    ],
                },
            ],
        }
        responses.add(responses.POST, url, json=v4_response, status=200)

        result = extractor.fetch_associations_batch(["101"], "contacts", "companies")

        assert "101" in result
        assocs = result["101"]
        assert len(assocs) == 2
        assert assocs[0] == {"id": "201", "type": "1", "category": "HUBSPOT_DEFINED"}
        assert assocs[1] == {"id": "201", "type": "42", "category": "USER_DEFINED"}

    @responses.activate
    def test_empty_results_returns_empty_dict(self, extractor):
        """Sin asociaciones, retorna dict vacío."""
        url = "https://api.hubapi.com/crm/v4/associations/contacts/companies/batch/read"
        responses.add(responses.POST, url, json={"results": []}, status=200)

        result = extractor.fetch_associations_batch(["101"], "contacts", "companies")
        assert result == {}


class TestFetchRecordsThenAssociations:
    @responses.activate
    def test_records_fetched_once_then_associations_added(self, extractor):
        """Registros se descargan 1 vez, asociaciones se agregan después."""
        records = [
            {"id": "101", "properties": {"firstname": "Juan"}, "archived": False},
            {"id": "102", "properties": {"firstname": "María"}, "archived": False},
        ]

        # Mock GET list endpoint (records without associations)
        _register_list_endpoint("contacts", records)

        # Mock v4 associations
        _register_v4_assoc_endpoint("contacts", "companies", {
            "101": [("201", "1")],
            "102": [("202", "1")],
        })

        batches = list(extractor.fetch_records_then_associations(
            properties=["firstname"], associations=["companies"]
        ))

        # 1 batch de 2 registros
        assert len(batches) == 1
        batch = batches[0]
        assert len(batch) == 2

        # Verificar que asociaciones se mergearon
        r101 = next(r for r in batch if r["id"] == "101")
        assert "companies" in r101["associations"]
        assocs = r101["associations"]["companies"]["results"]
        assert len(assocs) == 1
        assert assocs[0]["id"] == "201"

    @responses.activate
    def test_no_associations_skips_v4(self, extractor):
        """Sin asociaciones, no se llama al endpoint v4."""
        records = [{"id": "101", "properties": {"firstname": "Juan"}, "archived": False}]
        _register_list_endpoint("contacts", records)

        batches = list(extractor.fetch_all_records_with_chunked_assocs(
            properties=["firstname"], associations=[]
        ))

        assert len(batches) == 1
        # Solo 1 llamada GET (list), 0 POSTs v4
        assert len(responses.calls) == 1
        assert responses.calls[0].request.method == "GET"

    @responses.activate
    def test_multiple_association_types(self, extractor):
        """Múltiples tipos de asociación se manejan correctamente."""
        records = [
            {"id": "101", "properties": {"firstname": "Juan"}, "archived": False},
        ]
        _register_list_endpoint("contacts", records)

        # companies
        _register_v4_assoc_endpoint("contacts", "companies", {
            "101": [("201", "1")],
        })
        # deals
        _register_v4_assoc_endpoint("contacts", "deals", {
            "101": [("301", "3")],
        })

        batches = list(extractor.fetch_records_then_associations(
            properties=["firstname"],
            associations=["companies", "deals"],
        ))

        record = batches[0][0]
        assert "companies" in record["associations"]
        assert "deals" in record["associations"]
        assert record["associations"]["companies"]["results"][0]["id"] == "201"
        assert record["associations"]["deals"]["results"][0]["id"] == "301"

    @responses.activate
    def test_merged_structure_compatible_with_transform(self, extractor, monitor):
        """Estructura mergeada es compatible con extract_normalized_associations."""
        from etl.transform import extract_normalized_associations

        records = [
            {"id": "101", "properties": {"firstname": "Juan"}, "archived": False},
        ]
        _register_list_endpoint("contacts", records)
        _register_v4_assoc_endpoint("contacts", "companies", {
            "101": [("201", "1")],
        })

        batches = list(extractor.fetch_records_then_associations(
            properties=["firstname"], associations=["companies"]
        ))

        # extract_normalized_associations debe poder procesar el batch
        result = extract_normalized_associations(batches[0], "contacts", monitor)

        assert "companies_contacts" in result
        df = result["companies_contacts"]
        assert len(df) == 1
        assert df.iloc[0]["contacts_id"] == 101
