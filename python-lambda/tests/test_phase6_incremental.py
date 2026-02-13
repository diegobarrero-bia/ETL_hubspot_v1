"""Tests para Fase 6: Carga incremental con Search API."""
import json
from unittest.mock import MagicMock, patch

import pytest
import responses

from etl.hubspot import HubSpotExtractor
from etl.database import DatabaseLoader


@pytest.fixture
def extractor(fake_config, monitor):
    return HubSpotExtractor(fake_config, monitor)


@pytest.fixture
def loader(fake_config, monitor):
    with patch.object(DatabaseLoader, '_create_engine', return_value=MagicMock()):
        return DatabaseLoader(fake_config, monitor)


class TestSearchModifiedRecords:
    @responses.activate
    def test_builds_correct_filter(self, extractor):
        """Request debe tener filtro GTE con timestamp en ms."""
        url = "https://api.hubapi.com/crm/v3/objects/contacts/search"

        def callback(request):
            body = json.loads(request.body)
            filters = body["filterGroups"][0]["filters"]
            assert len(filters) == 1
            assert filters[0]["propertyName"] == "hs_lastmodifieddate"
            assert filters[0]["operator"] == "GTE"
            # Debe ser un string numérico (timestamp en ms)
            assert filters[0]["value"].isdigit()
            return (200, {}, json.dumps({"results": [], "total": 0}))

        responses.add_callback(responses.POST, url, callback=callback)

        extractor.search_modified_records(
            properties=["firstname"],
            since_timestamp="2024-06-01T10:00:00+00:00",
        )

    @responses.activate
    def test_exceeds_10k_triggers_fallback(self, extractor):
        """Si total > 10000, retorna ([], True)."""
        url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
        responses.add(
            responses.POST, url,
            json={"results": [{"id": "1"}], "total": 15000},
            status=200,
        )

        records, exceeded = extractor.search_modified_records(
            properties=["firstname"],
            since_timestamp="2024-06-01T10:00:00+00:00",
        )

        assert exceeded is True
        assert records == []

    @responses.activate
    def test_returns_all_paginated_results(self, extractor):
        """Debe paginar y devolver todos los resultados."""
        url = "https://api.hubapi.com/crm/v3/objects/contacts/search"

        # Página 1
        responses.add(
            responses.POST, url,
            json={
                "results": [{"id": "1"}, {"id": "2"}],
                "total": 4,
                "paging": {"next": {"after": "2"}},
            },
            status=200,
        )
        # Página 2
        responses.add(
            responses.POST, url,
            json={
                "results": [{"id": "3"}, {"id": "4"}],
                "total": 4,
            },
            status=200,
        )

        records, exceeded = extractor.search_modified_records(
            properties=["firstname"],
            since_timestamp="2024-06-01T10:00:00+00:00",
        )

        assert exceeded is False
        assert len(records) == 4

    @responses.activate
    def test_no_results_returns_empty(self, extractor):
        """Sin resultados retorna lista vacía y exceeded=False."""
        url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
        responses.add(
            responses.POST, url,
            json={"results": [], "total": 0},
            status=200,
        )

        records, exceeded = extractor.search_modified_records(
            properties=["firstname"],
            since_timestamp="2024-06-01T10:00:00+00:00",
        )

        assert exceeded is False
        assert records == []


class TestMetadataTable:
    def test_initialize_creates_table(self, loader):
        """initialize_metadata_table ejecuta CREATE TABLE."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_conn

        loader.initialize_metadata_table()

        mock_conn.execute.assert_called_once()
        sql = str(mock_conn.execute.call_args[0][0])
        assert "etl_sync_metadata" in sql
        assert "CREATE TABLE" in sql

    def test_get_last_sync_returns_none_when_empty(self, loader):
        """Sin registros en metadata, retorna None."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result
        loader.engine.connect.return_value = mock_conn

        result = loader.get_last_sync_timestamp()
        assert result is None

    def test_update_metadata_upserts(self, loader):
        """update_sync_metadata ejecuta INSERT ON CONFLICT."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_conn

        loader.update_sync_metadata("2024-06-01T10:00:00+00:00", 500, "full")

        sql = str(mock_conn.execute.call_args[0][0])
        assert "INSERT INTO" in sql
        assert "ON CONFLICT" in sql
        assert "etl_sync_metadata" in sql


class TestForceFullLoad:
    def test_config_has_force_full_load(self, fake_config):
        """ETLConfig debe tener force_full_load=False por defecto."""
        assert fake_config.force_full_load is False

    def test_config_force_full_load_true(self):
        """ETLConfig acepta force_full_load=True."""
        from etl.config import ETLConfig
        config = ETLConfig(
            object_type="contacts",
            access_token="token",
            db_host="localhost",
            db_port="5432",
            db_name="db",
            db_user="user",
            db_pass="pass",
            force_full_load=True,
        )
        assert config.force_full_load is True


class TestFlushAssociationsMode:
    def test_full_mode_does_truncate(self, loader):
        """mode='full' ejecuta TRUNCATE."""
        import pandas as pd
        batch = {"companies": pd.DataFrame({
            "contacts_id": [101],
            "companies_id": [201],
            "type_id": ["1"],
            "category": ["HUBSPOT_DEFINED"],
            "fivetran_synced": ["2024-01-01"],
        })}
        loader.accumulate_associations(batch)

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_conn

        with patch.object(pd.DataFrame, 'to_sql'):
            loader.flush_associations(mode="full")

        sql_calls = [str(c[0][0]) for c in mock_conn.execute.call_args_list]
        assert any("TRUNCATE" in s for s in sql_calls)

    def test_incremental_mode_no_truncate(self, loader):
        """mode='incremental' NO ejecuta TRUNCATE."""
        import pandas as pd
        batch = {"companies": pd.DataFrame({
            "contacts_id": [101],
            "companies_id": [201],
            "type_id": ["1"],
            "category": ["HUBSPOT_DEFINED"],
            "fivetran_synced": ["2024-01-01"],
        })}
        loader.accumulate_associations(batch)

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_conn

        # Mock raw_connection para el modo incremental
        mock_cursor = MagicMock()
        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_raw_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader.engine.raw_connection.return_value = mock_raw_conn

        with patch("etl.database.execute_values"):
            loader.flush_associations(mode="incremental")

        # Verificar que CREATE TABLE se ejecutó pero NO TRUNCATE
        sql_calls = [str(c[0][0]) for c in mock_conn.execute.call_args_list]
        assert any("CREATE TABLE" in s for s in sql_calls)
        assert not any("TRUNCATE" in s for s in sql_calls)
