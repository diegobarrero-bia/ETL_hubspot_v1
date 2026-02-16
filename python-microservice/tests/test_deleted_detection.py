"""Tests para la detección de registros eliminados (fivetran_deleted)."""
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

from etl.database import DatabaseLoader
from etl.hubspot import HubSpotExtractor


# -----------------------------------------------------------------
# Tests DatabaseLoader: reconcile_deleted_records
# -----------------------------------------------------------------

class TestReconcileDeletedRecords:
    """Tests para la reconciliación de registros eliminados en full load."""

    def _make_loader(self, fake_config, monitor):
        with patch.object(DatabaseLoader, '_create_engine', return_value=MagicMock()):
            return DatabaseLoader(fake_config, monitor)

    def test_reconcile_marks_missing_ids(self, fake_config, monitor):
        """IDs en PG que no vinieron en el full load deben marcarse como eliminados."""
        loader = self._make_loader(fake_config, monitor)

        # Simular que se cargaron IDs 1 y 3 (pero no 2)
        loader._loaded_ids = {1, 3}

        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1  # 1 registro marcado como eliminado

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        loader.engine.raw_connection.return_value = mock_conn

        count = loader.reconcile_deleted_records()

        assert count == 1
        assert monitor.metrics['records_deleted'] == 1

        # Verificar que se ejecutó el UPDATE con los IDs correctos
        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert 'fivetran_deleted' in sql
        assert 'ALL' in sql
        assert set(params[0]) == {1, 3}

    def test_reconcile_empty_loaded_ids_is_noop(self, fake_config, monitor):
        """Sin IDs cargados, no debe ejecutar UPDATE."""
        loader = self._make_loader(fake_config, monitor)
        loader._loaded_ids = set()

        count = loader.reconcile_deleted_records()

        assert count == 0
        assert monitor.metrics['records_deleted'] == 0
        loader.engine.raw_connection.assert_not_called()

    def test_upsert_tracks_loaded_ids(self, fake_config, monitor):
        """upsert_records debe acumular IDs en _loaded_ids."""
        loader = self._make_loader(fake_config, monitor)

        df = pd.DataFrame({
            'hs_object_id': [101, 102, 103],
            'name': ['a', 'b', 'c'],
        })

        # Mock execute_values para evitar que psycopg2 intente codificar el SQL
        with patch('etl.database.execute_values'):
            mock_conn = MagicMock()
            loader.engine.raw_connection.return_value = mock_conn
            loader.upsert_records(df)

        assert loader._loaded_ids == {101, 102, 103}


# -----------------------------------------------------------------
# Tests DatabaseLoader: mark_records_as_deleted
# -----------------------------------------------------------------

class TestMarkRecordsAsDeleted:
    """Tests para marcar IDs específicos como eliminados (incremental)."""

    def _make_loader(self, fake_config, monitor):
        with patch.object(DatabaseLoader, '_create_engine', return_value=MagicMock()):
            return DatabaseLoader(fake_config, monitor)

    def test_marks_specific_ids(self, fake_config, monitor):
        """mark_records_as_deleted debe hacer UPDATE con los IDs dados."""
        loader = self._make_loader(fake_config, monitor)

        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader.engine.raw_connection.return_value = mock_conn

        count = loader.mark_records_as_deleted([201, 202])

        assert count == 2
        assert monitor.metrics['records_deleted'] == 2

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert 'fivetran_deleted' in sql
        assert 'ANY' in sql
        assert params[0] == [201, 202]

    def test_empty_list_is_noop(self, fake_config, monitor):
        """Lista vacía no debe ejecutar UPDATE."""
        loader = self._make_loader(fake_config, monitor)

        count = loader.mark_records_as_deleted([])

        assert count == 0
        loader.engine.raw_connection.assert_not_called()


# -----------------------------------------------------------------
# Tests HubSpotExtractor: fetch_archived_record_ids
# -----------------------------------------------------------------

class TestFetchArchivedRecordIds:
    """Tests para la obtención de registros archivados."""

    def _make_extractor(self, fake_config, monitor):
        return HubSpotExtractor(fake_config, monitor)

    @patch.object(HubSpotExtractor, 'safe_request')
    def test_filters_by_timestamp(self, mock_request, fake_config, monitor):
        """Solo debe retornar IDs archivados después del timestamp."""
        extractor = self._make_extractor(fake_config, monitor)

        mock_request.return_value = MagicMock(json=MagicMock(return_value={
            'results': [
                {
                    'id': '101',
                    'archived': True,
                    'archivedAt': '2024-06-10T12:00:00Z',
                },
                {
                    'id': '102',
                    'archived': True,
                    'archivedAt': '2024-06-01T08:00:00Z',  # Before threshold
                },
                {
                    'id': '103',
                    'archived': True,
                    'archivedAt': '2024-06-10T15:00:00Z',
                },
            ],
            'paging': None,
        }))

        ids = extractor.fetch_archived_record_ids('2024-06-05T00:00:00+00:00')

        assert ids == [101, 103]

    @patch.object(HubSpotExtractor, 'safe_request')
    def test_handles_unsupported_custom_object(self, mock_request, fake_config, monitor):
        """Custom objects que no soportan archived=true deben retornar [] sin error."""
        extractor = self._make_extractor(fake_config, monitor)

        mock_request.side_effect = Exception("400 Bad Request")

        ids = extractor.fetch_archived_record_ids('2024-06-05T00:00:00+00:00')

        assert ids == []

    @patch.object(HubSpotExtractor, 'safe_request')
    def test_paginates_through_results(self, mock_request, fake_config, monitor):
        """Debe paginar hasta que no haya más resultados."""
        extractor = self._make_extractor(fake_config, monitor)

        page1 = MagicMock(json=MagicMock(return_value={
            'results': [
                {'id': '101', 'archived': True, 'archivedAt': '2024-06-10T12:00:00Z'},
            ],
            'paging': {'next': {'after': '101'}},
        }))
        page2 = MagicMock(json=MagicMock(return_value={
            'results': [
                {'id': '102', 'archived': True, 'archivedAt': '2024-06-10T13:00:00Z'},
            ],
            'paging': None,
        }))

        mock_request.side_effect = [page1, page2]

        ids = extractor.fetch_archived_record_ids('2024-06-01T00:00:00+00:00')

        assert ids == [101, 102]
        assert mock_request.call_count == 2
