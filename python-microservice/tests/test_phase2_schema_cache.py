"""Tests para Fase 2: Cache de schema inspection."""
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from etl.database import DatabaseLoader


@pytest.fixture
def loader(fake_config, monitor):
    """DatabaseLoader con engine mockeado."""
    with patch.object(DatabaseLoader, '_create_engine', return_value=MagicMock()):
        return DatabaseLoader(fake_config, monitor)


def _mock_inspector(existing_columns):
    """Crea un inspector mock con columnas existentes."""
    mock_insp = MagicMock()
    mock_insp.has_table.return_value = True
    mock_insp.get_columns.return_value = [{"name": c} for c in existing_columns]
    return mock_insp


class TestSchemaCacheInspection:
    @patch("etl.database.inspect")
    def test_sync_schema_inspects_db_only_once(self, mock_inspect, loader):
        """5 llamadas a sync_schema deben llamar inspect() solo 1 vez."""
        mock_inspect.return_value = _mock_inspector(["hs_object_id", "email", "firstname"])

        df = pd.DataFrame({"hs_object_id": [1], "email": ["a@b.com"], "firstname": ["Juan"]})

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_conn

        for _ in range(5):
            loader.sync_schema(df)

        assert mock_inspect.call_count == 1

    @patch("etl.database.inspect")
    def test_new_column_added_to_cache(self, mock_inspect, loader):
        """Columna nueva se agrega al cache, no se intenta crear dos veces."""
        mock_inspect.return_value = _mock_inspector(["hs_object_id"])

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_conn

        # Batch 1: tiene columna "email" que no existe en BD
        df1 = pd.DataFrame({"hs_object_id": [1], "email": ["a@b.com"]})
        loader.sync_schema(df1)

        # ALTER TABLE debe haberse llamado 1 vez (para "email")
        alter_calls_1 = mock_conn.execute.call_count
        assert alter_calls_1 == 1

        # Batch 2: tiene "email" (ya en cache) + "phone" (nueva)
        mock_conn.execute.reset_mock()
        df2 = pd.DataFrame({"hs_object_id": [2], "email": ["b@c.com"], "phone": ["123"]})
        loader.sync_schema(df2)

        # Solo "phone" debe generar ALTER TABLE (1 llamada)
        assert mock_conn.execute.call_count == 1

    @patch("etl.database.inspect")
    def test_no_new_columns_skips_alter(self, mock_inspect, loader):
        """Si no hay columnas nuevas, no se ejecuta ALTER TABLE."""
        mock_inspect.return_value = _mock_inspector(["hs_object_id", "email"])

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_conn

        df = pd.DataFrame({"hs_object_id": [1], "email": ["a@b.com"]})
        loader.sync_schema(df)

        # No se debe llamar begin() porque no hay columnas nuevas
        loader.engine.begin.assert_not_called()

    @patch("etl.database.inspect")
    def test_invalidate_cache_forces_reinspection(self, mock_inspect, loader):
        """invalidate_column_cache() fuerza una nueva inspección."""
        mock_inspect.return_value = _mock_inspector(["hs_object_id", "email"])

        df = pd.DataFrame({"hs_object_id": [1], "email": ["a@b.com"]})

        loader.sync_schema(df)
        assert mock_inspect.call_count == 1

        loader.invalidate_column_cache()

        loader.sync_schema(df)
        assert mock_inspect.call_count == 2

    def test_column_cache_starts_none(self, loader):
        """El cache empieza como None."""
        assert loader._column_cache is None
