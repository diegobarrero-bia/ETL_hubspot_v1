"""Tests para Fase 5: Upsert con psycopg2 execute_values."""
from unittest.mock import MagicMock, patch, call
import math

import numpy as np
import pandas as pd
import pytest

from etl.database import DatabaseLoader


@pytest.fixture
def loader(fake_config, monitor):
    """DatabaseLoader con engine mockeado."""
    with patch.object(DatabaseLoader, '_create_engine', return_value=MagicMock()):
        return DatabaseLoader(fake_config, monitor)


class TestUpsertRawSql:
    def test_sql_structure(self, loader):
        """SQL generado debe tener INSERT INTO + ON CONFLICT DO UPDATE."""
        df = pd.DataFrame({
            "hs_object_id": [101, 102],
            "email": ["a@b.com", "c@d.com"],
            "firstname": ["Juan", "María"],
        })

        mock_cursor = MagicMock()
        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_raw_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader.engine.raw_connection.return_value = mock_raw_conn

        with patch("etl.database.execute_values") as mock_exec:
            loader.upsert_records(df)

            mock_exec.assert_called_once()
            sql = mock_exec.call_args[0][1]

            assert 'INSERT INTO' in sql
            assert '"hs_object_id"' in sql
            assert '"email"' in sql
            assert 'ON CONFLICT ("hs_object_id") DO UPDATE SET' in sql
            # hs_object_id NO debe estar en el SET clause
            assert 'EXCLUDED."email"' in sql
            assert 'EXCLUDED."firstname"' in sql

    def test_nan_and_nat_converted_to_none(self, loader):
        """NaN y NaT deben convertirse a None en las tuplas."""
        df = pd.DataFrame({
            "hs_object_id": [101, 102],
            "email": ["a@b.com", None],
            "value": [1.5, float('nan')],
            "date": pd.to_datetime(["2024-01-01", pd.NaT]),
        })

        mock_cursor = MagicMock()
        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_raw_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader.engine.raw_connection.return_value = mock_raw_conn

        with patch("etl.database.execute_values") as mock_exec:
            loader.upsert_records(df)

            data = mock_exec.call_args[0][2]

            # Registro 2: email=None, value=nan→None, date=NaT→None
            row2 = data[1]
            assert row2[1] is None  # email
            assert row2[2] is None  # value (was NaN)
            assert row2[3] is None  # date (was NaT)

            # Registro 1: todos los valores deben ser reales
            row1 = data[0]
            assert row1[1] == "a@b.com"
            assert row1[2] == 1.5

    def test_empty_dataframe_skipped(self, loader):
        """DataFrame vacío no genera llamadas a BD."""
        df = pd.DataFrame()
        loader.upsert_records(df)

        loader.engine.raw_connection.assert_not_called()

    def test_commit_on_success(self, loader):
        """Tras upsert exitoso, se debe hacer commit."""
        df = pd.DataFrame({"hs_object_id": [101], "email": ["a@b.com"]})

        mock_cursor = MagicMock()
        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_raw_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader.engine.raw_connection.return_value = mock_raw_conn

        with patch("etl.database.execute_values"):
            loader.upsert_records(df)

        mock_raw_conn.commit.assert_called_once()
        mock_raw_conn.rollback.assert_not_called()

    def test_rollback_on_error(self, loader):
        """En caso de error, se debe hacer rollback."""
        df = pd.DataFrame({"hs_object_id": [101], "email": ["a@b.com"]})

        mock_cursor = MagicMock()
        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_raw_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader.engine.raw_connection.return_value = mock_raw_conn

        with patch("etl.database.execute_values", side_effect=Exception("DB error")):
            with pytest.raises(Exception, match="DB error"):
                loader.upsert_records(df)

        mock_raw_conn.rollback.assert_called_once()
        mock_raw_conn.commit.assert_not_called()

    def test_metrics_updated(self, loader, monitor):
        """db_upserts debe incrementarse por el número de registros."""
        df = pd.DataFrame({"hs_object_id": [101, 102, 103], "email": ["a", "b", "c"]})

        mock_cursor = MagicMock()
        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_raw_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        loader.engine.raw_connection.return_value = mock_raw_conn

        with patch("etl.database.execute_values"):
            loader.upsert_records(df)

        assert monitor.metrics['db_upserts'] == 3
