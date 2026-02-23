"""Tests para carga de pipelines con UPSERT + soft delete."""
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from etl.database import DatabaseLoader


@pytest.fixture
def loader(fake_config, monitor):
    """DatabaseLoader con engine mockeado."""
    with patch.object(DatabaseLoader, '_create_engine', return_value=MagicMock()):
        ldr = DatabaseLoader(fake_config, monitor)
        ldr.config.table_name = "deal"
        return ldr


def _make_pipeline_df(pipeline_ids, labels):
    """Helper para crear DataFrame de pipelines."""
    return pd.DataFrame({
        "pipeline_id": pipeline_ids,
        "label": labels,
        "display_order": list(range(len(pipeline_ids))),
        "created_at": ["2024-01-01T00:00:00-05:00"] * len(pipeline_ids),
        "updated_at": ["2024-06-01T00:00:00-05:00"] * len(pipeline_ids),
        "archived": [False] * len(pipeline_ids),
        "fivetran_deleted": [False] * len(pipeline_ids),
        "fivetran_synced": ["2024-06-15T00:00:00+00:00"] * len(pipeline_ids),
    })


def _make_stages_df(stage_ids, pipeline_ids):
    """Helper para crear DataFrame de stages."""
    return pd.DataFrame({
        "stage_id": stage_ids,
        "pipeline_id": pipeline_ids,
        "label": [f"Stage {s}" for s in stage_ids],
        "display_order": list(range(len(stage_ids))),
        "created_at": ["2024-01-01T00:00:00-05:00"] * len(stage_ids),
        "updated_at": ["2024-06-01T00:00:00-05:00"] * len(stage_ids),
        "archived": [False] * len(stage_ids),
        "state": ["open"] * len(stage_ids),
        "fivetran_deleted": [False] * len(stage_ids),
        "fivetran_synced": ["2024-06-15T00:00:00+00:00"] * len(stage_ids),
    })


class TestUpsertPipelinesSQL:
    @patch('etl.database.execute_values')
    def test_upsert_uses_on_conflict(self, mock_ev, loader):
        """SQL generado debe usar ON CONFLICT para pipelines."""
        df = _make_pipeline_df(["p1"], ["Pipeline 1"])
        mock_cursor = MagicMock()

        loader._upsert_dataframe(
            mock_cursor, "hubspot_etl", "deal_pipeline",
            df, pk="pipeline_id",
        )

        mock_ev.assert_called_once()
        sql_text = mock_ev.call_args[0][1]

        assert 'ON CONFLICT ("pipeline_id")' in sql_text
        assert 'DO UPDATE SET' in sql_text
        assert '"pipeline_id" = EXCLUDED' not in sql_text

    @patch('etl.database.execute_values')
    def test_upsert_stages_uses_on_conflict(self, mock_ev, loader):
        """SQL generado debe usar ON CONFLICT para stages."""
        df = _make_stages_df(["s1"], ["p1"])
        mock_cursor = MagicMock()

        loader._upsert_dataframe(
            mock_cursor, "hubspot_etl", "deal_pipeline_stage",
            df, pk="stage_id",
        )

        sql_text = mock_ev.call_args[0][1]

        assert 'ON CONFLICT ("stage_id")' in sql_text
        assert '"pipeline_id" = EXCLUDED."pipeline_id"' in sql_text

    @patch('etl.database.execute_values')
    def test_upsert_converts_nan_to_none(self, mock_ev, loader):
        """Valores NaN deben convertirse a None."""
        df = _make_pipeline_df(["p1"], ["Pipeline 1"])
        df.loc[0, "label"] = float('nan')
        mock_cursor = MagicMock()

        loader._upsert_dataframe(
            mock_cursor, "hubspot_etl", "deal_pipeline",
            df, pk="pipeline_id",
        )

        data = mock_ev.call_args[0][2]
        row = data[0]
        # label es la 2da columna (index 1)
        assert row[1] is None


class TestSoftDeleteMissing:
    def test_marks_missing_as_deleted(self, loader):
        """Registros no presentes en active_ids deben marcarse como deleted."""
        mock_cursor = MagicMock()

        loader._soft_delete_missing(
            mock_cursor, "hubspot_etl", "deal_pipeline",
            "pipeline_id", ["p1", "p2"], "2024-06-15T00:00:00",
        )

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        params = mock_cursor.execute.call_args[0][1]

        assert '"fivetran_deleted" = TRUE' in sql
        assert 'NOT IN' in sql
        assert '"fivetran_deleted" = FALSE' in sql
        assert params == ["2024-06-15T00:00:00", "p1", "p2"]

    def test_empty_active_ids_is_noop(self, loader):
        """Sin IDs activos no debe ejecutar ningún UPDATE."""
        mock_cursor = MagicMock()

        loader._soft_delete_missing(
            mock_cursor, "hubspot_etl", "deal_pipeline",
            "pipeline_id", [], "2024-06-15T00:00:00",
        )

        mock_cursor.execute.assert_not_called()

    def test_already_deleted_not_updated(self, loader):
        """SQL debe filtrar por fivetran_deleted = FALSE para no re-marcar."""
        mock_cursor = MagicMock()

        loader._soft_delete_missing(
            mock_cursor, "hubspot_etl", "deal_pipeline",
            "pipeline_id", ["p1"], "2024-06-15T00:00:00",
        )

        sql = mock_cursor.execute.call_args[0][0]
        assert '"fivetran_deleted" = FALSE' in sql


class TestLoadPipelinesIntegration:
    @patch('etl.database.execute_values')
    def test_full_flow_calls_upsert_and_soft_delete(self, mock_ev, loader):
        """load_pipelines debe llamar upsert + soft delete en transacción."""
        df_pip = _make_pipeline_df(["p1", "p2"], ["Pipeline 1", "Pipeline 2"])
        df_stg = _make_stages_df(["s1", "s2", "s3"], ["p1", "p1", "p2"])

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value = mock_cursor

        mock_sa_conn = MagicMock()
        mock_sa_conn.__enter__ = MagicMock(return_value=mock_sa_conn)
        mock_sa_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_sa_conn
        loader.engine.raw_connection.return_value = mock_raw_conn

        loader.load_pipelines(df_pip, df_stg)

        # execute_values llamado 2 veces (pipelines + stages)
        assert mock_ev.call_count == 2

        # cursor.execute llamado 2 veces (soft delete pipelines + stages)
        assert mock_cursor.execute.call_count == 2

        # Verificar commit
        mock_raw_conn.commit.assert_called_once()
        mock_raw_conn.rollback.assert_not_called()
        mock_raw_conn.close.assert_called_once()

        # Verificar métricas
        assert loader.monitor.metrics.get('pipelines_loaded') == 2
        assert loader.monitor.metrics.get('stages_loaded') == 3

    def test_empty_pipelines_is_noop(self, loader):
        """Sin pipelines no debe hacer nada."""
        loader.load_pipelines(pd.DataFrame(), pd.DataFrame())
        loader.engine.raw_connection.assert_not_called()

    def test_none_pipelines_is_noop(self, loader):
        """None como input no debe causar error."""
        loader.load_pipelines(None, None)
        loader.engine.raw_connection.assert_not_called()

    @patch('etl.database.execute_values')
    def test_pipelines_without_stages(self, mock_ev, loader):
        """Pipelines sin stages debe funcionar sin error."""
        df_pip = _make_pipeline_df(["p1"], ["Pipeline 1"])

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value = mock_cursor

        mock_sa_conn = MagicMock()
        mock_sa_conn.__enter__ = MagicMock(return_value=mock_sa_conn)
        mock_sa_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_sa_conn
        loader.engine.raw_connection.return_value = mock_raw_conn

        loader.load_pipelines(df_pip, pd.DataFrame())

        # execute_values solo para pipelines (no stages)
        assert mock_ev.call_count == 1

        # soft delete: solo pipelines (stages con lista vacía = noop)
        assert mock_cursor.execute.call_count == 1

        mock_raw_conn.commit.assert_called_once()
        assert loader.monitor.metrics.get('pipelines_loaded') == 1

    def test_rollback_on_error(self, loader):
        """Error durante upsert debe hacer rollback."""
        df_pip = _make_pipeline_df(["p1"], ["Pipeline 1"])

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value = mock_cursor

        mock_sa_conn = MagicMock()
        mock_sa_conn.__enter__ = MagicMock(return_value=mock_sa_conn)
        mock_sa_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_sa_conn
        loader.engine.raw_connection.return_value = mock_raw_conn

        with patch('etl.database.execute_values', side_effect=Exception("DB error")):
            with pytest.raises(Exception, match="DB error"):
                loader.load_pipelines(df_pip, pd.DataFrame())

        mock_raw_conn.rollback.assert_called_once()
        mock_raw_conn.commit.assert_not_called()
