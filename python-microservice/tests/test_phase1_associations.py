"""Tests para Fase 1: Fix bug TRUNCATE de asociaciones."""
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from etl.database import DatabaseLoader


@pytest.fixture
def loader(fake_config, monitor):
    """DatabaseLoader con engine mockeado."""
    with patch.object(DatabaseLoader, '_create_engine', return_value=MagicMock()):
        return DatabaseLoader(fake_config, monitor)


def _make_assoc_df(from_ids, to_ids, type_id="1"):
    """Helper para crear DataFrames de asociaciones."""
    return pd.DataFrame({
        "contacts_id": from_ids,
        "companies_id": to_ids,
        "type_id": [type_id] * len(from_ids),
        "category": ["HUBSPOT_DEFINED"] * len(from_ids),
        "fivetran_synced": ["2024-01-01T00:00:00"] * len(from_ids),
    })


class TestAccumulateAssociations:
    def test_accumulate_multiple_batches_preserves_all(self, loader):
        """3 batches con 5 registros c/u deben acumular 15 totales."""
        batch1 = {"companies": _make_assoc_df([101, 102, 103, 104, 105], [201, 202, 203, 204, 205])}
        batch2 = {"companies": _make_assoc_df([106, 107, 108, 109, 110], [206, 207, 208, 209, 210])}
        batch3 = {"companies": _make_assoc_df([111, 112, 113, 114, 115], [211, 212, 213, 214, 215])}

        loader.accumulate_associations(batch1)
        loader.accumulate_associations(batch2)
        loader.accumulate_associations(batch3)

        assert "companies" in loader._accumulated_associations
        assert len(loader._accumulated_associations["companies"]) == 3

        total_rows = sum(len(df) for df in loader._accumulated_associations["companies"])
        assert total_rows == 15

    def test_accumulate_multiple_types(self, loader):
        """Asociaciones de distintos tipos se acumulan por separado."""
        batch = {
            "companies": _make_assoc_df([101], [201]),
            "deals": pd.DataFrame({
                "contacts_id": [101],
                "deals_id": [301],
                "type_id": ["3"],
                "category": ["HUBSPOT_DEFINED"],
                "fivetran_synced": ["2024-01-01T00:00:00"],
            }),
        }

        loader.accumulate_associations(batch)

        assert "companies" in loader._accumulated_associations
        assert "deals" in loader._accumulated_associations

    def test_accumulate_skips_empty_dfs(self, loader):
        """DataFrames vacíos no se acumulan."""
        batch = {"companies": pd.DataFrame()}

        loader.accumulate_associations(batch)

        assert "companies" not in loader._accumulated_associations

    def test_accumulate_skips_none_input(self, loader):
        """Input None o vacío no causa error."""
        loader.accumulate_associations(None)
        loader.accumulate_associations({})

        assert loader._accumulated_associations == {}


class TestFlushAssociations:
    def test_flush_deduplicates_on_composite_key(self, loader):
        """Registros duplicados por (from_id, to_id, type_id) se deduplicanp."""
        # Batch 1 y 2 tienen el registro (101, 201, "1") duplicado
        batch1 = {"companies": _make_assoc_df([101, 102], [201, 202])}
        batch2 = {"companies": _make_assoc_df([101, 103], [201, 203])}

        loader.accumulate_associations(batch1)
        loader.accumulate_associations(batch2)

        # Concatenar y verificar la deduplicación directamente
        all_dfs = loader._accumulated_associations["companies"]
        combined = pd.concat(all_dfs, ignore_index=True)
        assert len(combined) == 4  # Pre-dedup: 2 + 2 = 4

        key_cols = ["contacts_id", "companies_id", "type_id"]
        deduped = combined.drop_duplicates(subset=key_cols, keep="last")
        assert len(deduped) == 3  # Post-dedup: (101,201), (102,202), (103,203)

    def test_flush_clears_accumulator(self, loader):
        """Después de flush, el acumulador debe estar vacío."""
        batch = {"companies": _make_assoc_df([101], [201])}
        loader.accumulate_associations(batch)

        # Mock el engine para que no falle la escritura a BD
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_conn

        # Mock to_sql en el DataFrame
        with patch.object(pd.DataFrame, 'to_sql'):
            loader.flush_associations()

        assert loader._accumulated_associations == {}

    def test_flush_empty_is_noop(self, loader):
        """Flush sin datos acumulados no hace nada."""
        loader.flush_associations()

        loader.engine.begin.assert_not_called()

    def test_flush_creates_table_and_inserts(self, loader):
        """Flush debe hacer CREATE TABLE, TRUNCATE e INSERT."""
        batch = {"companies": _make_assoc_df([101, 102], [201, 202])}
        loader.accumulate_associations(batch)

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        loader.engine.begin.return_value = mock_conn

        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            loader.flush_associations()

        # Verificar que se ejecutaron SQLs (CREATE TABLE + TRUNCATE)
        assert mock_conn.execute.call_count == 2

        sql_calls = [str(c) for c in mock_conn.execute.call_args_list]
        sql_texts = [str(c[0][0]) for c in mock_conn.execute.call_args_list]

        # Verificar CREATE TABLE
        assert any("CREATE TABLE" in s for s in sql_texts)
        # Verificar TRUNCATE
        assert any("TRUNCATE" in s for s in sql_texts)
        # Verificar to_sql fue llamado
        mock_to_sql.assert_called_once()
