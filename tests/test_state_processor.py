"""Tests for the state processor plugin infrastructure.

Verifies that registered state processors can:
- Extract keys from object state and provide them as extra columns
- Write extra columns atomically alongside object_state rows
- Handle the NULL case (no extra data for an object)
- Work correctly with both main and instance storage paths
"""

from psycopg.rows import dict_row
from tests.conftest import DSN
from zodb_pgjsonb.storage import ExtraColumn
from zodb_pgjsonb.storage import PGJsonbStorage

import psycopg
import pytest
import transaction as txn
import ZODB


# ── Test processor ──────────────────────────────────────────────────


class DummyProcessor:
    """Extracts a ``_test_extra`` key from state → writes it as a PG column."""

    ANNOTATION_KEY = "_test_extra"

    def get_extra_columns(self):
        return [
            ExtraColumn("test_label", "%(test_label)s"),
        ]

    def process(self, zoid, class_mod, class_name, state):
        pending = state.pop(self.ANNOTATION_KEY, None)
        if pending is None:
            return None
        return {"test_label": pending}


class NullSentinelProcessor:
    """Processor that handles both dict values and None sentinel."""

    ANNOTATION_KEY = "_pgcat"

    def get_extra_columns(self):
        return [
            ExtraColumn("cat_path", "%(cat_path)s"),
        ]

    def process(self, zoid, class_mod, class_name, state):
        if self.ANNOTATION_KEY not in state:
            return None
        pending = state.pop(self.ANNOTATION_KEY)
        if pending is None:
            # Sentinel: clear the column
            return {"cat_path": None}
        return {"cat_path": pending.get("path")}


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def _clean_db():
    """Drop tables for a clean slate, including the extra column."""
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS blob_state, object_state, transaction_log CASCADE"
        )
    conn.commit()
    conn.close()


@pytest.fixture
def storage(_clean_db):
    """PGJsonbStorage with DummyProcessor registered."""
    s = PGJsonbStorage(DSN)
    # Add the test_label column to object_state
    s._conn.execute("ALTER TABLE object_state ADD COLUMN IF NOT EXISTS test_label TEXT")
    s._conn.commit()
    s.register_state_processor(DummyProcessor())
    yield s
    s.close()


@pytest.fixture
def db(storage):
    database = ZODB.DB(storage)
    yield database
    database.close()


# ── Tests ───────────────────────────────────────────────────────────


class TestExtraColumn:
    """ExtraColumn dataclass basics."""

    def test_defaults(self):
        ec = ExtraColumn("foo", "%(foo)s")
        assert ec.name == "foo"
        assert ec.value_expr == "%(foo)s"
        assert ec.update_expr is None

    def test_custom_update_expr(self):
        ec = ExtraColumn("bar", "%(bar)s", "COALESCE(EXCLUDED.bar, object_state.bar)")
        assert ec.update_expr == "COALESCE(EXCLUDED.bar, object_state.bar)"


class TestStateProcessorRegistration:
    """Registration and discovery."""

    def test_register_processor(self, storage):
        # DummyProcessor is already registered by fixture
        assert len(storage._state_processors) == 1

    def test_get_extra_columns(self, storage):
        cols = storage._get_extra_columns()
        assert cols is not None
        assert len(cols) == 1
        assert cols[0].name == "test_label"

    def test_no_processors_returns_none(self, _clean_db):
        s = PGJsonbStorage(DSN)
        try:
            assert s._get_extra_columns() is None
            assert s._process_state(1, "mod", "cls", {}) is None
        finally:
            s.close()


class TestStateProcessorWritePath:
    """Extra columns written alongside object_state via ZODB.DB."""

    def test_extra_data_written_to_pg(self, db):
        """Object with annotation → extra column is written."""
        conn = db.open()
        root = conn.root()
        root["title"] = "Hello"
        root._test_extra = "my-label"
        txn.commit()

        # Read directly from PG to verify
        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT test_label, state FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()
        pg.close()
        conn.close()

        assert row is not None
        assert row["test_label"] == "my-label"
        # Annotation must be stripped from state JSONB
        assert "_test_extra" not in row["state"]

    def test_no_annotation_writes_null(self, db):
        """Object without annotation → extra column is NULL."""
        conn = db.open()
        root = conn.root()
        root["other"] = "value"
        txn.commit()

        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT test_label FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()
        pg.close()
        conn.close()

        assert row is not None
        assert row["test_label"] is None

    def test_annotation_update_on_second_commit(self, db):
        """Second commit with new annotation → extra column updated."""
        conn = db.open()
        root = conn.root()
        root["title"] = "v1"
        root._test_extra = "label-v1"
        txn.commit()

        root["title"] = "v2"
        root._test_extra = "label-v2"
        txn.commit()

        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT test_label FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()
        pg.close()
        conn.close()

        assert row["test_label"] == "label-v2"

    def test_abort_does_not_write_extra(self, db):
        """Transaction abort → no extra column data written."""
        conn = db.open()
        root = conn.root()
        root["title"] = "committed"
        txn.commit()

        # Now start a new change but abort
        root["title"] = "aborted"
        root._test_extra = "should-not-appear"
        txn.abort()

        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT test_label FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()
        pg.close()
        conn.close()

        # Should be NULL — the abort prevented the write
        assert row["test_label"] is None


class TestNullSentinelProcessor:
    """Processor that supports None sentinel for clearing columns."""

    @pytest.fixture
    def storage_with_sentinel(self, _clean_db):
        s = PGJsonbStorage(DSN)
        s._conn.execute(
            "ALTER TABLE object_state ADD COLUMN IF NOT EXISTS cat_path TEXT"
        )
        s._conn.commit()
        s.register_state_processor(NullSentinelProcessor())
        yield s
        s.close()

    @pytest.fixture
    def db_sentinel(self, storage_with_sentinel):
        database = ZODB.DB(storage_with_sentinel)
        yield database
        database.close()

    def test_set_then_clear(self, db_sentinel):
        """Set extra column, then clear it via None sentinel."""
        conn = db_sentinel.open()
        root = conn.root()

        # Set
        root._pgcat = {"path": "/plone/doc"}
        root["title"] = "Doc"
        txn.commit()

        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute("SELECT cat_path FROM object_state WHERE zoid = %s", (zoid,))
            row = cur.fetchone()
        assert row["cat_path"] == "/plone/doc"

        # Clear via None sentinel
        root._pgcat = None
        txn.commit()

        with pg.cursor() as cur:
            cur.execute("SELECT cat_path FROM object_state WHERE zoid = %s", (zoid,))
            row = cur.fetchone()
        pg.close()
        conn.close()

        assert row["cat_path"] is None
