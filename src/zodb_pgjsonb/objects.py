"""ObjectStore — JSONB CRUD operations with zodb-json-codec transcoding."""

from .schema import install_schema
from psycopg.rows import dict_row
from psycopg.types.json import Json
from ZODB.POSException import POSKeyError
from ZODB.utils import p64
from ZODB.utils import u64

import logging
import psycopg
import zodb_json_codec


logger = logging.getLogger(__name__)


class ObjectStore:
    """Handles JSONB object storage in PostgreSQL.

    Manages connections, transcoding, and CRUD operations.
    """

    def __init__(self, dsn):
        self._dsn = dsn
        self._conn = None
        self._pending = []

    def _get_conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, row_factory=dict_row)
        return self._conn

    def ensure_schema(self, *, history_preserving=False):
        """Create tables if they don't exist."""
        conn = self._get_conn()
        install_schema(conn, history_preserving=history_preserving)

    def load(self, oid):
        """Load current object state, returning (pickle_bytes, tid_bytes)."""
        zoid = u64(oid)
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()

        if row is None:
            raise POSKeyError(oid)

        # Reconstruct the record dict and encode back to pickle
        record = {
            "@cls": [row["class_mod"], row["class_name"]],
            "@s": row["state"],
        }
        data = zodb_json_codec.encode_zodb_record(record)
        return data, p64(row["tid"])

    def queue_store(
        self, *, oid, serial, data, class_mod, class_name, state, refs, transaction
    ):
        """Queue an object store operation for the current transaction."""
        self._pending.append(
            {
                "zoid": u64(oid),
                "class_mod": class_mod,
                "class_name": class_name,
                "state": state,
                "state_size": len(data),
                "refs": refs,
            }
        )

    def flush_pending(self, tid):
        """Write all pending stores to PostgreSQL."""
        if not self._pending:
            return

        conn = self._get_conn()
        tid_int = u64(tid)
        with conn.cursor() as cur:
            for obj in self._pending:
                cur.execute(
                    "INSERT INTO object_state "
                    "(zoid, tid, class_mod, class_name, state, state_size, refs) "
                    "VALUES (%(zoid)s, %(tid)s, %(class_mod)s, %(class_name)s, "
                    "%(state)s, %(state_size)s, %(refs)s) "
                    "ON CONFLICT (zoid) DO UPDATE SET "
                    "tid = EXCLUDED.tid, class_mod = EXCLUDED.class_mod, "
                    "class_name = EXCLUDED.class_name, state = EXCLUDED.state, "
                    "state_size = EXCLUDED.state_size, refs = EXCLUDED.refs",
                    {
                        "zoid": obj["zoid"],
                        "tid": tid_int,
                        "class_mod": obj["class_mod"],
                        "class_name": obj["class_name"],
                        "state": Json(obj["state"]),
                        "state_size": obj["state_size"],
                        "refs": obj["refs"],
                    },
                )
        self._pending.clear()

    def close(self):
        """Close the database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
        self._conn = None
