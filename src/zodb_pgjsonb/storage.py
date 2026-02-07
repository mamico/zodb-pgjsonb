"""PGJsonbStorage — ZODB storage using PostgreSQL JSONB.

Implements IStorage using psycopg3 (sync) and zodb-json-codec for
transparent pickle ↔ JSONB transcoding.  Extends ZODB.BaseStorage
which provides the 2PC protocol framework (locks, TID generation,
transaction tracking).
"""

import logging

import psycopg
import zope.interface
from psycopg.rows import dict_row
from psycopg.types.json import Json

from ZODB.BaseStorage import BaseStorage
from ZODB.ConflictResolution import ConflictResolvingStorage
from ZODB.POSException import ConflictError
from ZODB.POSException import POSKeyError
from ZODB.POSException import StorageTransactionError
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64

import zodb_json_codec

from .interfaces import IPGJsonbStorage
from .schema import install_schema


logger = logging.getLogger(__name__)


@zope.interface.implementer(IPGJsonbStorage)
class PGJsonbStorage(ConflictResolvingStorage, BaseStorage):
    """ZODB storage that stores object state as JSONB in PostgreSQL.

    Uses zodb-json-codec for transparent pickle ↔ JSON transcoding.
    ZODB sees pickle bytes; PostgreSQL sees queryable JSONB.

    Extends BaseStorage which handles:
    - Lock management (_lock, _commit_lock)
    - TID generation (monotonic timestamps)
    - OID allocation (new_oid)
    - 2PC protocol orchestration (tpc_begin/vote/finish/abort)

    We implement the hooks: _begin, _vote, _finish, _abort, _clear_temp,
    plus load, store, close, lastTransaction.
    """

    def __init__(self, dsn, name="pgjsonb", history_preserving=False):
        BaseStorage.__init__(self, name)
        self._dsn = dsn
        self._history_preserving = history_preserving
        self._ltid = z64

        # Pending stores for current transaction
        self._tmp = []

        # Database connections
        self._conn = psycopg.connect(dsn, row_factory=dict_row)

        # Initialize schema
        install_schema(self._conn, history_preserving=history_preserving)

        # Load max OID and last TID from database
        self._restore_state()

    def _restore_state(self):
        """Load max OID and last TID from existing data."""
        with self._conn.cursor() as cur:
            # Get max OID
            cur.execute("SELECT COALESCE(MAX(zoid), 0) AS max_oid FROM object_state")
            row = cur.fetchone()
            max_oid = row["max_oid"]
            if max_oid > 0:
                self._oid = p64(max_oid)

            # Get last TID
            cur.execute("SELECT COALESCE(MAX(tid), 0) AS max_tid FROM transaction_log")
            row = cur.fetchone()
            max_tid = row["max_tid"]
            if max_tid > 0:
                self._ltid = p64(max_tid)

    # ── IStorage: load ──────────────────────────────────────────────

    def load(self, oid, version=""):
        """Load current object state, returning (pickle_bytes, tid_bytes)."""
        zoid = u64(oid)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()

        if row is None:
            raise POSKeyError(oid)

        record = {
            "@cls": [row["class_mod"], row["class_name"]],
            "@s": row["state"],
        }
        data = zodb_json_codec.encode_zodb_record(record)
        return data, p64(row["tid"])

    def loadBefore(self, oid, tid):
        """Load object data before a given TID.

        Returns (data, start_tid, end_tid) or None.
        Raises POSKeyError if object doesn't exist at all.
        Returns None if object exists but has no revision before tid.
        """
        zoid = u64(oid)
        tid_int = u64(tid)

        with self._conn.cursor() as cur:
            # Check if object exists at all
            cur.execute(
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()

        if row is None:
            raise POSKeyError(oid)

        # Check if the current revision is before the requested TID
        if row["tid"] >= tid_int:
            return None

        record = {
            "@cls": [row["class_mod"], row["class_name"]],
            "@s": row["state"],
        }
        data = zodb_json_codec.encode_zodb_record(record)
        start_tid = p64(row["tid"])
        # end_tid: None = still current (no later revision in history-free mode)
        return data, start_tid, None

    def loadSerial(self, oid, serial):
        """Load a specific revision of an object."""
        zoid = u64(oid)
        tid_int = u64(serial)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s AND tid = %s",
                (zoid, tid_int),
            )
            row = cur.fetchone()

        if row is None:
            raise POSKeyError(oid)

        record = {
            "@cls": [row["class_mod"], row["class_name"]],
            "@s": row["state"],
        }
        return zodb_json_codec.encode_zodb_record(record)

    # ── IStorage: store ─────────────────────────────────────────────

    def store(self, oid, serial, data, version, transaction):
        """Queue an object for storage during the current transaction.

        Called between tpc_begin and tpc_vote.  Checks for conflicts
        (serial must match the committed TID for the object).
        """
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)

        if version:
            raise TypeError("versions are not supported")

        zoid = u64(oid)

        # Check for conflicts: serial must match current committed TID
        if serial != z64:  # z64 = new object, no conflict check needed
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT tid FROM object_state WHERE zoid = %s",
                    (zoid,),
                )
                row = cur.fetchone()
            if row is not None:
                committed_tid = p64(row["tid"])
                if committed_tid != serial:
                    # Try conflict resolution
                    committed_data = self.loadSerial(oid, committed_tid)
                    resolved = self.tryToResolveConflict(
                        oid, committed_tid, serial, data,
                        committedData=committed_data,
                    )
                    if resolved:
                        data = resolved
                        self._resolved.append(oid)
                    else:
                        raise ConflictError(oid=oid, serials=(committed_tid, serial))

        # Transcode pickle → JSONB-ready dict
        record = zodb_json_codec.decode_zodb_record(data)
        class_mod, class_name = record["@cls"]
        state = record["@s"]
        refs = _extract_refs(state)

        self._tmp.append({
            "zoid": zoid,
            "class_mod": class_mod,
            "class_name": class_name,
            "state": state,
            "state_size": len(data),
            "refs": refs,
        })

    # ── BaseStorage hooks (2PC) ─────────────────────────────────────

    def _begin(self, tid, u, d, e):
        """Called by BaseStorage.tpc_begin after acquiring commit lock."""
        # Start a PG savepoint so we can rollback on abort
        self._conn.execute("BEGIN")
        # Acquire advisory lock to serialize commits
        self._conn.execute("SELECT pg_advisory_xact_lock(0)")

    def _vote(self):
        """Called by BaseStorage.tpc_vote — flush pending stores to PG."""
        tid_int = u64(self._tid)

        with self._conn.cursor() as cur:
            # Write transaction metadata
            u, d, e = self._ude
            user = u.decode("utf-8") if isinstance(u, bytes) else u
            desc = d.decode("utf-8") if isinstance(d, bytes) else d
            cur.execute(
                "INSERT INTO transaction_log (tid, username, description, extension) "
                "VALUES (%s, %s, %s, %s)",
                (tid_int, user, desc, e),
            )

            # Write all pending object stores
            for obj in self._tmp:
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

        return self._resolved or None

    def _finish(self, tid, u, d, e):
        """Called by BaseStorage.tpc_finish — commit PG transaction.

        This MUST NOT FAIL.  After tpc_vote succeeds, tpc_finish
        must make changes permanent.
        """
        self._conn.commit()
        self._ltid = tid

    def _abort(self):
        """Called by BaseStorage.tpc_abort — rollback PG transaction."""
        try:
            self._conn.rollback()
        except Exception:
            logger.exception("Error during rollback")

    def _clear_temp(self):
        """Clear pending stores between transactions."""
        self._tmp.clear()

    # ── IStorage: metadata ──────────────────────────────────────────

    def lastTransaction(self):
        """Return TID of the last committed transaction."""
        return self._ltid

    def __len__(self):
        """Return approximate number of objects."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM object_state")
            row = cur.fetchone()
        return row["cnt"]

    def getSize(self):
        """Return approximate database size in bytes."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(state_size), 0) AS total FROM object_state"
            )
            row = cur.fetchone()
        return row["total"]

    def history(self, oid, size=1):
        """Return revision history for an object (limited in history-free mode)."""
        zoid = u64(oid)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT tid, class_mod, class_name, state_size "
                "FROM object_state WHERE zoid = %s "
                "ORDER BY tid DESC LIMIT %s",
                (zoid, size),
            )
            rows = cur.fetchall()

        if not rows:
            raise POSKeyError(oid)

        result = []
        for row in rows:
            from persistent.TimeStamp import TimeStamp
            ts = TimeStamp(p64(row["tid"]))
            result.append({
                "time": ts.timeTime(),
                "tid": p64(row["tid"]),
                "serial": p64(row["tid"]),
                "user_name": "",
                "description": "",
                "size": row["state_size"],
            })
        return result

    def pack(self, t, referencesf):
        """Pack the storage — remove unreachable objects.

        Uses pure SQL graph traversal via the refs column.
        The referencesf parameter is ignored (we use pre-extracted refs).
        """
        from .packer import pack as do_pack
        deleted_objects, deleted_blobs, s3_keys = do_pack(self._conn)
        return deleted_objects

    # ── IStorage: close ─────────────────────────────────────────────

    def close(self):
        """Close all database connections."""
        if self._conn and not self._conn.closed:
            self._conn.close()

    def cleanup(self):
        """Remove all data (used by tests)."""
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM blob_state")
            cur.execute("DELETE FROM object_state")
            cur.execute("DELETE FROM transaction_log")
        self._conn.commit()
        self._ltid = z64
        self._oid = z64


def _extract_refs(state):
    """Recursively walk decoded state and collect all @ref OIDs as integers."""
    refs = []
    if isinstance(state, dict):
        if "@ref" in state:
            ref_val = state["@ref"]
            # @ref is hex OID string, or [hex_oid, class_path]
            oid_hex = ref_val if isinstance(ref_val, str) else ref_val[0]
            refs.append(int(oid_hex, 16))
        else:
            for v in state.values():
                refs.extend(_extract_refs(v))
    elif isinstance(state, list):
        for item in state:
            refs.extend(_extract_refs(item))
    return refs
