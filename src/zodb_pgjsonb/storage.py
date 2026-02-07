"""PGJsonbStorage — ZODB storage using PostgreSQL JSONB.

Implements IStorage and IMVCCStorage using psycopg3 (sync) and
zodb-json-codec for transparent pickle ↔ JSONB transcoding.
"""

import logging
import threading

import transaction
import zope.interface

from ZODB import POSException
from ZODB.BaseStorage import BaseStorage
from ZODB.utils import p64
from ZODB.utils import u64

import zodb_json_codec

from .interfaces import IPGJsonbStorage
from .objects import ObjectStore
from .schema import install_schema


logger = logging.getLogger(__name__)


@zope.interface.implementer(IPGJsonbStorage)
class PGJsonbStorage(BaseStorage):
    """ZODB storage that stores object state as JSONB in PostgreSQL.

    Uses zodb-json-codec for transparent pickle ↔ JSON transcoding.
    ZODB sees pickle bytes; PostgreSQL sees queryable JSONB.
    """

    def __init__(self, dsn, name="pgjsonb", history_preserving=False):
        super().__init__(name)
        self._dsn = dsn
        self._history_preserving = history_preserving
        self._object_store = ObjectStore(dsn)
        self._lock = threading.Lock()

        # Initialize schema
        self._object_store.ensure_schema(history_preserving=history_preserving)

    def load(self, oid, version=""):
        """Load current object state, returning (pickle_bytes, tid)."""
        return self._object_store.load(oid)

    def store(self, oid, serial, data, version, transaction):
        """Queue an object for storage (called during tpc_vote)."""
        # Transcode pickle → JSONB-ready dict
        record = zodb_json_codec.decode_zodb_record(data)
        class_mod, class_name = record["@cls"]
        state = record["@s"]
        refs = _extract_refs(state)

        self._object_store.queue_store(
            oid=oid,
            serial=serial,
            data=data,
            class_mod=class_mod,
            class_name=class_name,
            state=state,
            refs=refs,
            transaction=transaction,
        )

    def close(self):
        """Close all connections."""
        self._object_store.close()


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
