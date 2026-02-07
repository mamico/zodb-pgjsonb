"""Zope interface declarations for zodb-pgjsonb."""

from zope.interface import Interface


class IPGJsonbStorage(Interface):
    """Marker interface for PostgreSQL JSONB storage."""


class IBlobManager(Interface):
    """Manages tiered blob storage (PG bytea + optional S3)."""

    def store_blob(cursor, oid, tid, blob_path):
        """Store a blob file. Small blobs go to PG, large to S3."""

    def load_blob(cursor, oid, tid):
        """Load a blob, returning a local file path."""
