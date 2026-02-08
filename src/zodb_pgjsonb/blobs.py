"""BlobManager — Tiered blob storage (PG bytea + optional S3)."""

from ZODB.utils import oid_repr

import logging
import os
import tempfile


logger = logging.getLogger(__name__)


def _oid_hex(oid):
    """Convert OID bytes to compact hex string."""
    return oid_repr(oid).lstrip("0x") or "0"


def _tid_hex(tid):
    """Convert TID bytes to compact hex string."""
    return oid_repr(tid).lstrip("0x") or "0"


class BlobManager:
    """Manages tiered blob storage.

    Small blobs (< threshold) are stored inline as PG bytea.
    Large blobs (>= threshold) are stored in S3 (if configured).
    Without S3 config, all blobs go to PG.
    """

    def __init__(
        self,
        *,
        s3_client=None,
        blob_threshold=1_048_576,
        cache_dir=None,
        temp_dir=None,
    ):
        self.threshold = blob_threshold
        self.s3_client = s3_client
        self.cache_dir = cache_dir
        self.temp_dir = temp_dir or tempfile.mkdtemp(prefix="zodb-pgjsonb-blobs-")

    def store_blob(self, cursor, oid, tid, blob_path):
        """Store a blob file. Route to PG or S3 based on size and config."""
        from ZODB.utils import u64

        size = os.path.getsize(blob_path)
        zoid = u64(oid)
        ztid = u64(tid)

        if self.s3_client and size >= self.threshold:
            s3_key = f"blobs/{_oid_hex(oid)}/{_tid_hex(tid)}.blob"
            self.s3_client.upload_file(blob_path, s3_key)
            cursor.execute(
                "INSERT INTO blob_state (zoid, tid, blob_size, s3_key) "
                "VALUES (%s, %s, %s, %s)",
                (zoid, ztid, size, s3_key),
            )
            logger.debug(
                "Stored blob %s/%s in S3 (%d bytes)", _oid_hex(oid), _tid_hex(tid), size
            )
        else:
            with open(blob_path, "rb") as f:
                data = f.read()
            cursor.execute(
                "INSERT INTO blob_state (zoid, tid, blob_size, data) "
                "VALUES (%s, %s, %s, %s)",
                (zoid, ztid, size, data),
            )
            logger.debug(
                "Stored blob %s/%s in PG (%d bytes)", _oid_hex(oid), _tid_hex(tid), size
            )

    def load_blob(self, cursor, oid, tid):
        """Load a blob, returning a local file path."""
        from ZODB.utils import u64

        zoid = u64(oid)
        ztid = u64(tid)

        cursor.execute(
            "SELECT data, s3_key FROM blob_state WHERE zoid = %s AND tid = %s",
            (zoid, ztid),
        )
        row = cursor.fetchone()

        if row is None:
            from ZODB.POSException import POSKeyError

            raise POSKeyError(oid)

        if row[1]:  # s3_key
            return self._load_from_s3(row[1], oid, tid)

        # Inline PG data → write to temp file
        return self._write_to_temp(row[0])

    def _load_from_s3(self, s3_key, oid, tid):
        """Download blob from S3, using cache if available."""
        # TODO: implement cache lookup
        fd, path = tempfile.mkstemp(dir=self.temp_dir, suffix=".blob")
        try:
            self.s3_client.download_file(s3_key, path)
        except Exception:
            os.close(fd)
            os.unlink(path)
            raise
        os.close(fd)
        return path

    def _write_to_temp(self, data):
        """Write blob data to a temp file and return the path."""
        fd, path = tempfile.mkstemp(dir=self.temp_dir, suffix=".blob")
        try:
            os.write(fd, data)
        finally:
            os.close(fd)
        return path

    def close(self):
        """Cleanup temp directory."""
        # temp_dir cleanup is left to the caller / garbage collection
        pass
