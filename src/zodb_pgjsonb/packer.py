"""Packer — Pure SQL graph traversal for object garbage collection.

Unlike RelStorage/FileStorage which must load and unpickle every object
to extract references, we use the pre-extracted `refs` column for a
server-side recursive CTE query. No data leaves PostgreSQL during pack.
"""

import logging

logger = logging.getLogger(__name__)

# Reachability query: find all objects reachable from root (zoid=0)
REACHABLE_QUERY = """\
WITH RECURSIVE reachable AS (
    SELECT zoid FROM object_state WHERE zoid = 0
    UNION
    SELECT unnest(o.refs)
    FROM object_state o
    JOIN reachable r ON o.zoid = r.zoid
)
SELECT zoid FROM reachable
"""


def pack(conn, pack_time=None):
    """Remove unreachable objects and their blobs.

    Args:
        conn: psycopg connection
        pack_time: unused for now (history-free mode doesn't need it)

    Returns:
        Tuple of (deleted_objects, deleted_blobs, s3_keys_to_delete)
    """
    s3_keys = []

    with conn.cursor() as cur:
        # Phase 1: Find reachable objects
        cur.execute(f"SELECT zoid INTO TEMP reachable_oids FROM ({REACHABLE_QUERY}) q")
        cur.execute("CREATE INDEX ON reachable_oids (zoid)")

        # Phase 2: Delete unreachable objects
        cur.execute(
            "DELETE FROM object_state "
            "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids)"
        )
        deleted_objects = cur.rowcount

        # Phase 3: Delete unreachable blobs, collecting S3 keys
        cur.execute(
            "DELETE FROM blob_state "
            "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids) "
            "RETURNING s3_key"
        )
        deleted_blobs = cur.rowcount
        for row in cur.fetchall():
            if row[0]:
                s3_keys.append(row[0])

        # Cleanup temp table
        cur.execute("DROP TABLE reachable_oids")

    conn.commit()

    logger.info(
        "Pack complete: removed %d objects, %d blobs, %d S3 keys to clean",
        deleted_objects,
        deleted_blobs,
        len(s3_keys),
    )

    return deleted_objects, deleted_blobs, s3_keys
