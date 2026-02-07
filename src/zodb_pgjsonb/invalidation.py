"""InvalidationChannel — LISTEN/NOTIFY for instant cache invalidation."""

import logging
import threading

import psycopg

from ZODB.utils import p64


logger = logging.getLogger(__name__)


class InvalidationChannel:
    """Uses PostgreSQL LISTEN/NOTIFY for instant object invalidation.

    Runs a dedicated connection in autocommit mode for LISTEN.
    Compatible with Zope's thread-based application server.
    """

    def __init__(self, dsn):
        self._dsn = dsn
        self._listen_conn = None
        self._last_tid = 0
        self._lock = threading.Lock()

    def start(self):
        """Open the LISTEN connection."""
        self._listen_conn = psycopg.connect(self._dsn, autocommit=True)
        self._listen_conn.execute("LISTEN zodb_invalidations")
        logger.info("Listening for ZODB invalidations on %s", self._dsn)

    def poll(self, conn):
        """Non-blocking poll for new invalidations.

        Returns (new_tid, [oid_bytes, ...]).
        """
        if self._listen_conn is None:
            return self._last_tid, []

        notifications = []
        for notify in self._listen_conn.notifies(timeout=0):
            notifications.append(int(notify.payload))

        if not notifications:
            return self._last_tid, []

        max_tid = max(notifications)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT zoid FROM object_state "
                "WHERE tid > %s AND tid <= %s",
                (self._last_tid, max_tid),
            )
            rows = cur.fetchall()

        with self._lock:
            self._last_tid = max_tid

        return max_tid, [p64(r[0]) for r in rows]

    def close(self):
        """Close the LISTEN connection."""
        if self._listen_conn and not self._listen_conn.closed:
            self._listen_conn.close()
        self._listen_conn = None
