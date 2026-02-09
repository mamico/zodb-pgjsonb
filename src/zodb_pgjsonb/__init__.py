"""ZODB storage adapter using PostgreSQL JSONB with zodb-json-codec transcoding."""

from .storage import ExtraColumn


__all__ = ["ExtraColumn"]
