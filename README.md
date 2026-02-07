# zodb-pgjsonb

ZODB storage adapter for PostgreSQL using JSONB, powered by [zodb-json-codec](https://github.com/bluedynamics/zodb-json-codec).

**Status: Pre-Alpha / Under Development**

## Overview

`zodb-pgjsonb` is a modern, PostgreSQL-only ZODB storage that stores object state as **JSONB** instead of pickle/bytea. This enables:

- **SQL Queryability**: Query ZODB objects directly with PostgreSQL JSONB operators and GIN indexes
- **Transparency**: ZODB sees pickle bytes; PostgreSQL sees queryable JSON
- **Performance**: Rust-based codec (2x faster than CPython pickle), LISTEN/NOTIFY invalidation, psycopg3
- **Tiered Blobs**: Small blobs in PG, large blobs in S3 (configurable threshold)
- **Security**: JSON has no code execution attack surface (unlike pickle deserialization)

## Architecture

```
ZODB.DB
  └→ PGJsonbStorage
       ├→ zodb-json-codec (pickle ↔ JSONB transcoding)
       ├→ psycopg3 (sync, thread-safe, Zope-compatible)
       ├→ LISTEN/NOTIFY (instant invalidation)
       └→ Tiered blobs (PG bytea + S3)
```

## Key Features

- **JSONB-native schema** with extracted class columns and GIN indexes
- **Pure SQL pack/GC** via pre-extracted `refs` column (no object loading needed)
- **Configurable history modes** (history-free or history-preserving)
- **zodbconvert compatible** for migration from RelStorage
- **ZConfig integration** for standard Zope deployment

## Development

```bash
# Install in development mode
cd sources/zodb-pgjsonb
uv pip install -e ".[test]"

# Run tests (requires PostgreSQL)
pytest
```

## License

ZPL-2.1 (Zope Public License)
