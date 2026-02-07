# Brainstorming: ZODB PostgreSQL JSONB Storage Adapter

## Vision

A modern, PostgreSQL-only ZODB storage that stores object state as **JSONB** (not pickle/bytea), using **zodb-json-codec** for transparent transcoding. Blobs use tiered storage (small in PG, large in S3 via z3blobs). Enables SQL queryability of ZODB data while remaining fully transparent to ZODB/Zope/Plone.

## Decisions Made

- **Direction C: Informed Independent** — new independent storage, study & borrow proven algorithms from RelStorage
- **Separate repo/package** — https://github.com/bluedynamics/zodb-pgjsonb (developed under `sources/zodb-pgjsonb/` in z3blobs workspace)
- **Both history modes (configurable)** — history-free and history-preserving, like RelStorage
- **Tiered blob storage** — small blobs in PG bytea, large blobs in S3 (configurable threshold)

---

## Architecture

```
ZODB.DB
  └→ PGJsonbStorage (IStorage + IMVCCStorage + IBlobStorageRestoreable)
       ├→ ObjectStore          — JSONB CRUD (store, load, exists)
       │    └→ zodb-json-codec   (pickle ↔ JSON transcoding)
       ├→ TransactionManager   — 2PC (tpc_begin/vote/finish/abort)
       │    └→ conflict resolution via ZODB transform hooks
       ├→ InvalidationChannel  — LISTEN/NOTIFY (instant, no polling)
       ├→ BlobManager          — tiered: PG bytea (<threshold) + S3 (>=threshold)
       │    └→ z3blobs S3Client   (for large blobs)
       ├→ Packer               — pure SQL graph traversal via refs column, no unpickling
       ├→ ConnectionPool       — psycopg3 sync pool (thread-safe, Zope-compatible)
       └→ SchemaManager        — DDL, migrations, index management
```

### What we borrow from RelStorage (algorithms, not code)

- Transaction ID allocation (monotonic, global lock via `pg_advisory_xact_lock`)
- MVCC semantics (snapshot isolation, `new_instance()` per connection)
- Pack algorithm concept (mark reachable, sweep unreachable) — but reimplemented as pure SQL graph traversal via `refs` column, no object loading needed
- Object cache invalidation protocol (adapted from polling → LISTEN/NOTIFY)
- Conflict resolution integration via `_crs_transform_record_data` / `_crs_untransform_record_data`

### What we design fresh

- Schema optimized for JSONB from day one
- psycopg3 sync API with pipeline mode and prepared statements (Zope-compatible)
- LISTEN/NOTIFY for instant invalidation
- Tiered blob storage with configurable threshold
- Configurable history modes (history-free / history-preserving)
- SQL query API as first-class citizen

---

## Schema Design

### History-Free Mode (default, recommended for Plone)

```sql
-- Transaction metadata
CREATE TABLE transaction_log (
    tid         BIGINT PRIMARY KEY,
    username    TEXT DEFAULT '',
    description TEXT DEFAULT '',
    extension   BYTEA DEFAULT ''
);

-- Current object state only (no history)
CREATE TABLE object_state (
    zoid        BIGINT PRIMARY KEY,
    tid         BIGINT NOT NULL REFERENCES transaction_log(tid),
    class_mod   TEXT NOT NULL,
    class_name  TEXT NOT NULL,
    state       JSONB,
    state_size  INTEGER NOT NULL,
    refs        BIGINT[] NOT NULL DEFAULT '{}'  -- extracted @ref OIDs for pack
);

-- Blob storage (tiered: inline or S3 reference)
CREATE TABLE blob_state (
    zoid        BIGINT NOT NULL,
    tid         BIGINT NOT NULL,
    blob_size   BIGINT NOT NULL,
    data        BYTEA,              -- inline data (NULL if in S3)
    s3_key      TEXT,               -- S3 key (NULL if inline)
    PRIMARY KEY (zoid, tid)
);

-- Commit lock (only one committer at a time)
-- Uses pg_advisory_xact_lock(0) instead of a table

-- Indexes for queryability
CREATE INDEX idx_object_class ON object_state (class_mod, class_name);
CREATE INDEX idx_object_state_gin ON object_state USING gin (state jsonb_path_ops);

-- Invalidation trigger
CREATE OR REPLACE FUNCTION notify_commit() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('zodb_invalidations', NEW.tid::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_commit
    AFTER INSERT ON transaction_log
    FOR EACH ROW EXECUTE FUNCTION notify_commit();
```

### History-Preserving Mode (adds history table)

```sql
-- Same as above, plus:
CREATE TABLE object_history (
    zoid        BIGINT NOT NULL,
    tid         BIGINT NOT NULL,
    class_mod   TEXT NOT NULL,
    class_name  TEXT NOT NULL,
    state       JSONB,
    state_size  INTEGER NOT NULL,
    refs        BIGINT[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (zoid, tid)
) PARTITION BY RANGE (tid);
-- Partitioning enables efficient history cleanup during pack

CREATE TABLE blob_history (
    zoid        BIGINT NOT NULL,
    tid         BIGINT NOT NULL,
    blob_size   BIGINT NOT NULL,
    data        BYTEA,
    s3_key      TEXT,
    PRIMARY KEY (zoid, tid)
);

-- Undo support requires tracking which TIDs are "undone"
CREATE TABLE pack_state (
    zoid        BIGINT PRIMARY KEY,
    tid         BIGINT NOT NULL
);
```

### Extracted References Column (`refs`) — Pack Optimization

**The problem with traditional ZODB pack:**
ZODB's `pack(pack_time, referencesf)` must trace the full object graph to find reachable objects. In RelStorage (and FileStorage), this means loading and unpickling *every single object* to extract persistent references via `referencesf()`. For a database with millions of objects, this is extremely slow and I/O intensive.

**Our solution:** Extract `@ref` OIDs at `store()` time into a `BIGINT[]` column. The codec already identifies all `@ref` markers in the JSONB — we just need to collect them.

```python
def _extract_refs(state) -> list[int]:
    """Recursively walk the decoded state and collect all @ref OIDs."""
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
```

**Pack becomes a pure SQL graph traversal:**

```sql
-- Phase 1: Mark reachable objects starting from root (zoid=0)
WITH RECURSIVE reachable AS (
    -- Root object
    SELECT zoid FROM object_state WHERE zoid = 0
    UNION
    -- All objects referenced by reachable objects
    SELECT unnest(o.refs)
    FROM object_state o
    JOIN reachable r ON o.zoid = r.zoid
)
SELECT zoid INTO TEMP reachable_oids FROM reachable;

-- Phase 2: Delete unreachable objects
DELETE FROM object_state
WHERE zoid NOT IN (SELECT zoid FROM reachable_oids);

-- Phase 3: Delete unreachable blobs (both PG and S3)
DELETE FROM blob_state
WHERE zoid NOT IN (SELECT zoid FROM reachable_oids)
RETURNING s3_key;  -- collect S3 keys for cleanup
```

**Performance improvement:**
- RelStorage: Must load + unpickle every object (N network roundtrips + N unpickle operations)
- PGJsonbStorage: Single recursive SQL query, no data leaves PostgreSQL, no Python involved
- For a 1M object database: minutes → seconds

**Index for pack:**
```sql
CREATE INDEX idx_object_refs ON object_state USING gin (refs);
```

### Schema Notes

- **Class columns inline** (not normalized FK): avoids JOIN on the hot path (`load()`), class strings are short and compress well via TOAST
- **`state` is `@s` only**: the `@cls` marker is extracted into `class_mod`/`class_name` columns, saving JSONB space and enabling efficient class-based queries
- **`state_size`**: original pickle size, useful for monitoring/debugging without parsing JSONB
- **Advisory locks** instead of lock tables: `pg_advisory_xact_lock(0)` for commit serialization, released automatically on transaction end
- **GIN index with `jsonb_path_ops`**: enables `@>` containment queries efficiently
- **History partitioning by TID**: enables efficient range-based pack operations

---

## Tiered Blob Storage

### Design

```python
class BlobManager:
    def __init__(self, dsn, s3_client=None, blob_threshold=1_048_576,
                 cache_dir=None, cache_size=1_073_741_824):
        self.threshold = blob_threshold  # default 1MB
        self.s3_client = s3_client       # None = PG-only mode
        self.cache = BlobCache(cache_dir, cache_size) if cache_dir else None

    def store_blob(self, cursor, oid, tid, blob_path):
        size = os.path.getsize(blob_path)
        if self.s3_client and size >= self.threshold:
            # Large blob → S3
            s3_key = f"blobs/{oid_hex(oid)}/{tid_hex(tid)}.blob"
            self.s3_client.upload_file(blob_path, s3_key)
            cursor.execute(
                "INSERT INTO blob_state (zoid, tid, blob_size, s3_key) VALUES (%s,%s,%s,%s)",
                (oid_int, tid_int, size, s3_key)
            )
        else:
            # Small blob or no S3 → PG bytea
            with open(blob_path, 'rb') as f:
                data = f.read()
            cursor.execute(
                "INSERT INTO blob_state (zoid, tid, blob_size, data) VALUES (%s,%s,%s,%s)",
                (oid_int, tid_int, size, data)
            )

    def load_blob(self, cursor, oid, tid):
        cursor.execute(
            "SELECT data, s3_key FROM blob_state WHERE zoid=%s AND tid=%s",
            (oid_int, tid_int)
        )
        row = cursor.fetchone()
        if row['s3_key']:
            # Download from S3 (with local cache)
            return self._load_from_s3(row['s3_key'], oid, tid)
        else:
            # Read from PG bytea
            return self._write_to_temp(row['data'])
```

### Configuration

```xml
<pgjsonb>
    dsn dbname='zodb' host='localhost'
    history-mode free
    blob-threshold 1048576

    <!-- Optional S3 for large blobs -->
    s3-bucket my-zodb-blobs
    s3-endpoint-url http://minio:9000
    s3-access-key $S3_ACCESS_KEY
    s3-secret-key $S3_SECRET_KEY
    blob-cache-dir /var/cache/zodb-blobs
    blob-cache-size 1073741824
</pgjsonb>
```

### Deployment Modes

| Mode | Config | Blobs stored in |
|------|--------|----------------|
| PG-only | No S3 config | All blobs in PG bytea |
| Tiered | S3 + threshold | Small in PG, large in S3 |
| S3-only | threshold=0 | All blobs in S3 |

---

## Transcoding Integration

### Store Path (ZODB → PostgreSQL)

```python
def store(self, oid, serial, data, version, transaction):
    # data = pickle bytes from ZODB
    record = zodb_json_codec.decode_zodb_record(data)
    # record = {"@cls": ["myapp.models", "Document"], "@s": {"title": "Hello", ...}}
    class_mod, class_name = record["@cls"]
    state = record["@s"]

    # Psycopg3 automatically converts Python dict → PG JSONB
    self._cursor.execute("""
        INSERT INTO object_state (zoid, tid, class_mod, class_name, state, state_size)
        VALUES (%(zoid)s, %(tid)s, %(class_mod)s, %(class_name)s, %(state)s, %(size)s)
        ON CONFLICT (zoid) DO UPDATE SET
            tid = EXCLUDED.tid, class_mod = EXCLUDED.class_mod,
            class_name = EXCLUDED.class_name, state = EXCLUDED.state,
            state_size = EXCLUDED.state_size
    """, {"zoid": oid_int, "tid": tid_int, "class_mod": class_mod,
          "class_name": class_name, "state": Json(state), "size": len(data)})
```

### Load Path (PostgreSQL → ZODB)

```python
def load(self, oid, version=''):
    self._cursor.execute(
        "SELECT tid, class_mod, class_name, state FROM object_state WHERE zoid = %s",
        (oid_int,)
    )
    row = self._cursor.fetchone()
    if not row:
        raise POSKeyError(oid)

    # Reconstruct the record dict and encode back to pickle
    record = {"@cls": [row["class_mod"], row["class_name"]], "@s": row["state"]}
    data = zodb_json_codec.encode_zodb_record(record)
    return data, p64(row["tid"])
```

### Conflict Resolution (via ZODB transform hooks)

```python
class PGJsonbStorage:
    # These are called by ZODB's ConflictResolution.py

    def _crs_untransform_record_data(self, data):
        """JSONB dict → pickle bytes (before conflict resolution)"""
        return zodb_json_codec.encode_zodb_record(data)

    def _crs_transform_record_data(self, data):
        """pickle bytes → JSONB dict (after conflict resolution)"""
        return zodb_json_codec.decode_zodb_record(data)
```

---

## Invalidation: LISTEN/NOTIFY

### Why not polling (like RelStorage)?

RelStorage polls a `transaction` table every N seconds to discover new TIDs. This means:
- Wasted queries when nothing changed (common in read-heavy apps)
- Latency = poll interval (typically 1-5 seconds)
- PG load scales with number of ZODB connections, not write rate

LISTEN/NOTIFY instead:
- Zero overhead when nothing changes
- Instant notification on commit (~1ms latency)
- PG load scales with write rate, not connection count
- Built into PostgreSQL (no extensions needed)

### Implementation

```python
class InvalidationChannel:
    def __init__(self, dsn):
        # Dedicated connection for LISTEN (can't share with transactions)
        self._listen_conn = psycopg.connect(dsn, autocommit=True)
        self._listen_conn.execute("LISTEN zodb_invalidations")
        self._last_tid = 0

    def poll(self):
        """Non-blocking poll for new invalidations. Returns (new_tid, [oid, ...])."""
        notifications = []
        for notify in self._listen_conn.notifies(timeout=0):
            notifications.append(int(notify.payload))

        if not notifications:
            return self._last_tid, []

        max_tid = max(notifications)
        # Fetch all OIDs changed since our last known TID
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT zoid FROM object_state WHERE tid > %s AND tid <= %s",
                (self._last_tid, max_tid)
            ).fetchall()
        self._last_tid = max_tid
        return max_tid, [p64(r[0]) for r in rows]
```

---

## Query Capabilities Unlocked

With JSONB in PostgreSQL, direct SQL queries on ZODB data become possible:

```sql
-- Find all Documents
SELECT zoid, state->>'title' as title
FROM object_state
WHERE class_mod = 'myapp.models' AND class_name = 'Document';

-- Full-text search on any attribute
SELECT zoid FROM object_state
WHERE state @> '{"title": "Annual Report"}';

-- Date range queries (using @dt marker)
SELECT zoid FROM object_state
WHERE (state->'created'->>'@dt')::timestamptz > '2025-01-01';

-- Objects by class (efficient with idx_object_class)
SELECT class_mod || '.' || class_name as class, count(*)
FROM object_state GROUP BY 1 ORDER BY 2 DESC;

-- Find objects referencing a specific OID (using @ref marker)
SELECT zoid FROM object_state
WHERE state @> '{"author": {"@ref": "0000000000000003"}}';

-- Application-specific generated columns (added per-deployment):
ALTER TABLE object_state ADD COLUMN
    title TEXT GENERATED ALWAYS AS (state->>'title') STORED;
-- Now: SELECT zoid FROM object_state WHERE title LIKE 'Report%';

-- Connect Grafana, Metabase, or any SQL tool directly
```

---

## Migration Strategy

### zodbconvert Works Out of the Box

RelStorage's `zodbconvert` is **fully storage-agnostic** — it only uses standard ZODB interfaces (`IStorage`, `IStorageIteration`, `IStorageRestoreable`). No custom migration tool needed.

**Required interfaces for PGJsonbStorage:**
- `iterator(start, end)` — yield transactions with records (source)
- `restore(oid, tid, data, version, prev_txn, transaction)` — receive records (destination)
- `tpc_begin/vote/finish` — standard 2PC
- `copyTransactionsFrom(source)` — can inherit from `ZODB.BaseStorage` or implement optimized version

**Optional but recommended:**
- `lastTransaction()` — enables incremental/resumable migration
- `zap_all()` — enables `--clear` flag
- `restoreBlob(oid, tid, data, blobfilename, prev_txn, txn)` — blob migration

**Usage — no custom code:**
```ini
# zodbconvert.cfg
<source>
    <relstorage>
        <postgresql>
            dsn dbname='zodb_old'
        </postgresql>
    </relstorage>
</source>
<destination>
    %import zodb_pgjsonb
    <pgjsonb>
        dsn dbname='zodb_new'
    </pgjsonb>
</destination>
```

```bash
zodbconvert zodbconvert.cfg
# or incremental:
zodbconvert --incremental zodbconvert.cfg
```

The transcoding happens transparently inside PGJsonbStorage's `restore()` method — zodbconvert sends pickle bytes, our storage decodes to JSONB.

### Verification

```python
# After migration, verify every object roundtrips correctly:
for oid in all_oids:
    original_pickle = src.load(oid)[0]
    roundtripped_pickle = dst.load(oid)[0]
    # Compare at the logical level (pickle bytes may differ in ordering)
    assert zodb_json_codec.decode_zodb_record(original_pickle) == \
           zodb_json_codec.decode_zodb_record(roundtripped_pickle)
```

---

## Package Structure

```
zodb-pgjsonb/                         # separate repo
├── src/zodb_pgjsonb/
│   ├── __init__.py                   # version, public API
│   ├── storage.py                    # PGJsonbStorage (IStorage, IMVCCStorage)
│   ├── schema.py                     # DDL, schema versions, migrations
│   ├── objects.py                    # ObjectStore — JSONB CRUD with transcoding
│   ├── txn.py                        # TransactionManager — 2PC coordinator
│   ├── invalidation.py               # LISTEN/NOTIFY channel
│   ├── blobs.py                      # BlobManager — tiered PG/S3
│   ├── packer.py                     # Pack/GC (history + objects + blobs)
│   ├── pool.py                       # psycopg3 connection pool wrapper
│   ├── config.py                     # ZConfig factory
│   ├── interfaces.py                 # Zope interface declarations
│   └── component.xml                 # ZConfig schema definition
├── tests/
│   ├── conftest.py                   # fixtures (PG testcontainer, moto S3)
│   ├── test_storage.py               # IStorage conformance
│   ├── test_mvcc.py                  # MVCC / invalidation
│   ├── test_blobs.py                 # blob tiering
│   ├── test_pack.py                  # pack/GC
│   ├── test_conflict.py              # conflict resolution roundtrip
│   ├── test_zodbconvert.py            # zodbconvert compatibility
│   └── test_queries.py               # SQL queryability verification
├── pyproject.toml
└── README.md
```

### Dependencies

```toml
[project]
dependencies = [
    "ZODB >= 5.8",
    "zodb-json-codec",
    "psycopg[binary,pool] >= 3.1",
    "zope.interface",
    "transaction",
    "ZConfig",
]

[project.optional-dependencies]
s3 = ["boto3 >= 1.26"]              # for tiered blob storage with S3
test = [
    "pytest",
    "pytest-cov",
    "testcontainers[postgres]",      # real PG in tests
    "moto[s3]",                      # mock S3 in tests
]
```

---

## Development Phases

### Phase 0: Prototype & Proof of Concept (1-2 weeks)
- Minimal `store()` / `load()` with hardcoded schema
- Prove end-to-end: ZODB.DB → pickle → codec → JSONB → PG → JSONB → codec → pickle → ZODB
- Run basic ZODB tests (store, load, multiple objects)
- Measure transcoding + PG round-trip overhead vs RelStorage

### Phase 1: Core IStorage (2-4 weeks)
- Full `IStorage` implementation (store, load, exists, new_oid, lastTransaction)
- Two-phase commit (tpc_begin/vote/finish/abort) with advisory locks
- Transaction log table
- History-free mode first
- Connection pooling (psycopg3)
- ZConfig integration
- Test suite with testcontainers

### Phase 2: MVCC + Invalidation (1-2 weeks)
- `IMVCCStorage` interface (`poll_invalidations`, `new_instance`)
- LISTEN/NOTIFY trigger + Python listener
- Per-instance connection management

### Phase 3: History-Preserving Mode (1-2 weeks)
- Add `object_history` table and partitioning
- Dual-table store/load logic (current + history)
- Pack with history cleanup
- Optional undo support (IStorageUndoable)

### Phase 4: Tiered Blob Storage (1-2 weeks)
- `BlobManager` with PG bytea + S3 tiering
- Local blob cache (reuse z3blobs `S3BlobCache` patterns)
- Blob GC during pack (both PG and S3)
- `IBlobStorage` / `IBlobStorageRestoreable` interfaces

### Phase 5: Production Hardening (2-4 weeks)
- Full ZODB conformance test suite
- Conflict resolution with BTrees
- Concurrent write stress tests
- Pack under load
- Verify zodbconvert compatibility (no custom tool needed)
- Performance benchmarks vs RelStorage
- Documentation

---

## Risk Analysis

| Risk | Impact | Mitigation |
|------|--------|------------|
| MVCC edge cases (stale reads, phantom reads) | High | Study RelStorage's MVCC closely, use PG snapshot isolation |
| Pack corrupting live data | High | Two-phase pack with advisory locks, extensive testing |
| Transcoding performance on large objects | Medium | Codec is 2x faster than pickle, PG I/O dominates |
| JSONB size overhead vs bytea | Medium | TOAST compression, GIN indexes offset with query capability |
| psycopg3 stability / API changes | Low | Pin versions, psycopg3 is mature (3.1+) |
| LISTEN/NOTIFY missed notifications | Medium | Combine with periodic poll fallback, track last-seen TID |
| S3 upload failure during tpc_vote | Medium | Follow z3blobs pattern: S3 in vote (can fail), cache in finish (must not fail) |

---

## Comparison: RelStorage vs PGJsonbStorage

| Feature | RelStorage | PGJsonbStorage |
|---------|-----------|----------------|
| Storage format | pickle/bytea | JSONB |
| SQL queryable | No | Yes (GIN indexes, jsonpath) |
| Databases | PG, MySQL, Oracle, SQLite | PostgreSQL only |
| Python driver | psycopg2 | psycopg3 sync (pipeline, prepared stmts) |
| Invalidation | Polling (1-5s latency) | LISTEN/NOTIFY (~1ms) |
| Blob storage | Filesystem or cloud (separate) | Tiered PG + S3 (integrated) |
| History modes | Both | Both (configurable) |
| Conflict resolution | Built-in | Via ZODB transform hooks |
| Pack algorithm | Load+unpickle every object | Pure SQL graph traversal via `refs` column |
| Codec dependency | None | zodb-json-codec (Rust) |
| Maturity | 15+ years | New |
