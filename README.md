# Monolith

Embedded key-value store in Zig. ACID transactions, MVCC, crash recovery.

## Features

- Embedded library (no server process)
- ACID transactions with configurable isolation levels
- Multi-version concurrency control (MVCC)
- Write-ahead logging for crash recovery
- Buffer pool with LRU eviction
- B+ tree storage with range scans
- Checkpointing and log truncation

## Installation

Add as a Zig dependency or clone directly:

```bash
git clone https://github.com/sudokatie/monolith
cd monolith
zig build
```

## Usage

```zig
const monolith = @import("monolith");
const DB = monolith.DB;

// Open database
var db = try DB.open(allocator, "path/to/db", .{
    .page_size = 4096,
    .cache_size = 64 * 1024 * 1024, // 64MB
    .sync_mode = .sync,
});
defer db.close();

// Simple operations
try db.put("key", "value");
const value = try db.get("key");
_ = try db.delete("key");

// Transaction
var txn = try db.begin();
errdefer txn.rollback() catch {};

try txn.put("key1", "value1");
try txn.put("key2", "value2");
const old = txn.get("key3");
_ = try txn.delete("key3");

try txn.commit();
txn.deinit();

// Range scan
var iter = try db.range("start", "end");
defer iter.close();

while (iter.next()) |entry| {
    // process entry.key, entry.value
}

// Snapshot for consistent reads
const snapshot = try db.snapshot();
defer snapshot.release();

const v = snapshot.get("key");
```

## Configuration

```zig
const config = monolith.DBConfig{
    // Page size in bytes (default 4096)
    .page_size = 4096,

    // Buffer pool cache size (default 64MB)
    .cache_size = 64 * 1024 * 1024,

    // Durability mode: .sync, .batch, .none
    .sync_mode = .sync,

    // Transaction isolation: .read_committed, .repeatable_read, .serializable
    .isolation_level = .read_committed,

    // WAL segment size (default 16MB)
    .wal_segment_size = 16 * 1024 * 1024,

    // Auto-checkpoint interval (records)
    .checkpoint_interval = 1000,

    // Enable/disable WAL
    .enable_wal = true,
};
```

## Architecture

```
+------------------+
| Public API       |  open, close, get, put, delete, begin, range, snapshot
+------------------+
         |
+------------------+
| Transaction Mgr  |  begin, commit, rollback, MVCC versioning
+------------------+
         |
+------------------+
| Buffer Pool      |  Page cache, LRU eviction, dirty tracking
+------------------+
         |
+------------------+
| Storage Engine   |  B+ tree, page management, free space
+------------------+
         |
+------------------+
| Write-Ahead Log  |  Durability, recovery, checkpointing
+------------------+
         |
+------------------+
| File I/O         |  Page-aligned reads/writes, fsync
+------------------+
```

## Durability Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| sync | fsync every commit | Maximum durability |
| batch | fsync periodically | Balanced performance |
| none | no fsync | Testing only |

## Isolation Levels

| Level | Behavior |
|-------|----------|
| read_committed | See latest committed values |
| repeatable_read | Snapshot at transaction start |
| serializable | Full isolation (via locking) |

## Performance

Target performance characteristics:

- Read latency: <10us (cached), <1ms (disk)
- Write throughput: >100K ops/sec (batched)
- Recovery time: <1 second per GB of WAL

Run benchmarks:

```bash
zig build bench
./zig-out/bin/bench
```

## Testing

```bash
zig build test
```

## Limitations

- Single-file database (no sharding)
- Key-value only (no SQL)
- Single node (no replication)
- No compression (v0.1)
- Fixed page size at creation

## File Format

Database file:
- Meta page 0: Database header, root pointer
- Meta page 1: Backup meta (atomic updates)
- Free list pages: Track free space
- Data pages: B+ tree nodes

WAL files:
- Header: Magic, version, LSN range
- Records: LSN, txn_id, type, key, value
- CRC32 validation per record

## License

MIT
