# Monolith

A high-performance embedded key-value store written in Zig.

## Features

- **B+ Tree Index**: Efficient ordered key-value storage with O(log n) operations
- **Write-Ahead Logging (WAL)**: Durability guarantees with crash recovery
- **ACID Transactions**: Full transaction support with configurable isolation levels
- **Buffer Pool**: LRU-based page caching with clock eviction algorithm
- **MVCC**: Multi-version concurrency control for snapshot isolation
- **Lock Manager**: Fine-grained locking with deadlock detection
- **Checkpointing**: Periodic checkpoints for fast recovery

## Quick Start

```zig
const std = @import("std");
const monolith = @import("monolith");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Open or create database
    const db = try monolith.Database.open(allocator, "mydb.db", .{});
    defer db.close();

    // Simple operations
    try db.put("hello", "world");

    if (try db.get("hello")) |value| {
        defer allocator.free(value);
        std.debug.print("Value: {s}\n", .{value});
    }

    // With transactions
    const txn = try db.begin();
    try db.putTxn(txn, "key1", "value1");
    try db.putTxn(txn, "key2", "value2");
    try db.commit(txn);

    // Range scan
    var iter = try db.range("a", "z");
    defer iter.deinit();

    while (try iter.next()) |kv| {
        defer allocator.free(kv.key);
        defer allocator.free(kv.value);
        std.debug.print("{s} = {s}\n", .{kv.key, kv.value});
    }
}
```

## Building

```bash
# Build library
zig build

# Run tests
zig build test

# Run benchmarks
zig build run -Doptimize=ReleaseFast
```

## Architecture

### Storage Layer

- **Page Management**: Fixed-size 4KB pages with checksums
- **File I/O**: Page-aligned reads/writes with fsync support
- **Meta Pages**: Double-buffered atomic metadata updates
- **Free Space Management**: Linked list freelist with persistence

### Index Layer

- **B+ Tree**: Variable-length keys and values with slotted pages
- **Binary Search**: Efficient key lookup within nodes
- **Splits**: Automatic node splitting on overflow

### Transaction Layer

- **WAL Records**: Insert, delete, update, begin, commit, abort
- **Recovery**: ARIES-style crash recovery with redo/undo
- **MVCC**: Version chains for snapshot isolation
- **Locking**: Shared/exclusive locks with timeout

## Configuration

```zig
const config = monolith.Config{
    .page_size = 4096,              // Page size in bytes
    .cache_size = 64 * 1024 * 1024, // 64MB buffer pool
    .sync_mode = .sync,              // sync, batch, or none
    .isolation_level = .read_committed,
    .checkpoint_interval = 1000,     // Checkpoint every N commits
};

const db = try monolith.Database.open(allocator, "db.dat", config);
```

## Performance

Typical performance on modern hardware (SSD, 4KB pages, 64MB cache):

| Operation | Throughput |
|-----------|------------|
| Sequential Insert | ~100K ops/sec |
| Sequential Read | ~500K ops/sec |
| Random Read | ~200K ops/sec |
| Mixed (50/50) | ~150K ops/sec |

## File Format

### Database File

```
[Meta Page 0][Meta Page 1][Data Pages...]
```

### Meta Page Layout (76 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic ("MONO") |
| 4 | 4 | Version |
| 8 | 4 | Page size |
| 12 | 8 | Root page ID |
| 20 | 8 | Freelist page ID |
| 28 | 8 | Total pages |
| 36 | 8 | Last txn ID |
| 44 | 8 | Checkpoint LSN |
| 52 | 20 | Reserved |
| 72 | 4 | CRC32 checksum |

### WAL File

```
[Header 32 bytes][Record 1][Record 2]...
```

## API Reference

### Database

- `open(allocator, path, config)` - Open or create database
- `close()` - Close database
- `get(key)` - Get value by key
- `put(key, value)` - Insert or update
- `delete(key)` - Delete by key
- `range(start, end)` - Range scan iterator
- `begin()` - Start transaction
- `commit(txn)` - Commit transaction
- `abort(txn)` - Abort transaction
- `flush()` - Flush buffers to disk
- `stats()` - Get database statistics

### Transaction

- `getId()` - Get transaction ID
- `isActive()` - Check if active
- `commit()` - Commit
- `abort()` - Abort

## Testing

```bash
# Run all tests
zig build test

# Run specific test file
zig test src/storage/btree.zig

# Run with debug output
zig build test -- --verbose
```

## License

MIT
