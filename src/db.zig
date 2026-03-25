const std = @import("std");
const types = @import("core/types.zig");
const errors = @import("core/errors.zig");
const file_mod = @import("storage/file.zig");
const meta_mod = @import("storage/meta.zig");
const freelist_mod = @import("storage/freelist.zig");
const buffer_mod = @import("storage/buffer.zig");
const btree_mod = @import("storage/btree.zig");
const wal_writer = @import("wal/writer.zig");
const wal_record = @import("wal/record.zig");
const wal_recovery = @import("wal/recovery.zig");
const wal_checkpoint = @import("wal/checkpoint.zig");
const txn_manager = @import("txn/manager.zig");
const lock_mod = @import("txn/lock.zig");

const PageId = types.PageId;
const TransactionId = types.TransactionId;
const LSN = types.LSN;
const Key = types.Key;
const Value = types.Value;
const Config = types.Config;
const SyncMode = types.SyncMode;
const IsolationLevel = types.IsolationLevel;
const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
const INVALID_LSN = types.INVALID_LSN;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;

const File = file_mod.File;
const MetaManager = meta_mod.MetaManager;
const Freelist = freelist_mod.Freelist;
const BufferPool = buffer_mod.BufferPool;
const BTree = btree_mod.BTree;
const WALWriter = wal_writer.WALWriter;
const Recovery = wal_recovery.Recovery;
const Checkpoint = wal_checkpoint.Checkpoint;
const TransactionManager = txn_manager.TransactionManager;
const Transaction = txn_manager.Transaction;
const LockManager = lock_mod.LockManager;
const LockMode = lock_mod.LockMode;

/// Database handle
pub const Database = struct {
    /// Allocator
    allocator: std.mem.Allocator,
    /// Database file
    file: File,
    /// WAL writer
    wal: WALWriter,
    /// Meta manager
    meta: MetaManager,
    /// Free list manager
    freelist: Freelist,
    /// Buffer pool
    buffer_pool: BufferPool,
    /// B+ tree
    btree: BTree,
    /// Transaction manager
    txn_manager: TransactionManager,
    /// Lock manager
    lock_manager: LockManager,
    /// Checkpoint manager
    checkpoint: Checkpoint,
    /// Database path
    path: []const u8,
    /// Configuration
    config: Config,
    /// Is open
    is_open: bool,

    /// Open or create a database
    pub fn open(allocator: std.mem.Allocator, path: []const u8, config: Config) !*Database {
        const db = try allocator.create(Database);
        errdefer allocator.destroy(db);

        // Copy path
        const path_copy = try allocator.alloc(u8, path.len);
        @memcpy(path_copy, path);

        // Construct WAL path
        const wal_path = try std.fmt.allocPrint(allocator, "{s}.wal", .{path});
        defer allocator.free(wal_path);

        // Open database file and WAL first (these don't take pointers to other components)
        const file = try File.open(allocator, path, config.page_size);
        const wal = try WALWriter.open(allocator, wal_path, config.sync_mode);

        // Calculate buffer pool size (number of frames)
        const pool_size = config.cache_size / config.page_size;

        // Store base values in db first
        db.* = .{
            .allocator = allocator,
            .file = file,
            .wal = wal,
            .meta = undefined,
            .freelist = undefined,
            .buffer_pool = undefined,
            .btree = undefined,
            .txn_manager = undefined,
            .lock_manager = undefined,
            .checkpoint = undefined,
            .path = path_copy,
            .config = config,
            .is_open = true,
        };

        // Now initialize components that take pointers (using db.* pointers)
        db.meta = try MetaManager.init(allocator, &db.file);
        db.buffer_pool = try BufferPool.init(allocator, &db.file, pool_size);
        db.freelist = try Freelist.init(allocator, &db.file, db.meta.getMeta().freelist_page_id);
        db.btree = BTree.init(allocator, &db.buffer_pool, &db.freelist, db.meta.getMeta().root_page_id);
        db.txn_manager = TransactionManager.init(allocator, &db.wal, config.isolation_level);
        db.lock_manager = LockManager.init(allocator, 1000);
        db.checkpoint = Checkpoint.init(allocator, &db.wal, &db.buffer_pool, config.checkpoint_interval);

        // Perform recovery if needed
        try db.recover();

        return db;
    }

    /// Close the database
    pub fn close(self: *Database) void {
        if (!self.is_open) return;

        // Flush all dirty pages
        self.buffer_pool.flushAll() catch {};

        // Update meta
        var new_meta = self.meta.getMeta().*;
        new_meta.root_page_id = self.btree.getRootPageId();
        new_meta.freelist_page_id = self.freelist.getHeadPageId();
        self.meta.update(new_meta) catch {};

        // Persist freelist
        self.freelist.persist() catch {};

        // Sync WAL
        self.wal.sync() catch {};

        // Close components
        self.wal.close();
        self.lock_manager.deinit();
        self.txn_manager.deinit();
        self.freelist.deinit();
        self.buffer_pool.deinit();
        self.file.close();
        self.allocator.free(self.path);
        self.is_open = false;

        self.allocator.destroy(self);
    }

    /// Recover from crash
    fn recover(self: *Database) !void {
        const wal_path = try std.fmt.allocPrint(self.allocator, "{s}.wal", .{self.path});
        defer self.allocator.free(wal_path);

        var recovery = Recovery.init(self.allocator, wal_path);
        defer recovery.deinit();

        var result = try recovery.recover();
        defer result.deinit();

        // Set next transaction ID
        self.txn_manager.setNextTxnId(@intCast(result.next_lsn));
    }

    /// Begin a new transaction
    pub fn begin(self: *Database) !*Transaction {
        return self.txn_manager.begin();
    }

    /// Begin a read-only transaction
    pub fn beginReadOnly(self: *Database) !*Transaction {
        return self.txn_manager.beginReadOnly();
    }

    /// Get a value by key
    pub fn get(self: *Database, key: Key) !?Value {
        return self.btree.search(key);
    }

    /// Put a key-value pair
    pub fn put(self: *Database, key: Key, value: Value) !void {
        try self.btree.insert(key, value);
    }

    /// Delete a key
    pub fn delete(self: *Database, key: Key) !void {
        try self.btree.delete(key);
    }

    /// Get with transaction
    pub fn getTxn(self: *Database, txn: *Transaction, key: Key) !?Value {
        // Acquire shared lock
        try self.lock_manager.acquire(txn.id, key, .shared);
        return self.btree.search(key);
    }

    /// Put with transaction
    pub fn putTxn(self: *Database, txn: *Transaction, key: Key, value: Value) !void {
        // Acquire exclusive lock
        try self.lock_manager.acquire(txn.id, key, .exclusive);

        // Log the operation
        _ = try txn.logInsert(self.btree.getRootPageId(), key, value);

        // Perform the insert
        try self.btree.insert(key, value);
    }

    /// Delete with transaction
    pub fn deleteTxn(self: *Database, txn: *Transaction, key: Key) !void {
        // Acquire exclusive lock
        try self.lock_manager.acquire(txn.id, key, .exclusive);

        // Log the operation
        _ = try txn.logDelete(self.btree.getRootPageId(), key);

        // Perform the delete
        try self.btree.delete(key);
    }

    /// Commit a transaction
    pub fn commit(self: *Database, txn: *Transaction) !void {
        try txn.commit();
        self.lock_manager.releaseAll(txn.id);
        try self.checkpoint.onCommit();
    }

    /// Abort a transaction
    pub fn abort(self: *Database, txn: *Transaction) !void {
        try txn.abort();
        self.lock_manager.releaseAll(txn.id);
    }

    /// Range scan
    pub fn range(self: *Database, start_key: ?Key, end_key: ?Key) !btree_mod.RangeIterator {
        return self.btree.range(start_key, end_key);
    }

    /// Force checkpoint
    pub fn forceCheckpoint(self: *Database) !void {
        const active_txns = try self.txn_manager.getActiveTxnIds();
        defer self.allocator.free(active_txns);

        try self.checkpoint.forceCheckpoint(active_txns, &[_]PageId{});
    }

    /// Flush all buffers to disk
    pub fn flush(self: *Database) !void {
        try self.buffer_pool.flushAll();
        try self.wal.sync();
    }

    /// Get database statistics
    pub fn stats(self: *Database) Stats {
        const buffer_stats = self.buffer_pool.stats();
        return .{
            .total_pages = self.file.pageCount(),
            .free_pages = self.freelist.freePageCount(),
            .buffer_pool_used = buffer_stats.used_frames,
            .buffer_pool_dirty = buffer_stats.dirty_frames,
            .active_transactions = self.txn_manager.activeCount(),
        };
    }

    /// Compact the database (vacuum)
    pub fn compact(self: *Database) !void {
        // Flush everything first
        try self.flush();

        // Force checkpoint to minimize WAL
        try self.forceCheckpoint();
    }
};

/// Database statistics
pub const Stats = struct {
    total_pages: u64,
    free_pages: u64,
    buffer_pool_used: usize,
    buffer_pool_dirty: usize,
    active_transactions: usize,
};

/// Quick database operations without explicit handle
pub fn quickOpen(allocator: std.mem.Allocator, path: []const u8) !*Database {
    return Database.open(allocator, path, Config{});
}

// Tests

test "Database open and close" {
    const allocator = std.testing.allocator;
    const path = "test_db_open.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("test_db_open.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("test_db_open.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, Config{});
    defer db.close();

    try std.testing.expect(db.is_open);
}

test "Database put and get" {
    const allocator = std.testing.allocator;
    const path = "test_db_put.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("test_db_put.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("test_db_put.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, Config{});
    defer db.close();

    try db.put("key1", "value1");

    const result = try db.get("key1");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("value1", result.?);
    allocator.free(result.?);
}

test "Database delete" {
    const allocator = std.testing.allocator;
    const path = "test_db_delete.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("test_db_delete.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("test_db_delete.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, Config{});
    defer db.close();

    try db.put("key1", "value1");
    try db.delete("key1");

    const result = try db.get("key1");
    try std.testing.expectEqual(@as(?Value, null), result);
}

test "Database transaction" {
    const allocator = std.testing.allocator;
    const path = "test_db_txn.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("test_db_txn.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("test_db_txn.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, Config{});
    defer db.close();

    const txn = try db.begin();
    try db.putTxn(txn, "key1", "value1");
    try db.commit(txn);

    const result = try db.get("key1");
    try std.testing.expect(result != null);
    allocator.free(result.?);
}

test "Database stats" {
    const allocator = std.testing.allocator;
    const path = "test_db_stats.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("test_db_stats.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("test_db_stats.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, Config{});
    defer db.close();

    const s = db.stats();
    try std.testing.expect(s.total_pages >= 2); // At least meta pages
    try std.testing.expectEqual(@as(usize, 0), s.active_transactions);
}

test "Database range scan" {
    const allocator = std.testing.allocator;
    const path = "test_db_range.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("test_db_range.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("test_db_range.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, Config{});
    defer db.close();

    try db.put("a", "1");
    try db.put("b", "2");
    try db.put("c", "3");

    var iter = try db.range("a", "c");
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |kv| {
        allocator.free(kv.key);
        allocator.free(kv.value);
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 2), count);
}

test "Database persistence" {
    const allocator = std.testing.allocator;
    const path = "test_db_persist.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("test_db_persist.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("test_db_persist.db.wal") catch {};
    }

    // Write data
    {
        const db = try Database.open(allocator, path, Config{});
        defer db.close();

        try db.put("persist_key", "persist_value");
        try db.flush();
    }

    // Read back
    {
        const db = try Database.open(allocator, path, Config{});
        defer db.close();

        const result = try db.get("persist_key");
        try std.testing.expect(result != null);
        try std.testing.expectEqualStrings("persist_value", result.?);
        allocator.free(result.?);
    }
}
