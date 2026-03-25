const std = @import("std");
const types = @import("core/types.zig");
const errors = @import("core/errors.zig");
const file_mod = @import("storage/file.zig");
const meta_mod = @import("storage/meta.zig");
const freelist_mod = @import("storage/freelist.zig");
const buffer_mod = @import("storage/buffer.zig");
const btree_mod = @import("storage/btree.zig");
const overflow_mod = @import("storage/overflow.zig");
const wal_writer = @import("wal/writer.zig");
const wal_record = @import("wal/record.zig");
const wal_recovery = @import("wal/recovery.zig");
const wal_checkpoint = @import("wal/checkpoint.zig");
const txn_manager = @import("txn/manager.zig");
const lock_mod = @import("txn/lock.zig");
const mvcc_mod = @import("txn/mvcc.zig");

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
const OverflowManager = overflow_mod.OverflowManager;
const MVCCStore = mvcc_mod.MVCCStore;

/// Overflow threshold - values larger than this use overflow pages
const OVERFLOW_THRESHOLD: usize = 1024; // 1KB

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
    /// Overflow page manager
    overflow_manager: OverflowManager,
    /// MVCC store for version visibility
    mvcc_store: MVCCStore,
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
            .overflow_manager = undefined,
            .mvcc_store = undefined,
            .path = path_copy,
            .config = config,
            .is_open = true,
        };

        // Now initialize components that take pointers (using db.* pointers)
        db.meta = try MetaManager.init(allocator, &db.file);
        db.buffer_pool = try BufferPool.init(allocator, &db.file, pool_size);
        db.freelist = try Freelist.init(allocator, &db.file, db.meta.getMeta().freelist_page_id);
        db.btree = BTree.initWithFillFactor(allocator, &db.buffer_pool, &db.freelist, db.meta.getMeta().root_page_id, config.fill_factor);
        db.txn_manager = TransactionManager.init(allocator, &db.wal, config.isolation_level);
        db.lock_manager = LockManager.init(allocator, 1000);
        db.checkpoint = Checkpoint.init(allocator, &db.wal, &db.buffer_pool, config.checkpoint_interval);
        db.overflow_manager = OverflowManager.init(allocator, &db.buffer_pool, &db.file);
        db.mvcc_store = MVCCStore.init(allocator);

        // Set database pointer for transactions
        db.txn_manager.setDatabase(db);

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
        new_meta.total_pages = self.file.pageCount();
        new_meta.last_txn_id = self.txn_manager.next_txn_id;
        self.meta.update(new_meta) catch {};

        // Persist freelist
        self.freelist.persist() catch {};

        // Sync WAL
        self.wal.sync() catch {};

        // Close components
        self.wal.close();
        self.mvcc_store.deinit();
        self.overflow_manager.deinit();
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

        // Check MVCC store first for uncommitted writes visible to this txn
        if (self.mvcc_store.get(key, txn.start_ts)) |mvcc_val| {
            // Copy the value since MVCC store owns it
            const copy = try self.allocator.alloc(u8, mvcc_val.len);
            @memcpy(copy, mvcc_val);
            return copy;
        }

        // Fall back to btree for committed data
        const result = try self.btree.search(key);
        if (result) |val| {
            // Check if this is an overflow pointer
            if (val.len == 9 and val[0] == 0xFF) {
                const overflow_page_id = std.mem.bytesToValue(PageId, val[1..9]);
                self.allocator.free(val); // Free the pointer buffer
                return try self.overflow_manager.readValue(overflow_page_id);
            }
            return val;
        }
        return null;
    }

    /// Put with transaction
    pub fn putTxn(self: *Database, txn: *Transaction, key: Key, value: Value) !void {
        // Acquire exclusive lock
        try self.lock_manager.acquire(txn.id, key, .exclusive);

        // Log the operation
        _ = try txn.logInsert(self.btree.getRootPageId(), key, value);

        // Write to MVCC store for visibility tracking
        _ = try self.mvcc_store.put(txn.id, key, value);

        // Check if value needs overflow pages
        if (value.len > OVERFLOW_THRESHOLD) {
            // Store in overflow pages
            const overflow_page_id = try self.overflow_manager.storeValue(value);

            // Store overflow pointer in btree (8 bytes for page ID)
            var pointer_buf: [9]u8 = undefined;
            pointer_buf[0] = 0xFF; // Marker for overflow pointer
            @memcpy(pointer_buf[1..9], std.mem.asBytes(&overflow_page_id));
            try self.btree.insert(key, &pointer_buf);
        } else {
            // Perform normal insert to btree for persistence
            try self.btree.insert(key, value);
        }
    }

    /// Delete with transaction
    pub fn deleteTxn(self: *Database, txn: *Transaction, key: Key) !void {
        // Acquire exclusive lock
        try self.lock_manager.acquire(txn.id, key, .exclusive);

        // Log the operation
        _ = try txn.logDelete(self.btree.getRootPageId(), key);

        // Mark deleted in MVCC store
        _ = try self.mvcc_store.delete(txn.id, key);

        // Perform the delete in btree
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

    /// Create a snapshot for consistent reads
    pub fn snapshot(self: *Database) !*Snapshot {
        const snap = try self.allocator.create(Snapshot);
        snap.* = .{
            .db = self,
            .read_ts = self.txn_manager.current_ts,
            .allocator = self.allocator,
        };
        return snap;
    }
};

/// Database snapshot for consistent point-in-time reads
pub const Snapshot = struct {
    /// Database reference
    db: *Database,
    /// Read timestamp (snapshot point)
    read_ts: u64,
    /// Allocator
    allocator: std.mem.Allocator,

    /// Get a value at this snapshot's point in time
    pub fn get(self: *Snapshot, key: Key) !?Value {
        // For now, just read from btree (MVCC integration would check visibility)
        return self.db.btree.search(key);
    }

    /// Release the snapshot
    pub fn release(self: *Snapshot) void {
        self.allocator.destroy(self);
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

test "Transaction put/get/delete API" {
    const allocator = std.testing.allocator;
    const path = "test_txn_api.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("test_txn_api.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("test_txn_api.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, Config{});
    defer db.close();

    // Test txn.put and txn.get
    var txn = try db.begin();
    try txn.put("txn_key1", "txn_value1");
    try txn.put("txn_key2", "txn_value2");

    const val = try txn.get("txn_key1");
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("txn_value1", val.?);
    allocator.free(val.?);

    // Test txn.delete
    try txn.delete("txn_key2");
    const deleted = try txn.get("txn_key2");
    try std.testing.expect(deleted == null);

    try txn.commit();

    // Verify committed data
    const after = try db.get("txn_key1");
    try std.testing.expect(after != null);
    try std.testing.expectEqualStrings("txn_value1", after.?);
    allocator.free(after.?);
}

// Type aliases for root.zig compatibility
pub const DB = Database;
pub const DBConfig = Config;
pub const DBTransaction = Transaction;
pub const DBIterator = btree_mod.RangeIterator;
pub const DBStats = Stats;
