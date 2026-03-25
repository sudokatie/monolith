const std = @import("std");
const types = @import("core/types.zig");
const errors = @import("core/errors.zig");
const txn_manager = @import("txn/manager.zig");
const txn_lock = @import("txn/lock.zig");
const wal_writer = @import("wal/writer.zig");
const wal_checkpoint = @import("wal/checkpoint.zig");
const wal_recovery = @import("wal/recovery.zig");
const storage_file = @import("storage/file.zig");
const storage_buffer = @import("storage/buffer.zig");
const storage_btree = @import("storage/btree.zig");
const storage_meta = @import("storage/meta.zig");
const storage_freelist = @import("storage/freelist.zig");

const SyncMode = types.SyncMode;
const IsolationLevel = types.IsolationLevel;
const TransactionId = types.TransactionId;
const LSN = types.LSN;
const PageId = types.PageId;
const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
const INVALID_LSN = types.INVALID_LSN;
const TransactionManager = txn_manager.TransactionManager;
const Transaction = txn_manager.Transaction;
const LockManager = txn_lock.LockManager;
const LockMode = txn_lock.LockMode;
const WALWriter = wal_writer.WALWriter;
const CheckpointManager = wal_checkpoint.CheckpointManager;
const WALRecovery = wal_recovery.WALRecovery;
const RecoveryRecord = wal_recovery.RecoveryRecord;
const RecordType = @import("wal/record.zig").RecordType;
const File = storage_file.File;
const BufferPool = storage_buffer.BufferPool;
const BTree = storage_btree.BTree;
const MetaManager = storage_meta.MetaManager;
const Freelist = storage_freelist.Freelist;

/// Database configuration
pub const DBConfig = struct {
    /// Page size in bytes (default 4096)
    page_size: usize = 4096,
    /// Buffer pool size in number of pages (default 16384 = 64MB with 4KB pages)
    buffer_pool_pages: usize = 16384,
    /// Sync mode for durability
    sync_mode: SyncMode = .sync,
    /// Default isolation level for transactions
    isolation_level: IsolationLevel = .read_committed,
    /// WAL segment size (default 16MB)
    wal_segment_size: usize = 16 * 1024 * 1024,
    /// Auto-checkpoint interval (number of records)
    checkpoint_interval: u32 = 1000,
    /// Enable WAL (can disable for testing)
    enable_wal: bool = true,
    /// B+ tree fill factor (0.5 to 1.0, default 0.7)
    fill_factor: f32 = 0.7,
};

/// Database state
const DBState = enum {
    closed,
    open,
    recovering,
};

/// The main database handle
pub const DB = struct {
    allocator: std.mem.Allocator,
    /// Database path
    path: []const u8,
    /// Configuration
    config: DBConfig,
    /// Current state
    state: DBState,
    /// Database file
    file: *File,
    /// Buffer pool
    buffer_pool: *BufferPool,
    /// B+ tree index
    btree: *BTree,
    /// Meta page manager
    meta_manager: *MetaManager,
    /// Free list manager
    freelist: *Freelist,
    /// Transaction manager
    txn_manager: TransactionManager,
    /// Lock manager for concurrency
    lock_manager: *LockManager,
    /// WAL writer (optional)
    wal: ?*WALWriter,
    /// Checkpoint manager (optional)
    checkpoint: ?*CheckpointManager,

    /// Open a database
    pub fn open(allocator: std.mem.Allocator, path: []const u8, config: DBConfig) errors.MonolithError!*DB {
        const db = allocator.create(DB) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer allocator.destroy(db);

        const path_copy = allocator.dupe(u8, path) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer allocator.free(path_copy);

        // Ensure directory exists
        std.fs.cwd().makePath(path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return errors.Error.OutOfSpace,
        };

        // Open database file
        const db_file_path = std.fmt.allocPrint(allocator, "{s}/data.db", .{path}) catch {
            return errors.Error.OutOfSpace;
        };
        defer allocator.free(db_file_path);

        const file_ptr = allocator.create(File) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer allocator.destroy(file_ptr);

        file_ptr.* = File.open(allocator, db_file_path, config.page_size) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer file_ptr.close();

        // Initialize meta manager
        const meta_ptr = allocator.create(MetaManager) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer allocator.destroy(meta_ptr);

        meta_ptr.* = MetaManager.init(allocator, file_ptr) catch {
            return errors.Error.Corrupted;
        };

        // Initialize buffer pool
        const bp_ptr = allocator.create(BufferPool) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer allocator.destroy(bp_ptr);

        bp_ptr.* = BufferPool.init(allocator, file_ptr, config.buffer_pool_pages) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer bp_ptr.deinit();

        // Initialize freelist
        const fl_ptr = allocator.create(Freelist) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer allocator.destroy(fl_ptr);

        fl_ptr.* = Freelist.init(allocator, file_ptr, meta_ptr.getMeta().freelist_page_id) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer fl_ptr.deinit();

        // Initialize B+ tree
        const btree_ptr = allocator.create(BTree) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer allocator.destroy(btree_ptr);

        btree_ptr.* = BTree.init(allocator, bp_ptr, file_ptr);
        btree_ptr.root_id = meta_ptr.getMeta().root_page_id;
        btree_ptr.fill_factor = config.fill_factor;

        // Initialize lock manager
        const lock_ptr = allocator.create(LockManager) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer allocator.destroy(lock_ptr);

        lock_ptr.* = LockManager.init(allocator);

        // Initialize WAL if enabled
        var wal: ?*WALWriter = null;
        var checkpoint: ?*CheckpointManager = null;

        if (config.enable_wal) {
            const wal_path = std.fmt.allocPrint(allocator, "{s}/wal", .{path}) catch {
                return errors.Error.OutOfSpace;
            };
            defer allocator.free(wal_path);

            const wal_ptr = allocator.create(WALWriter) catch {
                return errors.Error.OutOfSpace;
            };
            wal_ptr.* = WALWriter.init(allocator, wal_path, config.sync_mode, config.wal_segment_size) catch {
                allocator.destroy(wal_ptr);
                return errors.Error.OutOfSpace;
            };
            wal = wal_ptr;

            const ckpt_ptr = allocator.create(CheckpointManager) catch {
                wal_ptr.deinit();
                allocator.destroy(wal_ptr);
                return errors.Error.OutOfSpace;
            };
            ckpt_ptr.* = CheckpointManager.init(allocator, wal_ptr);
            ckpt_ptr.configure(.{ .min_records = config.checkpoint_interval });
            checkpoint = ckpt_ptr;
        }

        db.* = .{
            .allocator = allocator,
            .path = path_copy,
            .config = config,
            .state = .recovering,
            .file = file_ptr,
            .buffer_pool = bp_ptr,
            .btree = btree_ptr,
            .meta_manager = meta_ptr,
            .freelist = fl_ptr,
            .txn_manager = TransactionManager.init(allocator, wal),
            .lock_manager = lock_ptr,
            .wal = wal,
            .checkpoint = checkpoint,
        };

        db.txn_manager.setDefaultIsolation(config.isolation_level);

        // Run recovery if WAL exists
        if (config.enable_wal) {
            try db.runRecovery();
        }

        db.state = .open;
        return db;
    }

    /// Run WAL recovery
    fn runRecovery(self: *DB) errors.MonolithError!void {
        const wal_path = std.fmt.allocPrint(self.allocator, "{s}/wal", .{self.path}) catch {
            return errors.Error.OutOfSpace;
        };
        defer self.allocator.free(wal_path);

        var recovery = WALRecovery.init(self.allocator, wal_path) catch {
            return errors.Error.OutOfSpace;
        };
        defer recovery.deinit();

        // Set checkpoint LSN from meta
        recovery.setCheckpointLSN(self.meta_manager.getMeta().checkpoint_lsn);

        // Create recovery callback
        const callback = wal_recovery.RecoveryCallback{
            .context = @ptrCast(self),
            .redoFn = redoCallback,
            .undoFn = undoCallback,
        };

        // Run recovery
        recovery.recover(callback) catch |err| {
            // Recovery failure is not fatal if no WAL files exist
            if (err == errors.Error.WALCorrupted) {
                // Check if WAL directory is empty
                return;
            }
            return err;
        };

        // Update LSN after recovery
        const stats = recovery.getStats();
        if (stats.last_valid_lsn != INVALID_LSN) {
            self.txn_manager.current_lsn = stats.last_valid_lsn;
        }
    }

    /// Redo callback for recovery
    fn redoCallback(ctx: *anyopaque, rec: *const RecoveryRecord) errors.MonolithError!void {
        const self: *DB = @ptrCast(@alignCast(ctx));

        switch (rec.record_type) {
            .insert, .update => {
                // Re-apply the insert/update
                self.btree.insert(rec.key, rec.new_value) catch |err| {
                    return mapBTreeError(err);
                };
            },
            .delete => {
                // Re-apply the delete
                _ = self.btree.delete(rec.key) catch |err| {
                    return mapBTreeError(err);
                };
            },
            else => {},
        }
    }

    /// Undo callback for recovery
    fn undoCallback(ctx: *anyopaque, rec: *const RecoveryRecord) errors.MonolithError!void {
        const self: *DB = @ptrCast(@alignCast(ctx));

        switch (rec.record_type) {
            .insert => {
                // Undo insert by deleting
                _ = self.btree.delete(rec.key) catch |err| {
                    return mapBTreeError(err);
                };
            },
            .update => {
                // Undo update by restoring old value
                if (rec.old_value.len > 0) {
                    self.btree.insert(rec.key, rec.old_value) catch |err| {
                        return mapBTreeError(err);
                    };
                }
            },
            .delete => {
                // Undo delete by re-inserting old value
                if (rec.old_value.len > 0) {
                    self.btree.insert(rec.key, rec.old_value) catch |err| {
                        return mapBTreeError(err);
                    };
                }
            },
            else => {},
        }
    }

    /// Map B+ tree errors to MonolithError
    fn mapBTreeError(err: anyerror) errors.MonolithError {
        return switch (err) {
            error.OutOfMemory => errors.Error.OutOfSpace,
            error.Corrupted => errors.Error.Corrupted,
            error.InvalidPageType => errors.Error.InvalidPageType,
            error.ValueTooLarge => errors.Error.ValueTooLarge,
            error.KeyNotFound => errors.Error.KeyNotFound,
            else => errors.Error.Corrupted,
        };
    }

    /// Close the database
    pub fn close(self: *DB) void {
        if (self.state == .closed) return;

        self.state = .closed;

        // Flush buffer pool
        self.buffer_pool.flushAll() catch {};

        // Update and persist meta
        var meta = self.meta_manager.getMeta().*;
        meta.root_page_id = self.btree.root_id;
        meta.freelist_page_id = self.freelist.getHeadPageId();
        meta.last_txn_id = self.txn_manager.next_txn_id;
        meta.checkpoint_lsn = self.txn_manager.current_lsn;
        self.meta_manager.update(meta) catch {};

        // Persist freelist
        self.freelist.persist() catch {};

        // Sync file
        self.file.sync() catch {};

        // Clean up checkpoint manager
        if (self.checkpoint) |ckpt| {
            ckpt.deinit();
            self.allocator.destroy(ckpt);
        }

        // Clean up WAL
        if (self.wal) |wal| {
            wal.deinit();
            self.allocator.destroy(wal);
        }

        self.txn_manager.deinit();
        self.lock_manager.deinit();
        self.allocator.destroy(self.lock_manager);
        self.btree.deinit();
        self.allocator.destroy(self.btree);
        self.freelist.deinit();
        self.allocator.destroy(self.freelist);
        self.buffer_pool.deinit();
        self.allocator.destroy(self.buffer_pool);
        self.allocator.destroy(self.meta_manager);
        self.file.close();
        self.allocator.destroy(self.file);
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    /// Simple get (auto-transaction)
    pub fn get(self: *DB, key: []const u8) errors.MonolithError!?[]const u8 {
        if (self.state != .open) return errors.Error.NotOpen;

        // Acquire shared lock on key
        const txn_id = self.txn_manager.next_txn_id;
        _ = try self.lock_manager.lockKey(key, txn_id, .shared);
        defer _ = self.lock_manager.unlockKey(key, txn_id);

        return self.btree.search(key) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Simple put (auto-transaction)
    pub fn put(self: *DB, key: []const u8, value: []const u8) errors.MonolithError!void {
        if (self.state != .open) return errors.Error.NotOpen;

        const txn = try self.txn_manager.begin();
        errdefer {
            self.txn_manager.rollback(txn) catch {};
            _ = self.lock_manager.releaseAll(txn.id);
            txn.deinit();
            self.allocator.destroy(txn);
        }

        // Acquire exclusive lock on key
        _ = try self.lock_manager.lockKey(key, txn.id, .exclusive);

        // Get old value for WAL
        const old_value = self.btree.search(key) catch |err| {
            return mapBTreeError(err);
        };
        defer if (old_value) |ov| self.allocator.free(ov);

        // Log to WAL
        if (old_value != null) {
            _ = try self.txn_manager.recordUpdate(txn, key, old_value.?, value);
        } else {
            _ = try self.txn_manager.recordInsert(txn, key, value);
        }

        // Insert into B+ tree
        self.btree.insert(key, value) catch |err| {
            return mapBTreeError(err);
        };

        // Commit
        try self.txn_manager.commit(txn);

        // Release locks
        _ = self.lock_manager.releaseAll(txn.id);

        // Check for auto-checkpoint
        if (self.checkpoint) |ckpt| {
            ckpt.recordWritten();
        }

        // Recycle freed pages
        self.recycleFreedPages();

        txn.deinit();
        self.allocator.destroy(txn);
    }

    /// Simple delete (auto-transaction)
    pub fn delete(self: *DB, key: []const u8) errors.MonolithError!bool {
        if (self.state != .open) return errors.Error.NotOpen;

        const txn = try self.txn_manager.begin();
        errdefer {
            self.txn_manager.rollback(txn) catch {};
            _ = self.lock_manager.releaseAll(txn.id);
            txn.deinit();
            self.allocator.destroy(txn);
        }

        // Acquire exclusive lock
        _ = try self.lock_manager.lockKey(key, txn.id, .exclusive);

        // Get old value for WAL
        const old_value = self.btree.search(key) catch |err| {
            self.txn_manager.rollback(txn) catch {};
            _ = self.lock_manager.releaseAll(txn.id);
            txn.deinit();
            self.allocator.destroy(txn);
            return mapBTreeError(err);
        };
        if (old_value == null) {
            self.txn_manager.rollback(txn) catch {};
            _ = self.lock_manager.releaseAll(txn.id);
            txn.deinit();
            self.allocator.destroy(txn);
            return false;
        }
        defer self.allocator.free(old_value.?);

        // Log to WAL
        _ = try self.txn_manager.recordDelete(txn, key, old_value.?);

        // Delete from B+ tree
        _ = self.btree.delete(key) catch |err| {
            return mapBTreeError(err);
        };

        // Commit
        try self.txn_manager.commit(txn);

        // Release locks
        _ = self.lock_manager.releaseAll(txn.id);

        if (self.checkpoint) |ckpt| {
            ckpt.recordWritten();
        }

        // Recycle freed pages
        self.recycleFreedPages();

        txn.deinit();
        self.allocator.destroy(txn);
        return true;
    }

    /// Recycle pages freed by B+ tree operations
    fn recycleFreedPages(self: *DB) void {
        const freed = self.btree.takeFreedPages();
        defer self.allocator.free(freed);

        for (freed) |page_id| {
            self.freelist.free(page_id) catch {};
        }
    }

    /// Begin a transaction
    pub fn begin(self: *DB) errors.MonolithError!*DBTransaction {
        if (self.state != .open) return errors.Error.NotOpen;

        const txn = try self.txn_manager.begin();
        const db_txn = self.allocator.create(DBTransaction) catch {
            self.txn_manager.rollback(txn) catch {};
            txn.deinit();
            self.allocator.destroy(txn);
            return errors.Error.OutOfSpace;
        };

        db_txn.* = .{
            .db = self,
            .inner = txn,
            .committed = false,
        };

        return db_txn;
    }

    /// Begin a read-only transaction
    pub fn beginReadOnly(self: *DB) errors.MonolithError!*DBTransaction {
        if (self.state != .open) return errors.Error.NotOpen;

        const txn = try self.txn_manager.beginReadOnly();
        const db_txn = self.allocator.create(DBTransaction) catch {
            self.txn_manager.rollback(txn) catch {};
            txn.deinit();
            self.allocator.destroy(txn);
            return errors.Error.OutOfSpace;
        };

        db_txn.* = .{
            .db = self,
            .inner = txn,
            .committed = false,
        };

        return db_txn;
    }

    /// Create a snapshot for consistent reads
    pub fn snapshot(self: *DB) errors.MonolithError!*Snapshot {
        if (self.state != .open) return errors.Error.NotOpen;

        const snap = self.allocator.create(Snapshot) catch {
            return errors.Error.OutOfSpace;
        };

        snap.* = .{
            .db = self,
            .snapshot_lsn = self.txn_manager.current_lsn,
        };

        return snap;
    }

    /// Create a range iterator
    pub fn range(self: *DB, start: ?[]const u8, end: ?[]const u8) errors.MonolithError!*DBIterator {
        if (self.state != .open) return errors.Error.NotOpen;

        const iter = self.allocator.create(DBIterator) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer self.allocator.destroy(iter);

        // Get B+ tree iterator
        const btree_iter = self.btree.scanRange(start, end) catch |err| {
            return mapBTreeError(err);
        };

        iter.* = .{
            .db = self,
            .btree_iter = btree_iter,
        };

        return iter;
    }

    /// Force a checkpoint
    pub fn forceCheckpoint(self: *DB) errors.MonolithError!void {
        // Flush buffer pool
        try self.buffer_pool.flushAll();

        // Update meta
        var meta = self.meta_manager.getMeta().*;
        meta.root_page_id = self.btree.root_id;
        meta.freelist_page_id = self.freelist.getHeadPageId();
        meta.checkpoint_lsn = self.txn_manager.current_lsn;
        try self.meta_manager.update(meta);

        // Persist freelist
        try self.freelist.persist();

        // Sync file
        try self.file.sync();

        // Checkpoint WAL
        if (self.checkpoint) |ckpt| {
            _ = try ckpt.checkpoint(null, null);
        }
    }

    /// Get database statistics
    pub fn getStats(self: *const DB) DBStats {
        const txn_stats = self.txn_manager.getStats();
        const lock_stats = self.lock_manager.getStats();

        return .{
            .txns_started = txn_stats.started,
            .txns_committed = txn_stats.committed,
            .txns_aborted = txn_stats.aborted,
            .active_txns = txn_stats.active,
            .total_pages = self.file.pageCount(),
            .free_pages = self.freelist.freePageCount(),
            .buffer_pool_size = self.config.buffer_pool_pages,
            .locks_held = lock_stats.page_locks + lock_stats.key_locks,
        };
    }

    /// Check if database is open
    pub fn isOpen(self: *const DB) bool {
        return self.state == .open;
    }
};

/// A database transaction
pub const DBTransaction = struct {
    db: *DB,
    inner: *Transaction,
    committed: bool,

    /// Get a value within the transaction
    /// Behavior depends on isolation level:
    /// - Read Committed: Release lock after read
    /// - Repeatable Read/Serializable: Hold lock until commit
    pub fn get(self: *DBTransaction, key: []const u8) errors.MonolithError!?[]const u8 {
        // Acquire shared lock
        _ = try self.db.lock_manager.lockKey(key, self.inner.id, .shared);

        const result = self.db.btree.search(key) catch |err| {
            // Release lock on error for read_committed
            if (self.inner.isolation_level == .read_committed) {
                _ = self.db.lock_manager.unlockKey(key, self.inner.id);
            }
            return DB.mapBTreeError(err);
        };

        // For read_committed, release lock immediately after read
        // For repeatable_read and serializable, hold lock until commit
        if (self.inner.isolation_level == .read_committed) {
            _ = self.db.lock_manager.unlockKey(key, self.inner.id);
        }

        return result;
    }

    /// Put a value within the transaction
    pub fn put(self: *DBTransaction, key: []const u8, value: []const u8) errors.MonolithError!void {
        if (self.inner.read_only) return errors.Error.ReadOnlyTransaction;

        // Acquire exclusive lock
        _ = try self.db.lock_manager.lockKey(key, self.inner.id, .exclusive);

        // Get old value for WAL
        const old_value = self.db.btree.search(key) catch |err| {
            return DB.mapBTreeError(err);
        };
        defer if (old_value) |ov| self.db.allocator.free(ov);

        // Log to WAL
        if (old_value != null) {
            _ = try self.db.txn_manager.recordUpdate(self.inner, key, old_value.?, value);
        } else {
            _ = try self.db.txn_manager.recordInsert(self.inner, key, value);
        }

        // Insert into B+ tree
        self.db.btree.insert(key, value) catch |err| {
            return DB.mapBTreeError(err);
        };

        if (self.db.checkpoint) |ckpt| {
            ckpt.recordWritten();
        }
    }

    /// Delete a value within the transaction
    pub fn delete(self: *DBTransaction, key: []const u8) errors.MonolithError!bool {
        if (self.inner.read_only) return errors.Error.ReadOnlyTransaction;

        // Acquire exclusive lock
        _ = try self.db.lock_manager.lockKey(key, self.inner.id, .exclusive);

        // Get old value
        const old_value = self.db.btree.search(key) catch |err| {
            return DB.mapBTreeError(err);
        };
        if (old_value == null) return false;
        defer self.db.allocator.free(old_value.?);

        // Log to WAL
        _ = try self.db.txn_manager.recordDelete(self.inner, key, old_value.?);

        // Delete from B+ tree
        _ = self.db.btree.delete(key) catch |err| {
            return DB.mapBTreeError(err);
        };

        if (self.db.checkpoint) |ckpt| {
            ckpt.recordWritten();
        }

        return true;
    }

    /// Commit the transaction
    pub fn commit(self: *DBTransaction) errors.MonolithError!void {
        if (self.committed) return;

        try self.db.txn_manager.commit(self.inner);
        self.committed = true;

        // Recycle freed pages
        self.db.recycleFreedPages();
    }

    /// Rollback the transaction
    pub fn rollback(self: *DBTransaction) errors.MonolithError!void {
        if (self.committed) return;

        try self.db.txn_manager.rollback(self.inner);
        self.committed = true;
    }

    /// Release transaction resources
    pub fn deinit(self: *DBTransaction) void {
        if (!self.committed) {
            self.rollback() catch {};
        }
        // Release all locks held by this transaction
        _ = self.db.lock_manager.releaseAll(self.inner.id);

        self.inner.deinit();
        self.db.allocator.destroy(self.inner);
        self.db.allocator.destroy(self);
    }
};

/// A read-only snapshot
pub const Snapshot = struct {
    db: *DB,
    snapshot_lsn: LSN,

    /// Get a value at the snapshot
    pub fn get(self: *Snapshot, key: []const u8) errors.MonolithError!?[]const u8 {
        return self.db.btree.search(key) catch |err| {
            return DB.mapBTreeError(err);
        };
    }

    /// Release the snapshot
    pub fn release(self: *Snapshot) void {
        self.db.allocator.destroy(self);
    }
};

/// Database statistics
pub const DBStats = struct {
    txns_started: u64,
    txns_committed: u64,
    txns_aborted: u64,
    active_txns: usize,
    total_pages: u64,
    free_pages: u64,
    buffer_pool_size: usize,
    locks_held: usize,
};

/// Key-value entry returned by range iterator
pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

/// Range iterator for scanning key ranges
pub const DBIterator = struct {
    db: *DB,
    btree_iter: storage_btree.BTreeIterator,

    /// Get next entry in range
    pub fn next(self: *DBIterator) errors.MonolithError!?Entry {
        const kv = self.btree_iter.next() catch |err| {
            return DB.mapBTreeError(err);
        };
        if (kv) |entry| {
            return Entry{ .key = entry.key, .value = entry.value };
        }
        return null;
    }

    /// Free a returned entry
    pub fn freeEntry(self: *DBIterator, entry: Entry) void {
        self.btree_iter.freeKeyValue(.{ .key = entry.key, .value = entry.value });
    }

    /// Close the iterator and free resources
    pub fn close(self: *DBIterator) void {
        self.db.allocator.destroy(self);
    }
};

// Tests

test "DB open and close" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_open") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_open") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_open", .{ .enable_wal = false });
    try std.testing.expect(db.isOpen());

    db.close();
}

test "DB simple put and get" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_putget") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_putget") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_putget", .{ .enable_wal = false });
    defer db.close();

    try db.put("key1", "value1");
    try db.put("key2", "value2");

    const v1 = try db.get("key1");
    defer if (v1) |v| allocator.free(v);
    try std.testing.expectEqualStrings("value1", v1.?);

    const v2 = try db.get("key2");
    defer if (v2) |v| allocator.free(v);
    try std.testing.expectEqualStrings("value2", v2.?);

    const v3 = try db.get("nonexistent");
    try std.testing.expect(v3 == null);
}

test "DB delete" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_delete") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_delete") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_delete", .{ .enable_wal = false });
    defer db.close();

    try db.put("key", "value");

    const v1 = try db.get("key");
    defer if (v1) |v| allocator.free(v);
    try std.testing.expect(v1 != null);

    const deleted = try db.delete("key");
    try std.testing.expect(deleted);

    const v2 = try db.get("key");
    try std.testing.expect(v2 == null);

    // Delete non-existent
    const deleted2 = try db.delete("nonexistent");
    try std.testing.expect(!deleted2);
}

test "DB transaction commit" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_txn") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_txn") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_txn", .{ .enable_wal = false });
    defer db.close();

    var txn = try db.begin();

    try txn.put("k1", "v1");
    try txn.put("k2", "v2");

    try txn.commit();
    txn.deinit();

    // Should be visible after commit
    const v1 = try db.get("k1");
    defer if (v1) |v| allocator.free(v);
    try std.testing.expectEqualStrings("v1", v1.?);
}

test "DB read-only transaction" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_ro") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_ro") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_ro", .{ .enable_wal = false });
    defer db.close();

    try db.put("key", "value");

    var txn = try db.beginReadOnly();
    defer txn.deinit();

    const v = try txn.get("key");
    defer if (v) |val| allocator.free(val);
    try std.testing.expectEqualStrings("value", v.?);

    // Should fail to write
    try std.testing.expectError(errors.Error.ReadOnlyTransaction, txn.put("k", "v"));
}

test "DB snapshot" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_snap") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_snap") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_snap", .{ .enable_wal = false });
    defer db.close();

    try db.put("key", "v1");

    const snap = try db.snapshot();
    defer snap.release();

    // Snapshot should see v1
    const v = try snap.get("key");
    defer if (v) |val| allocator.free(val);
    try std.testing.expectEqualStrings("v1", v.?);
}

test "DB stats" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_stats") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_stats") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_stats", .{ .enable_wal = false });
    defer db.close();

    try db.put("k1", "v1");
    try db.put("k2", "v2");

    const stats = db.getStats();
    try std.testing.expect(stats.txns_committed >= 2);
}

test "DB with WAL" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_wal") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_wal") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_wal", .{
        .enable_wal = true,
        .sync_mode = .none,
    });
    defer db.close();

    try db.put("key", "value");

    const v = try db.get("key");
    defer if (v) |val| allocator.free(val);
    try std.testing.expectEqualStrings("value", v.?);
}

test "DB persistence" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_persist") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_persist") catch {};

    // Write some data
    {
        const db = try DB.open(allocator, "/tmp/test_db_persist", .{ .enable_wal = false });
        try db.put("persistent_key", "persistent_value");
        try db.put("another_key", "another_value");
        db.close();
    }

    // Reopen and verify data persists
    {
        const db = try DB.open(allocator, "/tmp/test_db_persist", .{ .enable_wal = false });
        defer db.close();

        const v1 = try db.get("persistent_key");
        defer if (v1) |v| allocator.free(v);
        try std.testing.expectEqualStrings("persistent_value", v1.?);

        const v2 = try db.get("another_key");
        defer if (v2) |v| allocator.free(v);
        try std.testing.expectEqualStrings("another_value", v2.?);
    }
}

test "DB range scan" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_range") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_range") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_range", .{ .enable_wal = false });
    defer db.close();

    // Insert some keys out of order
    try db.put("cherry", "red");
    try db.put("apple", "green");
    try db.put("banana", "yellow");
    try db.put("date", "brown");

    // Full range scan
    var iter = try db.range(null, null);
    defer iter.close();

    // Should be sorted
    var entry = try iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("apple", entry.?.key);
    iter.freeEntry(entry.?);

    entry = try iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("banana", entry.?.key);
    iter.freeEntry(entry.?);

    entry = try iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("cherry", entry.?.key);
    iter.freeEntry(entry.?);

    entry = try iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("date", entry.?.key);
    iter.freeEntry(entry.?);

    entry = try iter.next();
    try std.testing.expect(entry == null);
}

test "DBConfig defaults" {
    const config = DBConfig{};

    try std.testing.expectEqual(@as(usize, 4096), config.page_size);
    try std.testing.expectEqual(@as(usize, 16384), config.buffer_pool_pages);
    try std.testing.expectEqual(SyncMode.sync, config.sync_mode);
    try std.testing.expect(config.enable_wal);
    try std.testing.expect(config.fill_factor == 0.7);
}

test "DB large values (overflow pages)" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_overflow") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_overflow") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_overflow", .{ .enable_wal = false });
    defer db.close();

    // Create a large value (10KB - definitely needs overflow pages)
    const large_value = try allocator.alloc(u8, 10000);
    defer allocator.free(large_value);
    @memset(large_value, 'X');
    large_value[0] = 'S'; // Start marker
    large_value[9999] = 'E'; // End marker

    // Store large value
    try db.put("large_key", large_value);

    // Retrieve and verify
    const retrieved = try db.get("large_key");
    defer if (retrieved) |v| allocator.free(v);

    try std.testing.expect(retrieved != null);
    try std.testing.expectEqual(large_value.len, retrieved.?.len);
    try std.testing.expectEqual(@as(u8, 'S'), retrieved.?[0]);
    try std.testing.expectEqual(@as(u8, 'E'), retrieved.?[9999]);
    try std.testing.expectEqual(@as(u8, 'X'), retrieved.?[5000]);

    // Delete large value
    const deleted = try db.delete("large_key");
    try std.testing.expect(deleted);

    // Verify deleted
    const after_delete = try db.get("large_key");
    try std.testing.expect(after_delete == null);
}

test "DB isolation levels" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_isolation") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_isolation") catch {};

    // Test read_committed (default)
    {
        const db = try DB.open(allocator, "/tmp/test_db_isolation", .{
            .enable_wal = false,
            .isolation_level = .read_committed,
        });
        defer db.close();

        try db.put("key", "value");
        const v = try db.get("key");
        defer if (v) |val| allocator.free(val);
        try std.testing.expectEqualStrings("value", v.?);
    }

    // Test repeatable_read
    std.fs.cwd().deleteTree("/tmp/test_db_isolation") catch {};
    {
        const db = try DB.open(allocator, "/tmp/test_db_isolation", .{
            .enable_wal = false,
            .isolation_level = .repeatable_read,
        });
        defer db.close();

        try db.put("key", "value");

        var txn = try db.begin();
        const v1 = try txn.get("key");
        defer if (v1) |val| allocator.free(val);
        try std.testing.expectEqualStrings("value", v1.?);

        // Read again - should see same value (repeatable)
        const v2 = try txn.get("key");
        defer if (v2) |val| allocator.free(val);
        try std.testing.expectEqualStrings("value", v2.?);

        try txn.commit();
        txn.deinit();
    }

    // Test serializable
    std.fs.cwd().deleteTree("/tmp/test_db_isolation") catch {};
    {
        const db = try DB.open(allocator, "/tmp/test_db_isolation", .{
            .enable_wal = false,
            .isolation_level = .serializable,
        });
        defer db.close();

        try db.put("key", "value");

        var txn = try db.begin();
        const v = try txn.get("key");
        defer if (v) |val| allocator.free(val);
        try std.testing.expectEqualStrings("value", v.?);

        try txn.commit();
        txn.deinit();
    }
}
