const std = @import("std");
const types = @import("core/types.zig");
const errors = @import("core/errors.zig");
const txn_manager = @import("txn/manager.zig");
const txn_mvcc = @import("txn/mvcc.zig");
const wal_writer = @import("wal/writer.zig");
const wal_checkpoint = @import("wal/checkpoint.zig");

const Config = types.Config;
const SyncMode = types.SyncMode;
const IsolationLevel = types.IsolationLevel;
const TransactionId = types.TransactionId;
const LSN = types.LSN;
const TransactionManager = txn_manager.TransactionManager;
const Transaction = txn_manager.Transaction;
const MVCCStore = txn_mvcc.MVCCStore;
const WALWriter = wal_writer.WALWriter;
const CheckpointManager = wal_checkpoint.CheckpointManager;

/// Database configuration
pub const DBConfig = struct {
    /// Page size in bytes (default 4096)
    page_size: usize = 4096,
    /// Buffer pool cache size in bytes (default 64MB)
    cache_size: usize = 64 * 1024 * 1024,
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
    /// Transaction manager
    txn_manager: TransactionManager,
    /// MVCC store
    mvcc: MVCCStore,
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
            wal_ptr.* = try WALWriter.init(allocator, wal_path, config.sync_mode, config.wal_segment_size);
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
            .state = .open,
            .txn_manager = TransactionManager.init(allocator, wal),
            .mvcc = MVCCStore.init(allocator),
            .wal = wal,
            .checkpoint = checkpoint,
        };

        db.txn_manager.setDefaultIsolation(config.isolation_level);

        return db;
    }

    /// Close the database
    pub fn close(self: *DB) void {
        if (self.state == .closed) return;

        self.state = .closed;

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
        self.mvcc.deinit();
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    /// Simple get (auto-transaction)
    pub fn get(self: *DB, key: []const u8) errors.MonolithError!?[]const u8 {
        if (self.state != .open) return errors.Error.NotOpen;

        // Use a read-only transaction
        const txn = try self.txn_manager.beginReadOnly();
        defer {
            self.txn_manager.commit(txn) catch {};
            txn.deinit();
            self.allocator.destroy(txn);
        }

        return self.mvcc.get(key, txn.id, txn.snapshot_lsn, txn.isolation_level);
    }

    /// Simple put (auto-transaction)
    pub fn put(self: *DB, key: []const u8, value: []const u8) errors.MonolithError!void {
        if (self.state != .open) return errors.Error.NotOpen;

        const txn = try self.txn_manager.begin();
        errdefer {
            self.txn_manager.rollback(txn) catch {};
            txn.deinit();
            self.allocator.destroy(txn);
        }

        const lsn = try self.txn_manager.recordInsert(txn, key, value);
        try self.mvcc.put(key, value, txn.id, lsn);

        try self.txn_manager.commit(txn);
        try self.mvcc.commitTransaction(txn.id, self.txn_manager.current_lsn);

        // Check for auto-checkpoint
        if (self.checkpoint) |ckpt| {
            ckpt.recordWritten();
        }

        txn.deinit();
        self.allocator.destroy(txn);
    }

    /// Simple delete (auto-transaction)
    pub fn delete(self: *DB, key: []const u8) errors.MonolithError!bool {
        if (self.state != .open) return errors.Error.NotOpen;

        // Check if key exists
        const old_value = self.mvcc.get(key, 0, self.txn_manager.current_lsn, .read_committed);
        if (old_value == null) return false;

        const txn = try self.txn_manager.begin();
        errdefer {
            self.txn_manager.rollback(txn) catch {};
            txn.deinit();
            self.allocator.destroy(txn);
        }

        const lsn = try self.txn_manager.recordDelete(txn, key, old_value.?);
        _ = self.mvcc.delete(key, txn.id, lsn);

        try self.txn_manager.commit(txn);
        try self.mvcc.commitTransaction(txn.id, self.txn_manager.current_lsn);

        if (self.checkpoint) |ckpt| {
            ckpt.recordWritten();
        }

        txn.deinit();
        self.allocator.destroy(txn);
        return true;
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
    /// Returns keys in [start, end) range in sorted order
    /// Pass null for start to begin from first key
    /// Pass null for end to iterate to last key
    pub fn range(self: *DB, start: ?[]const u8, end: ?[]const u8) errors.MonolithError!*DBIterator {
        if (self.state != .open) return errors.Error.NotOpen;

        const iter = self.allocator.create(DBIterator) catch {
            return errors.Error.OutOfSpace;
        };
        errdefer self.allocator.destroy(iter);

        // Collect and sort keys in range
        var keys: std.ArrayListUnmanaged([]const u8) = .empty;
        errdefer {
            for (keys.items) |k| self.allocator.free(k);
            keys.deinit(self.allocator);
        }

        var chain_iter = self.mvcc.chains.iterator();
        while (chain_iter.next()) |entry| {
            const key = entry.key_ptr.*;

            // Check range bounds
            if (start) |s| {
                if (std.mem.order(u8, key, s) == .lt) continue;
            }
            if (end) |e| {
                if (std.mem.order(u8, key, e) != .lt) continue;
            }

            // Check visibility - only include if visible
            const value = self.mvcc.get(key, 0, self.txn_manager.current_lsn, .read_committed);
            if (value != null) {
                const key_copy = self.allocator.dupe(u8, key) catch {
                    return errors.Error.OutOfSpace;
                };
                keys.append(self.allocator, key_copy) catch {
                    self.allocator.free(key_copy);
                    return errors.Error.OutOfSpace;
                };
            }
        }

        // Sort keys
        std.mem.sort([]const u8, keys.items, {}, struct {
            fn cmp(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.cmp);

        const owned_keys = keys.toOwnedSlice(self.allocator) catch {
            return errors.Error.OutOfSpace;
        };

        iter.* = .{
            .db = self,
            .keys = owned_keys,
            .index = 0,
            .snapshot_lsn = self.txn_manager.current_lsn,
        };

        return iter;
    }

    /// Force a checkpoint
    pub fn forceCheckpoint(self: *DB) errors.MonolithError!void {
        if (self.checkpoint) |ckpt| {
            _ = try ckpt.checkpoint(null, null);
        }
    }

    /// Get database statistics
    pub fn getStats(self: *const DB) DBStats {
        const txn_stats = self.txn_manager.getStats();
        const mvcc_stats = self.mvcc.getStats();

        return .{
            .txns_started = txn_stats.started,
            .txns_committed = txn_stats.committed,
            .txns_aborted = txn_stats.aborted,
            .active_txns = txn_stats.active,
            .total_keys = mvcc_stats.total_keys,
            .total_versions = mvcc_stats.total_versions,
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
    pub fn get(self: *DBTransaction, key: []const u8) ?[]const u8 {
        return self.db.mvcc.get(
            key,
            self.inner.id,
            self.inner.snapshot_lsn,
            self.inner.isolation_level,
        );
    }

    /// Put a value within the transaction
    pub fn put(self: *DBTransaction, key: []const u8, value: []const u8) errors.MonolithError!void {
        if (self.inner.read_only) return errors.Error.ReadOnlyTransaction;

        const lsn = try self.db.txn_manager.recordInsert(self.inner, key, value);
        try self.db.mvcc.put(key, value, self.inner.id, lsn);

        if (self.db.checkpoint) |ckpt| {
            ckpt.recordWritten();
        }
    }

    /// Delete a value within the transaction
    pub fn delete(self: *DBTransaction, key: []const u8) errors.MonolithError!bool {
        if (self.inner.read_only) return errors.Error.ReadOnlyTransaction;

        const old_value = self.get(key);
        if (old_value == null) return false;

        const lsn = try self.db.txn_manager.recordDelete(self.inner, key, old_value.?);
        _ = self.db.mvcc.delete(key, self.inner.id, lsn);

        if (self.db.checkpoint) |ckpt| {
            ckpt.recordWritten();
        }

        return true;
    }

    /// Commit the transaction
    pub fn commit(self: *DBTransaction) errors.MonolithError!void {
        if (self.committed) return;

        try self.db.txn_manager.commit(self.inner);
        try self.db.mvcc.commitTransaction(self.inner.id, self.db.txn_manager.current_lsn);
        self.committed = true;
    }

    /// Rollback the transaction
    pub fn rollback(self: *DBTransaction) errors.MonolithError!void {
        if (self.committed) return;

        try self.db.txn_manager.rollback(self.inner);
        try self.db.mvcc.abortTransaction(self.inner.id);
        self.committed = true; // Prevent double rollback
    }

    /// Release transaction resources
    pub fn deinit(self: *DBTransaction) void {
        if (!self.committed) {
            self.rollback() catch {};
        }
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
    pub fn get(self: *Snapshot, key: []const u8) ?[]const u8 {
        return self.db.mvcc.get(key, 0, self.snapshot_lsn, .repeatable_read);
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
    total_keys: usize,
    total_versions: u64,
};

/// Key-value entry returned by range iterator
pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

/// Range iterator for scanning key ranges
pub const DBIterator = struct {
    db: *DB,
    keys: [][]const u8,
    index: usize,
    snapshot_lsn: LSN,

    /// Get next entry in range
    /// Returns null when iteration complete
    pub fn next(self: *DBIterator) ?Entry {
        if (self.index >= self.keys.len) return null;

        const key = self.keys[self.index];
        self.index += 1;

        // Get value at snapshot
        const value = self.db.mvcc.get(key, 0, self.snapshot_lsn, .read_committed);
        if (value) |v| {
            return Entry{ .key = key, .value = v };
        }

        // Key was deleted, try next
        return self.next();
    }

    /// Close the iterator and free resources
    pub fn close(self: *DBIterator) void {
        for (self.keys) |key| {
            self.db.allocator.free(key);
        }
        self.db.allocator.free(self.keys);
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
    try std.testing.expectEqualStrings("value1", v1.?);

    const v2 = try db.get("key2");
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
    try std.testing.expect((try db.get("key")) != null);

    const deleted = try db.delete("key");
    try std.testing.expect(deleted);

    try std.testing.expect((try db.get("key")) == null);

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

    // Not visible outside transaction yet (in a proper implementation)
    // For now, MVCC makes it visible after put

    try txn.commit();
    txn.deinit();

    // Should be visible after commit
    const v1 = try db.get("k1");
    try std.testing.expectEqualStrings("v1", v1.?);
}

test "DB transaction rollback" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_rollback") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_rollback") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_rollback", .{ .enable_wal = false });
    defer db.close();

    // First, put something
    try db.put("existing", "value");

    var txn = try db.begin();
    try txn.put("new_key", "new_value");
    try txn.rollback();
    txn.deinit();

    // Existing should still be there
    const v = try db.get("existing");
    try std.testing.expectEqualStrings("value", v.?);
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

    const v = txn.get("key");
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
    const v = snap.get("key");
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
    try std.testing.expect(stats.total_keys >= 2);
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
    try std.testing.expectEqualStrings("value", v.?);
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
    var entry = iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("apple", entry.?.key);

    entry = iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("banana", entry.?.key);

    entry = iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("cherry", entry.?.key);

    entry = iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("date", entry.?.key);

    entry = iter.next();
    try std.testing.expect(entry == null);
}

test "DB range scan bounded" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_db_range_bound") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_db_range_bound") catch {};

    const db = try DB.open(allocator, "/tmp/test_db_range_bound", .{ .enable_wal = false });
    defer db.close();

    try db.put("a", "1");
    try db.put("b", "2");
    try db.put("c", "3");
    try db.put("d", "4");
    try db.put("e", "5");

    // Range [b, d) - should include b, c but not d
    var iter = try db.range("b", "d");
    defer iter.close();

    var entry = iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("b", entry.?.key);

    entry = iter.next();
    try std.testing.expect(entry != null);
    try std.testing.expectEqualStrings("c", entry.?.key);

    entry = iter.next();
    try std.testing.expect(entry == null);
}

test "DBConfig defaults" {
    const config = DBConfig{};

    try std.testing.expectEqual(@as(usize, 4096), config.page_size);
    try std.testing.expectEqual(@as(usize, 64 * 1024 * 1024), config.cache_size);
    try std.testing.expectEqual(SyncMode.sync, config.sync_mode);
    try std.testing.expect(config.enable_wal);
}

test "DBStats" {
    const stats = DBStats{
        .txns_started = 100,
        .txns_committed = 90,
        .txns_aborted = 10,
        .active_txns = 5,
        .total_keys = 1000,
        .total_versions = 2500,
    };

    try std.testing.expectEqual(@as(u64, 100), stats.txns_started);
    try std.testing.expectEqual(@as(usize, 1000), stats.total_keys);
}
