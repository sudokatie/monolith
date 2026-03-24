const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const wal_writer = @import("../wal/writer.zig");
const wal_record = @import("../wal/record.zig");

const TransactionId = types.TransactionId;
const TxnState = types.TxnState;
const IsolationLevel = types.IsolationLevel;
const LSN = types.LSN;
const INVALID_TXN_ID = types.INVALID_TXN_ID;
const INVALID_LSN = types.INVALID_LSN;
const WALWriter = wal_writer.WALWriter;

/// Write set entry - tracks a modification made by a transaction
pub const WriteEntry = struct {
    key: []const u8,
    old_value: ?[]const u8, // null for inserts
    new_value: ?[]const u8, // null for deletes
    lsn: LSN,

    pub fn deinit(self: *WriteEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        if (self.old_value) |v| allocator.free(v);
        if (self.new_value) |v| allocator.free(v);
    }
};

/// Read set entry - tracks a key read by a transaction
pub const ReadEntry = struct {
    key: []const u8,
    version_lsn: LSN, // LSN of the version that was read

    pub fn deinit(self: *ReadEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
    }
};

/// A single transaction
pub const Transaction = struct {
    /// Transaction ID
    id: TransactionId,
    /// Current state
    state: TxnState,
    /// Isolation level
    isolation_level: IsolationLevel,
    /// Snapshot LSN (for repeatable read)
    snapshot_lsn: LSN,
    /// Begin LSN
    begin_lsn: LSN,
    /// Write set
    write_set: std.ArrayList(WriteEntry),
    /// Read set (for conflict detection)
    read_set: std.ArrayList(ReadEntry),
    /// Allocator
    allocator: std.mem.Allocator,
    /// Read-only flag
    read_only: bool,

    pub fn init(
        allocator: std.mem.Allocator,
        id: TransactionId,
        isolation_level: IsolationLevel,
        snapshot_lsn: LSN,
        begin_lsn: LSN,
    ) Transaction {
        return .{
            .id = id,
            .state = .active,
            .isolation_level = isolation_level,
            .snapshot_lsn = snapshot_lsn,
            .begin_lsn = begin_lsn,
            .write_set = .{},
            .read_set = .{},
            .allocator = allocator,
            .read_only = false,
        };
    }

    pub fn deinit(self: *Transaction) void {
        for (self.write_set.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.write_set.deinit(self.allocator);

        for (self.read_set.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.read_set.deinit(self.allocator);
    }

    /// Check if transaction is active
    pub fn isActive(self: *const Transaction) bool {
        return self.state == .active;
    }

    /// Check if transaction is committed
    pub fn isCommitted(self: *const Transaction) bool {
        return self.state == .committed;
    }

    /// Check if transaction is aborted
    pub fn isAborted(self: *const Transaction) bool {
        return self.state == .aborted;
    }

    /// Add a read to the read set
    pub fn trackRead(self: *Transaction, key: []const u8, version_lsn: LSN) !void {
        const key_copy = try self.allocator.dupe(u8, key);
        try self.read_set.append(self.allocator, .{
            .key = key_copy,
            .version_lsn = version_lsn,
        });
    }

    /// Add a write to the write set
    pub fn trackWrite(
        self: *Transaction,
        key: []const u8,
        old_value: ?[]const u8,
        new_value: ?[]const u8,
        lsn: LSN,
    ) !void {
        if (self.read_only) {
            return errors.Error.ReadOnlyTransaction;
        }

        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        const old_copy = if (old_value) |v| try self.allocator.dupe(u8, v) else null;
        errdefer if (old_copy) |v| self.allocator.free(v);

        const new_copy = if (new_value) |v| try self.allocator.dupe(u8, v) else null;

        try self.write_set.append(self.allocator, .{
            .key = key_copy,
            .old_value = old_copy,
            .new_value = new_copy,
            .lsn = lsn,
        });
    }

    /// Get the number of writes in this transaction
    pub fn getWriteCount(self: *const Transaction) usize {
        return self.write_set.items.len;
    }

    /// Get the number of reads in this transaction
    pub fn getReadCount(self: *const Transaction) usize {
        return self.read_set.items.len;
    }

    /// Mark as read-only
    pub fn setReadOnly(self: *Transaction) void {
        self.read_only = true;
    }
};

/// Transaction Manager - coordinates all transactions
pub const TransactionManager = struct {
    allocator: std.mem.Allocator,
    /// WAL writer for durability
    wal: ?*WALWriter,
    /// Next transaction ID to assign
    next_txn_id: TransactionId,
    /// Active transactions
    active_txns: std.AutoHashMap(TransactionId, *Transaction),
    /// Current LSN (for snapshots)
    current_lsn: LSN,
    /// Default isolation level
    default_isolation: IsolationLevel,
    /// Statistics
    txns_started: u64,
    txns_committed: u64,
    txns_aborted: u64,

    pub fn init(allocator: std.mem.Allocator, wal: ?*WALWriter) TransactionManager {
        return .{
            .allocator = allocator,
            .wal = wal,
            .next_txn_id = 1,
            .active_txns = std.AutoHashMap(TransactionId, *Transaction).init(allocator),
            .current_lsn = 1,
            .default_isolation = .read_committed,
            .txns_started = 0,
            .txns_committed = 0,
            .txns_aborted = 0,
        };
    }

    pub fn deinit(self: *TransactionManager) void {
        // Clean up any remaining active transactions
        var it = self.active_txns.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.active_txns.deinit();
    }

    /// Set the default isolation level
    pub fn setDefaultIsolation(self: *TransactionManager, level: IsolationLevel) void {
        self.default_isolation = level;
    }

    /// Begin a new transaction
    pub fn begin(self: *TransactionManager) errors.MonolithError!*Transaction {
        return self.beginWithIsolation(self.default_isolation);
    }

    /// Begin a new transaction with specific isolation level
    pub fn beginWithIsolation(self: *TransactionManager, isolation: IsolationLevel) errors.MonolithError!*Transaction {
        const txn_id = self.allocateTransactionId();
        const snapshot_lsn = self.current_lsn;

        // Write begin record to WAL
        var begin_lsn: LSN = 0;
        if (self.wal) |wal| {
            begin_lsn = try wal.writeBegin(txn_id);
            self.current_lsn = wal.currentLSN();
        }

        // Create transaction
        const txn = self.allocator.create(Transaction) catch {
            return errors.Error.OutOfSpace;
        };
        txn.* = Transaction.init(self.allocator, txn_id, isolation, snapshot_lsn, begin_lsn);

        // Track active transaction
        self.active_txns.put(txn_id, txn) catch {
            txn.deinit();
            self.allocator.destroy(txn);
            return errors.Error.OutOfSpace;
        };

        self.txns_started += 1;
        return txn;
    }

    /// Begin a read-only transaction
    pub fn beginReadOnly(self: *TransactionManager) errors.MonolithError!*Transaction {
        const txn = try self.begin();
        txn.setReadOnly();
        return txn;
    }

    /// Commit a transaction
    pub fn commit(self: *TransactionManager, txn: *Transaction) errors.MonolithError!void {
        if (!txn.isActive()) {
            return errors.Error.TransactionInactive;
        }

        // Write commit record to WAL
        if (self.wal) |wal| {
            _ = try wal.writeCommit(txn.id);
            self.current_lsn = wal.currentLSN();
        }

        // Update state
        txn.state = .committed;
        self.txns_committed += 1;

        // Remove from active transactions
        _ = self.active_txns.remove(txn.id);
    }

    /// Rollback/abort a transaction
    pub fn rollback(self: *TransactionManager, txn: *Transaction) errors.MonolithError!void {
        if (!txn.isActive()) {
            return errors.Error.TransactionInactive;
        }

        // Write abort record to WAL
        if (self.wal) |wal| {
            _ = try wal.writeAbort(txn.id);
            self.current_lsn = wal.currentLSN();
        }

        // Update state
        txn.state = .aborted;
        self.txns_aborted += 1;

        // Remove from active transactions
        _ = self.active_txns.remove(txn.id);
    }

    /// Record an insert operation
    pub fn recordInsert(
        self: *TransactionManager,
        txn: *Transaction,
        key: []const u8,
        value: []const u8,
    ) errors.MonolithError!LSN {
        if (!txn.isActive()) {
            return errors.Error.TransactionInactive;
        }
        if (txn.read_only) {
            return errors.Error.ReadOnlyTransaction;
        }

        var lsn: LSN = 0;
        if (self.wal) |wal| {
            lsn = try wal.writeInsert(txn.id, key, value);
            self.current_lsn = wal.currentLSN();
        }

        try txn.trackWrite(key, null, value, lsn);
        return lsn;
    }

    /// Record an update operation
    pub fn recordUpdate(
        self: *TransactionManager,
        txn: *Transaction,
        key: []const u8,
        old_value: []const u8,
        new_value: []const u8,
    ) errors.MonolithError!LSN {
        if (!txn.isActive()) {
            return errors.Error.TransactionInactive;
        }
        if (txn.read_only) {
            return errors.Error.ReadOnlyTransaction;
        }

        var lsn: LSN = 0;
        if (self.wal) |wal| {
            lsn = try wal.writeUpdate(txn.id, key, old_value, new_value);
            self.current_lsn = wal.currentLSN();
        }

        try txn.trackWrite(key, old_value, new_value, lsn);
        return lsn;
    }

    /// Record a delete operation
    pub fn recordDelete(
        self: *TransactionManager,
        txn: *Transaction,
        key: []const u8,
        old_value: []const u8,
    ) errors.MonolithError!LSN {
        if (!txn.isActive()) {
            return errors.Error.TransactionInactive;
        }
        if (txn.read_only) {
            return errors.Error.ReadOnlyTransaction;
        }

        var lsn: LSN = 0;
        if (self.wal) |wal| {
            lsn = try wal.writeDelete(txn.id, key, old_value);
            self.current_lsn = wal.currentLSN();
        }

        try txn.trackWrite(key, old_value, null, lsn);
        return lsn;
    }

    /// Record a read operation (for conflict detection)
    pub fn recordRead(
        self: *TransactionManager,
        txn: *Transaction,
        key: []const u8,
        version_lsn: LSN,
    ) errors.MonolithError!void {
        _ = self;
        if (!txn.isActive()) {
            return errors.Error.TransactionInactive;
        }

        try txn.trackRead(key, version_lsn);
    }

    /// Get a transaction by ID
    pub fn getTransaction(self: *TransactionManager, txn_id: TransactionId) ?*Transaction {
        return self.active_txns.get(txn_id);
    }

    /// Get number of active transactions
    pub fn getActiveCount(self: *const TransactionManager) usize {
        return self.active_txns.count();
    }

    /// Get transaction statistics
    pub fn getStats(self: *const TransactionManager) TxnStats {
        return .{
            .started = self.txns_started,
            .committed = self.txns_committed,
            .aborted = self.txns_aborted,
            .active = self.active_txns.count(),
        };
    }

    /// Allocate next transaction ID
    fn allocateTransactionId(self: *TransactionManager) TransactionId {
        const id = self.next_txn_id;
        self.next_txn_id += 1;
        return id;
    }

    /// Set next transaction ID (for recovery)
    pub fn setNextTransactionId(self: *TransactionManager, id: TransactionId) void {
        self.next_txn_id = id;
    }

    /// Update current LSN (for recovery)
    pub fn setCurrentLSN(self: *TransactionManager, lsn: LSN) void {
        self.current_lsn = lsn;
    }
};

/// Transaction statistics
pub const TxnStats = struct {
    started: u64,
    committed: u64,
    aborted: u64,
    active: usize,
};

// Tests

test "Transaction init and deinit" {
    const allocator = std.testing.allocator;

    var txn = Transaction.init(allocator, 1, .read_committed, 100, 1);
    defer txn.deinit();

    try std.testing.expectEqual(@as(TransactionId, 1), txn.id);
    try std.testing.expectEqual(TxnState.active, txn.state);
    try std.testing.expect(txn.isActive());
    try std.testing.expect(!txn.isCommitted());
}

test "Transaction track read and write" {
    const allocator = std.testing.allocator;

    var txn = Transaction.init(allocator, 1, .read_committed, 100, 1);
    defer txn.deinit();

    try txn.trackRead("key1", 50);
    try txn.trackWrite("key2", null, "value2", 60);
    try txn.trackWrite("key3", "old", "new", 70);

    try std.testing.expectEqual(@as(usize, 1), txn.getReadCount());
    try std.testing.expectEqual(@as(usize, 2), txn.getWriteCount());
}

test "Transaction read-only" {
    const allocator = std.testing.allocator;

    var txn = Transaction.init(allocator, 1, .read_committed, 100, 1);
    defer txn.deinit();

    txn.setReadOnly();

    const result = txn.trackWrite("key", null, "value", 1);
    try std.testing.expectError(errors.Error.ReadOnlyTransaction, result);
}

test "TransactionManager init" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null);
    defer manager.deinit();

    try std.testing.expectEqual(@as(usize, 0), manager.getActiveCount());
}

test "TransactionManager begin and commit" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null);
    defer manager.deinit();

    var txn = try manager.begin();
    try std.testing.expect(txn.isActive());
    try std.testing.expectEqual(@as(usize, 1), manager.getActiveCount());

    try manager.commit(txn);
    try std.testing.expect(txn.isCommitted());
    try std.testing.expectEqual(@as(usize, 0), manager.getActiveCount());

    // Clean up
    txn.deinit();
    allocator.destroy(txn);
}

test "TransactionManager begin and rollback" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null);
    defer manager.deinit();

    var txn = try manager.begin();
    try manager.rollback(txn);

    try std.testing.expect(txn.isAborted());
    try std.testing.expectEqual(@as(usize, 0), manager.getActiveCount());

    const stats = manager.getStats();
    try std.testing.expectEqual(@as(u64, 1), stats.started);
    try std.testing.expectEqual(@as(u64, 0), stats.committed);
    try std.testing.expectEqual(@as(u64, 1), stats.aborted);

    txn.deinit();
    allocator.destroy(txn);
}

test "TransactionManager multiple transactions" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null);
    defer manager.deinit();

    var txn1 = try manager.begin();
    var txn2 = try manager.begin();
    var txn3 = try manager.begin();

    try std.testing.expectEqual(@as(usize, 3), manager.getActiveCount());

    // Different IDs
    try std.testing.expect(txn1.id != txn2.id);
    try std.testing.expect(txn2.id != txn3.id);

    try manager.commit(txn1);
    try manager.rollback(txn2);
    try manager.commit(txn3);

    try std.testing.expectEqual(@as(usize, 0), manager.getActiveCount());

    const stats = manager.getStats();
    try std.testing.expectEqual(@as(u64, 3), stats.started);
    try std.testing.expectEqual(@as(u64, 2), stats.committed);
    try std.testing.expectEqual(@as(u64, 1), stats.aborted);

    txn1.deinit();
    allocator.destroy(txn1);
    txn2.deinit();
    allocator.destroy(txn2);
    txn3.deinit();
    allocator.destroy(txn3);
}

test "TransactionManager record operations" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null);
    defer manager.deinit();

    var txn = try manager.begin();

    _ = try manager.recordInsert(txn, "key1", "value1");
    _ = try manager.recordUpdate(txn, "key2", "old", "new");
    _ = try manager.recordDelete(txn, "key3", "deleted");

    try std.testing.expectEqual(@as(usize, 3), txn.getWriteCount());

    try manager.commit(txn);

    txn.deinit();
    allocator.destroy(txn);
}

test "TransactionManager with WAL" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_txn_wal") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_txn_wal") catch {};

    var wal = try WALWriter.init(allocator, "/tmp/test_txn_wal", .none, wal_writer.DEFAULT_SEGMENT_SIZE);
    defer wal.deinit();

    var manager = TransactionManager.init(allocator, &wal);
    defer manager.deinit();

    var txn = try manager.begin();
    _ = try manager.recordInsert(txn, "key", "value");
    try manager.commit(txn);

    // WAL should have records
    try std.testing.expect(wal.getPosition() > wal_writer.WAL_HEADER_SIZE);

    txn.deinit();
    allocator.destroy(txn);
}

test "TransactionManager inactive transaction errors" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null);
    defer manager.deinit();

    var txn = try manager.begin();
    try manager.commit(txn);

    // Should fail - already committed
    try std.testing.expectError(errors.Error.TransactionInactive, manager.commit(txn));
    try std.testing.expectError(errors.Error.TransactionInactive, manager.rollback(txn));
    try std.testing.expectError(errors.Error.TransactionInactive, manager.recordInsert(txn, "k", "v"));

    txn.deinit();
    allocator.destroy(txn);
}

test "TransactionManager read-only transaction" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null);
    defer manager.deinit();

    var txn = try manager.beginReadOnly();

    // Reads should work
    try manager.recordRead(txn, "key", 50);
    try std.testing.expectEqual(@as(usize, 1), txn.getReadCount());

    // Writes should fail
    try std.testing.expectError(errors.Error.ReadOnlyTransaction, manager.recordInsert(txn, "k", "v"));

    try manager.commit(txn);

    txn.deinit();
    allocator.destroy(txn);
}

test "TransactionManager isolation levels" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null);
    defer manager.deinit();

    manager.setDefaultIsolation(.repeatable_read);

    var txn = try manager.begin();
    try std.testing.expectEqual(IsolationLevel.repeatable_read, txn.isolation_level);

    var txn2 = try manager.beginWithIsolation(.serializable);
    try std.testing.expectEqual(IsolationLevel.serializable, txn2.isolation_level);

    try manager.rollback(txn);
    try manager.rollback(txn2);

    txn.deinit();
    allocator.destroy(txn);
    txn2.deinit();
    allocator.destroy(txn2);
}

test "TxnStats" {
    const stats = TxnStats{
        .started = 100,
        .committed = 80,
        .aborted = 15,
        .active = 5,
    };

    try std.testing.expectEqual(@as(u64, 100), stats.started);
    try std.testing.expectEqual(@as(u64, 80), stats.committed);
}
