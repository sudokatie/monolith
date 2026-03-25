const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const wal_writer = @import("../wal/writer.zig");
const wal_record = @import("../wal/record.zig");

const TransactionId = types.TransactionId;
const LSN = types.LSN;
const TxnState = types.TxnState;
const IsolationLevel = types.IsolationLevel;
const Key = types.Key;
const Value = types.Value;
const PageId = types.PageId;
const INVALID_TXN_ID = types.INVALID_TXN_ID;
const INVALID_LSN = types.INVALID_LSN;

const WALWriter = wal_writer.WALWriter;
const RecordType = wal_record.RecordType;
const InsertData = wal_record.InsertData;
const DeleteData = wal_record.DeleteData;

/// Transaction handle
pub const Transaction = struct {
    /// Transaction ID
    id: TransactionId,
    /// Current state
    state: TxnState,
    /// Isolation level
    isolation: IsolationLevel,
    /// Read-only flag
    read_only: bool,
    /// Start timestamp (for MVCC)
    start_ts: u64,
    /// Last LSN in this transaction
    last_lsn: LSN,
    /// Transaction manager reference
    manager: *TransactionManager,

    /// Check if transaction is active
    pub fn isActive(self: *const Transaction) bool {
        return self.state == .active;
    }

    /// Commit the transaction
    pub fn commit(self: *Transaction) !void {
        if (self.state != .active) {
            return errors.Error.TransactionInactive;
        }

        // Write commit record
        if (self.manager.wal) |wal| {
            _ = try wal.append(self.id, self.last_lsn, .commit, "");
            try wal.sync();
        }

        self.state = .committed;
        self.manager.onCommit(self.id);
    }

    /// Abort the transaction
    pub fn abort(self: *Transaction) !void {
        if (self.state != .active) {
            return errors.Error.TransactionInactive;
        }

        // Write abort record
        if (self.manager.wal) |wal| {
            _ = try wal.append(self.id, self.last_lsn, .abort, "");
        }

        self.state = .aborted;
        self.manager.onAbort(self.id);
    }

    /// Log an insert operation
    pub fn logInsert(self: *Transaction, page_id: PageId, key: Key, value: Value) !LSN {
        if (self.state != .active) {
            return errors.Error.TransactionInactive;
        }
        if (self.read_only) {
            return errors.Error.ReadOnlyTransaction;
        }

        if (self.manager.wal) |wal| {
            const insert = InsertData{
                .page_id = page_id,
                .key = key,
                .value = value,
            };

            const data = try self.manager.allocator.alloc(u8, insert.serializedSize());
            defer self.manager.allocator.free(data);
            insert.serialize(data);

            self.last_lsn = try wal.append(self.id, self.last_lsn, .insert, data);
            return self.last_lsn;
        }

        return INVALID_LSN;
    }

    /// Log a delete operation
    pub fn logDelete(self: *Transaction, page_id: PageId, key: Key) !LSN {
        if (self.state != .active) {
            return errors.Error.TransactionInactive;
        }
        if (self.read_only) {
            return errors.Error.ReadOnlyTransaction;
        }

        if (self.manager.wal) |wal| {
            const delete = DeleteData{
                .page_id = page_id,
                .key = key,
            };

            const data = try self.manager.allocator.alloc(u8, delete.serializedSize());
            defer self.manager.allocator.free(data);
            delete.serialize(data);

            self.last_lsn = try wal.append(self.id, self.last_lsn, .delete, data);
            return self.last_lsn;
        }

        return INVALID_LSN;
    }

    /// Get transaction ID
    pub fn getId(self: *const Transaction) TransactionId {
        return self.id;
    }
};

/// Transaction manager
pub const TransactionManager = struct {
    /// Allocator
    allocator: std.mem.Allocator,
    /// WAL writer (optional)
    wal: ?*WALWriter,
    /// Next transaction ID
    next_txn_id: TransactionId,
    /// Active transactions
    active_txns: std.AutoHashMap(TransactionId, *Transaction),
    /// All transactions (for cleanup)
    all_txns: std.ArrayListUnmanaged(*Transaction),
    /// Current timestamp (for MVCC)
    current_ts: u64,
    /// Default isolation level
    default_isolation: IsolationLevel,
    /// Mutex for thread safety
    mutex: std.Thread.Mutex,

    /// Initialize transaction manager
    pub fn init(allocator: std.mem.Allocator, wal: ?*WALWriter, default_isolation: IsolationLevel) TransactionManager {
        return .{
            .allocator = allocator,
            .wal = wal,
            .next_txn_id = 1,
            .active_txns = std.AutoHashMap(TransactionId, *Transaction).init(allocator),
            .all_txns = .{},
            .current_ts = 1,
            .default_isolation = default_isolation,
            .mutex = .{},
        };
    }

    /// Free resources
    pub fn deinit(self: *TransactionManager) void {
        // Destroy all transactions
        for (self.all_txns.items) |txn| {
            self.allocator.destroy(txn);
        }
        self.all_txns.deinit(self.allocator);
        self.active_txns.deinit();
    }

    /// Begin a new transaction
    pub fn begin(self: *TransactionManager) !*Transaction {
        return self.beginWithOptions(self.default_isolation, false);
    }

    /// Begin a read-only transaction
    pub fn beginReadOnly(self: *TransactionManager) !*Transaction {
        return self.beginWithOptions(self.default_isolation, true);
    }

    /// Begin with specific options
    pub fn beginWithOptions(self: *TransactionManager, isolation: IsolationLevel, read_only: bool) !*Transaction {
        self.mutex.lock();
        defer self.mutex.unlock();

        const txn_id = self.next_txn_id;
        self.next_txn_id += 1;

        const ts = self.current_ts;
        self.current_ts += 1;

        const txn = try self.allocator.create(Transaction);
        txn.* = .{
            .id = txn_id,
            .state = .active,
            .isolation = isolation,
            .read_only = read_only,
            .start_ts = ts,
            .last_lsn = INVALID_LSN,
            .manager = self,
        };

        // Track for cleanup
        try self.all_txns.append(self.allocator, txn);

        // Log begin record
        if (self.wal) |wal| {
            txn.last_lsn = try wal.append(txn_id, INVALID_LSN, .begin, "");
        }

        try self.active_txns.put(txn_id, txn);

        return txn;
    }

    /// Get an active transaction by ID
    pub fn getTransaction(self: *TransactionManager, txn_id: TransactionId) ?*Transaction {
        return self.active_txns.get(txn_id);
    }

    /// Called when a transaction commits
    fn onCommit(self: *TransactionManager, txn_id: TransactionId) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Remove from active set, but don't destroy - transaction object may still be referenced
        _ = self.active_txns.remove(txn_id);
    }

    /// Called when a transaction aborts
    fn onAbort(self: *TransactionManager, txn_id: TransactionId) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Remove from active set, but don't destroy - transaction object may still be referenced
        _ = self.active_txns.remove(txn_id);
    }

    /// Get count of active transactions
    pub fn activeCount(self: *const TransactionManager) usize {
        return self.active_txns.count();
    }

    /// Get list of active transaction IDs
    pub fn getActiveTxnIds(self: *TransactionManager) ![]TransactionId {
        var ids = try self.allocator.alloc(TransactionId, self.active_txns.count());
        var i: usize = 0;
        var iter = self.active_txns.keyIterator();
        while (iter.next()) |key| {
            ids[i] = key.*;
            i += 1;
        }
        return ids;
    }

    /// Set next transaction ID (for recovery)
    pub fn setNextTxnId(self: *TransactionManager, txn_id: TransactionId) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (txn_id > self.next_txn_id) {
            self.next_txn_id = txn_id;
        }
    }
};

// Tests

test "TransactionManager begin and commit" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null, .read_committed);
    defer manager.deinit();

    const txn = try manager.begin();
    try std.testing.expectEqual(@as(TransactionId, 1), txn.getId());
    try std.testing.expect(txn.isActive());
    try std.testing.expectEqual(@as(usize, 1), manager.activeCount());

    try txn.commit();
    try std.testing.expectEqual(TxnState.committed, txn.state);
    try std.testing.expectEqual(@as(usize, 0), manager.activeCount());
}

test "TransactionManager begin and abort" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null, .read_committed);
    defer manager.deinit();

    const txn = try manager.begin();
    try std.testing.expectEqual(@as(usize, 1), manager.activeCount());

    try txn.abort();
    try std.testing.expectEqual(TxnState.aborted, txn.state);
    try std.testing.expectEqual(@as(usize, 0), manager.activeCount());
}

test "TransactionManager multiple transactions" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null, .read_committed);
    defer manager.deinit();

    const txn1 = try manager.begin();
    const txn2 = try manager.begin();
    const txn3 = try manager.begin();

    try std.testing.expectEqual(@as(TransactionId, 1), txn1.getId());
    try std.testing.expectEqual(@as(TransactionId, 2), txn2.getId());
    try std.testing.expectEqual(@as(TransactionId, 3), txn3.getId());
    try std.testing.expectEqual(@as(usize, 3), manager.activeCount());

    try txn2.commit();
    try std.testing.expectEqual(@as(usize, 2), manager.activeCount());

    try txn1.abort();
    try std.testing.expectEqual(@as(usize, 1), manager.activeCount());

    try txn3.commit();
    try std.testing.expectEqual(@as(usize, 0), manager.activeCount());
}

test "TransactionManager read-only transaction" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null, .read_committed);
    defer manager.deinit();

    const txn = try manager.beginReadOnly();
    try std.testing.expect(txn.read_only);

    // Read-only cannot log inserts
    const result = txn.logInsert(0, "key", "value");
    try std.testing.expectError(errors.Error.ReadOnlyTransaction, result);

    try txn.commit();
}

test "TransactionManager with WAL" {
    const allocator = std.testing.allocator;
    const path = "test_txn_wal.wal";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var wal = try WALWriter.open(allocator, path, .sync);
    defer wal.close();

    var manager = TransactionManager.init(allocator, &wal, .read_committed);
    defer manager.deinit();

    const txn = try manager.begin();
    _ = try txn.logInsert(42, "key", "value");
    try txn.commit();

    // Verify WAL has records
    try std.testing.expect(wal.getCurrentLSN() > 1);
}

test "TransactionManager double commit error" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null, .read_committed);
    defer manager.deinit();

    const txn = try manager.begin();
    try txn.commit();

    // Second commit should fail
    const result = txn.commit();
    try std.testing.expectError(errors.Error.TransactionInactive, result);
}

test "TransactionManager get active txn ids" {
    const allocator = std.testing.allocator;

    var manager = TransactionManager.init(allocator, null, .read_committed);
    defer manager.deinit();

    _ = try manager.begin();
    _ = try manager.begin();

    const ids = try manager.getActiveTxnIds();
    defer allocator.free(ids);

    try std.testing.expectEqual(@as(usize, 2), ids.len);
}
