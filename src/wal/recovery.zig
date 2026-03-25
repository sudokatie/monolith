const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const record = @import("record.zig");
const writer = @import("writer.zig");

const LSN = types.LSN;
const TransactionId = types.TransactionId;
const PageId = types.PageId;
const INVALID_LSN = types.INVALID_LSN;
const INVALID_TXN_ID = types.INVALID_TXN_ID;

const Record = record.Record;
const RecordType = record.RecordType;
const InsertData = record.InsertData;
const DeleteData = record.DeleteData;
const CheckpointData = record.CheckpointData;
const WALReader = writer.WALReader;

/// Transaction status during recovery
pub const TxnStatus = enum {
    active,
    committed,
    aborted,
};

/// Transaction info during recovery
pub const TxnInfo = struct {
    /// Transaction ID
    txn_id: TransactionId,
    /// Current status
    status: TxnStatus,
    /// Last LSN for this transaction
    last_lsn: LSN,
    /// First LSN for this transaction
    first_lsn: LSN,
};

/// Recovery result
pub const RecoveryResult = struct {
    /// Transactions that need to be redone
    redo_txns: std.ArrayListUnmanaged(TransactionId),
    /// Transactions that need to be undone
    undo_txns: std.ArrayListUnmanaged(TransactionId),
    /// Last checkpoint LSN
    checkpoint_lsn: LSN,
    /// Next LSN to use
    next_lsn: LSN,
    /// Allocator
    allocator: std.mem.Allocator,

    pub fn deinit(self: *RecoveryResult) void {
        self.redo_txns.deinit(self.allocator);
        self.undo_txns.deinit(self.allocator);
    }
};

/// Recovery manager for crash recovery
pub const Recovery = struct {
    /// Allocator
    allocator: std.mem.Allocator,
    /// WAL path
    wal_path: []const u8,
    /// Transaction table
    txn_table: std.AutoHashMap(TransactionId, TxnInfo),
    /// Dirty page table (page_id -> recovery LSN)
    dirty_pages: std.AutoHashMap(PageId, LSN),
    /// Last checkpoint LSN
    checkpoint_lsn: LSN,

    /// Initialize recovery manager
    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8) Recovery {
        return .{
            .allocator = allocator,
            .wal_path = wal_path,
            .txn_table = std.AutoHashMap(TransactionId, TxnInfo).init(allocator),
            .dirty_pages = std.AutoHashMap(PageId, LSN).init(allocator),
            .checkpoint_lsn = INVALID_LSN,
        };
    }

    /// Free resources
    pub fn deinit(self: *Recovery) void {
        self.txn_table.deinit();
        self.dirty_pages.deinit();
    }

    /// Perform ARIES-style recovery
    pub fn recover(self: *Recovery) !RecoveryResult {
        var reader = WALReader.open(self.allocator, self.wal_path) catch |err| {
            if (err == error.FileNotFound) {
                // No WAL - nothing to recover
                return RecoveryResult{
                    .redo_txns = .{},
                    .undo_txns = .{},
                    .checkpoint_lsn = INVALID_LSN,
                    .next_lsn = 1,
                    .allocator = self.allocator,
                };
            }
            return err;
        };
        defer reader.close();

        // Phase 1: Analysis - scan from checkpoint (or start)
        try self.analysisPhase(&reader);

        // Phase 2: Redo - replay committed transactions
        reader.seekTo(self.getRedoStart()) catch {};
        try self.redoPhase(&reader);

        // Phase 3: Undo - rollback active transactions
        try self.undoPhase();

        // Build result
        var result = RecoveryResult{
            .redo_txns = .{},
            .undo_txns = .{},
            .checkpoint_lsn = self.checkpoint_lsn,
            .next_lsn = self.getNextLSN(),
            .allocator = self.allocator,
        };

        var iter = self.txn_table.iterator();
        while (iter.next()) |entry| {
            const info = entry.value_ptr.*;
            switch (info.status) {
                .committed => try result.redo_txns.append(self.allocator, info.txn_id),
                .active, .aborted => try result.undo_txns.append(self.allocator, info.txn_id),
            }
        }

        return result;
    }

    /// Analysis phase: build transaction and dirty page tables
    fn analysisPhase(self: *Recovery, reader: *WALReader) !void {
        while (try reader.readNext()) |rec| {
            defer {
                var r = rec;
                r.deinit(self.allocator);
            }

            switch (rec.record_type) {
                .begin => {
                    try self.txn_table.put(rec.txn_id, .{
                        .txn_id = rec.txn_id,
                        .status = .active,
                        .last_lsn = rec.lsn,
                        .first_lsn = rec.lsn,
                    });
                },
                .commit => {
                    if (self.txn_table.getPtr(rec.txn_id)) |info| {
                        info.status = .committed;
                        info.last_lsn = rec.lsn;
                    }
                },
                .abort => {
                    if (self.txn_table.getPtr(rec.txn_id)) |info| {
                        info.status = .aborted;
                        info.last_lsn = rec.lsn;
                    }
                },
                .insert, .delete, .update, .page_write => {
                    if (self.txn_table.getPtr(rec.txn_id)) |info| {
                        info.last_lsn = rec.lsn;
                    }

                    // Track dirty pages
                    if (rec.data.len >= 8) {
                        const page_id = std.mem.bytesToValue(PageId, rec.data[0..8]);
                        if (!self.dirty_pages.contains(page_id)) {
                            try self.dirty_pages.put(page_id, rec.lsn);
                        }
                    }
                },
                .checkpoint_begin => {
                    self.checkpoint_lsn = rec.lsn;
                },
                .checkpoint_end => {
                    // Process checkpoint data
                    if (rec.data.len > 0) {
                        var cp_data = CheckpointData.deserialize(rec.data, self.allocator) catch continue;
                        defer cp_data.deinit(self.allocator);

                        // Mark active transactions from checkpoint
                        for (cp_data.active_txns) |txn_id| {
                            if (!self.txn_table.contains(txn_id)) {
                                try self.txn_table.put(txn_id, .{
                                    .txn_id = txn_id,
                                    .status = .active,
                                    .last_lsn = INVALID_LSN,
                                    .first_lsn = INVALID_LSN,
                                });
                            }
                        }

                        // Add dirty pages from checkpoint
                        for (cp_data.dirty_pages) |page_id| {
                            if (!self.dirty_pages.contains(page_id)) {
                                try self.dirty_pages.put(page_id, self.checkpoint_lsn);
                            }
                        }
                    }
                },
                .clr => {},
            }
        }
    }

    /// Get starting point for redo
    fn getRedoStart(self: *const Recovery) u64 {
        if (self.checkpoint_lsn != INVALID_LSN) {
            // Start from checkpoint
            return @intCast(self.checkpoint_lsn);
        }
        return writer.WAL_HEADER_SIZE;
    }

    /// Get next LSN
    fn getNextLSN(self: *const Recovery) LSN {
        var max_lsn: LSN = 0;
        var iter = self.txn_table.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.last_lsn > max_lsn) {
                max_lsn = entry.value_ptr.last_lsn;
            }
        }
        return max_lsn + 1;
    }

    /// Redo phase: replay operations from committed transactions
    fn redoPhase(self: *Recovery, reader: *WALReader) !void {
        // In a full implementation, this would replay all logged operations
        // that need to be redone (based on page LSNs)
        _ = self;
        _ = reader;
        // For now, we just scan through without actually redoing
        // The actual redo would be done by the database layer
    }

    /// Undo phase: rollback active transactions
    fn undoPhase(self: *Recovery) !void {
        // In a full implementation, this would follow the undo chain
        // for each active transaction and undo their operations
        // For now, we just mark them as needing undo
        var iter = self.txn_table.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.status == .active) {
                entry.value_ptr.status = .aborted;
            }
        }
    }

    /// Get transaction info
    pub fn getTxnInfo(self: *const Recovery, txn_id: TransactionId) ?TxnInfo {
        return self.txn_table.get(txn_id);
    }

    /// Get dirty pages
    pub fn getDirtyPages(self: *const Recovery) []PageId {
        var pages: std.ArrayListUnmanaged(PageId) = .{};
        var iter = self.dirty_pages.keyIterator();
        while (iter.next()) |key| {
            pages.append(self.allocator, key.*) catch {};
        }
        return pages.toOwnedSlice(self.allocator) catch &[_]PageId{};
    }
};

// Tests

test "Recovery empty WAL" {
    const allocator = std.testing.allocator;
    const path = "test_recovery_empty.wal";

    std.fs.cwd().deleteFile(path) catch {};

    var recovery = Recovery.init(allocator, path);
    defer recovery.deinit();

    var result = try recovery.recover();
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.redo_txns.items.len);
    try std.testing.expectEqual(@as(usize, 0), result.undo_txns.items.len);
    try std.testing.expectEqual(@as(LSN, 1), result.next_lsn);
}

test "Recovery with committed transaction" {
    const allocator = std.testing.allocator;
    const path = "test_recovery_commit.wal";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    // Write WAL
    {
        var wal = try writer.WALWriter.open(allocator, path, .sync);
        defer wal.close();

        _ = try wal.append(1, INVALID_LSN, .begin, "");
        _ = try wal.append(1, 1, .insert, &[_]u8{ 1, 0, 0, 0, 0, 0, 0, 0 });
        _ = try wal.append(1, 2, .commit, "");
    }

    // Recover
    var recovery = Recovery.init(allocator, path);
    defer recovery.deinit();

    var result = try recovery.recover();
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.redo_txns.items.len);
    try std.testing.expectEqual(@as(usize, 0), result.undo_txns.items.len);
}

test "Recovery with active transaction" {
    const allocator = std.testing.allocator;
    const path = "test_recovery_active.wal";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    // Write WAL without commit
    {
        var wal = try writer.WALWriter.open(allocator, path, .sync);
        defer wal.close();

        _ = try wal.append(1, INVALID_LSN, .begin, "");
        _ = try wal.append(1, 1, .insert, &[_]u8{ 1, 0, 0, 0, 0, 0, 0, 0 });
        // No commit!
    }

    // Recover
    var recovery = Recovery.init(allocator, path);
    defer recovery.deinit();

    var result = try recovery.recover();
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.redo_txns.items.len);
    try std.testing.expectEqual(@as(usize, 1), result.undo_txns.items.len);
}

test "Recovery transaction info" {
    const allocator = std.testing.allocator;
    const path = "test_recovery_info.wal";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    {
        var wal = try writer.WALWriter.open(allocator, path, .sync);
        defer wal.close();

        _ = try wal.append(1, INVALID_LSN, .begin, "");
        _ = try wal.append(1, 1, .commit, "");
        _ = try wal.append(2, INVALID_LSN, .begin, "");
        // Txn 2 not committed
    }

    var recovery = Recovery.init(allocator, path);
    defer recovery.deinit();

    var result = try recovery.recover();
    defer result.deinit();

    const txn1 = recovery.getTxnInfo(1);
    try std.testing.expect(txn1 != null);
    try std.testing.expectEqual(TxnStatus.committed, txn1.?.status);

    const txn2 = recovery.getTxnInfo(2);
    try std.testing.expect(txn2 != null);
    try std.testing.expectEqual(TxnStatus.aborted, txn2.?.status);
}
