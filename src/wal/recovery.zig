const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const record = @import("record.zig");
const writer = @import("writer.zig");

const LSN = types.LSN;
const TransactionId = types.TransactionId;
const TxnState = types.TxnState;
const INVALID_LSN = types.INVALID_LSN;
const INVALID_TXN_ID = types.INVALID_TXN_ID;
const Record = record.Record;
const RecordType = record.RecordType;
const RecordHeader = record.RecordHeader;
const RECORD_HEADER_SIZE = record.RECORD_HEADER_SIZE;
const WALHeader = writer.WALHeader;
const WAL_HEADER_SIZE = writer.WAL_HEADER_SIZE;

/// Transaction state during recovery
pub const TxnRecoveryState = struct {
    txn_id: TransactionId,
    state: TxnState,
    /// LSN of begin record
    begin_lsn: LSN,
    /// LSN of commit/abort record (or 0 if still active)
    end_lsn: LSN,
    /// Records belonging to this transaction (for undo)
    records: std.ArrayList(RecoveryRecord),

    pub fn init(txn_id: TransactionId, begin_lsn: LSN) TxnRecoveryState {
        return .{
            .txn_id = txn_id,
            .state = .active,
            .begin_lsn = begin_lsn,
            .end_lsn = 0,
            .records = .{},
        };
    }

    pub fn deinit(self: *TxnRecoveryState, allocator: std.mem.Allocator) void {
        for (self.records.items) |*rec| {
            rec.deinit(allocator);
        }
        self.records.deinit(allocator);
    }

    pub fn addRecord(self: *TxnRecoveryState, allocator: std.mem.Allocator, rec: RecoveryRecord) !void {
        try self.records.append(allocator, rec);
    }

    pub fn markCommitted(self: *TxnRecoveryState, commit_lsn: LSN) void {
        self.state = .committed;
        self.end_lsn = commit_lsn;
    }

    pub fn markAborted(self: *TxnRecoveryState, abort_lsn: LSN) void {
        self.state = .aborted;
        self.end_lsn = abort_lsn;
    }
};

/// Simplified record for recovery (just what we need for redo/undo)
pub const RecoveryRecord = struct {
    lsn: LSN,
    record_type: RecordType,
    key: []const u8,
    old_value: []const u8,
    new_value: []const u8,

    pub fn fromRecord(allocator: std.mem.Allocator, rec: *const Record) !RecoveryRecord {
        return .{
            .lsn = rec.lsn(),
            .record_type = rec.recordType(),
            .key = if (rec.key.len > 0) try allocator.dupe(u8, rec.key) else &[_]u8{},
            .old_value = if (rec.old_value.len > 0) try allocator.dupe(u8, rec.old_value) else &[_]u8{},
            .new_value = if (rec.new_value.len > 0) try allocator.dupe(u8, rec.new_value) else &[_]u8{},
        };
    }

    pub fn deinit(self: *RecoveryRecord, allocator: std.mem.Allocator) void {
        if (self.key.len > 0) allocator.free(self.key);
        if (self.old_value.len > 0) allocator.free(self.old_value);
        if (self.new_value.len > 0) allocator.free(self.new_value);
    }
};

/// Callback for applying redo/undo operations
pub const RecoveryCallback = struct {
    context: *anyopaque,
    redoFn: *const fn (ctx: *anyopaque, rec: *const RecoveryRecord) errors.MonolithError!void,
    undoFn: *const fn (ctx: *anyopaque, rec: *const RecoveryRecord) errors.MonolithError!void,

    pub fn redo(self: RecoveryCallback, rec: *const RecoveryRecord) errors.MonolithError!void {
        return self.redoFn(self.context, rec);
    }

    pub fn undo(self: RecoveryCallback, rec: *const RecoveryRecord) errors.MonolithError!void {
        return self.undoFn(self.context, rec);
    }
};

/// WAL Recovery manager
pub const WALRecovery = struct {
    allocator: std.mem.Allocator,
    /// Directory containing WAL files
    dir_path: []const u8,
    /// Transaction states
    transactions: std.AutoHashMap(TransactionId, TxnRecoveryState),
    /// Checkpoint LSN (start recovery from here)
    checkpoint_lsn: LSN,
    /// Last valid LSN found
    last_valid_lsn: LSN,
    /// Records to redo (in LSN order)
    redo_records: std.ArrayList(RecoveryRecord),
    /// Statistics
    records_read: u64,
    records_redone: u64,
    records_undone: u64,
    txns_committed: u64,
    txns_aborted: u64,

    pub fn init(allocator: std.mem.Allocator, dir_path: []const u8) !WALRecovery {
        return .{
            .allocator = allocator,
            .dir_path = try allocator.dupe(u8, dir_path),
            .transactions = std.AutoHashMap(TransactionId, TxnRecoveryState).init(allocator),
            .checkpoint_lsn = INVALID_LSN,
            .last_valid_lsn = INVALID_LSN,
            .redo_records = .{},
            .records_read = 0,
            .records_redone = 0,
            .records_undone = 0,
            .txns_committed = 0,
            .txns_aborted = 0,
        };
    }

    pub fn deinit(self: *WALRecovery) void {
        // Clean up transaction states
        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.transactions.deinit();

        // Clean up redo records
        for (self.redo_records.items) |*rec| {
            rec.deinit(self.allocator);
        }
        self.redo_records.deinit(self.allocator);

        self.allocator.free(self.dir_path);
    }

    /// Set checkpoint LSN to start recovery from
    pub fn setCheckpointLSN(self: *WALRecovery, lsn: LSN) void {
        self.checkpoint_lsn = lsn;
    }

    /// Scan WAL files and build recovery state
    pub fn analyze(self: *WALRecovery) errors.MonolithError!void {
        // Find all WAL segment files
        var segment: u64 = 0;
        while (true) : (segment += 1) {
            const filename = try self.getSegmentFilename(segment);
            defer self.allocator.free(filename);

            const file = std.fs.cwd().openFile(filename, .{ .mode = .read_only }) catch |err| switch (err) {
                error.FileNotFound => break, // No more segments
                else => return errors.Error.WALCorrupted,
            };
            defer file.close();

            try self.analyzeSegment(file);
        }
    }

    /// Analyze a single WAL segment
    fn analyzeSegment(self: *WALRecovery, file: std.fs.File) errors.MonolithError!void {
        // Read and validate header
        var header_buf: [WAL_HEADER_SIZE]u8 = undefined;
        const header_read = file.read(&header_buf) catch {
            return errors.Error.WALCorrupted;
        };
        if (header_read < WAL_HEADER_SIZE) {
            return; // Empty or truncated segment
        }

        _ = WALHeader.deserialize(&header_buf) catch {
            return errors.Error.WALCorrupted;
        };

        // Read records
        var position: u64 = WAL_HEADER_SIZE;
        const file_size = file.stat() catch {
            return errors.Error.WALCorrupted;
        };

        while (position < file_size.size) {
            // Read record header to get length
            file.seekTo(position) catch {
                return errors.Error.WALCorrupted;
            };

            var len_buf: [4]u8 = undefined;
            const len_read = file.read(&len_buf) catch break;
            if (len_read < 4) break;

            const total_length = std.mem.bytesToValue(u32, &len_buf);
            if (total_length < record.MIN_RECORD_SIZE or total_length > 16 * 1024 * 1024) {
                break; // Invalid record length, stop here
            }

            // Read full record
            file.seekTo(position) catch break;
            const record_buf = self.allocator.alloc(u8, total_length) catch {
                return errors.Error.OutOfSpace;
            };
            defer self.allocator.free(record_buf);

            const bytes_read = file.read(record_buf) catch break;
            if (bytes_read < total_length) break; // Partial record

            // Deserialize record
            var rec = Record.deserialize(self.allocator, record_buf) catch |err| switch (err) {
                errors.Error.WALCorrupted => break, // CRC mismatch, stop here
                else => return err,
            };
            defer rec.deinit();

            // Skip if before checkpoint
            if (self.checkpoint_lsn != INVALID_LSN and rec.lsn() < self.checkpoint_lsn) {
                position += total_length;
                continue;
            }

            // Process record
            try self.processRecord(&rec);
            self.records_read += 1;
            self.last_valid_lsn = rec.lsn();

            position += total_length;
        }
    }

    /// Process a single record during analysis
    fn processRecord(self: *WALRecovery, rec: *const Record) errors.MonolithError!void {
        const txn_id = rec.txnId();

        switch (rec.recordType()) {
            .begin => {
                // Start tracking new transaction
                const state = TxnRecoveryState.init(txn_id, rec.lsn());
                try self.transactions.put(txn_id, state);
            },
            .commit => {
                // Mark transaction as committed
                if (self.transactions.getPtr(txn_id)) |state| {
                    state.markCommitted(rec.lsn());
                    self.txns_committed += 1;
                }
            },
            .abort => {
                // Mark transaction as aborted
                if (self.transactions.getPtr(txn_id)) |state| {
                    state.markAborted(rec.lsn());
                    self.txns_aborted += 1;
                }
            },
            .insert, .update, .delete => {
                // Data modification record
                const recovery_rec = RecoveryRecord.fromRecord(self.allocator, rec) catch {
                    return errors.Error.OutOfSpace;
                };

                // Add to transaction's record list (for potential undo)
                if (self.transactions.getPtr(txn_id)) |state| {
                    const rec_copy = RecoveryRecord.fromRecord(self.allocator, rec) catch {
                        return errors.Error.OutOfSpace;
                    };
                    state.addRecord(self.allocator, rec_copy) catch {
                        return errors.Error.OutOfSpace;
                    };
                }

                // Add to redo list
                self.redo_records.append(self.allocator, recovery_rec) catch {
                    return errors.Error.OutOfSpace;
                };
            },
            .checkpoint => {
                // Checkpoint record - could update checkpoint_lsn
            },
        }
    }

    /// Perform redo phase - replay committed transactions
    pub fn redo(self: *WALRecovery, callback: RecoveryCallback) errors.MonolithError!void {
        // Sort redo records by LSN
        std.mem.sort(RecoveryRecord, self.redo_records.items, {}, struct {
            fn lessThan(_: void, a: RecoveryRecord, b: RecoveryRecord) bool {
                return a.lsn < b.lsn;
            }
        }.lessThan);

        // Redo records from committed transactions only
        for (self.redo_records.items) |*rec| {
            // Find the transaction this record belongs to
            var txn_committed = false;
            var it = self.transactions.iterator();
            while (it.next()) |entry| {
                const state = entry.value_ptr;
                for (state.records.items) |txn_rec| {
                    if (txn_rec.lsn == rec.lsn) {
                        txn_committed = (state.state == .committed);
                        break;
                    }
                }
            }

            if (txn_committed) {
                try callback.redo(rec);
                self.records_redone += 1;
            }
        }
    }

    /// Perform undo phase - rollback uncommitted transactions
    pub fn undo(self: *WALRecovery, callback: RecoveryCallback) errors.MonolithError!void {
        // Find uncommitted (active) transactions
        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            const state = entry.value_ptr;
            if (state.state == .active) {
                // Undo in reverse order
                var i = state.records.items.len;
                while (i > 0) {
                    i -= 1;
                    try callback.undo(&state.records.items[i]);
                    self.records_undone += 1;
                }
            }
        }
    }

    /// Run full recovery (analyze + redo + undo)
    pub fn recover(self: *WALRecovery, callback: RecoveryCallback) errors.MonolithError!void {
        try self.analyze();
        try self.redo(callback);
        try self.undo(callback);
    }

    /// Get the last valid LSN found during recovery
    pub fn getLastValidLSN(self: *const WALRecovery) LSN {
        return self.last_valid_lsn;
    }

    /// Get recovery statistics
    pub fn getStats(self: *const WALRecovery) RecoveryStats {
        return .{
            .records_read = self.records_read,
            .records_redone = self.records_redone,
            .records_undone = self.records_undone,
            .txns_committed = self.txns_committed,
            .txns_aborted = self.txns_aborted,
            .last_valid_lsn = self.last_valid_lsn,
        };
    }

    /// Get number of uncommitted transactions
    pub fn getUncommittedCount(self: *const WALRecovery) u64 {
        var count: u64 = 0;
        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.state == .active) {
                count += 1;
            }
        }
        return count;
    }

    fn getSegmentFilename(self: *WALRecovery, segment: u64) errors.MonolithError![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/wal_{d:0>8}.log", .{ self.dir_path, segment }) catch {
            return errors.Error.OutOfSpace;
        };
    }
};

/// Recovery statistics
pub const RecoveryStats = struct {
    records_read: u64,
    records_redone: u64,
    records_undone: u64,
    txns_committed: u64,
    txns_aborted: u64,
    last_valid_lsn: LSN,
};

/// WAL reader for iterating through records
pub const WALReader = struct {
    allocator: std.mem.Allocator,
    dir_path: []const u8,
    current_segment: u64,
    current_file: ?std.fs.File,
    position: u64,
    start_lsn: LSN,

    pub fn init(allocator: std.mem.Allocator, dir_path: []const u8, start_lsn: LSN) !WALReader {
        return .{
            .allocator = allocator,
            .dir_path = try allocator.dupe(u8, dir_path),
            .current_segment = 0,
            .current_file = null,
            .position = WAL_HEADER_SIZE,
            .start_lsn = start_lsn,
        };
    }

    pub fn deinit(self: *WALReader) void {
        if (self.current_file) |file| {
            file.close();
        }
        self.allocator.free(self.dir_path);
    }

    /// Read next record (returns null at end)
    pub fn next(self: *WALReader) errors.MonolithError!?Record {
        while (true) {
            // Open current segment if needed
            if (self.current_file == null) {
                const filename = try self.getSegmentFilename(self.current_segment);
                defer self.allocator.free(filename);

                self.current_file = std.fs.cwd().openFile(filename, .{ .mode = .read_only }) catch |err| switch (err) {
                    error.FileNotFound => return null, // No more segments
                    else => return errors.Error.WALCorrupted,
                };
                self.position = WAL_HEADER_SIZE;
            }

            const file = self.current_file.?;

            // Get file size
            const stat = file.stat() catch {
                return errors.Error.WALCorrupted;
            };

            // Check if at end of segment
            if (self.position >= stat.size) {
                file.close();
                self.current_file = null;
                self.current_segment += 1;
                continue;
            }

            // Read record length
            file.seekTo(self.position) catch {
                return errors.Error.WALCorrupted;
            };

            var len_buf: [4]u8 = undefined;
            const len_read = file.read(&len_buf) catch {
                return errors.Error.WALCorrupted;
            };
            if (len_read < 4) {
                // Move to next segment
                file.close();
                self.current_file = null;
                self.current_segment += 1;
                continue;
            }

            const total_length = std.mem.bytesToValue(u32, &len_buf);
            if (total_length < record.MIN_RECORD_SIZE) {
                return errors.Error.WALCorrupted;
            }

            // Read full record
            file.seekTo(self.position) catch {
                return errors.Error.WALCorrupted;
            };

            const record_buf = self.allocator.alloc(u8, total_length) catch {
                return errors.Error.OutOfSpace;
            };
            defer self.allocator.free(record_buf);

            const bytes_read = file.read(record_buf) catch {
                return errors.Error.WALCorrupted;
            };
            if (bytes_read < total_length) {
                // Partial record at end
                file.close();
                self.current_file = null;
                self.current_segment += 1;
                continue;
            }

            // Deserialize
            const rec = Record.deserialize(self.allocator, record_buf) catch |err| switch (err) {
                errors.Error.WALCorrupted => {
                    // CRC failure, stop reading
                    return null;
                },
                else => return err,
            };

            self.position += total_length;

            // Skip if before start LSN
            if (rec.lsn() < self.start_lsn) {
                var mutable_rec = rec;
                mutable_rec.deinit();
                continue;
            }

            return rec;
        }
    }

    fn getSegmentFilename(self: *WALReader, segment: u64) errors.MonolithError![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/wal_{d:0>8}.log", .{ self.dir_path, segment }) catch {
            return errors.Error.OutOfSpace;
        };
    }
};

// Tests

test "TxnRecoveryState lifecycle" {
    const allocator = std.testing.allocator;

    var state = TxnRecoveryState.init(100, 1);
    defer state.deinit(allocator);

    try std.testing.expectEqual(@as(TransactionId, 100), state.txn_id);
    try std.testing.expectEqual(TxnState.active, state.state);
    try std.testing.expectEqual(@as(LSN, 1), state.begin_lsn);

    state.markCommitted(10);
    try std.testing.expectEqual(TxnState.committed, state.state);
    try std.testing.expectEqual(@as(LSN, 10), state.end_lsn);
}

test "RecoveryRecord from Record" {
    const allocator = std.testing.allocator;

    var rec = try Record.createInsert(allocator, 5, 1, "testkey", "testval");
    defer rec.deinit();

    var recovery_rec = try RecoveryRecord.fromRecord(allocator, &rec);
    defer recovery_rec.deinit(allocator);

    try std.testing.expectEqual(@as(LSN, 5), recovery_rec.lsn);
    try std.testing.expectEqual(RecordType.insert, recovery_rec.record_type);
    try std.testing.expectEqualStrings("testkey", recovery_rec.key);
    try std.testing.expectEqualStrings("testval", recovery_rec.new_value);
}

test "WALRecovery init and deinit" {
    const allocator = std.testing.allocator;

    var recovery = try WALRecovery.init(allocator, "/tmp/test_recovery");
    defer recovery.deinit();

    try std.testing.expectEqual(INVALID_LSN, recovery.checkpoint_lsn);
    try std.testing.expectEqual(INVALID_LSN, recovery.last_valid_lsn);
}

test "WALRecovery with no files" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_recovery_empty") catch {};
    std.fs.cwd().makePath("/tmp/test_recovery_empty") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_recovery_empty") catch {};

    var recovery = try WALRecovery.init(allocator, "/tmp/test_recovery_empty");
    defer recovery.deinit();

    // Should not error with no files
    try recovery.analyze();

    const stats = recovery.getStats();
    try std.testing.expectEqual(@as(u64, 0), stats.records_read);
}

test "WALRecovery full cycle" {
    const allocator = std.testing.allocator;

    // Set up test directory
    std.fs.cwd().deleteTree("/tmp/test_recovery_full") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_recovery_full") catch {};

    // Write some WAL records
    var wal_writer = try writer.WALWriter.init(allocator, "/tmp/test_recovery_full", .none, writer.DEFAULT_SEGMENT_SIZE);

    // Transaction 1: begin, insert, commit
    _ = try wal_writer.writeBegin(1);
    _ = try wal_writer.writeInsert(1, "key1", "value1");
    _ = try wal_writer.writeCommit(1);

    // Transaction 2: begin, insert (no commit - will be undone)
    _ = try wal_writer.writeBegin(2);
    _ = try wal_writer.writeInsert(2, "key2", "value2");

    // Transaction 3: begin, insert, abort
    _ = try wal_writer.writeBegin(3);
    _ = try wal_writer.writeInsert(3, "key3", "value3");
    _ = try wal_writer.writeAbort(3);

    try wal_writer.flush();
    wal_writer.deinit();

    // Now recover
    var recovery = try WALRecovery.init(allocator, "/tmp/test_recovery_full");
    defer recovery.deinit();

    try recovery.analyze();

    const stats = recovery.getStats();
    try std.testing.expect(stats.records_read > 0);
    try std.testing.expectEqual(@as(u64, 1), stats.txns_committed);
    try std.testing.expectEqual(@as(u64, 1), stats.txns_aborted);
    try std.testing.expectEqual(@as(u64, 1), recovery.getUncommittedCount());
}

test "WALReader iterate records" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_reader") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_reader") catch {};

    // Write some records
    var wal_writer = try writer.WALWriter.init(allocator, "/tmp/test_reader", .none, writer.DEFAULT_SEGMENT_SIZE);
    _ = try wal_writer.writeBegin(1);
    _ = try wal_writer.writeInsert(1, "k1", "v1");
    _ = try wal_writer.writeInsert(1, "k2", "v2");
    _ = try wal_writer.writeCommit(1);
    try wal_writer.flush();
    wal_writer.deinit();

    // Read them back
    var reader = try WALReader.init(allocator, "/tmp/test_reader", 1);
    defer reader.deinit();

    var count: u32 = 0;
    while (try reader.next()) |rec| {
        var mutable_rec = rec;
        defer mutable_rec.deinit();
        count += 1;
    }

    try std.testing.expectEqual(@as(u32, 4), count); // begin + 2 inserts + commit
}

test "WALReader with start LSN" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_reader_lsn") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_reader_lsn") catch {};

    // Write records with LSNs 1, 2, 3, 4
    var wal_writer = try writer.WALWriter.init(allocator, "/tmp/test_reader_lsn", .none, writer.DEFAULT_SEGMENT_SIZE);
    _ = try wal_writer.writeBegin(1);
    _ = try wal_writer.writeInsert(1, "k1", "v1");
    _ = try wal_writer.writeInsert(1, "k2", "v2");
    _ = try wal_writer.writeCommit(1);
    try wal_writer.flush();
    wal_writer.deinit();

    // Read starting from LSN 3
    var reader = try WALReader.init(allocator, "/tmp/test_reader_lsn", 3);
    defer reader.deinit();

    var count: u32 = 0;
    while (try reader.next()) |rec| {
        var mutable_rec = rec;
        defer mutable_rec.deinit();
        try std.testing.expect(rec.lsn() >= 3);
        count += 1;
    }

    try std.testing.expectEqual(@as(u32, 2), count); // Only LSN 3 and 4
}

test "RecoveryStats" {
    const stats = RecoveryStats{
        .records_read = 100,
        .records_redone = 50,
        .records_undone = 10,
        .txns_committed = 5,
        .txns_aborted = 2,
        .last_valid_lsn = 999,
    };

    try std.testing.expectEqual(@as(u64, 100), stats.records_read);
    try std.testing.expectEqual(@as(LSN, 999), stats.last_valid_lsn);
}
