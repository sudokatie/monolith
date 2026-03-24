const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const record = @import("record.zig");

const LSN = types.LSN;
const TransactionId = types.TransactionId;
const SyncMode = types.SyncMode;
const INVALID_LSN = types.INVALID_LSN;
const Record = record.Record;
const RecordHeader = record.RecordHeader;
const RECORD_HEADER_SIZE = record.RECORD_HEADER_SIZE;

/// WAL file magic number
pub const WAL_MAGIC: u32 = 0x57414C30; // "WAL0"

/// WAL file header size
pub const WAL_HEADER_SIZE: usize = 32;

/// Default segment size (16MB)
pub const DEFAULT_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

/// WAL file header structure
/// Layout:
///   0-3:   magic (u32)
///   4-7:   version (u32)
///   8-15:  start_lsn (u64) - first LSN in this segment
///   16-23: end_lsn (u64) - last LSN written (updated on close)
///   24-31: reserved
pub const WALHeader = struct {
    magic: u32,
    version: u32,
    start_lsn: LSN,
    end_lsn: LSN,

    pub fn init(start_lsn: LSN) WALHeader {
        return .{
            .magic = WAL_MAGIC,
            .version = 1,
            .start_lsn = start_lsn,
            .end_lsn = start_lsn,
        };
    }

    pub fn serialize(self: WALHeader, buffer: []u8) void {
        std.debug.assert(buffer.len >= WAL_HEADER_SIZE);

        @memcpy(buffer[0..4], std.mem.asBytes(&self.magic));
        @memcpy(buffer[4..8], std.mem.asBytes(&self.version));
        @memcpy(buffer[8..16], std.mem.asBytes(&self.start_lsn));
        @memcpy(buffer[16..24], std.mem.asBytes(&self.end_lsn));
        @memset(buffer[24..32], 0); // reserved
    }

    pub fn deserialize(buffer: []const u8) errors.Error!WALHeader {
        if (buffer.len < WAL_HEADER_SIZE) {
            return errors.Error.WALCorrupted;
        }

        const magic = std.mem.bytesToValue(u32, buffer[0..4]);
        if (magic != WAL_MAGIC) {
            return errors.Error.InvalidMagic;
        }

        const version = std.mem.bytesToValue(u32, buffer[4..8]);
        if (version != 1) {
            return errors.Error.VersionMismatch;
        }

        return .{
            .magic = magic,
            .version = version,
            .start_lsn = std.mem.bytesToValue(LSN, buffer[8..16]),
            .end_lsn = std.mem.bytesToValue(LSN, buffer[16..24]),
        };
    }
};

/// WAL Writer for appending log records
pub const WALWriter = struct {
    /// Allocator for buffers
    allocator: std.mem.Allocator,
    /// Base directory for WAL files
    dir_path: []const u8,
    /// Current WAL file handle
    file: ?std.fs.File,
    /// Current segment number
    segment_num: u64,
    /// Current position in file
    position: u64,
    /// Maximum segment size before rotation
    segment_size: usize,
    /// Sync mode
    sync_mode: SyncMode,
    /// Next LSN to assign
    next_lsn: LSN,
    /// Write buffer for batching
    write_buffer: std.ArrayList(u8),
    /// Number of records in buffer (for batch sync)
    buffered_records: u32,
    /// Batch size before auto-sync
    batch_size: u32,

    /// Initialize a new WAL writer
    pub fn init(
        allocator: std.mem.Allocator,
        dir_path: []const u8,
        sync_mode: SyncMode,
        segment_size: usize,
    ) errors.MonolithError!WALWriter {
        // Ensure directory exists
        std.fs.cwd().makePath(dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return errors.Error.OutOfSpace,
        };

        return .{
            .allocator = allocator,
            .dir_path = try allocator.dupe(u8, dir_path),
            .file = null,
            .segment_num = 0,
            .position = 0,
            .segment_size = segment_size,
            .sync_mode = sync_mode,
            .next_lsn = 1, // LSN starts at 1
            .write_buffer = .{},
            .buffered_records = 0,
            .batch_size = 100, // Sync every 100 records in batch mode
        };
    }

    /// Clean up resources
    pub fn deinit(self: *WALWriter) void {
        self.close() catch {};
        self.write_buffer.deinit(self.allocator);
        self.allocator.free(self.dir_path);
    }

    /// Open or create the current segment file
    pub fn open(self: *WALWriter) errors.MonolithError!void {
        if (self.file != null) return;

        const filename = try self.getSegmentFilename(self.segment_num);
        defer self.allocator.free(filename);

        // Try to open existing or create new
        const file = std.fs.cwd().openFile(filename, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => blk: {
                // Create new segment
                const new_file = std.fs.cwd().createFile(filename, .{ .read = true }) catch {
                    return errors.Error.OutOfSpace;
                };
                // Write header
                var header_buf: [WAL_HEADER_SIZE]u8 = undefined;
                const header = WALHeader.init(self.next_lsn);
                header.serialize(&header_buf);
                new_file.writeAll(&header_buf) catch {
                    new_file.close();
                    return errors.Error.OutOfSpace;
                };
                self.position = WAL_HEADER_SIZE;
                break :blk new_file;
            },
            else => return errors.Error.OutOfSpace,
        };

        self.file = file;

        // If existing file, seek to end
        if (self.position == 0) {
            const stat = file.stat() catch {
                return errors.Error.WALCorrupted;
            };
            self.position = stat.size;
            file.seekTo(self.position) catch {
                return errors.Error.WALCorrupted;
            };
        }
    }

    /// Close the current segment
    pub fn close(self: *WALWriter) errors.MonolithError!void {
        // Flush any buffered data
        try self.flush();

        if (self.file) |file| {
            // Update header with final LSN
            var header_buf: [WAL_HEADER_SIZE]u8 = undefined;
            file.seekTo(0) catch {};
            _ = file.read(&header_buf) catch {};

            var header = WALHeader.deserialize(&header_buf) catch WALHeader.init(1);
            header.end_lsn = self.next_lsn - 1;
            header.serialize(&header_buf);

            file.seekTo(0) catch {};
            file.writeAll(&header_buf) catch {};

            file.sync() catch {};
            file.close();
            self.file = null;
        }
    }

    /// Generate segment filename
    fn getSegmentFilename(self: *WALWriter, segment: u64) errors.MonolithError![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/wal_{d:0>8}.log", .{ self.dir_path, segment }) catch {
            return errors.Error.OutOfSpace;
        };
    }

    /// Get next LSN and increment counter
    pub fn getNextLSN(self: *WALWriter) LSN {
        const lsn = self.next_lsn;
        self.next_lsn += 1;
        return lsn;
    }

    /// Get current LSN (next to be assigned)
    pub fn currentLSN(self: *const WALWriter) LSN {
        return self.next_lsn;
    }

    /// Append a record to the WAL
    pub fn append(self: *WALWriter, rec: *const Record) errors.MonolithError!LSN {
        try self.open();

        const total_size = rec.totalSize();

        // Check if we need to rotate (use getPosition to include buffered data)
        if (self.getPosition() + total_size > self.segment_size) {
            try self.rotate();
        }

        // Serialize record
        const old_len = self.write_buffer.items.len;
        try self.write_buffer.resize(self.allocator, old_len + total_size);
        try rec.serialize(self.write_buffer.items[old_len..][0..total_size]);

        self.buffered_records += 1;

        // Handle sync mode
        switch (self.sync_mode) {
            .sync => {
                try self.flush();
                try self.sync();
            },
            .batch => {
                if (self.buffered_records >= self.batch_size) {
                    try self.flush();
                    try self.sync();
                }
            },
            .none => {
                // Flush to OS but don't sync
                if (self.write_buffer.items.len > 64 * 1024) {
                    try self.flush();
                }
            },
        }

        return rec.lsn();
    }

    /// Write an insert record
    pub fn writeInsert(
        self: *WALWriter,
        txn_id: TransactionId,
        key: []const u8,
        value: []const u8,
    ) errors.MonolithError!LSN {
        const lsn = self.getNextLSN();
        var rec = try Record.createInsert(self.allocator, lsn, txn_id, key, value);
        defer rec.deinit();
        return self.append(&rec);
    }

    /// Write an update record
    pub fn writeUpdate(
        self: *WALWriter,
        txn_id: TransactionId,
        key: []const u8,
        old_value: []const u8,
        new_value: []const u8,
    ) errors.MonolithError!LSN {
        const lsn = self.getNextLSN();
        var rec = try Record.createUpdate(self.allocator, lsn, txn_id, key, old_value, new_value);
        defer rec.deinit();
        return self.append(&rec);
    }

    /// Write a delete record
    pub fn writeDelete(
        self: *WALWriter,
        txn_id: TransactionId,
        key: []const u8,
        old_value: []const u8,
    ) errors.MonolithError!LSN {
        const lsn = self.getNextLSN();
        var rec = try Record.createDelete(self.allocator, lsn, txn_id, key, old_value);
        defer rec.deinit();
        return self.append(&rec);
    }

    /// Write a begin transaction record
    pub fn writeBegin(self: *WALWriter, txn_id: TransactionId) errors.MonolithError!LSN {
        const lsn = self.getNextLSN();
        var rec = Record.createBegin(self.allocator, lsn, txn_id);
        defer rec.deinit();
        return self.append(&rec);
    }

    /// Write a commit record
    pub fn writeCommit(self: *WALWriter, txn_id: TransactionId) errors.MonolithError!LSN {
        const lsn = self.getNextLSN();
        var rec = Record.createCommit(self.allocator, lsn, txn_id);
        defer rec.deinit();
        // Always sync on commit for durability
        const result = try self.append(&rec);
        try self.flush();
        try self.sync();
        return result;
    }

    /// Write an abort record
    pub fn writeAbort(self: *WALWriter, txn_id: TransactionId) errors.MonolithError!LSN {
        const lsn = self.getNextLSN();
        var rec = Record.createAbort(self.allocator, lsn, txn_id);
        defer rec.deinit();
        return self.append(&rec);
    }

    /// Write a checkpoint record
    pub fn writeCheckpoint(self: *WALWriter) errors.MonolithError!LSN {
        const lsn = self.getNextLSN();
        var rec = Record.createCheckpoint(self.allocator, lsn);
        defer rec.deinit();
        // Always sync checkpoint
        const result = try self.append(&rec);
        try self.flush();
        try self.sync();
        return result;
    }

    /// Flush buffered data to file
    pub fn flush(self: *WALWriter) errors.MonolithError!void {
        if (self.write_buffer.items.len == 0) return;

        if (self.file) |file| {
            file.writeAll(self.write_buffer.items) catch {
                return errors.Error.OutOfSpace;
            };
            self.position += self.write_buffer.items.len;
        }

        self.write_buffer.clearRetainingCapacity();
        self.buffered_records = 0;
    }

    /// Sync file to disk
    pub fn sync(self: *WALWriter) errors.MonolithError!void {
        if (self.file) |file| {
            file.sync() catch {
                return errors.Error.OutOfSpace;
            };
        }
    }

    /// Rotate to a new segment
    pub fn rotate(self: *WALWriter) errors.MonolithError!void {
        try self.close();
        self.segment_num += 1;
        self.position = 0;
        try self.open();
    }

    /// Get the current segment number
    pub fn getSegmentNum(self: *const WALWriter) u64 {
        return self.segment_num;
    }

    /// Get current file position
    pub fn getPosition(self: *const WALWriter) u64 {
        return self.position + self.write_buffer.items.len;
    }

    /// Check if rotation is needed
    pub fn needsRotation(self: *const WALWriter, record_size: usize) bool {
        return self.getPosition() + record_size > self.segment_size;
    }

    /// Set the starting LSN (for recovery)
    pub fn setNextLSN(self: *WALWriter, lsn: LSN) void {
        self.next_lsn = lsn;
    }

    /// Delete old segments up to (but not including) the given segment number
    pub fn deleteOldSegments(self: *WALWriter, keep_from_segment: u64) errors.MonolithError!u64 {
        var deleted: u64 = 0;
        var seg: u64 = 0;
        while (seg < keep_from_segment) : (seg += 1) {
            const filename = try self.getSegmentFilename(seg);
            defer self.allocator.free(filename);
            std.fs.cwd().deleteFile(filename) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => continue,
            };
            deleted += 1;
        }
        return deleted;
    }
};

// Tests

test "WALHeader serialize and deserialize" {
    var buffer: [WAL_HEADER_SIZE]u8 = undefined;

    const header = WALHeader.init(100);
    header.serialize(&buffer);

    const loaded = try WALHeader.deserialize(&buffer);
    try std.testing.expectEqual(WAL_MAGIC, loaded.magic);
    try std.testing.expectEqual(@as(u32, 1), loaded.version);
    try std.testing.expectEqual(@as(LSN, 100), loaded.start_lsn);
    try std.testing.expectEqual(@as(LSN, 100), loaded.end_lsn);
}

test "WALHeader invalid magic" {
    var buffer: [WAL_HEADER_SIZE]u8 = [_]u8{0} ** WAL_HEADER_SIZE;
    const result = WALHeader.deserialize(&buffer);
    try std.testing.expectError(errors.Error.InvalidMagic, result);
}

test "WALWriter init and deinit" {
    const allocator = std.testing.allocator;

    var writer = try WALWriter.init(allocator, "/tmp/test_wal_init", .sync, DEFAULT_SEGMENT_SIZE);
    defer writer.deinit();

    try std.testing.expectEqual(@as(LSN, 1), writer.currentLSN());
    try std.testing.expectEqual(SyncMode.sync, writer.sync_mode);
}

test "WALWriter getNextLSN" {
    const allocator = std.testing.allocator;

    var writer = try WALWriter.init(allocator, "/tmp/test_wal_lsn", .none, DEFAULT_SEGMENT_SIZE);
    defer writer.deinit();

    const lsn1 = writer.getNextLSN();
    const lsn2 = writer.getNextLSN();
    const lsn3 = writer.getNextLSN();

    try std.testing.expectEqual(@as(LSN, 1), lsn1);
    try std.testing.expectEqual(@as(LSN, 2), lsn2);
    try std.testing.expectEqual(@as(LSN, 3), lsn3);
    try std.testing.expectEqual(@as(LSN, 4), writer.currentLSN());
}

test "WALWriter write and flush" {
    const allocator = std.testing.allocator;

    // Clean up from previous runs
    std.fs.cwd().deleteTree("/tmp/test_wal_write") catch {};

    var writer = try WALWriter.init(allocator, "/tmp/test_wal_write", .none, DEFAULT_SEGMENT_SIZE);
    defer {
        writer.deinit();
        std.fs.cwd().deleteTree("/tmp/test_wal_write") catch {};
    }

    // Write some records
    const lsn1 = try writer.writeInsert(1, "key1", "value1");
    const lsn2 = try writer.writeUpdate(1, "key1", "value1", "value2");
    const lsn3 = try writer.writeDelete(1, "key2", "oldval");
    const lsn4 = try writer.writeCommit(1);

    try std.testing.expectEqual(@as(LSN, 1), lsn1);
    try std.testing.expectEqual(@as(LSN, 2), lsn2);
    try std.testing.expectEqual(@as(LSN, 3), lsn3);
    try std.testing.expectEqual(@as(LSN, 4), lsn4);

    // Position should have advanced
    try std.testing.expect(writer.getPosition() > WAL_HEADER_SIZE);
}

test "WALWriter sync modes" {
    const allocator = std.testing.allocator;

    // Test batch mode
    std.fs.cwd().deleteTree("/tmp/test_wal_batch") catch {};
    var batch_writer = try WALWriter.init(allocator, "/tmp/test_wal_batch", .batch, DEFAULT_SEGMENT_SIZE);
    defer {
        batch_writer.deinit();
        std.fs.cwd().deleteTree("/tmp/test_wal_batch") catch {};
    }
    batch_writer.batch_size = 5;

    // Write 4 records - should not auto-sync
    _ = try batch_writer.writeInsert(1, "k1", "v1");
    _ = try batch_writer.writeInsert(1, "k2", "v2");
    _ = try batch_writer.writeInsert(1, "k3", "v3");
    _ = try batch_writer.writeInsert(1, "k4", "v4");
    try std.testing.expect(batch_writer.buffered_records == 4);

    // 5th record should trigger flush
    _ = try batch_writer.writeInsert(1, "k5", "v5");
    try std.testing.expect(batch_writer.buffered_records == 0);
}

test "WALWriter segment rotation" {
    const allocator = std.testing.allocator;

    // Use small segment size for testing (must be larger than header + one record)
    const small_segment = 256;

    std.fs.cwd().deleteTree("/tmp/test_wal_rotate") catch {};
    var writer = try WALWriter.init(allocator, "/tmp/test_wal_rotate", .none, small_segment);
    defer {
        writer.deinit();
        std.fs.cwd().deleteTree("/tmp/test_wal_rotate") catch {};
    }

    // Write records until rotation - each record is about 60+ bytes
    // With 256 byte segments and 32 byte header, we can fit ~3-4 records per segment
    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        _ = try writer.writeInsert(1, "key", "value_that_is_moderately_long_to_fill_segment_faster");
    }

    // Should have rotated to at least segment 1
    try std.testing.expect(writer.getSegmentNum() >= 1);
}

test "WALWriter transaction records" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_wal_txn") catch {};
    var writer = try WALWriter.init(allocator, "/tmp/test_wal_txn", .none, DEFAULT_SEGMENT_SIZE);
    defer {
        writer.deinit();
        std.fs.cwd().deleteTree("/tmp/test_wal_txn") catch {};
    }

    // Write a complete transaction
    const begin_lsn = try writer.writeBegin(100);
    const insert_lsn = try writer.writeInsert(100, "mykey", "myval");
    const commit_lsn = try writer.writeCommit(100);

    try std.testing.expect(begin_lsn < insert_lsn);
    try std.testing.expect(insert_lsn < commit_lsn);
}

test "WALWriter checkpoint" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_wal_ckpt") catch {};
    var writer = try WALWriter.init(allocator, "/tmp/test_wal_ckpt", .none, DEFAULT_SEGMENT_SIZE);
    defer {
        writer.deinit();
        std.fs.cwd().deleteTree("/tmp/test_wal_ckpt") catch {};
    }

    _ = try writer.writeInsert(1, "key", "val");
    const ckpt_lsn = try writer.writeCheckpoint();

    try std.testing.expect(ckpt_lsn > 0);
    // Checkpoint should force flush
    try std.testing.expect(writer.buffered_records == 0);
}

test "WALWriter setNextLSN" {
    const allocator = std.testing.allocator;

    var writer = try WALWriter.init(allocator, "/tmp/test_wal_setlsn", .none, DEFAULT_SEGMENT_SIZE);
    defer writer.deinit();

    writer.setNextLSN(1000);
    try std.testing.expectEqual(@as(LSN, 1000), writer.currentLSN());

    const lsn = writer.getNextLSN();
    try std.testing.expectEqual(@as(LSN, 1000), lsn);
    try std.testing.expectEqual(@as(LSN, 1001), writer.currentLSN());
}

test "WALWriter needsRotation" {
    const allocator = std.testing.allocator;

    var writer = try WALWriter.init(allocator, "/tmp/test_wal_needrot", .none, 100);
    defer writer.deinit();

    // Initially at position 0, small record should not need rotation
    try std.testing.expect(!writer.needsRotation(50));

    // Large record should need rotation
    try std.testing.expect(writer.needsRotation(200));
}
