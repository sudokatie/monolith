const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const record = @import("record.zig");

const LSN = types.LSN;
const TransactionId = types.TransactionId;
const PageId = types.PageId;
const INVALID_LSN = types.INVALID_LSN;
const SyncMode = types.SyncMode;

const Record = record.Record;
const RecordType = record.RecordType;
const RECORD_HEADER_SIZE = record.RECORD_HEADER_SIZE;

/// Default WAL segment size (16MB)
pub const DEFAULT_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

/// WAL file header size
pub const WAL_HEADER_SIZE: usize = 32;

/// WAL writer for append-only logging
pub const WALWriter = struct {
    /// File handle
    file: std.fs.File,
    /// Current write position
    position: u64,
    /// Current LSN
    current_lsn: LSN,
    /// Sync mode
    sync_mode: SyncMode,
    /// Allocator
    allocator: std.mem.Allocator,
    /// Write buffer for batching
    write_buffer: std.ArrayListUnmanaged(u8),
    /// Buffer size before flush
    buffer_limit: usize,
    /// File path for reopening
    path: []const u8,

    /// Open or create WAL file
    pub fn open(allocator: std.mem.Allocator, path: []const u8, sync_mode: SyncMode) !WALWriter {
        const file = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try std.fs.cwd().createFile(path, .{ .read = true }),
            else => return err,
        };

        // Get current size
        const stat = try file.stat();
        var position: u64 = stat.size;
        var current_lsn: LSN = 1;

        // Initialize header if new file
        if (position == 0) {
            var header: [WAL_HEADER_SIZE]u8 = undefined;
            @memset(&header, 0);
            // Magic "MWAL"
            header[0] = 'M';
            header[1] = 'W';
            header[2] = 'A';
            header[3] = 'L';
            // Version
            header[4] = 1;

            _ = try file.write(&header);
            try file.sync();
            position = WAL_HEADER_SIZE;
        } else {
            // Read existing header and find last LSN
            try file.seekTo(0);
            var header: [WAL_HEADER_SIZE]u8 = undefined;
            _ = try file.read(&header);

            if (header[0] != 'M' or header[1] != 'W' or header[2] != 'A' or header[3] != 'L') {
                return errors.Error.WALCorrupted;
            }

            // Scan to find last LSN
            current_lsn = try scanForLastLSN(allocator, file, position);
        }

        // Copy path for later
        const path_copy = try allocator.alloc(u8, path.len);
        @memcpy(path_copy, path);

        return .{
            .file = file,
            .position = position,
            .current_lsn = current_lsn,
            .sync_mode = sync_mode,
            .allocator = allocator,
            .write_buffer = .{},
            .buffer_limit = 64 * 1024, // 64KB buffer
            .path = path_copy,
        };
    }

    /// Close WAL writer
    pub fn close(self: *WALWriter) void {
        self.flush() catch {};
        self.file.close();
        self.write_buffer.deinit(self.allocator);
        self.allocator.free(self.path);
    }

    /// Append a record and return its LSN
    pub fn append(self: *WALWriter, txn_id: TransactionId, prev_lsn: LSN, record_type: RecordType, data: []const u8) !LSN {
        const lsn = self.current_lsn;
        self.current_lsn += 1;

        const rec = Record{
            .lsn = lsn,
            .prev_lsn = prev_lsn,
            .txn_id = txn_id,
            .record_type = record_type,
            .data = data,
        };

        const size = rec.serializedSize();
        const start = self.write_buffer.items.len;

        try self.write_buffer.resize(self.allocator, start + size);
        rec.serialize(self.write_buffer.items[start..]);

        // Check if we should flush
        if (self.write_buffer.items.len >= self.buffer_limit or self.sync_mode == .sync) {
            try self.flush();
        }

        return lsn;
    }

    /// Flush buffered writes to disk
    pub fn flush(self: *WALWriter) !void {
        if (self.write_buffer.items.len == 0) return;

        try self.file.seekTo(self.position);
        _ = try self.file.write(self.write_buffer.items);
        self.position += self.write_buffer.items.len;
        self.write_buffer.clearRetainingCapacity();

        if (self.sync_mode != .none) {
            try self.file.sync();
        }
    }

    /// Sync to disk
    pub fn sync(self: *WALWriter) !void {
        try self.flush();
        try self.file.sync();
    }

    /// Get current LSN
    pub fn getCurrentLSN(self: *const WALWriter) LSN {
        return self.current_lsn;
    }

    /// Get flush position
    pub fn getFlushPosition(self: *const WALWriter) u64 {
        return self.position;
    }

    /// Truncate WAL to given position (after checkpoint)
    pub fn truncate(self: *WALWriter, new_size: u64) !void {
        try self.flush();

        // On non-Windows systems we can set the file length
        // For simplicity, we'll just track position
        if (new_size < self.position) {
            self.position = new_size;
        }
    }
};

/// Scan WAL file to find last LSN
fn scanForLastLSN(allocator: std.mem.Allocator, file: std.fs.File, end_position: u64) !LSN {
    var last_lsn: LSN = 1;
    var pos: u64 = WAL_HEADER_SIZE;

    var header_buf: [RECORD_HEADER_SIZE]u8 = undefined;

    while (pos < end_position) {
        try file.seekTo(pos);
        const bytes_read = try file.read(&header_buf);

        if (bytes_read < RECORD_HEADER_SIZE) break;

        const lsn = std.mem.bytesToValue(LSN, header_buf[0..8]);
        const length = std.mem.bytesToValue(u32, header_buf[25..29]);

        if (length < RECORD_HEADER_SIZE or pos + length > end_position) {
            break;
        }

        if (lsn >= last_lsn) {
            last_lsn = lsn + 1;
        }

        pos += length;
    }

    _ = allocator;
    return last_lsn;
}

/// WAL reader for recovery
pub const WALReader = struct {
    /// File handle
    file: std.fs.File,
    /// Current read position
    position: u64,
    /// End of file position
    end_position: u64,
    /// Allocator
    allocator: std.mem.Allocator,

    /// Open WAL for reading
    pub fn open(allocator: std.mem.Allocator, path: []const u8) !WALReader {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_only });

        const stat = try file.stat();
        const end_position = stat.size;

        // Verify header
        var header: [WAL_HEADER_SIZE]u8 = undefined;
        _ = try file.read(&header);

        if (header[0] != 'M' or header[1] != 'W' or header[2] != 'A' or header[3] != 'L') {
            file.close();
            return errors.Error.WALCorrupted;
        }

        return .{
            .file = file,
            .position = WAL_HEADER_SIZE,
            .end_position = end_position,
            .allocator = allocator,
        };
    }

    /// Close reader
    pub fn close(self: *WALReader) void {
        self.file.close();
    }

    /// Read next record (caller owns returned data)
    pub fn readNext(self: *WALReader) !?Record {
        if (self.position >= self.end_position) {
            return null;
        }

        try self.file.seekTo(self.position);

        // Read header first
        var header_buf: [RECORD_HEADER_SIZE]u8 = undefined;
        const header_read = try self.file.read(&header_buf);

        if (header_read < RECORD_HEADER_SIZE) {
            return null;
        }

        const length = std.mem.bytesToValue(u32, header_buf[25..29]);

        if (length < RECORD_HEADER_SIZE or self.position + length > self.end_position) {
            return null;
        }

        // Read full record
        const buffer = try self.allocator.alloc(u8, length);
        defer self.allocator.free(buffer);

        try self.file.seekTo(self.position);
        const bytes_read = try self.file.read(buffer);

        if (bytes_read < length) {
            return null;
        }

        self.position += length;

        const rec = try Record.deserialize(buffer, self.allocator);
        return rec;
    }

    /// Seek to specific position
    pub fn seekTo(self: *WALReader, pos: u64) !void {
        if (pos < WAL_HEADER_SIZE) {
            self.position = WAL_HEADER_SIZE;
        } else {
            self.position = pos;
        }
    }

    /// Get current position
    pub fn getPosition(self: *const WALReader) u64 {
        return self.position;
    }
};

// Tests

test "WALWriter open and close" {
    const allocator = std.testing.allocator;
    const path = "test_wal_open.wal";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var writer = try WALWriter.open(allocator, path, .sync);
    defer writer.close();

    try std.testing.expectEqual(@as(LSN, 1), writer.getCurrentLSN());
}

test "WALWriter append and flush" {
    const allocator = std.testing.allocator;
    const path = "test_wal_append.wal";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var writer = try WALWriter.open(allocator, path, .none);
    defer writer.close();

    const lsn1 = try writer.append(1, INVALID_LSN, .begin, "");
    const lsn2 = try writer.append(1, lsn1, .insert, "test data");
    const lsn3 = try writer.append(1, lsn2, .commit, "");

    try std.testing.expectEqual(@as(LSN, 1), lsn1);
    try std.testing.expectEqual(@as(LSN, 2), lsn2);
    try std.testing.expectEqual(@as(LSN, 3), lsn3);

    try writer.flush();
}

test "WALReader read records" {
    const allocator = std.testing.allocator;
    const path = "test_wal_read.wal";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    // Write some records
    {
        var writer = try WALWriter.open(allocator, path, .sync);
        defer writer.close();

        _ = try writer.append(1, INVALID_LSN, .begin, "");
        _ = try writer.append(1, 1, .insert, "hello");
        _ = try writer.append(1, 2, .commit, "");
    }

    // Read them back
    {
        var reader = try WALReader.open(allocator, path);
        defer reader.close();

        var rec1 = (try reader.readNext()) orelse return error.TestUnexpectedResult;
        defer rec1.deinit(allocator);
        try std.testing.expectEqual(RecordType.begin, rec1.record_type);

        var rec2 = (try reader.readNext()) orelse return error.TestUnexpectedResult;
        defer rec2.deinit(allocator);
        try std.testing.expectEqual(RecordType.insert, rec2.record_type);
        try std.testing.expectEqualStrings("hello", rec2.data);

        var rec3 = (try reader.readNext()) orelse return error.TestUnexpectedResult;
        defer rec3.deinit(allocator);
        try std.testing.expectEqual(RecordType.commit, rec3.record_type);

        const rec4 = try reader.readNext();
        try std.testing.expectEqual(@as(?Record, null), rec4);
    }
}

test "WALWriter reopen continues LSN" {
    const allocator = std.testing.allocator;
    const path = "test_wal_reopen.wal";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    // Write some records
    {
        var writer = try WALWriter.open(allocator, path, .sync);
        defer writer.close();

        _ = try writer.append(1, INVALID_LSN, .begin, "");
        _ = try writer.append(1, 1, .commit, "");
    }

    // Reopen and verify LSN continues
    {
        var writer = try WALWriter.open(allocator, path, .sync);
        defer writer.close();

        try std.testing.expectEqual(@as(LSN, 3), writer.getCurrentLSN());

        const lsn = try writer.append(2, INVALID_LSN, .begin, "");
        try std.testing.expectEqual(@as(LSN, 3), lsn);
    }
}
