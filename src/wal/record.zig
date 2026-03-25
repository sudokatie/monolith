const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");

const PageId = types.PageId;
const TransactionId = types.TransactionId;
const LSN = types.LSN;
const INVALID_LSN = types.INVALID_LSN;

/// WAL record types
pub const RecordType = enum(u8) {
    /// Insert a key-value pair
    insert = 1,
    /// Delete a key
    delete = 2,
    /// Update a value
    update = 3,
    /// Transaction begin
    begin = 4,
    /// Transaction commit
    commit = 5,
    /// Transaction abort
    abort = 6,
    /// Checkpoint start
    checkpoint_begin = 7,
    /// Checkpoint end
    checkpoint_end = 8,
    /// Page write (for redo)
    page_write = 9,
    /// Compensation log record (for undo)
    clr = 10,
};

/// WAL record header size
/// Layout:
///   0-7:   LSN (u64)
///   8-15:  prev_lsn (u64)
///   16-23: txn_id (u64)
///   24:    record_type (u8)
///   25-28: length (u32) - total record length including header
///   29-32: checksum (u32)
pub const RECORD_HEADER_SIZE: usize = 33;

/// WAL record structure
pub const Record = struct {
    /// Log sequence number
    lsn: LSN,
    /// Previous LSN in this transaction (for undo chain)
    prev_lsn: LSN,
    /// Transaction ID
    txn_id: TransactionId,
    /// Record type
    record_type: RecordType,
    /// Record data (variable length)
    data: []const u8,

    /// Calculate total serialized size
    pub fn serializedSize(self: *const Record) usize {
        return RECORD_HEADER_SIZE + self.data.len;
    }

    /// Serialize record to buffer
    pub fn serialize(self: *const Record, buffer: []u8) void {
        std.debug.assert(buffer.len >= self.serializedSize());

        // LSN
        @memcpy(buffer[0..8], std.mem.asBytes(&self.lsn));

        // Previous LSN
        @memcpy(buffer[8..16], std.mem.asBytes(&self.prev_lsn));

        // Transaction ID
        @memcpy(buffer[16..24], std.mem.asBytes(&self.txn_id));

        // Record type
        buffer[24] = @intFromEnum(self.record_type);

        // Length
        const length: u32 = @intCast(self.serializedSize());
        @memcpy(buffer[25..29], std.mem.asBytes(&length));

        // Data
        @memcpy(buffer[RECORD_HEADER_SIZE .. RECORD_HEADER_SIZE + self.data.len], self.data);

        // Checksum (over everything except checksum field)
        const checksum = calculateChecksum(buffer[0..25], buffer[RECORD_HEADER_SIZE .. RECORD_HEADER_SIZE + self.data.len]);
        @memcpy(buffer[29..33], std.mem.asBytes(&checksum));
    }

    /// Deserialize record from buffer
    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !Record {
        if (buffer.len < RECORD_HEADER_SIZE) {
            return errors.Error.InvalidWALRecord;
        }

        const lsn = std.mem.bytesToValue(LSN, buffer[0..8]);
        const prev_lsn = std.mem.bytesToValue(LSN, buffer[8..16]);
        const txn_id = std.mem.bytesToValue(TransactionId, buffer[16..24]);
        const record_type_byte = buffer[24];
        const length = std.mem.bytesToValue(u32, buffer[25..29]);
        const stored_checksum = std.mem.bytesToValue(u32, buffer[29..33]);

        if (length < RECORD_HEADER_SIZE or buffer.len < length) {
            return errors.Error.InvalidWALRecord;
        }

        const data_len = length - RECORD_HEADER_SIZE;
        const calculated = calculateChecksum(buffer[0..25], buffer[RECORD_HEADER_SIZE .. RECORD_HEADER_SIZE + data_len]);

        if (stored_checksum != calculated) {
            return errors.Error.WALCorrupted;
        }

        const record_type = std.meta.intToEnum(RecordType, record_type_byte) catch {
            return errors.Error.InvalidWALRecord;
        };

        // Copy data
        const data = try allocator.alloc(u8, data_len);
        @memcpy(data, buffer[RECORD_HEADER_SIZE .. RECORD_HEADER_SIZE + data_len]);

        return .{
            .lsn = lsn,
            .prev_lsn = prev_lsn,
            .txn_id = txn_id,
            .record_type = record_type,
            .data = data,
        };
    }

    /// Free allocated data
    pub fn deinit(self: *Record, allocator: std.mem.Allocator) void {
        if (self.data.len > 0) {
            allocator.free(self.data);
        }
    }
};

/// Calculate CRC32 checksum over header and data
fn calculateChecksum(header: []const u8, data: []const u8) u32 {
    var crc = std.hash.Crc32.init();
    crc.update(header);
    crc.update(data);
    return crc.final();
}

/// Insert record data format
/// Layout:
///   0-7:   page_id (u64)
///   8-9:   key_len (u16)
///   10-11: value_len (u16)
///   12+:   key data
///   12+key_len+: value data
pub const InsertData = struct {
    page_id: PageId,
    key: []const u8,
    value: []const u8,

    pub fn serializedSize(self: *const InsertData) usize {
        return 12 + self.key.len + self.value.len;
    }

    pub fn serialize(self: *const InsertData, buffer: []u8) void {
        @memcpy(buffer[0..8], std.mem.asBytes(&self.page_id));
        const key_len: u16 = @intCast(self.key.len);
        const value_len: u16 = @intCast(self.value.len);
        @memcpy(buffer[8..10], std.mem.asBytes(&key_len));
        @memcpy(buffer[10..12], std.mem.asBytes(&value_len));
        @memcpy(buffer[12 .. 12 + self.key.len], self.key);
        @memcpy(buffer[12 + self.key.len .. 12 + self.key.len + self.value.len], self.value);
    }

    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !InsertData {
        if (buffer.len < 12) return errors.Error.InvalidWALRecord;

        const page_id = std.mem.bytesToValue(PageId, buffer[0..8]);
        const key_len = std.mem.bytesToValue(u16, buffer[8..10]);
        const value_len = std.mem.bytesToValue(u16, buffer[10..12]);

        if (buffer.len < 12 + key_len + value_len) {
            return errors.Error.InvalidWALRecord;
        }

        const key = try allocator.alloc(u8, key_len);
        @memcpy(key, buffer[12 .. 12 + key_len]);

        const value = try allocator.alloc(u8, value_len);
        @memcpy(value, buffer[12 + key_len .. 12 + key_len + value_len]);

        return .{
            .page_id = page_id,
            .key = key,
            .value = value,
        };
    }

    pub fn deinit(self: *InsertData, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
    }
};

/// Delete record data format
pub const DeleteData = struct {
    page_id: PageId,
    key: []const u8,

    pub fn serializedSize(self: *const DeleteData) usize {
        return 10 + self.key.len;
    }

    pub fn serialize(self: *const DeleteData, buffer: []u8) void {
        @memcpy(buffer[0..8], std.mem.asBytes(&self.page_id));
        const key_len: u16 = @intCast(self.key.len);
        @memcpy(buffer[8..10], std.mem.asBytes(&key_len));
        @memcpy(buffer[10 .. 10 + self.key.len], self.key);
    }

    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !DeleteData {
        if (buffer.len < 10) return errors.Error.InvalidWALRecord;

        const page_id = std.mem.bytesToValue(PageId, buffer[0..8]);
        const key_len = std.mem.bytesToValue(u16, buffer[8..10]);

        if (buffer.len < 10 + key_len) {
            return errors.Error.InvalidWALRecord;
        }

        const key = try allocator.alloc(u8, key_len);
        @memcpy(key, buffer[10 .. 10 + key_len]);

        return .{
            .page_id = page_id,
            .key = key,
        };
    }

    pub fn deinit(self: *DeleteData, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
    }
};

/// Update record data format (includes old_value for undo)
/// Layout:
///   0-7:   page_id (u64)
///   8-9:   key_len (u16)
///   10-11: old_value_len (u16)
///   12-13: new_value_len (u16)
///   14+:   key data
///   14+key_len+: old_value data
///   14+key_len+old_value_len+: new_value data
pub const UpdateData = struct {
    page_id: PageId,
    key: []const u8,
    old_value: []const u8,
    new_value: []const u8,

    pub fn serializedSize(self: *const UpdateData) usize {
        return 14 + self.key.len + self.old_value.len + self.new_value.len;
    }

    pub fn serialize(self: *const UpdateData, buffer: []u8) void {
        @memcpy(buffer[0..8], std.mem.asBytes(&self.page_id));
        const key_len: u16 = @intCast(self.key.len);
        const old_len: u16 = @intCast(self.old_value.len);
        const new_len: u16 = @intCast(self.new_value.len);
        @memcpy(buffer[8..10], std.mem.asBytes(&key_len));
        @memcpy(buffer[10..12], std.mem.asBytes(&old_len));
        @memcpy(buffer[12..14], std.mem.asBytes(&new_len));

        var offset: usize = 14;
        @memcpy(buffer[offset .. offset + self.key.len], self.key);
        offset += self.key.len;
        @memcpy(buffer[offset .. offset + self.old_value.len], self.old_value);
        offset += self.old_value.len;
        @memcpy(buffer[offset .. offset + self.new_value.len], self.new_value);
    }

    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !UpdateData {
        if (buffer.len < 14) return errors.Error.InvalidWALRecord;

        const page_id = std.mem.bytesToValue(PageId, buffer[0..8]);
        const key_len = std.mem.bytesToValue(u16, buffer[8..10]);
        const old_len = std.mem.bytesToValue(u16, buffer[10..12]);
        const new_len = std.mem.bytesToValue(u16, buffer[12..14]);

        const total_len = 14 + key_len + old_len + new_len;
        if (buffer.len < total_len) {
            return errors.Error.InvalidWALRecord;
        }

        var offset: usize = 14;
        const key = try allocator.alloc(u8, key_len);
        @memcpy(key, buffer[offset .. offset + key_len]);
        offset += key_len;

        const old_value = try allocator.alloc(u8, old_len);
        @memcpy(old_value, buffer[offset .. offset + old_len]);
        offset += old_len;

        const new_value = try allocator.alloc(u8, new_len);
        @memcpy(new_value, buffer[offset .. offset + new_len]);

        return .{
            .page_id = page_id,
            .key = key,
            .old_value = old_value,
            .new_value = new_value,
        };
    }

    pub fn deinit(self: *UpdateData, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.old_value);
        allocator.free(self.new_value);
    }
};

/// Checkpoint data format
pub const CheckpointData = struct {
    /// Active transaction IDs at checkpoint time
    active_txns: []TransactionId,
    /// Dirty pages at checkpoint time
    dirty_pages: []PageId,

    pub fn serializedSize(self: *const CheckpointData) usize {
        return 8 + self.active_txns.len * 8 + self.dirty_pages.len * 8;
    }

    pub fn serialize(self: *const CheckpointData, buffer: []u8) void {
        const txn_count: u32 = @intCast(self.active_txns.len);
        const page_count: u32 = @intCast(self.dirty_pages.len);

        @memcpy(buffer[0..4], std.mem.asBytes(&txn_count));
        @memcpy(buffer[4..8], std.mem.asBytes(&page_count));

        var offset: usize = 8;
        for (self.active_txns) |txn_id| {
            @memcpy(buffer[offset .. offset + 8], std.mem.asBytes(&txn_id));
            offset += 8;
        }
        for (self.dirty_pages) |page_id| {
            @memcpy(buffer[offset .. offset + 8], std.mem.asBytes(&page_id));
            offset += 8;
        }
    }

    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !CheckpointData {
        if (buffer.len < 8) return errors.Error.InvalidWALRecord;

        const txn_count = std.mem.bytesToValue(u32, buffer[0..4]);
        const page_count = std.mem.bytesToValue(u32, buffer[4..8]);

        const expected_len = 8 + txn_count * 8 + page_count * 8;
        if (buffer.len < expected_len) {
            return errors.Error.InvalidWALRecord;
        }

        const active_txns = try allocator.alloc(TransactionId, txn_count);
        errdefer allocator.free(active_txns);

        const dirty_pages = try allocator.alloc(PageId, page_count);
        errdefer allocator.free(dirty_pages);

        var offset: usize = 8;
        for (0..txn_count) |i| {
            active_txns[i] = std.mem.bytesToValue(TransactionId, buffer[offset .. offset + 8]);
            offset += 8;
        }
        for (0..page_count) |i| {
            dirty_pages[i] = std.mem.bytesToValue(PageId, buffer[offset .. offset + 8]);
            offset += 8;
        }

        return .{
            .active_txns = active_txns,
            .dirty_pages = dirty_pages,
        };
    }

    pub fn deinit(self: *CheckpointData, allocator: std.mem.Allocator) void {
        allocator.free(self.active_txns);
        allocator.free(self.dirty_pages);
    }
};

// Tests

test "Record serialize and deserialize" {
    const allocator = std.testing.allocator;

    const data = "test data";
    const record = Record{
        .lsn = 100,
        .prev_lsn = 50,
        .txn_id = 1,
        .record_type = .insert,
        .data = data,
    };

    var buffer: [128]u8 = undefined;
    record.serialize(&buffer);

    var deserialized = try Record.deserialize(&buffer, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(LSN, 100), deserialized.lsn);
    try std.testing.expectEqual(@as(LSN, 50), deserialized.prev_lsn);
    try std.testing.expectEqual(@as(TransactionId, 1), deserialized.txn_id);
    try std.testing.expectEqual(RecordType.insert, deserialized.record_type);
    try std.testing.expectEqualStrings(data, deserialized.data);
}

test "Record checksum validation" {
    const allocator = std.testing.allocator;

    const record = Record{
        .lsn = 1,
        .prev_lsn = 0,
        .txn_id = 1,
        .record_type = .commit,
        .data = "",
    };

    var buffer: [64]u8 = undefined;
    record.serialize(&buffer);

    // Corrupt data
    buffer[0] ^= 0xFF;

    const result = Record.deserialize(&buffer, allocator);
    try std.testing.expectError(errors.Error.WALCorrupted, result);
}

test "InsertData serialize and deserialize" {
    const allocator = std.testing.allocator;

    const insert = InsertData{
        .page_id = 42,
        .key = "mykey",
        .value = "myvalue",
    };

    var buffer: [64]u8 = undefined;
    insert.serialize(&buffer);

    var deserialized = try InsertData.deserialize(&buffer, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(PageId, 42), deserialized.page_id);
    try std.testing.expectEqualStrings("mykey", deserialized.key);
    try std.testing.expectEqualStrings("myvalue", deserialized.value);
}

test "DeleteData serialize and deserialize" {
    const allocator = std.testing.allocator;

    const delete = DeleteData{
        .page_id = 100,
        .key = "deletekey",
    };

    var buffer: [32]u8 = undefined;
    delete.serialize(&buffer);

    var deserialized = try DeleteData.deserialize(&buffer, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(PageId, 100), deserialized.page_id);
    try std.testing.expectEqualStrings("deletekey", deserialized.key);
}

test "CheckpointData serialize and deserialize" {
    const allocator = std.testing.allocator;

    var active_txns = [_]TransactionId{ 1, 2, 3 };
    var dirty_pages = [_]PageId{ 10, 20 };

    const checkpoint = CheckpointData{
        .active_txns = &active_txns,
        .dirty_pages = &dirty_pages,
    };

    var buffer: [64]u8 = undefined;
    checkpoint.serialize(&buffer);

    var deserialized = try CheckpointData.deserialize(&buffer, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), deserialized.active_txns.len);
    try std.testing.expectEqual(@as(usize, 2), deserialized.dirty_pages.len);
    try std.testing.expectEqual(@as(TransactionId, 1), deserialized.active_txns[0]);
    try std.testing.expectEqual(@as(PageId, 10), deserialized.dirty_pages[0]);
}

test "RecordType values" {
    try std.testing.expectEqual(@as(u8, 1), @intFromEnum(RecordType.insert));
    try std.testing.expectEqual(@as(u8, 5), @intFromEnum(RecordType.commit));
}
