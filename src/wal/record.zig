const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");

const LSN = types.LSN;
const TransactionId = types.TransactionId;
const INVALID_LSN = types.INVALID_LSN;
const INVALID_TXN_ID = types.INVALID_TXN_ID;

/// WAL record type enumeration
pub const RecordType = enum(u8) {
    /// Insert a new key-value pair
    insert = 1,
    /// Update an existing key-value pair
    update = 2,
    /// Delete a key-value pair
    delete = 3,
    /// Transaction commit marker
    commit = 4,
    /// Transaction abort marker
    abort = 5,
    /// Checkpoint marker
    checkpoint = 6,
    /// Begin transaction marker
    begin = 7,
};

/// Fixed header size for all WAL records
/// Layout:
///   0-3:   total_length (u32) - includes header, payload, and CRC
///   4-11:  lsn (u64)
///   12-19: txn_id (u64)
///   20:    record_type (u8)
///   21-23: reserved (3 bytes)
///   24-27: key_length (u32)
///   28-31: old_value_length (u32)
///   32-35: new_value_length (u32)
pub const RECORD_HEADER_SIZE: usize = 36;

/// CRC size at end of record
pub const RECORD_CRC_SIZE: usize = 4;

/// Minimum record size (header + CRC, no payload)
pub const MIN_RECORD_SIZE: usize = RECORD_HEADER_SIZE + RECORD_CRC_SIZE;

/// Maximum key size (64KB)
pub const MAX_KEY_SIZE: usize = 64 * 1024;

/// Maximum value size (1MB)
pub const MAX_VALUE_SIZE: usize = 1024 * 1024;

/// WAL record header
pub const RecordHeader = struct {
    /// Total record length including header, payload, and CRC
    total_length: u32,
    /// Log sequence number
    lsn: LSN,
    /// Transaction ID
    txn_id: TransactionId,
    /// Record type
    record_type: RecordType,
    /// Key length in bytes
    key_length: u32,
    /// Old value length (for update/delete, 0 otherwise)
    old_value_length: u32,
    /// New value length (for insert/update, 0 otherwise)
    new_value_length: u32,

    /// Calculate total record size for given payload sizes
    pub fn calculateTotalLength(key_len: usize, old_val_len: usize, new_val_len: usize) u32 {
        return @intCast(RECORD_HEADER_SIZE + key_len + old_val_len + new_val_len + RECORD_CRC_SIZE);
    }

    /// Serialize header to buffer
    pub fn serialize(self: RecordHeader, buffer: []u8) void {
        std.debug.assert(buffer.len >= RECORD_HEADER_SIZE);

        // total_length (4 bytes)
        @memcpy(buffer[0..4], std.mem.asBytes(&self.total_length));
        // lsn (8 bytes)
        @memcpy(buffer[4..12], std.mem.asBytes(&self.lsn));
        // txn_id (8 bytes)
        @memcpy(buffer[12..20], std.mem.asBytes(&self.txn_id));
        // record_type (1 byte)
        buffer[20] = @intFromEnum(self.record_type);
        // reserved (3 bytes)
        @memset(buffer[21..24], 0);
        // key_length (4 bytes)
        @memcpy(buffer[24..28], std.mem.asBytes(&self.key_length));
        // old_value_length (4 bytes)
        @memcpy(buffer[28..32], std.mem.asBytes(&self.old_value_length));
        // new_value_length (4 bytes)
        @memcpy(buffer[32..36], std.mem.asBytes(&self.new_value_length));
    }

    /// Deserialize header from buffer
    pub fn deserialize(buffer: []const u8) errors.Error!RecordHeader {
        if (buffer.len < RECORD_HEADER_SIZE) {
            return errors.Error.InvalidWALRecord;
        }

        const total_length = std.mem.bytesToValue(u32, buffer[0..4]);
        const lsn = std.mem.bytesToValue(LSN, buffer[4..12]);
        const txn_id = std.mem.bytesToValue(TransactionId, buffer[12..20]);
        const record_type_byte = buffer[20];
        const key_length = std.mem.bytesToValue(u32, buffer[24..28]);
        const old_value_length = std.mem.bytesToValue(u32, buffer[28..32]);
        const new_value_length = std.mem.bytesToValue(u32, buffer[32..36]);

        const record_type = std.meta.intToEnum(RecordType, record_type_byte) catch {
            return errors.Error.InvalidWALRecord;
        };

        // Validate total_length
        const expected_length = calculateTotalLength(key_length, old_value_length, new_value_length);
        if (total_length != expected_length) {
            return errors.Error.InvalidWALRecord;
        }

        return .{
            .total_length = total_length,
            .lsn = lsn,
            .txn_id = txn_id,
            .record_type = record_type,
            .key_length = key_length,
            .old_value_length = old_value_length,
            .new_value_length = new_value_length,
        };
    }
};

/// A WAL record with full payload
pub const Record = struct {
    /// Record header
    header: RecordHeader,
    /// Key bytes (owned, allocated)
    key: []const u8,
    /// Old value bytes (owned, allocated, may be empty)
    old_value: []const u8,
    /// New value bytes (owned, allocated, may be empty)
    new_value: []const u8,
    /// Allocator used for key/value storage
    allocator: std.mem.Allocator,

    /// Create an insert record
    pub fn createInsert(
        allocator: std.mem.Allocator,
        record_lsn: LSN,
        txn_id: TransactionId,
        key: []const u8,
        value: []const u8,
    ) errors.MonolithError!Record {
        if (key.len > MAX_KEY_SIZE) return errors.Error.KeyTooLarge;
        if (value.len > MAX_VALUE_SIZE) return errors.Error.ValueTooLarge;

        const key_copy = try allocator.dupe(u8, key);
        errdefer allocator.free(key_copy);
        const value_copy = try allocator.dupe(u8, value);

        return .{
            .header = .{
                .total_length = RecordHeader.calculateTotalLength(key.len, 0, value.len),
                .lsn = record_lsn,
                .txn_id = txn_id,
                .record_type = .insert,
                .key_length = @intCast(key.len),
                .old_value_length = 0,
                .new_value_length = @intCast(value.len),
            },
            .key = key_copy,
            .old_value = &[_]u8{},
            .new_value = value_copy,
            .allocator = allocator,
        };
    }

    /// Create an update record
    pub fn createUpdate(
        allocator: std.mem.Allocator,
        record_lsn: LSN,
        txn_id: TransactionId,
        key: []const u8,
        old_value: []const u8,
        new_value: []const u8,
    ) errors.MonolithError!Record {
        if (key.len > MAX_KEY_SIZE) return errors.Error.KeyTooLarge;
        if (old_value.len > MAX_VALUE_SIZE) return errors.Error.ValueTooLarge;
        if (new_value.len > MAX_VALUE_SIZE) return errors.Error.ValueTooLarge;

        const key_copy = try allocator.dupe(u8, key);
        errdefer allocator.free(key_copy);
        const old_copy = try allocator.dupe(u8, old_value);
        errdefer allocator.free(old_copy);
        const new_copy = try allocator.dupe(u8, new_value);

        return .{
            .header = .{
                .total_length = RecordHeader.calculateTotalLength(key.len, old_value.len, new_value.len),
                .lsn = record_lsn,
                .txn_id = txn_id,
                .record_type = .update,
                .key_length = @intCast(key.len),
                .old_value_length = @intCast(old_value.len),
                .new_value_length = @intCast(new_value.len),
            },
            .key = key_copy,
            .old_value = old_copy,
            .new_value = new_copy,
            .allocator = allocator,
        };
    }

    /// Create a delete record
    pub fn createDelete(
        allocator: std.mem.Allocator,
        record_lsn: LSN,
        txn_id: TransactionId,
        key: []const u8,
        old_value: []const u8,
    ) errors.MonolithError!Record {
        if (key.len > MAX_KEY_SIZE) return errors.Error.KeyTooLarge;
        if (old_value.len > MAX_VALUE_SIZE) return errors.Error.ValueTooLarge;

        const key_copy = try allocator.dupe(u8, key);
        errdefer allocator.free(key_copy);
        const old_copy = try allocator.dupe(u8, old_value);

        return .{
            .header = .{
                .total_length = RecordHeader.calculateTotalLength(key.len, old_value.len, 0),
                .lsn = record_lsn,
                .txn_id = txn_id,
                .record_type = .delete,
                .key_length = @intCast(key.len),
                .old_value_length = @intCast(old_value.len),
                .new_value_length = 0,
            },
            .key = key_copy,
            .old_value = old_copy,
            .new_value = &[_]u8{},
            .allocator = allocator,
        };
    }

    /// Create a commit record
    pub fn createCommit(
        allocator: std.mem.Allocator,
        record_lsn: LSN,
        txn_id: TransactionId,
    ) Record {
        return .{
            .header = .{
                .total_length = RecordHeader.calculateTotalLength(0, 0, 0),
                .lsn = record_lsn,
                .txn_id = txn_id,
                .record_type = .commit,
                .key_length = 0,
                .old_value_length = 0,
                .new_value_length = 0,
            },
            .key = &[_]u8{},
            .old_value = &[_]u8{},
            .new_value = &[_]u8{},
            .allocator = allocator,
        };
    }

    /// Create an abort record
    pub fn createAbort(
        allocator: std.mem.Allocator,
        record_lsn: LSN,
        txn_id: TransactionId,
    ) Record {
        return .{
            .header = .{
                .total_length = RecordHeader.calculateTotalLength(0, 0, 0),
                .lsn = record_lsn,
                .txn_id = txn_id,
                .record_type = .abort,
                .key_length = 0,
                .old_value_length = 0,
                .new_value_length = 0,
            },
            .key = &[_]u8{},
            .old_value = &[_]u8{},
            .new_value = &[_]u8{},
            .allocator = allocator,
        };
    }

    /// Create a begin transaction record
    pub fn createBegin(
        allocator: std.mem.Allocator,
        record_lsn: LSN,
        txn_id: TransactionId,
    ) Record {
        return .{
            .header = .{
                .total_length = RecordHeader.calculateTotalLength(0, 0, 0),
                .lsn = record_lsn,
                .txn_id = txn_id,
                .record_type = .begin,
                .key_length = 0,
                .old_value_length = 0,
                .new_value_length = 0,
            },
            .key = &[_]u8{},
            .old_value = &[_]u8{},
            .new_value = &[_]u8{},
            .allocator = allocator,
        };
    }

    /// Create a checkpoint record
    pub fn createCheckpoint(
        allocator: std.mem.Allocator,
        record_lsn: LSN,
    ) Record {
        return .{
            .header = .{
                .total_length = RecordHeader.calculateTotalLength(0, 0, 0),
                .lsn = record_lsn,
                .txn_id = INVALID_TXN_ID,
                .record_type = .checkpoint,
                .key_length = 0,
                .old_value_length = 0,
                .new_value_length = 0,
            },
            .key = &[_]u8{},
            .old_value = &[_]u8{},
            .new_value = &[_]u8{},
            .allocator = allocator,
        };
    }

    /// Get LSN
    pub fn lsn(self: *const Record) LSN {
        return self.header.lsn;
    }

    /// Get transaction ID
    pub fn txnId(self: *const Record) TransactionId {
        return self.header.txn_id;
    }

    /// Get record type
    pub fn recordType(self: *const Record) RecordType {
        return self.header.record_type;
    }

    /// Get total serialized size
    pub fn totalSize(self: *const Record) usize {
        return self.header.total_length;
    }

    /// Serialize record to buffer
    pub fn serialize(self: *const Record, buffer: []u8) errors.Error!void {
        if (buffer.len < self.header.total_length) {
            return errors.Error.InvalidWALRecord;
        }

        // Serialize header
        self.header.serialize(buffer[0..RECORD_HEADER_SIZE]);

        // Serialize payload
        var offset: usize = RECORD_HEADER_SIZE;

        // Key
        if (self.key.len > 0) {
            @memcpy(buffer[offset .. offset + self.key.len], self.key);
            offset += self.key.len;
        }

        // Old value
        if (self.old_value.len > 0) {
            @memcpy(buffer[offset .. offset + self.old_value.len], self.old_value);
            offset += self.old_value.len;
        }

        // New value
        if (self.new_value.len > 0) {
            @memcpy(buffer[offset .. offset + self.new_value.len], self.new_value);
            offset += self.new_value.len;
        }

        // Calculate and write CRC (over header + payload, not including CRC itself)
        const crc = calculateRecordCRC(buffer[0..offset]);
        @memcpy(buffer[offset .. offset + RECORD_CRC_SIZE], std.mem.asBytes(&crc));
    }

    /// Deserialize record from buffer
    pub fn deserialize(allocator: std.mem.Allocator, buffer: []const u8) errors.MonolithError!Record {
        // Parse header
        const header = try RecordHeader.deserialize(buffer);

        // Validate buffer has full record
        if (buffer.len < header.total_length) {
            return errors.Error.InvalidWALRecord;
        }

        // Verify CRC
        const crc_offset = header.total_length - RECORD_CRC_SIZE;
        const stored_crc = std.mem.bytesToValue(u32, buffer[crc_offset..][0..4]);
        const calculated_crc = calculateRecordCRC(buffer[0..crc_offset]);
        if (stored_crc != calculated_crc) {
            return errors.Error.WALCorrupted;
        }

        // Extract payload
        var offset: usize = RECORD_HEADER_SIZE;

        // Key
        const key = if (header.key_length > 0) blk: {
            const key_data = buffer[offset .. offset + header.key_length];
            offset += header.key_length;
            break :blk try allocator.dupe(u8, key_data);
        } else &[_]u8{};
        errdefer if (key.len > 0) allocator.free(key);

        // Old value
        const old_value = if (header.old_value_length > 0) blk: {
            const old_data = buffer[offset .. offset + header.old_value_length];
            offset += header.old_value_length;
            break :blk try allocator.dupe(u8, old_data);
        } else &[_]u8{};
        errdefer if (old_value.len > 0) allocator.free(old_value);

        // New value
        const new_value = if (header.new_value_length > 0) blk: {
            const new_data = buffer[offset .. offset + header.new_value_length];
            break :blk try allocator.dupe(u8, new_data);
        } else &[_]u8{};

        return .{
            .header = header,
            .key = key,
            .old_value = old_value,
            .new_value = new_value,
            .allocator = allocator,
        };
    }

    /// Free allocated memory
    pub fn deinit(self: *Record) void {
        if (self.key.len > 0) {
            self.allocator.free(self.key);
        }
        if (self.old_value.len > 0) {
            self.allocator.free(self.old_value);
        }
        if (self.new_value.len > 0) {
            self.allocator.free(self.new_value);
        }
        self.* = undefined;
    }

    /// Check if record has payload (key/values)
    pub fn hasPayload(self: *const Record) bool {
        return self.key.len > 0 or self.old_value.len > 0 or self.new_value.len > 0;
    }

    /// Check if record is a transaction control record
    pub fn isTransactionControl(self: *const Record) bool {
        return switch (self.header.record_type) {
            .begin, .commit, .abort => true,
            else => false,
        };
    }

    /// Check if record is a data modification record
    pub fn isDataRecord(self: *const Record) bool {
        return switch (self.header.record_type) {
            .insert, .update, .delete => true,
            else => false,
        };
    }
};

/// Calculate CRC32 for a WAL record
pub fn calculateRecordCRC(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

/// Read just the record header and total length from a buffer
/// Useful for scanning through WAL without deserializing full records
pub fn peekRecordLength(buffer: []const u8) errors.Error!u32 {
    if (buffer.len < 4) {
        return errors.Error.InvalidWALRecord;
    }
    return std.mem.bytesToValue(u32, buffer[0..4]);
}

/// Read just the LSN from a buffer
pub fn peekRecordLSN(buffer: []const u8) errors.Error!LSN {
    if (buffer.len < 12) {
        return errors.Error.InvalidWALRecord;
    }
    return std.mem.bytesToValue(LSN, buffer[4..12]);
}

// Tests

test "RecordHeader serialize and deserialize" {
    const header = RecordHeader{
        .total_length = RecordHeader.calculateTotalLength(5, 10, 15),
        .lsn = 12345,
        .txn_id = 99,
        .record_type = .update,
        .key_length = 5,
        .old_value_length = 10,
        .new_value_length = 15,
    };

    var buffer: [RECORD_HEADER_SIZE]u8 = undefined;
    header.serialize(&buffer);

    const loaded = try RecordHeader.deserialize(&buffer);
    try std.testing.expectEqual(header.total_length, loaded.total_length);
    try std.testing.expectEqual(header.lsn, loaded.lsn);
    try std.testing.expectEqual(header.txn_id, loaded.txn_id);
    try std.testing.expectEqual(header.record_type, loaded.record_type);
    try std.testing.expectEqual(header.key_length, loaded.key_length);
    try std.testing.expectEqual(header.old_value_length, loaded.old_value_length);
    try std.testing.expectEqual(header.new_value_length, loaded.new_value_length);
}

test "RecordHeader invalid type" {
    var buffer: [RECORD_HEADER_SIZE]u8 = [_]u8{0} ** RECORD_HEADER_SIZE;
    buffer[20] = 255; // Invalid record type

    const result = RecordHeader.deserialize(&buffer);
    try std.testing.expectError(errors.Error.InvalidWALRecord, result);
}

test "RecordHeader length mismatch" {
    var buffer: [RECORD_HEADER_SIZE]u8 = undefined;
    const header = RecordHeader{
        .total_length = 999, // Wrong length
        .lsn = 1,
        .txn_id = 1,
        .record_type = .insert,
        .key_length = 5,
        .old_value_length = 0,
        .new_value_length = 10,
    };
    header.serialize(&buffer);

    const result = RecordHeader.deserialize(&buffer);
    try std.testing.expectError(errors.Error.InvalidWALRecord, result);
}

test "Record createInsert and serialize" {
    const allocator = std.testing.allocator;

    var record = try Record.createInsert(allocator, 100, 5, "mykey", "myvalue");
    defer record.deinit();

    try std.testing.expectEqual(@as(LSN, 100), record.lsn());
    try std.testing.expectEqual(@as(TransactionId, 5), record.txnId());
    try std.testing.expectEqual(RecordType.insert, record.recordType());
    try std.testing.expectEqualStrings("mykey", record.key);
    try std.testing.expectEqualStrings("myvalue", record.new_value);
    try std.testing.expectEqual(@as(usize, 0), record.old_value.len);
    try std.testing.expect(record.isDataRecord());
    try std.testing.expect(!record.isTransactionControl());

    // Serialize
    var buffer: [256]u8 = undefined;
    try record.serialize(&buffer);

    // Deserialize and verify
    var loaded = try Record.deserialize(allocator, &buffer);
    defer loaded.deinit();

    try std.testing.expectEqual(record.lsn(), loaded.lsn());
    try std.testing.expectEqual(record.txnId(), loaded.txnId());
    try std.testing.expectEqual(record.recordType(), loaded.recordType());
    try std.testing.expectEqualStrings(record.key, loaded.key);
    try std.testing.expectEqualStrings(record.new_value, loaded.new_value);
}

test "Record createUpdate" {
    const allocator = std.testing.allocator;

    var record = try Record.createUpdate(allocator, 200, 10, "key", "old", "new");
    defer record.deinit();

    try std.testing.expectEqual(RecordType.update, record.recordType());
    try std.testing.expectEqualStrings("key", record.key);
    try std.testing.expectEqualStrings("old", record.old_value);
    try std.testing.expectEqualStrings("new", record.new_value);

    // Round-trip
    var buffer: [256]u8 = undefined;
    try record.serialize(&buffer);

    var loaded = try Record.deserialize(allocator, &buffer);
    defer loaded.deinit();

    try std.testing.expectEqualStrings("old", loaded.old_value);
    try std.testing.expectEqualStrings("new", loaded.new_value);
}

test "Record createDelete" {
    const allocator = std.testing.allocator;

    var record = try Record.createDelete(allocator, 300, 15, "delkey", "delvalue");
    defer record.deinit();

    try std.testing.expectEqual(RecordType.delete, record.recordType());
    try std.testing.expectEqualStrings("delkey", record.key);
    try std.testing.expectEqualStrings("delvalue", record.old_value);
    try std.testing.expectEqual(@as(usize, 0), record.new_value.len);

    // Round-trip
    var buffer: [256]u8 = undefined;
    try record.serialize(&buffer);

    var loaded = try Record.deserialize(allocator, &buffer);
    defer loaded.deinit();

    try std.testing.expectEqual(RecordType.delete, loaded.recordType());
}

test "Record createCommit" {
    const allocator = std.testing.allocator;

    var record = Record.createCommit(allocator, 400, 20);
    defer record.deinit();

    try std.testing.expectEqual(RecordType.commit, record.recordType());
    try std.testing.expectEqual(@as(LSN, 400), record.lsn());
    try std.testing.expectEqual(@as(TransactionId, 20), record.txnId());
    try std.testing.expect(!record.hasPayload());
    try std.testing.expect(record.isTransactionControl());

    // Round-trip
    var buffer: [256]u8 = undefined;
    try record.serialize(&buffer);

    var loaded = try Record.deserialize(allocator, &buffer);
    defer loaded.deinit();

    try std.testing.expectEqual(RecordType.commit, loaded.recordType());
}

test "Record createAbort" {
    const allocator = std.testing.allocator;

    var record = Record.createAbort(allocator, 500, 25);
    defer record.deinit();

    try std.testing.expectEqual(RecordType.abort, record.recordType());
    try std.testing.expect(record.isTransactionControl());

    // Round-trip
    var buffer: [256]u8 = undefined;
    try record.serialize(&buffer);

    var loaded = try Record.deserialize(allocator, &buffer);
    defer loaded.deinit();

    try std.testing.expectEqual(RecordType.abort, loaded.recordType());
}

test "Record createBegin" {
    const allocator = std.testing.allocator;

    var record = Record.createBegin(allocator, 600, 30);
    defer record.deinit();

    try std.testing.expectEqual(RecordType.begin, record.recordType());
    try std.testing.expect(record.isTransactionControl());
}

test "Record createCheckpoint" {
    const allocator = std.testing.allocator;

    var record = Record.createCheckpoint(allocator, 700);
    defer record.deinit();

    try std.testing.expectEqual(RecordType.checkpoint, record.recordType());
    try std.testing.expectEqual(INVALID_TXN_ID, record.txnId());
    try std.testing.expect(!record.isTransactionControl());
    try std.testing.expect(!record.isDataRecord());
}

test "Record CRC validation" {
    const allocator = std.testing.allocator;

    var record = try Record.createInsert(allocator, 100, 5, "key", "value");
    defer record.deinit();

    var buffer: [256]u8 = undefined;
    try record.serialize(&buffer);

    // Corrupt one byte in the payload
    buffer[RECORD_HEADER_SIZE + 2] ^= 0xFF;

    // Should fail CRC check
    const result = Record.deserialize(allocator, &buffer);
    try std.testing.expectError(errors.Error.WALCorrupted, result);
}

test "peekRecordLength" {
    const allocator = std.testing.allocator;

    var record = try Record.createInsert(allocator, 1, 1, "key", "value");
    defer record.deinit();

    var buffer: [256]u8 = undefined;
    try record.serialize(&buffer);

    const length = try peekRecordLength(&buffer);
    try std.testing.expectEqual(record.header.total_length, length);
}

test "peekRecordLSN" {
    const allocator = std.testing.allocator;

    var record = try Record.createInsert(allocator, 12345, 1, "key", "value");
    defer record.deinit();

    var buffer: [256]u8 = undefined;
    try record.serialize(&buffer);

    const lsn = try peekRecordLSN(&buffer);
    try std.testing.expectEqual(@as(LSN, 12345), lsn);
}

test "Record key too large" {
    const allocator = std.testing.allocator;

    const huge_key = try allocator.alloc(u8, MAX_KEY_SIZE + 1);
    defer allocator.free(huge_key);

    const result = Record.createInsert(allocator, 1, 1, huge_key, "value");
    try std.testing.expectError(errors.Error.KeyTooLarge, result);
}

test "Record value too large" {
    const allocator = std.testing.allocator;

    const huge_value = try allocator.alloc(u8, MAX_VALUE_SIZE + 1);
    defer allocator.free(huge_value);

    const result = Record.createInsert(allocator, 1, 1, "key", huge_value);
    try std.testing.expectError(errors.Error.ValueTooLarge, result);
}

test "calculateRecordCRC consistency" {
    const data1 = "test data for CRC";
    const data2 = "test data for CRC";
    const data3 = "different data";

    const crc1 = calculateRecordCRC(data1);
    const crc2 = calculateRecordCRC(data2);
    const crc3 = calculateRecordCRC(data3);

    try std.testing.expectEqual(crc1, crc2);
    try std.testing.expect(crc1 != crc3);
}
