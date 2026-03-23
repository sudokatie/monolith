const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");

const PageId = types.PageId;
const PageType = types.PageType;
const LSN = types.LSN;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;

/// Page header size in bytes
pub const PAGE_HEADER_SIZE: usize = 24;

/// Page header structure
/// Layout:
///   0-7:   page_id (u64)
///   8:     page_type (u8)
///   9-15:  reserved (7 bytes)
///   16-23: lsn (u64)
pub const PageHeader = struct {
    page_id: PageId,
    page_type: PageType,
    lsn: LSN,

    pub fn init(page_id: PageId, page_type: PageType) PageHeader {
        return .{
            .page_id = page_id,
            .page_type = page_type,
            .lsn = types.INVALID_LSN,
        };
    }

    pub fn serialize(self: PageHeader, buffer: []u8) void {
        std.debug.assert(buffer.len >= PAGE_HEADER_SIZE);

        // Write page_id (8 bytes)
        @memcpy(buffer[0..8], std.mem.asBytes(&self.page_id));

        // Write page_type (1 byte)
        buffer[8] = @intFromEnum(self.page_type);

        // Reserved bytes (7 bytes) - zero fill
        @memset(buffer[9..16], 0);

        // Write LSN (8 bytes)
        @memcpy(buffer[16..24], std.mem.asBytes(&self.lsn));
    }

    pub fn deserialize(buffer: []const u8) errors.Error!PageHeader {
        if (buffer.len < PAGE_HEADER_SIZE) {
            return errors.Error.Corrupted;
        }

        const page_id = std.mem.bytesToValue(PageId, buffer[0..8]);
        const page_type_byte = buffer[8];
        const lsn = std.mem.bytesToValue(LSN, buffer[16..24]);

        const page_type = std.meta.intToEnum(PageType, page_type_byte) catch {
            return errors.Error.InvalidPageType;
        };

        return .{
            .page_id = page_id,
            .page_type = page_type,
            .lsn = lsn,
        };
    }
};

/// A database page
pub const Page = struct {
    /// Header information
    header: PageHeader,
    /// Raw page data (excluding header)
    data: []u8,
    /// Full buffer (header + data)
    buffer: []u8,
    /// Page size
    page_size: usize,
    /// Dirty flag
    dirty: bool,

    /// Initialize a new page from a buffer
    pub fn init(buffer: []u8, page_id: PageId, page_type: PageType) Page {
        const header = PageHeader.init(page_id, page_type);
        header.serialize(buffer[0..PAGE_HEADER_SIZE]);

        return .{
            .header = header,
            .data = buffer[PAGE_HEADER_SIZE..],
            .buffer = buffer,
            .page_size = buffer.len,
            .dirty = true,
        };
    }

    /// Load a page from a buffer (deserialize header)
    pub fn load(buffer: []u8) errors.Error!Page {
        const header = try PageHeader.deserialize(buffer[0..PAGE_HEADER_SIZE]);

        return .{
            .header = header,
            .data = buffer[PAGE_HEADER_SIZE..],
            .buffer = buffer,
            .page_size = buffer.len,
            .dirty = false,
        };
    }

    /// Get the page ID
    pub fn id(self: *const Page) PageId {
        return self.header.page_id;
    }

    /// Get the page type
    pub fn pageType(self: *const Page) PageType {
        return self.header.page_type;
    }

    /// Get the LSN
    pub fn lsn(self: *const Page) LSN {
        return self.header.lsn;
    }

    /// Set the LSN
    pub fn setLsn(self: *Page, new_lsn: LSN) void {
        self.header.lsn = new_lsn;
        self.dirty = true;
    }

    /// Mark the page as dirty
    pub fn markDirty(self: *Page) void {
        self.dirty = true;
    }

    /// Check if the page is dirty
    pub fn isDirty(self: *const Page) bool {
        return self.dirty;
    }

    /// Get usable data size
    pub fn dataSize(self: *const Page) usize {
        return self.page_size - PAGE_HEADER_SIZE - CHECKSUM_SIZE;
    }

    /// Serialize the page to its buffer
    pub fn serialize(self: *Page) void {
        self.header.serialize(self.buffer[0..PAGE_HEADER_SIZE]);
        self.writeChecksum();
    }

    /// Calculate and write checksum
    fn writeChecksum(self: *Page) void {
        const data_end = self.page_size - CHECKSUM_SIZE;
        const checksum = calculateChecksum(self.buffer[0..data_end]);
        @memcpy(self.buffer[data_end..self.page_size], std.mem.asBytes(&checksum));
    }

    /// Verify the page checksum
    pub fn verifyChecksum(self: *const Page) bool {
        const data_end = self.page_size - CHECKSUM_SIZE;
        const stored_checksum = std.mem.bytesToValue(u32, self.buffer[data_end..self.page_size][0..4]);
        const calculated = calculateChecksum(self.buffer[0..data_end]);
        return stored_checksum == calculated;
    }
};

/// Checksum size in bytes
pub const CHECKSUM_SIZE: usize = 4;

/// Calculate CRC32 checksum
pub fn calculateChecksum(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

// Tests

test "PageHeader init and serialize" {
    var buffer: [PAGE_HEADER_SIZE]u8 = undefined;

    const header = PageHeader.init(42, .leaf);
    header.serialize(&buffer);

    const loaded = try PageHeader.deserialize(&buffer);
    try std.testing.expectEqual(@as(PageId, 42), loaded.page_id);
    try std.testing.expectEqual(PageType.leaf, loaded.page_type);
    try std.testing.expectEqual(types.INVALID_LSN, loaded.lsn);
}

test "PageHeader with LSN" {
    var buffer: [PAGE_HEADER_SIZE]u8 = undefined;

    var header = PageHeader.init(100, .internal);
    header.lsn = 12345;
    header.serialize(&buffer);

    const loaded = try PageHeader.deserialize(&buffer);
    try std.testing.expectEqual(@as(PageId, 100), loaded.page_id);
    try std.testing.expectEqual(PageType.internal, loaded.page_type);
    try std.testing.expectEqual(@as(LSN, 12345), loaded.lsn);
}

test "PageHeader deserialize invalid type" {
    var buffer: [PAGE_HEADER_SIZE]u8 = [_]u8{0} ** PAGE_HEADER_SIZE;
    buffer[8] = 255; // Invalid page type

    const result = PageHeader.deserialize(&buffer);
    try std.testing.expectError(errors.Error.InvalidPageType, result);
}

test "Page init" {
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    var page = Page.init(&buffer, 1, .leaf);

    try std.testing.expectEqual(@as(PageId, 1), page.id());
    try std.testing.expectEqual(PageType.leaf, page.pageType());
    try std.testing.expect(page.isDirty());
}

test "Page load" {
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    // Create and serialize a page
    var page = Page.init(&buffer, 42, .internal);
    page.setLsn(999);
    page.serialize();

    // Load it back
    var loaded = try Page.load(&buffer);

    try std.testing.expectEqual(@as(PageId, 42), loaded.id());
    try std.testing.expectEqual(PageType.internal, loaded.pageType());
    try std.testing.expectEqual(@as(LSN, 999), loaded.lsn());
    try std.testing.expect(!loaded.isDirty());
}

test "Page checksum" {
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    var page = Page.init(&buffer, 1, .leaf);
    page.data[0] = 0xAB;
    page.data[1] = 0xCD;
    page.serialize();

    try std.testing.expect(page.verifyChecksum());

    // Corrupt the data
    buffer[100] ^= 0xFF;

    // Checksum should fail
    const corrupted = try Page.load(&buffer);
    try std.testing.expect(!corrupted.verifyChecksum());
}

test "Page dataSize" {
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;
    const page = Page.init(&buffer, 1, .leaf);

    const expected_size = DEFAULT_PAGE_SIZE - PAGE_HEADER_SIZE - CHECKSUM_SIZE;
    try std.testing.expectEqual(expected_size, page.dataSize());
}

test "calculateChecksum" {
    const data = "test data for checksum";
    const checksum = calculateChecksum(data);

    // Same data should produce same checksum
    try std.testing.expectEqual(checksum, calculateChecksum(data));

    // Different data should produce different checksum
    const checksum2 = calculateChecksum("different data");
    try std.testing.expect(checksum != checksum2);
}
