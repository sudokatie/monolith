const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const page_mod = @import("page.zig");
const file_mod = @import("file.zig");
const buffer_mod = @import("buffer.zig");

const PageId = types.PageId;
const PageType = types.PageType;
const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;
const PAGE_HEADER_SIZE = page_mod.PAGE_HEADER_SIZE;
const CHECKSUM_SIZE = page_mod.CHECKSUM_SIZE;
const BufferPool = buffer_mod.BufferPool;
const File = file_mod.File;

/// Overflow page header size (after page header)
/// Layout:
///   0-7:   next_page (PageId for chaining)
///   8-11:  data_length (u32, length of data in this page)
///   12-15: total_length (u32, total value length across all pages, only in first page)
pub const OVERFLOW_HEADER_SIZE: usize = 16;

/// Calculate usable data space per overflow page
pub fn dataPerPage(page_size: usize) usize {
    return page_size - PAGE_HEADER_SIZE - OVERFLOW_HEADER_SIZE - CHECKSUM_SIZE;
}

/// Threshold for when to use overflow pages (value won't fit in ~half a node)
pub fn overflowThreshold(page_size: usize) usize {
    // Use overflow if value is larger than 1/4 of usable page space
    // This leaves room for keys and other values in leaf nodes
    return (page_size - PAGE_HEADER_SIZE - CHECKSUM_SIZE) / 4;
}

/// Overflow page header
pub const OverflowHeader = struct {
    next_page: PageId,
    data_length: u32,
    total_length: u32,

    pub fn serialize(self: OverflowHeader, buffer: []u8) void {
        const offset = PAGE_HEADER_SIZE;
        @memcpy(buffer[offset .. offset + 8], std.mem.asBytes(&self.next_page));
        @memcpy(buffer[offset + 8 .. offset + 12], std.mem.asBytes(&self.data_length));
        @memcpy(buffer[offset + 12 .. offset + 16], std.mem.asBytes(&self.total_length));
    }

    pub fn deserialize(buffer: []const u8) OverflowHeader {
        const offset = PAGE_HEADER_SIZE;
        return .{
            .next_page = std.mem.bytesToValue(PageId, buffer[offset .. offset + 8]),
            .data_length = std.mem.bytesToValue(u32, buffer[offset + 8 .. offset + 12]),
            .total_length = std.mem.bytesToValue(u32, buffer[offset + 12 .. offset + 16]),
        };
    }
};

/// Overflow page manager
pub const OverflowManager = struct {
    allocator: std.mem.Allocator,
    buffer_pool: *BufferPool,
    file: *File,
    page_size: usize,
    /// Pages freed during operations (for recycling)
    freed_pages: std.ArrayListUnmanaged(PageId),

    pub fn init(allocator: std.mem.Allocator, buffer_pool: *BufferPool, file: *File) OverflowManager {
        return .{
            .allocator = allocator,
            .buffer_pool = buffer_pool,
            .file = file,
            .page_size = buffer_pool.page_size,
            .freed_pages = .{},
        };
    }

    pub fn deinit(self: *OverflowManager) void {
        self.freed_pages.deinit(self.allocator);
    }

    /// Take freed pages for recycling
    pub fn takeFreedPages(self: *OverflowManager) []PageId {
        return self.freed_pages.toOwnedSlice(self.allocator) catch &[_]PageId{};
    }

    /// Check if a value should use overflow pages
    pub fn needsOverflow(self: *OverflowManager, value_len: usize) bool {
        return value_len > overflowThreshold(self.page_size);
    }

    /// Store a large value in overflow pages
    /// Returns the page ID of the first overflow page
    pub fn storeValue(self: *OverflowManager, value: []const u8) !PageId {
        if (value.len == 0) return INVALID_PAGE_ID;

        const data_per_page = dataPerPage(self.page_size);
        const total_length: u32 = @intCast(value.len);

        var first_page_id: PageId = INVALID_PAGE_ID;
        var prev_page_id: PageId = INVALID_PAGE_ID;
        var offset: usize = 0;

        while (offset < value.len) {
            // Allocate new page
            const page_id = try self.file.allocatePage();
            const frame = try self.buffer_pool.newPage(page_id, .overflow);

            // Calculate how much data fits in this page
            const remaining = value.len - offset;
            const chunk_len = @min(remaining, data_per_page);

            // Write overflow header
            const header = OverflowHeader{
                .next_page = INVALID_PAGE_ID, // Will update if there's another page
                .data_length = @intCast(chunk_len),
                .total_length = if (offset == 0) total_length else 0,
            };
            header.serialize(frame.buffer);

            // Write data
            const data_start = PAGE_HEADER_SIZE + OVERFLOW_HEADER_SIZE;
            @memcpy(frame.buffer[data_start .. data_start + chunk_len], value[offset .. offset + chunk_len]);

            // Link previous page to this one
            if (prev_page_id != INVALID_PAGE_ID) {
                const prev_frame = try self.buffer_pool.fetchPage(prev_page_id);
                var prev_header = OverflowHeader.deserialize(prev_frame.buffer);
                prev_header.next_page = page_id;
                prev_header.serialize(prev_frame.buffer);
                self.buffer_pool.unpinPage(prev_page_id, true);
            }

            self.buffer_pool.unpinPage(page_id, true);

            if (first_page_id == INVALID_PAGE_ID) {
                first_page_id = page_id;
            }
            prev_page_id = page_id;
            offset += chunk_len;
        }

        return first_page_id;
    }

    /// Read a value from overflow pages
    /// Caller must free the returned slice
    pub fn readValue(self: *OverflowManager, first_page_id: PageId) ![]u8 {
        if (first_page_id == INVALID_PAGE_ID) {
            return self.allocator.alloc(u8, 0) catch return error.OutOfMemory;
        }

        // Read first page to get total length
        const first_frame = try self.buffer_pool.fetchPage(first_page_id);
        const first_header = OverflowHeader.deserialize(first_frame.buffer);
        self.buffer_pool.unpinPage(first_page_id, false);

        const total_length = first_header.total_length;
        if (total_length == 0) {
            return self.allocator.alloc(u8, 0) catch return error.OutOfMemory;
        }

        // Allocate buffer for full value
        const result = self.allocator.alloc(u8, total_length) catch {
            return error.OutOfMemory;
        };
        errdefer self.allocator.free(result);

        // Read all pages
        var current_page_id = first_page_id;
        var offset: usize = 0;

        while (current_page_id != INVALID_PAGE_ID and offset < total_length) {
            const frame = try self.buffer_pool.fetchPage(current_page_id);
            const header = OverflowHeader.deserialize(frame.buffer);

            const data_start = PAGE_HEADER_SIZE + OVERFLOW_HEADER_SIZE;
            const chunk_len = header.data_length;
            @memcpy(result[offset .. offset + chunk_len], frame.buffer[data_start .. data_start + chunk_len]);

            self.buffer_pool.unpinPage(current_page_id, false);

            offset += chunk_len;
            current_page_id = header.next_page;
        }

        return result;
    }

    /// Free overflow pages starting from first_page_id
    pub fn freeValue(self: *OverflowManager, first_page_id: PageId) !void {
        if (first_page_id == INVALID_PAGE_ID) return;

        var current_page_id = first_page_id;

        while (current_page_id != INVALID_PAGE_ID) {
            const frame = try self.buffer_pool.fetchPage(current_page_id);
            const header = OverflowHeader.deserialize(frame.buffer);
            const next_page = header.next_page;

            self.buffer_pool.unpinPage(current_page_id, false);

            // Mark page as freed
            try self.freed_pages.append(self.allocator, current_page_id);

            current_page_id = next_page;
        }
    }

    /// Update a value in overflow pages (may reuse or allocate new pages)
    pub fn updateValue(self: *OverflowManager, old_first_page: PageId, new_value: []const u8) !PageId {
        // Simple implementation: free old pages and store new value
        try self.freeValue(old_first_page);
        return self.storeValue(new_value);
    }
};

/// Marker value stored in leaf node to indicate overflow
/// First 8 bytes = OVERFLOW_MARKER, next 8 bytes = overflow page ID
pub const OVERFLOW_MARKER: u64 = 0x4F564552464C4F57; // "OVERFLOW" in ASCII
pub const OVERFLOW_MARKER_BYTES: [8]u8 = [_]u8{ 'O', 'V', 'E', 'R', 'F', 'L', 'O', 'W' };

/// Check if a value is an overflow pointer
pub fn isOverflowPointer(value: []const u8) bool {
    if (value.len != 16) return false;
    return std.mem.eql(u8, value[0..8], &OVERFLOW_MARKER_BYTES);
}

/// Extract overflow page ID from pointer
pub fn getOverflowPageId(value: []const u8) PageId {
    if (!isOverflowPointer(value)) return INVALID_PAGE_ID;
    return std.mem.bytesToValue(PageId, value[8..16]);
}

/// Create overflow pointer value
pub fn makeOverflowPointer(page_id: PageId) [16]u8 {
    var result: [16]u8 = undefined;
    @memcpy(result[0..8], &OVERFLOW_MARKER_BYTES);
    @memcpy(result[8..16], std.mem.asBytes(&page_id));
    return result;
}

// Tests

test "OverflowManager store and read" {
    const allocator = std.testing.allocator;
    const path = "/tmp/test_overflow.db";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var bp = try BufferPool.init(allocator, &file, 64);
    defer bp.deinit();

    var om = OverflowManager.init(allocator, &bp, &file);
    defer om.deinit();

    // Store a large value
    const large_value = try allocator.alloc(u8, 10000);
    defer allocator.free(large_value);
    @memset(large_value, 0xAB);
    large_value[0] = 'S';
    large_value[9999] = 'E';

    const page_id = try om.storeValue(large_value);
    try std.testing.expect(page_id != INVALID_PAGE_ID);

    // Read it back
    const read_value = try om.readValue(page_id);
    defer allocator.free(read_value);

    try std.testing.expectEqual(large_value.len, read_value.len);
    try std.testing.expectEqual(@as(u8, 'S'), read_value[0]);
    try std.testing.expectEqual(@as(u8, 'E'), read_value[9999]);
    try std.testing.expectEqual(@as(u8, 0xAB), read_value[5000]);
}

test "OverflowManager free pages" {
    const allocator = std.testing.allocator;
    const path = "/tmp/test_overflow_free.db";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var bp = try BufferPool.init(allocator, &file, 64);
    defer bp.deinit();

    var om = OverflowManager.init(allocator, &bp, &file);
    defer om.deinit();

    // Store a value that spans multiple pages
    const large_value = try allocator.alloc(u8, 20000);
    defer allocator.free(large_value);
    @memset(large_value, 0xCD);

    const page_id = try om.storeValue(large_value);

    // Free it
    try om.freeValue(page_id);

    // Check freed pages were tracked
    const freed = om.takeFreedPages();
    defer allocator.free(freed);
    try std.testing.expect(freed.len >= 5); // Should be at least 5 pages for 20KB
}

test "overflow pointer encoding" {
    const page_id: PageId = 12345;
    const pointer = makeOverflowPointer(page_id);

    try std.testing.expect(isOverflowPointer(&pointer));
    try std.testing.expectEqual(page_id, getOverflowPageId(&pointer));

    // Non-overflow value
    const normal: [16]u8 = [_]u8{'H'} ++ [_]u8{0} ** 15;
    try std.testing.expect(!isOverflowPointer(&normal));
}

test "overflowThreshold" {
    const threshold = overflowThreshold(4096);
    // Should be around 1000 bytes for 4KB pages
    try std.testing.expect(threshold > 500);
    try std.testing.expect(threshold < 2000);
}
