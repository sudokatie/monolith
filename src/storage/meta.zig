const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const page_mod = @import("page.zig");
const file_mod = @import("file.zig");

const PageId = types.PageId;
const TransactionId = types.TransactionId;
const LSN = types.LSN;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;
const DB_MAGIC = types.DB_MAGIC;
const DB_VERSION = types.DB_VERSION;
const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
const INVALID_TXN_ID = types.INVALID_TXN_ID;
const INVALID_LSN = types.INVALID_LSN;

/// Meta page size (header portion)
pub const META_SIZE: usize = 76;

/// Meta page 0 (primary)
pub const META_PAGE_0: PageId = 0;

/// Meta page 1 (backup/alternate)
pub const META_PAGE_1: PageId = 1;

/// Database metadata structure
/// Layout per SPECS.md:
///   0-3:   Magic number (u32)
///   4-7:   Version (u32)
///   8-11:  Page size (u32)
///   12-19: Root page ID (u64)
///   20-27: Free list page ID (u64)
///   28-35: Total pages (u64)
///   36-43: Last transaction ID (u64)
///   44-51: Checkpoint LSN (u64)
///   52-71: Reserved (20 bytes)
///   72-75: Checksum (u32)
pub const Meta = struct {
    magic: u32,
    version: u32,
    page_size: u32,
    root_page_id: PageId,
    freelist_page_id: PageId,
    total_pages: u64,
    last_txn_id: TransactionId,
    checkpoint_lsn: LSN,

    /// Create a new meta with defaults
    pub fn init(page_size: u32) Meta {
        return .{
            .magic = DB_MAGIC,
            .version = DB_VERSION,
            .page_size = page_size,
            .root_page_id = INVALID_PAGE_ID,
            .freelist_page_id = INVALID_PAGE_ID,
            .total_pages = 2, // meta page 0 and 1
            .last_txn_id = INVALID_TXN_ID,
            .checkpoint_lsn = INVALID_LSN,
        };
    }

    /// Serialize meta to buffer
    pub fn serialize(self: *const Meta, buffer: []u8) void {
        std.debug.assert(buffer.len >= META_SIZE);

        // Magic (4 bytes)
        @memcpy(buffer[0..4], std.mem.asBytes(&self.magic));

        // Version (4 bytes)
        @memcpy(buffer[4..8], std.mem.asBytes(&self.version));

        // Page size (4 bytes)
        @memcpy(buffer[8..12], std.mem.asBytes(&self.page_size));

        // Root page ID (8 bytes)
        @memcpy(buffer[12..20], std.mem.asBytes(&self.root_page_id));

        // Free list page ID (8 bytes)
        @memcpy(buffer[20..28], std.mem.asBytes(&self.freelist_page_id));

        // Total pages (8 bytes)
        @memcpy(buffer[28..36], std.mem.asBytes(&self.total_pages));

        // Last transaction ID (8 bytes)
        @memcpy(buffer[36..44], std.mem.asBytes(&self.last_txn_id));

        // Checkpoint LSN (8 bytes)
        @memcpy(buffer[44..52], std.mem.asBytes(&self.checkpoint_lsn));

        // Reserved (20 bytes) - zero fill
        @memset(buffer[52..72], 0);

        // Checksum (4 bytes)
        const checksum = calculateMetaChecksum(buffer[0..72]);
        @memcpy(buffer[72..76], std.mem.asBytes(&checksum));
    }

    /// Deserialize meta from buffer
    pub fn deserialize(buffer: []const u8) errors.Error!Meta {
        if (buffer.len < META_SIZE) {
            return errors.Error.Corrupted;
        }

        // Verify checksum first
        const stored_checksum = std.mem.bytesToValue(u32, buffer[72..76]);
        const calculated = calculateMetaChecksum(buffer[0..72]);
        if (stored_checksum != calculated) {
            return errors.Error.ChecksumMismatch;
        }

        // Read fields
        const magic = std.mem.bytesToValue(u32, buffer[0..4]);
        if (magic != DB_MAGIC) {
            return errors.Error.InvalidMagic;
        }

        const version = std.mem.bytesToValue(u32, buffer[4..8]);
        if (version != DB_VERSION) {
            return errors.Error.VersionMismatch;
        }

        return .{
            .magic = magic,
            .version = version,
            .page_size = std.mem.bytesToValue(u32, buffer[8..12]),
            .root_page_id = std.mem.bytesToValue(PageId, buffer[12..20]),
            .freelist_page_id = std.mem.bytesToValue(PageId, buffer[20..28]),
            .total_pages = std.mem.bytesToValue(u64, buffer[28..36]),
            .last_txn_id = std.mem.bytesToValue(TransactionId, buffer[36..44]),
            .checkpoint_lsn = std.mem.bytesToValue(LSN, buffer[44..52]),
        };
    }

    /// Check if this meta is valid (has magic, version)
    pub fn isValid(buffer: []const u8) bool {
        if (buffer.len < META_SIZE) return false;

        const magic = std.mem.bytesToValue(u32, buffer[0..4]);
        if (magic != DB_MAGIC) return false;

        const stored_checksum = std.mem.bytesToValue(u32, buffer[72..76]);
        const calculated = calculateMetaChecksum(buffer[0..72]);
        return stored_checksum == calculated;
    }
};

/// Calculate CRC32 checksum for meta data
fn calculateMetaChecksum(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

/// Meta page manager for double-buffered atomic updates
pub const MetaManager = struct {
    /// Database file handle
    file: *file_mod.File,
    /// Current meta data
    meta: Meta,
    /// Which page is current (0 or 1)
    current_page: PageId,
    /// Page size
    page_size: usize,
    /// Allocator for buffers
    allocator: std.mem.Allocator,

    /// Initialize meta manager with existing file
    /// Reads and validates meta pages, picks the valid one
    pub fn init(allocator: std.mem.Allocator, file: *file_mod.File) !MetaManager {
        const page_size = file.page_size;

        // If file is empty, create new database
        if (file.pageCount() == 0) {
            var manager = MetaManager{
                .file = file,
                .meta = Meta.init(@intCast(page_size)),
                .current_page = META_PAGE_0,
                .page_size = page_size,
                .allocator = allocator,
            };

            // Write initial meta pages
            try manager.writeMetaPage(META_PAGE_0);
            try manager.writeMetaPage(META_PAGE_1);
            try file.sync();

            return manager;
        }

        // Read both meta pages and pick the valid one
        const buffer0 = try allocator.alloc(u8, page_size);
        defer allocator.free(buffer0);
        const buffer1 = try allocator.alloc(u8, page_size);
        defer allocator.free(buffer1);

        var meta0_valid = false;
        var meta1_valid = false;
        var meta0: Meta = undefined;
        var meta1: Meta = undefined;

        // Try to read meta page 0
        if (file.pageCount() > META_PAGE_0) {
            file.readPage(META_PAGE_0, buffer0) catch {};
            if (Meta.isValid(buffer0)) {
                if (Meta.deserialize(buffer0)) |m| {
                    meta0 = m;
                    meta0_valid = true;
                } else |_| {}
            }
        }

        // Try to read meta page 1
        if (file.pageCount() > META_PAGE_1) {
            file.readPage(META_PAGE_1, buffer1) catch {};
            if (Meta.isValid(buffer1)) {
                if (Meta.deserialize(buffer1)) |m| {
                    meta1 = m;
                    meta1_valid = true;
                } else |_| {}
            }
        }

        // Pick the valid meta page
        if (meta0_valid and meta1_valid) {
            // Both valid - pick one with higher transaction ID
            if (meta1.last_txn_id > meta0.last_txn_id) {
                return .{
                    .file = file,
                    .meta = meta1,
                    .current_page = META_PAGE_1,
                    .page_size = page_size,
                    .allocator = allocator,
                };
            } else {
                return .{
                    .file = file,
                    .meta = meta0,
                    .current_page = META_PAGE_0,
                    .page_size = page_size,
                    .allocator = allocator,
                };
            }
        } else if (meta0_valid) {
            return .{
                .file = file,
                .meta = meta0,
                .current_page = META_PAGE_0,
                .page_size = page_size,
                .allocator = allocator,
            };
        } else if (meta1_valid) {
            return .{
                .file = file,
                .meta = meta1,
                .current_page = META_PAGE_1,
                .page_size = page_size,
                .allocator = allocator,
            };
        } else {
            // Neither valid - corrupted database
            return errors.Error.Corrupted;
        }
    }

    /// Write current meta to a specific page
    fn writeMetaPage(self: *MetaManager, page_id: PageId) !void {
        const buffer = try self.allocator.alloc(u8, self.page_size);
        defer self.allocator.free(buffer);

        @memset(buffer, 0);
        self.meta.serialize(buffer);

        try self.file.writePage(page_id, buffer);
    }

    /// Update meta atomically using double-buffering
    /// Writes to alternate page, syncs, then updates current_page
    pub fn update(self: *MetaManager, new_meta: Meta) !void {
        // Write to alternate page
        const alternate_page = if (self.current_page == META_PAGE_0) META_PAGE_1 else META_PAGE_0;

        self.meta = new_meta;
        try self.writeMetaPage(alternate_page);
        try self.file.sync();

        // Now the alternate page is durable - update current pointer
        self.current_page = alternate_page;
    }

    /// Get current metadata
    pub fn getMeta(self: *const MetaManager) *const Meta {
        return &self.meta;
    }

    /// Get mutable metadata for modification before update
    pub fn getMetaMut(self: *MetaManager) *Meta {
        return &self.meta;
    }

    /// Verify page size matches
    pub fn verifyPageSize(self: *const MetaManager, expected: usize) errors.Error!void {
        if (self.meta.page_size != expected) {
            return errors.Error.PageSizeMismatch;
        }
    }
};

// Tests

test "Meta init and serialize" {
    var buffer: [META_SIZE]u8 = undefined;

    const meta = Meta.init(4096);
    meta.serialize(&buffer);

    const loaded = try Meta.deserialize(&buffer);

    try std.testing.expectEqual(DB_MAGIC, loaded.magic);
    try std.testing.expectEqual(DB_VERSION, loaded.version);
    try std.testing.expectEqual(@as(u32, 4096), loaded.page_size);
    try std.testing.expectEqual(INVALID_PAGE_ID, loaded.root_page_id);
}

test "Meta with values" {
    var buffer: [META_SIZE]u8 = undefined;

    var meta = Meta.init(4096);
    meta.root_page_id = 42;
    meta.freelist_page_id = 10;
    meta.total_pages = 100;
    meta.last_txn_id = 999;
    meta.checkpoint_lsn = 12345;

    meta.serialize(&buffer);

    const loaded = try Meta.deserialize(&buffer);

    try std.testing.expectEqual(@as(PageId, 42), loaded.root_page_id);
    try std.testing.expectEqual(@as(PageId, 10), loaded.freelist_page_id);
    try std.testing.expectEqual(@as(u64, 100), loaded.total_pages);
    try std.testing.expectEqual(@as(TransactionId, 999), loaded.last_txn_id);
    try std.testing.expectEqual(@as(LSN, 12345), loaded.checkpoint_lsn);
}

test "Meta checksum validation" {
    var buffer: [META_SIZE]u8 = undefined;

    const meta = Meta.init(4096);
    meta.serialize(&buffer);

    // Verify valid
    try std.testing.expect(Meta.isValid(&buffer));

    // Corrupt data
    buffer[10] ^= 0xFF;

    // Should now be invalid
    try std.testing.expect(!Meta.isValid(&buffer));
}

test "Meta invalid magic" {
    var buffer: [META_SIZE]u8 = undefined;
    @memset(&buffer, 0);

    // Write wrong magic
    const bad_magic: u32 = 0xDEADBEEF;
    @memcpy(buffer[0..4], std.mem.asBytes(&bad_magic));

    // Calculate and write checksum for the bad data
    const checksum = calculateMetaChecksum(buffer[0..72]);
    @memcpy(buffer[72..76], std.mem.asBytes(&checksum));

    const result = Meta.deserialize(&buffer);
    try std.testing.expectError(errors.Error.InvalidMagic, result);
}

test "MetaManager new database" {
    const allocator = std.testing.allocator;
    const path = "test_meta_new.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var manager = try MetaManager.init(allocator, &file);

    try std.testing.expectEqual(DB_MAGIC, manager.getMeta().magic);
    try std.testing.expectEqual(@as(u64, 2), manager.getMeta().total_pages);
}

test "MetaManager update" {
    const allocator = std.testing.allocator;
    const path = "test_meta_update.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var manager = try MetaManager.init(allocator, &file);

    // Initial state - current_page should be 0
    try std.testing.expectEqual(META_PAGE_0, manager.current_page);

    // Update with new values
    var new_meta = manager.getMeta().*;
    new_meta.root_page_id = 100;
    new_meta.last_txn_id = 42;

    try manager.update(new_meta);

    // Should have switched to alternate page
    try std.testing.expectEqual(META_PAGE_1, manager.current_page);
    try std.testing.expectEqual(@as(PageId, 100), manager.getMeta().root_page_id);
    try std.testing.expectEqual(@as(TransactionId, 42), manager.getMeta().last_txn_id);
}

test "MetaManager recovery" {
    const allocator = std.testing.allocator;
    const path = "test_meta_recovery.db";

    std.fs.cwd().deleteFile(path) catch {};

    // Create and write
    {
        var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
        defer file.close();

        var manager = try MetaManager.init(allocator, &file);

        // Update twice to get data on both pages
        var meta1 = manager.getMeta().*;
        meta1.last_txn_id = 1;
        try manager.update(meta1);

        var meta2 = manager.getMeta().*;
        meta2.last_txn_id = 2;
        try manager.update(meta2);
    }

    // Reopen and verify recovery picks the right page
    {
        var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
        defer {
            file.close();
            std.fs.cwd().deleteFile(path) catch {};
        }

        var manager = try MetaManager.init(allocator, &file);

        // Should have recovered with highest txn_id
        try std.testing.expectEqual(@as(TransactionId, 2), manager.getMeta().last_txn_id);
    }
}

test "MetaManager page size mismatch" {
    const allocator = std.testing.allocator;
    const path = "test_meta_pagesize.db";

    std.fs.cwd().deleteFile(path) catch {};

    // Create with one page size
    {
        var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
        defer file.close();

        _ = try MetaManager.init(allocator, &file);
    }

    // Reopen with same page size
    {
        var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
        defer {
            file.close();
            std.fs.cwd().deleteFile(path) catch {};
        }

        var manager = try MetaManager.init(allocator, &file);

        // Should pass
        try manager.verifyPageSize(DEFAULT_PAGE_SIZE);

        // Should fail with different size
        try std.testing.expectError(errors.Error.PageSizeMismatch, manager.verifyPageSize(8192));
    }
}
