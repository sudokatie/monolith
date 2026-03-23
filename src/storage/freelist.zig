const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const page_mod = @import("page.zig");
const file_mod = @import("file.zig");

const PageId = types.PageId;
const PageType = types.PageType;
const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;
const PAGE_HEADER_SIZE = page_mod.PAGE_HEADER_SIZE;
const CHECKSUM_SIZE = page_mod.CHECKSUM_SIZE;

/// Free list page header size (after page header)
/// Layout:
///   0-7:   next_page (PageId for linked list)
///   8-15:  count (number of free page IDs in this page)
pub const FREELIST_HEADER_SIZE: usize = 16;

/// Calculate maximum free page IDs per freelist page
pub fn maxFreeIdsPerPage(page_size: usize) usize {
    const usable = page_size - PAGE_HEADER_SIZE - FREELIST_HEADER_SIZE - CHECKSUM_SIZE;
    return usable / @sizeOf(PageId);
}

/// Free list page structure
pub const FreelistPage = struct {
    /// Next free list page in chain (INVALID_PAGE_ID if last)
    next_page: PageId,
    /// Number of free page IDs stored in this page
    count: u64,
    /// Array of free page IDs
    free_ids: std.ArrayListUnmanaged(PageId),
    /// Page size
    page_size: usize,
    /// Allocator for the list
    allocator: std.mem.Allocator,

    /// Initialize an empty freelist page
    pub fn init(allocator: std.mem.Allocator, page_size: usize) FreelistPage {
        return .{
            .next_page = INVALID_PAGE_ID,
            .count = 0,
            .free_ids = .{},
            .page_size = page_size,
            .allocator = allocator,
        };
    }

    /// Free resources
    pub fn deinit(self: *FreelistPage) void {
        self.free_ids.deinit(self.allocator);
    }

    /// Serialize to buffer
    pub fn serialize(self: *const FreelistPage, buffer: []u8) void {
        const offset = PAGE_HEADER_SIZE;

        // Write next_page (8 bytes)
        @memcpy(buffer[offset .. offset + 8], std.mem.asBytes(&self.next_page));

        // Write count (8 bytes)
        @memcpy(buffer[offset + 8 .. offset + 16], std.mem.asBytes(&self.count));

        // Write free page IDs
        const ids_offset = offset + FREELIST_HEADER_SIZE;
        for (self.free_ids.items, 0..) |id, i| {
            const id_offset = ids_offset + i * @sizeOf(PageId);
            @memcpy(buffer[id_offset .. id_offset + 8], std.mem.asBytes(&id));
        }
    }

    /// Deserialize from buffer
    pub fn deserialize(self: *FreelistPage, buffer: []const u8) !void {
        const offset = PAGE_HEADER_SIZE;

        self.next_page = std.mem.bytesToValue(PageId, buffer[offset .. offset + 8]);
        self.count = std.mem.bytesToValue(u64, buffer[offset + 8 .. offset + 16]);

        // Read free page IDs
        self.free_ids.clearRetainingCapacity();
        try self.free_ids.ensureTotalCapacity(self.allocator, @intCast(self.count));

        const ids_offset = offset + FREELIST_HEADER_SIZE;
        for (0..@intCast(self.count)) |i| {
            const id_offset = ids_offset + i * @sizeOf(PageId);
            const id = std.mem.bytesToValue(PageId, buffer[id_offset .. id_offset + 8]);
            try self.free_ids.append(self.allocator, id);
        }
    }

    /// Check if this page is full
    pub fn isFull(self: *const FreelistPage) bool {
        return self.free_ids.items.len >= maxFreeIdsPerPage(self.page_size);
    }

    /// Check if this page is empty
    pub fn isEmpty(self: *const FreelistPage) bool {
        return self.free_ids.items.len == 0;
    }

    /// Add a free page ID (must check isFull first)
    pub fn addFreeId(self: *FreelistPage, page_id: PageId) !void {
        try self.free_ids.append(self.allocator, page_id);
        self.count = self.free_ids.items.len;
    }

    /// Pop a free page ID (returns null if empty)
    pub fn popFreeId(self: *FreelistPage) ?PageId {
        if (self.free_ids.items.len == 0) return null;
        const id = self.free_ids.pop();
        self.count = self.free_ids.items.len;
        return id;
    }
};

/// Free space manager
pub const Freelist = struct {
    /// File handle
    file: *file_mod.File,
    /// Head of freelist (first freelist page ID)
    head_page_id: PageId,
    /// In-memory cache of free page IDs for fast allocation
    cached_ids: std.ArrayListUnmanaged(PageId),
    /// Allocator
    allocator: std.mem.Allocator,
    /// Page size
    page_size: usize,
    /// Total free pages count
    free_count: u64,

    /// Initialize freelist manager
    pub fn init(allocator: std.mem.Allocator, file: *file_mod.File, head_page_id: PageId) !Freelist {
        var freelist = Freelist{
            .file = file,
            .head_page_id = head_page_id,
            .cached_ids = .{},
            .allocator = allocator,
            .page_size = file.page_size,
            .free_count = 0,
        };

        // Load existing free list if head is valid
        if (head_page_id != INVALID_PAGE_ID) {
            try freelist.loadFromDisk();
        }

        return freelist;
    }

    /// Free resources
    pub fn deinit(self: *Freelist) void {
        self.cached_ids.deinit(self.allocator);
    }

    /// Load all free page IDs from disk into memory cache
    fn loadFromDisk(self: *Freelist) !void {
        self.cached_ids.clearRetainingCapacity();
        self.free_count = 0;

        const buffer = try self.allocator.alloc(u8, self.page_size);
        defer self.allocator.free(buffer);

        var freelist_page = FreelistPage.init(self.allocator, self.page_size);
        defer freelist_page.deinit();

        var current_page = self.head_page_id;

        while (current_page != INVALID_PAGE_ID) {
            try self.file.readPage(current_page, buffer);

            // Verify page type
            const loaded = try page_mod.Page.load(buffer);
            if (loaded.pageType() != .freelist) {
                return errors.Error.Corrupted;
            }

            try freelist_page.deserialize(buffer);

            // Add all IDs to cache
            for (freelist_page.free_ids.items) |id| {
                try self.cached_ids.append(self.allocator, id);
            }

            self.free_count += freelist_page.count;
            current_page = freelist_page.next_page;
        }
    }

    /// Allocate a free page (returns INVALID_PAGE_ID if none available)
    pub fn allocate(self: *Freelist) ?PageId {
        if (self.cached_ids.items.len == 0) {
            return null;
        }

        const id = self.cached_ids.pop();
        self.free_count -= 1;
        return id;
    }

    /// Return a page to the free list
    pub fn free(self: *Freelist, page_id: PageId) !void {
        try self.cached_ids.append(self.allocator, page_id);
        self.free_count += 1;
    }

    /// Persist free list to disk
    pub fn persist(self: *Freelist) !void {
        if (self.cached_ids.items.len == 0) {
            // No free pages - clear head
            self.head_page_id = INVALID_PAGE_ID;
            return;
        }

        const buffer = try self.allocator.alloc(u8, self.page_size);
        defer self.allocator.free(buffer);

        var freelist_page = FreelistPage.init(self.allocator, self.page_size);
        defer freelist_page.deinit();

        const max_per_page = maxFreeIdsPerPage(self.page_size);
        const pages_needed: usize = (self.cached_ids.items.len + max_per_page - 1) / max_per_page;

        // Allocate freelist pages from file if needed
        const freelist_pages = try self.allocator.alloc(PageId, pages_needed);
        defer self.allocator.free(freelist_pages);

        // For simplicity, we'll allocate new pages at end of file for freelist
        // In a real implementation, we'd reuse existing freelist pages
        for (freelist_pages, 0..) |*fp, i| {
            if (self.head_page_id != INVALID_PAGE_ID and i == 0) {
                fp.* = self.head_page_id; // Reuse existing head
            } else {
                fp.* = try self.file.allocatePage();
            }
        }

        self.head_page_id = freelist_pages[0];

        // Write free IDs across pages
        var idx: usize = 0;
        for (freelist_pages, 0..) |page_id, page_idx| {
            freelist_page.free_ids.clearRetainingCapacity();
            freelist_page.count = 0;

            // Fill this page
            const end = @min(idx + max_per_page, self.cached_ids.items.len);
            while (idx < end) : (idx += 1) {
                try freelist_page.addFreeId(self.cached_ids.items[idx]);
            }

            // Set next pointer
            if (page_idx + 1 < freelist_pages.len) {
                freelist_page.next_page = freelist_pages[page_idx + 1];
            } else {
                freelist_page.next_page = INVALID_PAGE_ID;
            }

            // Write to buffer
            @memset(buffer, 0);
            var page = page_mod.Page.init(buffer, page_id, .freelist);
            freelist_page.serialize(buffer);
            page.serialize();

            try self.file.writePage(page_id, buffer);
        }
    }

    /// Get total count of free pages
    pub fn freePageCount(self: *const Freelist) u64 {
        return self.free_count;
    }

    /// Get head page ID
    pub fn getHeadPageId(self: *const Freelist) PageId {
        return self.head_page_id;
    }
};

// Tests

test "maxFreeIdsPerPage" {
    // 4096 - 24 (page header) - 16 (freelist header) - 4 (checksum) = 4052
    // 4052 / 8 (PageId size) = 506
    const max = maxFreeIdsPerPage(DEFAULT_PAGE_SIZE);
    try std.testing.expectEqual(@as(usize, 506), max);
}

test "FreelistPage init and serialize" {
    const allocator = std.testing.allocator;

    var flp = FreelistPage.init(allocator, DEFAULT_PAGE_SIZE);
    defer flp.deinit();

    try flp.addFreeId(10);
    try flp.addFreeId(20);
    try flp.addFreeId(30);

    try std.testing.expectEqual(@as(u64, 3), flp.count);
    try std.testing.expect(!flp.isEmpty());
    try std.testing.expect(!flp.isFull());
}

test "FreelistPage serialize and deserialize" {
    const allocator = std.testing.allocator;

    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&buffer, 0);

    // Create and populate
    var flp1 = FreelistPage.init(allocator, DEFAULT_PAGE_SIZE);
    defer flp1.deinit();

    flp1.next_page = 42;
    try flp1.addFreeId(100);
    try flp1.addFreeId(200);
    try flp1.addFreeId(300);

    flp1.serialize(&buffer);

    // Deserialize into new struct
    var flp2 = FreelistPage.init(allocator, DEFAULT_PAGE_SIZE);
    defer flp2.deinit();

    try flp2.deserialize(&buffer);

    try std.testing.expectEqual(@as(PageId, 42), flp2.next_page);
    try std.testing.expectEqual(@as(u64, 3), flp2.count);
    try std.testing.expectEqual(@as(PageId, 100), flp2.free_ids.items[0]);
    try std.testing.expectEqual(@as(PageId, 200), flp2.free_ids.items[1]);
    try std.testing.expectEqual(@as(PageId, 300), flp2.free_ids.items[2]);
}

test "FreelistPage pop" {
    const allocator = std.testing.allocator;

    var flp = FreelistPage.init(allocator, DEFAULT_PAGE_SIZE);
    defer flp.deinit();

    try flp.addFreeId(10);
    try flp.addFreeId(20);

    const id1 = flp.popFreeId();
    try std.testing.expectEqual(@as(?PageId, 20), id1);

    const id2 = flp.popFreeId();
    try std.testing.expectEqual(@as(?PageId, 10), id2);

    const id3 = flp.popFreeId();
    try std.testing.expectEqual(@as(?PageId, null), id3);

    try std.testing.expect(flp.isEmpty());
}

test "Freelist allocate and free" {
    const allocator = std.testing.allocator;
    const path = "test_freelist.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var freelist = try Freelist.init(allocator, &file, INVALID_PAGE_ID);
    defer freelist.deinit();

    // Initially empty
    try std.testing.expectEqual(@as(?PageId, null), freelist.allocate());
    try std.testing.expectEqual(@as(u64, 0), freelist.freePageCount());

    // Add some free pages
    try freelist.free(10);
    try freelist.free(20);
    try freelist.free(30);

    try std.testing.expectEqual(@as(u64, 3), freelist.freePageCount());

    // Allocate them back (LIFO order)
    try std.testing.expectEqual(@as(?PageId, 30), freelist.allocate());
    try std.testing.expectEqual(@as(?PageId, 20), freelist.allocate());
    try std.testing.expectEqual(@as(?PageId, 10), freelist.allocate());
    try std.testing.expectEqual(@as(?PageId, null), freelist.allocate());
}

test "Freelist persist and reload" {
    const allocator = std.testing.allocator;
    const path = "test_freelist_persist.db";

    std.fs.cwd().deleteFile(path) catch {};

    var head_page_id: PageId = INVALID_PAGE_ID;

    // Create and persist
    {
        var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
        defer file.close();

        // Reserve pages 0 and 1 for meta, start freelist at 2
        _ = try file.allocatePage(); // 0
        _ = try file.allocatePage(); // 1

        var freelist = try Freelist.init(allocator, &file, INVALID_PAGE_ID);
        defer freelist.deinit();

        // Add free pages
        try freelist.free(100);
        try freelist.free(200);
        try freelist.free(300);

        try freelist.persist();
        try file.sync();

        head_page_id = freelist.getHeadPageId();
    }

    // Reload and verify
    {
        var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
        defer {
            file.close();
            std.fs.cwd().deleteFile(path) catch {};
        }

        var freelist = try Freelist.init(allocator, &file, head_page_id);
        defer freelist.deinit();

        try std.testing.expectEqual(@as(u64, 3), freelist.freePageCount());

        // Order might differ due to reload, but all should be there
        var found = [3]bool{ false, false, false };
        for (freelist.cached_ids.items) |id| {
            if (id == 100) found[0] = true;
            if (id == 200) found[1] = true;
            if (id == 300) found[2] = true;
        }
        try std.testing.expect(found[0] and found[1] and found[2]);
    }
}

test "Freelist many pages" {
    const allocator = std.testing.allocator;
    const path = "test_freelist_many.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var freelist = try Freelist.init(allocator, &file, INVALID_PAGE_ID);
    defer freelist.deinit();

    // Add more pages than fit in one freelist page
    const count: usize = 600; // More than 506
    for (0..count) |i| {
        try freelist.free(@intCast(i + 1000));
    }

    try std.testing.expectEqual(@as(u64, count), freelist.freePageCount());

    // Persist and reload
    try freelist.persist();

    // Allocate all back
    for (0..count) |_| {
        const id = freelist.allocate();
        try std.testing.expect(id != null);
    }

    try std.testing.expectEqual(@as(?PageId, null), freelist.allocate());
}
