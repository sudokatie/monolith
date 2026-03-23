const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const page_mod = @import("page.zig");
const file_mod = @import("file.zig");

const PageId = types.PageId;
const PageType = types.PageType;
const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;
const Page = page_mod.Page;

/// Buffer frame - holds a cached page
pub const BufferFrame = struct {
    /// Page ID (INVALID_PAGE_ID if frame is empty)
    page_id: PageId,
    /// Page buffer
    buffer: []u8,
    /// Pin count (0 = unpinned)
    pin_count: u32,
    /// Dirty flag
    dirty: bool,
    /// Usage count for LRU-K approximation (clock algorithm)
    reference_bit: bool,
};

/// Buffer pool manager
pub const BufferPool = struct {
    /// Array of buffer frames
    frames: []BufferFrame,
    /// Page ID to frame index mapping
    page_table: std.AutoHashMap(PageId, usize),
    /// File handle for reading/writing pages
    file: *file_mod.File,
    /// Number of frames in pool
    pool_size: usize,
    /// Page size
    page_size: usize,
    /// Allocator
    allocator: std.mem.Allocator,
    /// Clock hand for eviction
    clock_hand: usize,

    /// Initialize buffer pool
    pub fn init(allocator: std.mem.Allocator, file: *file_mod.File, pool_size: usize) !BufferPool {
        const page_size = file.page_size;

        // Allocate frames
        const frames = try allocator.alloc(BufferFrame, pool_size);
        errdefer allocator.free(frames);

        // Initialize each frame
        for (frames) |*frame| {
            frame.buffer = try allocator.alloc(u8, page_size);
            frame.page_id = INVALID_PAGE_ID;
            frame.pin_count = 0;
            frame.dirty = false;
            frame.reference_bit = false;
        }

        return .{
            .frames = frames,
            .page_table = std.AutoHashMap(PageId, usize).init(allocator),
            .file = file,
            .pool_size = pool_size,
            .page_size = page_size,
            .allocator = allocator,
            .clock_hand = 0,
        };
    }

    /// Free resources
    pub fn deinit(self: *BufferPool) void {
        // Flush all dirty pages first
        self.flushAll() catch {};

        for (self.frames) |*frame| {
            self.allocator.free(frame.buffer);
        }
        self.allocator.free(self.frames);
        self.page_table.deinit();
    }

    /// Fetch a page, loading from disk if not cached
    /// Returns a pinned buffer - caller must unpin when done
    pub fn fetchPage(self: *BufferPool, page_id: PageId) !*BufferFrame {
        // Check if already in cache
        if (self.page_table.get(page_id)) |frame_idx| {
            const frame = &self.frames[frame_idx];
            frame.pin_count += 1;
            frame.reference_bit = true;
            return frame;
        }

        // Find a victim frame
        const frame_idx = try self.findVictim();
        const frame = &self.frames[frame_idx];

        // If victim has a page, evict it
        if (frame.page_id != INVALID_PAGE_ID) {
            try self.evictFrame(frame_idx);
        }

        // Load the new page
        try self.file.readPage(page_id, frame.buffer);

        // Update frame
        frame.page_id = page_id;
        frame.pin_count = 1;
        frame.dirty = false;
        frame.reference_bit = true;

        // Update page table
        try self.page_table.put(page_id, frame_idx);

        return frame;
    }

    /// Create a new page and pin it
    pub fn newPage(self: *BufferPool, page_id: PageId, page_type: PageType) !*BufferFrame {
        // Find a victim frame
        const frame_idx = try self.findVictim();
        const frame = &self.frames[frame_idx];

        // If victim has a page, evict it
        if (frame.page_id != INVALID_PAGE_ID) {
            try self.evictFrame(frame_idx);
        }

        // Initialize new page
        @memset(frame.buffer, 0);
        _ = Page.init(frame.buffer, page_id, page_type);

        // Update frame
        frame.page_id = page_id;
        frame.pin_count = 1;
        frame.dirty = true;
        frame.reference_bit = true;

        // Update page table
        try self.page_table.put(page_id, frame_idx);

        return frame;
    }

    /// Unpin a page (decrement pin count)
    pub fn unpinPage(self: *BufferPool, page_id: PageId, is_dirty: bool) void {
        if (self.page_table.get(page_id)) |frame_idx| {
            const frame = &self.frames[frame_idx];
            if (frame.pin_count > 0) {
                frame.pin_count -= 1;
            }
            if (is_dirty) {
                frame.dirty = true;
            }
        }
    }

    /// Mark a page as dirty
    pub fn markDirty(self: *BufferPool, page_id: PageId) void {
        if (self.page_table.get(page_id)) |frame_idx| {
            self.frames[frame_idx].dirty = true;
        }
    }

    /// Flush a specific page to disk
    pub fn flushPage(self: *BufferPool, page_id: PageId) !void {
        if (self.page_table.get(page_id)) |frame_idx| {
            const frame = &self.frames[frame_idx];
            if (frame.dirty) {
                // Serialize page (write header and checksum)
                var page = try Page.load(frame.buffer);
                page.serialize();

                try self.file.writePage(page_id, frame.buffer);
                frame.dirty = false;
            }
        }
    }

    /// Flush all dirty pages to disk
    pub fn flushAll(self: *BufferPool) !void {
        for (self.frames) |*frame| {
            if (frame.page_id != INVALID_PAGE_ID and frame.dirty) {
                var page = try Page.load(frame.buffer);
                page.serialize();

                try self.file.writePage(frame.page_id, frame.buffer);
                frame.dirty = false;
            }
        }
        try self.file.sync();
    }

    /// Find an empty or evictable frame using clock algorithm
    fn findVictim(self: *BufferPool) !usize {
        // First pass: look for empty frames
        for (self.frames, 0..) |*frame, i| {
            if (frame.page_id == INVALID_PAGE_ID) {
                return i;
            }
        }

        // Clock sweep to find victim
        var iterations: usize = 0;
        const max_iterations = self.pool_size * 2;

        while (iterations < max_iterations) : (iterations += 1) {
            const frame = &self.frames[self.clock_hand];

            if (frame.pin_count == 0) {
                if (frame.reference_bit) {
                    // Give second chance
                    frame.reference_bit = false;
                } else {
                    // Found victim
                    const victim = self.clock_hand;
                    self.clock_hand = (self.clock_hand + 1) % self.pool_size;
                    return victim;
                }
            }

            self.clock_hand = (self.clock_hand + 1) % self.pool_size;
        }

        // All pages are pinned
        return errors.Error.BufferPoolFull;
    }

    /// Evict a frame (flush if dirty, remove from page table)
    fn evictFrame(self: *BufferPool, frame_idx: usize) !void {
        const frame = &self.frames[frame_idx];

        if (frame.dirty) {
            var page = try Page.load(frame.buffer);
            page.serialize();

            try self.file.writePage(frame.page_id, frame.buffer);
        }

        _ = self.page_table.remove(frame.page_id);
        frame.page_id = INVALID_PAGE_ID;
        frame.pin_count = 0;
        frame.dirty = false;
        frame.reference_bit = false;
    }

    /// Get statistics
    pub fn stats(self: *const BufferPool) BufferPoolStats {
        var pinned: usize = 0;
        var dirty: usize = 0;
        var used: usize = 0;

        for (self.frames) |frame| {
            if (frame.page_id != INVALID_PAGE_ID) {
                used += 1;
                if (frame.pin_count > 0) pinned += 1;
                if (frame.dirty) dirty += 1;
            }
        }

        return .{
            .pool_size = self.pool_size,
            .used_frames = used,
            .pinned_frames = pinned,
            .dirty_frames = dirty,
        };
    }
};

/// Buffer pool statistics
pub const BufferPoolStats = struct {
    pool_size: usize,
    used_frames: usize,
    pinned_frames: usize,
    dirty_frames: usize,
};

// Tests

test "BufferPool init and deinit" {
    const allocator = std.testing.allocator;
    const path = "test_buffer_init.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var pool = try BufferPool.init(allocator, &file, 10);
    defer pool.deinit();

    const s = pool.stats();
    try std.testing.expectEqual(@as(usize, 10), s.pool_size);
    try std.testing.expectEqual(@as(usize, 0), s.used_frames);
}

test "BufferPool newPage" {
    const allocator = std.testing.allocator;
    const path = "test_buffer_newpage.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Allocate pages in file first
    _ = try file.allocatePage();

    var pool = try BufferPool.init(allocator, &file, 10);
    defer pool.deinit();

    const frame = try pool.newPage(0, .leaf);

    try std.testing.expectEqual(@as(PageId, 0), frame.page_id);
    try std.testing.expectEqual(@as(u32, 1), frame.pin_count);
    try std.testing.expect(frame.dirty);

    // Check stats
    const s = pool.stats();
    try std.testing.expectEqual(@as(usize, 1), s.used_frames);
    try std.testing.expectEqual(@as(usize, 1), s.pinned_frames);
    try std.testing.expectEqual(@as(usize, 1), s.dirty_frames);

    pool.unpinPage(0, false);
}

test "BufferPool fetch and cache" {
    const allocator = std.testing.allocator;
    const path = "test_buffer_fetch.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var pool = try BufferPool.init(allocator, &file, 10);
    defer pool.deinit();

    // Create and flush a page
    const frame1 = try pool.newPage(0, .leaf);
    frame1.buffer[100] = 0xAB;
    pool.unpinPage(0, true);
    try pool.flushPage(0);

    // Fetch same page - should come from cache
    const frame2 = try pool.fetchPage(0);
    try std.testing.expectEqual(@as(u8, 0xAB), frame2.buffer[100]);
    pool.unpinPage(0, false);
}

test "BufferPool eviction" {
    const allocator = std.testing.allocator;
    const path = "test_buffer_evict.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Small pool to force eviction
    var pool = try BufferPool.init(allocator, &file, 3);
    defer pool.deinit();

    // Allocate pages
    for (0..5) |_| {
        _ = try file.allocatePage();
    }

    // Create more pages than pool size
    for (0..3) |i| {
        const frame = try pool.newPage(@intCast(i), .leaf);
        frame.buffer[0] = @intCast(i);
        pool.unpinPage(@intCast(i), true);
    }

    // All 3 frames used
    try std.testing.expectEqual(@as(usize, 3), pool.stats().used_frames);

    // Create a 4th page - should evict one
    const frame4 = try pool.newPage(3, .leaf);
    frame4.buffer[0] = 3;
    pool.unpinPage(3, true);

    // Still 3 frames (one was evicted)
    try std.testing.expectEqual(@as(usize, 3), pool.stats().used_frames);
}

test "BufferPool pin prevents eviction" {
    const allocator = std.testing.allocator;
    const path = "test_buffer_pin.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var pool = try BufferPool.init(allocator, &file, 2);
    defer pool.deinit();

    // Allocate pages
    for (0..4) |_| {
        _ = try file.allocatePage();
    }

    // Create page 0 and keep it pinned
    _ = try pool.newPage(0, .leaf);
    // Don't unpin!

    // Create page 1 and unpin
    _ = try pool.newPage(1, .leaf);
    pool.unpinPage(1, true);

    // Create page 2 - should evict page 1 (unpinned), not page 0 (pinned)
    _ = try pool.newPage(2, .leaf);

    // Page 0 should still be in cache
    try std.testing.expect(pool.page_table.contains(0));
    // Page 1 should be evicted
    try std.testing.expect(!pool.page_table.contains(1));
    // Page 2 should be in cache
    try std.testing.expect(pool.page_table.contains(2));

    // Unpin remaining pages
    pool.unpinPage(0, false);
    pool.unpinPage(2, false);
}

test "BufferPool flushAll" {
    const allocator = std.testing.allocator;
    const path = "test_buffer_flush.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var pool = try BufferPool.init(allocator, &file, 10);
    defer pool.deinit();

    // Create some dirty pages
    for (0..3) |i| {
        const frame = try pool.newPage(@intCast(i), .leaf);
        frame.buffer[50] = @intCast(i + 100);
        pool.unpinPage(@intCast(i), true);
    }

    try std.testing.expectEqual(@as(usize, 3), pool.stats().dirty_frames);

    try pool.flushAll();

    try std.testing.expectEqual(@as(usize, 0), pool.stats().dirty_frames);
}

test "BufferPool full error" {
    const allocator = std.testing.allocator;
    const path = "test_buffer_full.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try file_mod.File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var pool = try BufferPool.init(allocator, &file, 2);
    defer pool.deinit();

    // Create 2 pinned pages
    _ = try pool.newPage(0, .leaf);
    _ = try pool.newPage(1, .leaf);

    // Try to create a 3rd - all frames pinned
    const result = pool.newPage(2, .leaf);
    try std.testing.expectError(errors.Error.BufferPoolFull, result);

    pool.unpinPage(0, false);
    pool.unpinPage(1, false);
}
