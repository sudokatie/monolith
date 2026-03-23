const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const page_mod = @import("page.zig");

const PageId = types.PageId;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;
const Page = page_mod.Page;

/// File I/O wrapper for page-aligned database file operations
pub const File = struct {
    /// Underlying file handle
    handle: std.fs.File,
    /// Page size for this file
    page_size: usize,
    /// Current file size in pages
    page_count: u64,
    /// Path to the file (for error messages)
    path: []const u8,
    /// Allocator for path copy
    allocator: std.mem.Allocator,

    /// Open or create a database file
    pub fn open(allocator: std.mem.Allocator, path: []const u8, page_size: usize) !File {
        // Try to open existing file
        const handle = std.fs.cwd().openFile(path, .{
            .mode = .read_write,
        }) catch |err| switch (err) {
            error.FileNotFound => {
                // Create new file
                const new_handle = try std.fs.cwd().createFile(path, .{
                    .read = true,
                    .truncate = false,
                });
                const path_copy = try allocator.dupe(u8, path);
                return .{
                    .handle = new_handle,
                    .page_size = page_size,
                    .page_count = 0,
                    .path = path_copy,
                    .allocator = allocator,
                };
            },
            else => return err,
        };

        // Get current file size
        const stat = try handle.stat();
        const file_size = stat.size;

        // Verify alignment
        if (file_size > 0 and file_size % page_size != 0) {
            handle.close();
            return errors.Error.Corrupted;
        }

        const path_copy = try allocator.dupe(u8, path);

        return .{
            .handle = handle,
            .page_size = page_size,
            .page_count = file_size / page_size,
            .path = path_copy,
            .allocator = allocator,
        };
    }

    /// Close the file
    pub fn close(self: *File) void {
        self.handle.close();
        self.allocator.free(self.path);
    }

    /// Read a page from the file into the provided buffer
    pub fn readPage(self: *File, page_id: PageId, buffer: []u8) !void {
        if (buffer.len != self.page_size) {
            return errors.Error.InvalidConfig;
        }

        if (page_id >= self.page_count) {
            return errors.Error.PageNotFound;
        }

        const offset = page_id * self.page_size;
        try self.handle.seekTo(offset);
        const bytes_read = try self.handle.readAll(buffer);

        if (bytes_read != self.page_size) {
            return errors.Error.Corrupted;
        }
    }

    /// Write a page to the file from the provided buffer
    pub fn writePage(self: *File, page_id: PageId, buffer: []const u8) !void {
        if (buffer.len != self.page_size) {
            return errors.Error.InvalidConfig;
        }

        // Grow file if necessary
        if (page_id >= self.page_count) {
            try self.growTo(page_id + 1);
        }

        const offset = page_id * self.page_size;
        try self.handle.seekTo(offset);
        try self.handle.writeAll(buffer);
    }

    /// Sync the file to disk (fdatasync)
    pub fn sync(self: *File) !void {
        try self.handle.sync();
    }

    /// Grow the file to hold at least `min_pages` pages
    pub fn growTo(self: *File, min_pages: u64) !void {
        if (min_pages <= self.page_count) {
            return;
        }

        const new_size = min_pages * self.page_size;
        try self.handle.setEndPos(new_size);
        self.page_count = min_pages;
    }

    /// Allocate a new page at the end of the file
    /// Returns the page ID of the new page
    pub fn allocatePage(self: *File) !PageId {
        const new_page_id = self.page_count;
        try self.growTo(new_page_id + 1);
        return new_page_id;
    }

    /// Get the current number of pages in the file
    pub fn pageCount(self: *const File) u64 {
        return self.page_count;
    }

    /// Get the file size in bytes
    pub fn fileSize(self: *const File) u64 {
        return self.page_count * self.page_size;
    }

    /// Truncate the file to the specified number of pages
    pub fn truncate(self: *File, page_count: u64) !void {
        const new_size = page_count * self.page_size;
        try self.handle.setEndPos(new_size);
        self.page_count = page_count;
    }
};

// Tests

test "File open and close - new file" {
    const allocator = std.testing.allocator;
    const path = "test_file_new.db";

    // Clean up from previous test runs
    std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    try std.testing.expectEqual(@as(u64, 0), file.pageCount());
    try std.testing.expectEqual(@as(u64, 0), file.fileSize());
}

test "File write and read page" {
    const allocator = std.testing.allocator;
    const path = "test_file_rw.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create a test page buffer
    var write_buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&write_buffer, 0);
    write_buffer[0] = 0xDE;
    write_buffer[1] = 0xAD;
    write_buffer[2] = 0xBE;
    write_buffer[3] = 0xEF;

    // Write to page 0
    try file.writePage(0, &write_buffer);

    try std.testing.expectEqual(@as(u64, 1), file.pageCount());

    // Read it back
    var read_buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;
    try file.readPage(0, &read_buffer);

    try std.testing.expectEqualSlices(u8, &write_buffer, &read_buffer);
}

test "File multiple pages" {
    const allocator = std.testing.allocator;
    const path = "test_file_multi.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Write to multiple pages (not sequential)
    var buffer0: [DEFAULT_PAGE_SIZE]u8 = undefined;
    var buffer2: [DEFAULT_PAGE_SIZE]u8 = undefined;
    var buffer5: [DEFAULT_PAGE_SIZE]u8 = undefined;

    @memset(&buffer0, 0x00);
    @memset(&buffer2, 0x22);
    @memset(&buffer5, 0x55);

    try file.writePage(0, &buffer0);
    try file.writePage(2, &buffer2);
    try file.writePage(5, &buffer5);

    // File should have grown to 6 pages
    try std.testing.expectEqual(@as(u64, 6), file.pageCount());
    try std.testing.expectEqual(@as(u64, 6 * DEFAULT_PAGE_SIZE), file.fileSize());

    // Read back and verify
    var read0: [DEFAULT_PAGE_SIZE]u8 = undefined;
    var read2: [DEFAULT_PAGE_SIZE]u8 = undefined;
    var read5: [DEFAULT_PAGE_SIZE]u8 = undefined;

    try file.readPage(0, &read0);
    try file.readPage(2, &read2);
    try file.readPage(5, &read5);

    try std.testing.expectEqualSlices(u8, &buffer0, &read0);
    try std.testing.expectEqualSlices(u8, &buffer2, &read2);
    try std.testing.expectEqualSlices(u8, &buffer5, &read5);
}

test "File read non-existent page" {
    const allocator = std.testing.allocator;
    const path = "test_file_notfound.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;
    const result = file.readPage(100, &buffer);

    try std.testing.expectError(errors.Error.PageNotFound, result);
}

test "File allocatePage" {
    const allocator = std.testing.allocator;
    const path = "test_file_alloc.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    const page0 = try file.allocatePage();
    const page1 = try file.allocatePage();
    const page2 = try file.allocatePage();

    try std.testing.expectEqual(@as(PageId, 0), page0);
    try std.testing.expectEqual(@as(PageId, 1), page1);
    try std.testing.expectEqual(@as(PageId, 2), page2);
    try std.testing.expectEqual(@as(u64, 3), file.pageCount());
}

test "File reopen existing" {
    const allocator = std.testing.allocator;
    const path = "test_file_reopen.db";

    std.fs.cwd().deleteFile(path) catch {};

    // Create and write
    {
        var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
        defer file.close();

        var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;
        @memset(&buffer, 0xAB);
        try file.writePage(0, &buffer);
        try file.writePage(1, &buffer);
        try file.sync();
    }

    // Reopen and verify
    {
        var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
        defer {
            file.close();
            std.fs.cwd().deleteFile(path) catch {};
        }

        try std.testing.expectEqual(@as(u64, 2), file.pageCount());

        var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;
        try file.readPage(0, &buffer);
        try std.testing.expectEqual(@as(u8, 0xAB), buffer[0]);
    }
}

test "File truncate" {
    const allocator = std.testing.allocator;
    const path = "test_file_truncate.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Grow to 10 pages
    try file.growTo(10);
    try std.testing.expectEqual(@as(u64, 10), file.pageCount());

    // Truncate to 5
    try file.truncate(5);
    try std.testing.expectEqual(@as(u64, 5), file.pageCount());
}

test "File sync" {
    const allocator = std.testing.allocator;
    const path = "test_file_sync.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;
    @memset(&buffer, 0x42);

    try file.writePage(0, &buffer);
    try file.sync();

    // If we get here without error, sync worked
}

test "File wrong buffer size" {
    const allocator = std.testing.allocator;
    const path = "test_file_bufsize.db";

    std.fs.cwd().deleteFile(path) catch {};

    var file = try File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer {
        file.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Write with wrong size buffer
    var small_buffer: [100]u8 = undefined;
    const result = file.writePage(0, &small_buffer);

    try std.testing.expectError(errors.Error.InvalidConfig, result);
}
