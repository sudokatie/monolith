const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const record = @import("record.zig");
const writer_mod = @import("writer.zig");
const buffer_mod = @import("../storage/buffer.zig");

const LSN = types.LSN;
const TransactionId = types.TransactionId;
const PageId = types.PageId;
const INVALID_LSN = types.INVALID_LSN;

const RecordType = record.RecordType;
const CheckpointData = record.CheckpointData;
const WALWriter = writer_mod.WALWriter;
const BufferPool = buffer_mod.BufferPool;

/// Checkpoint manager
pub const Checkpoint = struct {
    /// WAL writer
    wal: *WALWriter,
    /// Buffer pool
    buffer_pool: *BufferPool,
    /// Allocator
    allocator: std.mem.Allocator,
    /// Checkpoint interval (number of transactions)
    interval: u32,
    /// Transactions since last checkpoint
    txn_count: u32,
    /// Last checkpoint LSN
    last_checkpoint_lsn: LSN,

    /// Initialize checkpoint manager
    pub fn init(allocator: std.mem.Allocator, wal: *WALWriter, buffer_pool: *BufferPool, interval: u32) Checkpoint {
        return .{
            .wal = wal,
            .buffer_pool = buffer_pool,
            .allocator = allocator,
            .interval = interval,
            .txn_count = 0,
            .last_checkpoint_lsn = INVALID_LSN,
        };
    }

    /// Notify that a transaction has committed
    pub fn onCommit(self: *Checkpoint) !void {
        self.txn_count += 1;

        if (self.txn_count >= self.interval) {
            try self.performCheckpoint(&[_]TransactionId{}, &[_]PageId{});
        }
    }

    /// Perform a checkpoint
    pub fn performCheckpoint(self: *Checkpoint, active_txns: []const TransactionId, dirty_pages: []const PageId) !void {
        // Write checkpoint begin record
        const begin_lsn = try self.wal.append(0, INVALID_LSN, .checkpoint_begin, "");

        // Flush all dirty pages from buffer pool
        try self.buffer_pool.flushAll();

        // Serialize checkpoint data
        const data_size = 8 + active_txns.len * 8 + dirty_pages.len * 8;
        const data = try self.allocator.alloc(u8, data_size);
        defer self.allocator.free(data);

        const txn_count: u32 = @intCast(active_txns.len);
        const page_count: u32 = @intCast(dirty_pages.len);

        @memcpy(data[0..4], std.mem.asBytes(&txn_count));
        @memcpy(data[4..8], std.mem.asBytes(&page_count));

        var offset: usize = 8;
        for (active_txns) |txn_id| {
            @memcpy(data[offset .. offset + 8], std.mem.asBytes(&txn_id));
            offset += 8;
        }
        for (dirty_pages) |page_id| {
            @memcpy(data[offset .. offset + 8], std.mem.asBytes(&page_id));
            offset += 8;
        }

        // Write checkpoint end record with data
        _ = try self.wal.append(0, begin_lsn, .checkpoint_end, data);

        // Sync WAL
        try self.wal.sync();

        self.last_checkpoint_lsn = begin_lsn;
        self.txn_count = 0;
    }

    /// Force a checkpoint regardless of interval
    pub fn forceCheckpoint(self: *Checkpoint, active_txns: []const TransactionId, dirty_pages: []const PageId) !void {
        try self.performCheckpoint(active_txns, dirty_pages);
    }

    /// Get last checkpoint LSN
    pub fn getLastCheckpointLSN(self: *const Checkpoint) LSN {
        return self.last_checkpoint_lsn;
    }

    /// Perform checkpoint and truncate WAL
    /// This removes log records before the checkpoint that are no longer needed
    pub fn checkpointAndTruncate(self: *Checkpoint, active_txns: []const TransactionId, dirty_pages: []const PageId) !void {
        try self.performCheckpoint(active_txns, dirty_pages);

        // Truncate WAL after checkpoint - we can discard records before checkpoint
        // since all dirty pages have been flushed
        // Note: In a production system, you'd want to keep some records for
        // point-in-time recovery, but for basic crash recovery, truncation is safe
        try self.wal.truncate(self.wal.position);
    }

    /// Check if checkpoint is needed
    pub fn needsCheckpoint(self: *const Checkpoint) bool {
        return self.txn_count >= self.interval;
    }
};

/// Fuzzy checkpoint implementation (allows concurrent operations)
pub const FuzzyCheckpoint = struct {
    /// Base checkpoint manager
    base: Checkpoint,
    /// Is checkpoint in progress?
    in_progress: bool,
    /// Pages being checkpointed
    checkpoint_pages: std.ArrayListUnmanaged(PageId),
    /// Allocator
    allocator: std.mem.Allocator,

    /// Initialize fuzzy checkpoint manager
    pub fn init(allocator: std.mem.Allocator, wal: *WALWriter, buffer_pool: *BufferPool, interval: u32) FuzzyCheckpoint {
        return .{
            .base = Checkpoint.init(allocator, wal, buffer_pool, interval),
            .in_progress = false,
            .checkpoint_pages = .{},
            .allocator = allocator,
        };
    }

    /// Free resources
    pub fn deinit(self: *FuzzyCheckpoint) void {
        self.checkpoint_pages.deinit(self.allocator);
    }

    /// Start a fuzzy checkpoint (non-blocking)
    pub fn startCheckpoint(self: *FuzzyCheckpoint, active_txns: []const TransactionId) !LSN {
        if (self.in_progress) {
            return errors.Error.AlreadyOpen;
        }

        self.in_progress = true;
        self.checkpoint_pages.clearRetainingCapacity();

        // Write checkpoint begin record
        const begin_lsn = try self.base.wal.append(0, INVALID_LSN, .checkpoint_begin, "");

        // Collect dirty pages to checkpoint
        const stats = self.base.buffer_pool.stats();
        _ = stats;
        _ = active_txns;

        // In a real implementation, we'd collect the dirty page list here
        // and checkpoint them incrementally

        return begin_lsn;
    }

    /// Complete the fuzzy checkpoint
    pub fn completeCheckpoint(self: *FuzzyCheckpoint, begin_lsn: LSN, active_txns: []const TransactionId) !void {
        if (!self.in_progress) {
            return;
        }

        // Serialize checkpoint data
        const dirty_pages = self.checkpoint_pages.items;
        const data_size = 8 + active_txns.len * 8 + dirty_pages.len * 8;
        const data = try self.base.allocator.alloc(u8, data_size);
        defer self.base.allocator.free(data);

        const txn_count: u32 = @intCast(active_txns.len);
        const page_count: u32 = @intCast(dirty_pages.len);

        @memcpy(data[0..4], std.mem.asBytes(&txn_count));
        @memcpy(data[4..8], std.mem.asBytes(&page_count));

        var offset: usize = 8;
        for (active_txns) |txn_id| {
            @memcpy(data[offset .. offset + 8], std.mem.asBytes(&txn_id));
            offset += 8;
        }
        for (dirty_pages) |page_id| {
            @memcpy(data[offset .. offset + 8], std.mem.asBytes(&page_id));
            offset += 8;
        }

        // Write checkpoint end
        _ = try self.base.wal.append(0, begin_lsn, .checkpoint_end, data);
        try self.base.wal.sync();

        self.base.last_checkpoint_lsn = begin_lsn;
        self.base.txn_count = 0;
        self.in_progress = false;
    }

    /// Cancel an in-progress checkpoint
    pub fn cancelCheckpoint(self: *FuzzyCheckpoint) void {
        self.in_progress = false;
        self.checkpoint_pages.clearRetainingCapacity();
    }

    /// Is checkpoint in progress?
    pub fn isInProgress(self: *const FuzzyCheckpoint) bool {
        return self.in_progress;
    }
};

// Tests

test "Checkpoint basic" {
    const allocator = std.testing.allocator;
    const wal_path = "test_checkpoint.wal";
    const db_path = "test_checkpoint.db";

    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};
    defer {
        std.fs.cwd().deleteFile(wal_path) catch {};
        std.fs.cwd().deleteFile(db_path) catch {};
    }

    const file_mod = @import("../storage/file.zig");

    var file = try file_mod.File.open(allocator, db_path, types.DEFAULT_PAGE_SIZE);
    defer file.close();

    var buffer_pool = try BufferPool.init(allocator, &file, 10);
    defer buffer_pool.deinit();

    var wal = try WALWriter.open(allocator, wal_path, .sync);
    defer wal.close();

    var checkpoint = Checkpoint.init(allocator, &wal, &buffer_pool, 10);

    try std.testing.expectEqual(INVALID_LSN, checkpoint.getLastCheckpointLSN());
    try std.testing.expect(!checkpoint.needsCheckpoint());

    // Simulate commits
    for (0..10) |_| {
        try checkpoint.onCommit();
    }

    // After 10 commits, should have checkpointed
    try std.testing.expect(checkpoint.getLastCheckpointLSN() != INVALID_LSN);
}

test "Checkpoint force" {
    const allocator = std.testing.allocator;
    const wal_path = "test_checkpoint_force.wal";
    const db_path = "test_checkpoint_force.db";

    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};
    defer {
        std.fs.cwd().deleteFile(wal_path) catch {};
        std.fs.cwd().deleteFile(db_path) catch {};
    }

    const file_mod = @import("../storage/file.zig");

    var file = try file_mod.File.open(allocator, db_path, types.DEFAULT_PAGE_SIZE);
    defer file.close();

    var buffer_pool = try BufferPool.init(allocator, &file, 10);
    defer buffer_pool.deinit();

    var wal = try WALWriter.open(allocator, wal_path, .sync);
    defer wal.close();

    var checkpoint = Checkpoint.init(allocator, &wal, &buffer_pool, 1000);

    // Force checkpoint
    var active_txns = [_]TransactionId{ 1, 2, 3 };
    var dirty_pages = [_]PageId{ 10, 20 };

    try checkpoint.forceCheckpoint(&active_txns, &dirty_pages);

    try std.testing.expect(checkpoint.getLastCheckpointLSN() != INVALID_LSN);
}

test "FuzzyCheckpoint" {
    const allocator = std.testing.allocator;
    const wal_path = "test_fuzzy_checkpoint.wal";
    const db_path = "test_fuzzy_checkpoint.db";

    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};
    defer {
        std.fs.cwd().deleteFile(wal_path) catch {};
        std.fs.cwd().deleteFile(db_path) catch {};
    }

    const file_mod = @import("../storage/file.zig");

    var file = try file_mod.File.open(allocator, db_path, types.DEFAULT_PAGE_SIZE);
    defer file.close();

    var buffer_pool = try BufferPool.init(allocator, &file, 10);
    defer buffer_pool.deinit();

    var wal = try WALWriter.open(allocator, wal_path, .sync);
    defer wal.close();

    var checkpoint = FuzzyCheckpoint.init(allocator, &wal, &buffer_pool, 100);
    defer checkpoint.deinit();

    try std.testing.expect(!checkpoint.isInProgress());

    // Start checkpoint
    var active_txns = [_]TransactionId{1};
    const begin_lsn = try checkpoint.startCheckpoint(&active_txns);

    try std.testing.expect(checkpoint.isInProgress());

    // Complete checkpoint
    try checkpoint.completeCheckpoint(begin_lsn, &active_txns);

    try std.testing.expect(!checkpoint.isInProgress());
    try std.testing.expect(checkpoint.base.getLastCheckpointLSN() != INVALID_LSN);
}
