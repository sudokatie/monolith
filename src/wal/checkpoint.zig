const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const wal_writer = @import("writer.zig");
const wal_record = @import("record.zig");

const LSN = types.LSN;
const INVALID_LSN = types.INVALID_LSN;
const WALWriter = wal_writer.WALWriter;

/// Callback interface for flushing dirty pages
pub const FlushCallback = struct {
    context: *anyopaque,
    flushFn: *const fn (ctx: *anyopaque) errors.MonolithError!void,

    pub fn flush(self: FlushCallback) errors.MonolithError!void {
        return self.flushFn(self.context);
    }
};

/// Callback interface for updating meta page
pub const MetaCallback = struct {
    context: *anyopaque,
    updateFn: *const fn (ctx: *anyopaque, checkpoint_lsn: LSN) errors.MonolithError!void,

    pub fn update(self: MetaCallback, checkpoint_lsn: LSN) errors.MonolithError!void {
        return self.updateFn(self.context, checkpoint_lsn);
    }
};

/// Checkpoint configuration
pub const CheckpointConfig = struct {
    /// Minimum number of records before checkpoint
    min_records: u32 = 1000,
    /// Minimum WAL size before checkpoint (bytes)
    min_wal_size: u64 = 16 * 1024 * 1024, // 16MB
    /// Force checkpoint regardless of thresholds
    force: bool = false,
};

/// Checkpoint manager
pub const CheckpointManager = struct {
    allocator: std.mem.Allocator,
    /// WAL writer reference
    wal: *WALWriter,
    /// Last checkpoint LSN
    last_checkpoint_lsn: LSN,
    /// Records since last checkpoint
    records_since_checkpoint: u64,
    /// Configuration
    config: CheckpointConfig,

    pub fn init(allocator: std.mem.Allocator, wal: *WALWriter) CheckpointManager {
        return .{
            .allocator = allocator,
            .wal = wal,
            .last_checkpoint_lsn = INVALID_LSN,
            .records_since_checkpoint = 0,
            .config = .{},
        };
    }

    pub fn deinit(self: *CheckpointManager) void {
        _ = self;
        // No resources to clean up
    }

    /// Set checkpoint configuration
    pub fn configure(self: *CheckpointManager, config: CheckpointConfig) void {
        self.config = config;
    }

    /// Check if checkpoint is needed based on thresholds
    pub fn needsCheckpoint(self: *const CheckpointManager) bool {
        if (self.config.force) return true;
        if (self.records_since_checkpoint >= self.config.min_records) return true;
        if (self.wal.getPosition() >= self.config.min_wal_size) return true;
        return false;
    }

    /// Increment record count (call after each WAL write)
    pub fn recordWritten(self: *CheckpointManager) void {
        self.records_since_checkpoint += 1;
    }

    /// Perform a checkpoint
    /// 1. Flush all dirty pages (via callback)
    /// 2. Write checkpoint record to WAL
    /// 3. Update meta page (via callback)
    /// 4. Optionally truncate old WAL segments
    pub fn checkpoint(
        self: *CheckpointManager,
        flush_callback: ?FlushCallback,
        meta_callback: ?MetaCallback,
    ) errors.MonolithError!LSN {
        // Step 1: Flush all dirty pages
        if (flush_callback) |cb| {
            try cb.flush();
        }

        // Step 2: Write checkpoint record to WAL
        const checkpoint_lsn = try self.wal.writeCheckpoint();

        // Step 3: Update meta page with checkpoint LSN
        if (meta_callback) |cb| {
            try cb.update(checkpoint_lsn);
        }

        // Update state
        self.last_checkpoint_lsn = checkpoint_lsn;
        self.records_since_checkpoint = 0;

        return checkpoint_lsn;
    }

    /// Perform checkpoint and truncate old WAL segments
    pub fn checkpointAndTruncate(
        self: *CheckpointManager,
        flush_callback: ?FlushCallback,
        meta_callback: ?MetaCallback,
    ) errors.MonolithError!CheckpointResult {
        const checkpoint_lsn = try self.checkpoint(flush_callback, meta_callback);

        // Keep only current segment and newer
        const current_segment = self.wal.getSegmentNum();
        const deleted = try self.wal.deleteOldSegments(current_segment);

        return .{
            .checkpoint_lsn = checkpoint_lsn,
            .segments_deleted = deleted,
        };
    }

    /// Get the last checkpoint LSN
    pub fn getLastCheckpointLSN(self: *const CheckpointManager) LSN {
        return self.last_checkpoint_lsn;
    }

    /// Set the last checkpoint LSN (for recovery)
    pub fn setLastCheckpointLSN(self: *CheckpointManager, lsn: LSN) void {
        self.last_checkpoint_lsn = lsn;
    }

    /// Get records written since last checkpoint
    pub fn getRecordsSinceCheckpoint(self: *const CheckpointManager) u64 {
        return self.records_since_checkpoint;
    }
};

/// Result of a checkpoint with truncation
pub const CheckpointResult = struct {
    checkpoint_lsn: LSN,
    segments_deleted: u64,
};

/// Simple checkpoint scheduler
pub const CheckpointScheduler = struct {
    manager: *CheckpointManager,
    interval_records: u32,
    last_check_records: u64,

    pub fn init(manager: *CheckpointManager, interval_records: u32) CheckpointScheduler {
        return .{
            .manager = manager,
            .interval_records = interval_records,
            .last_check_records = 0,
        };
    }

    /// Called after each record write
    /// Returns true if checkpoint was triggered
    pub fn onRecordWritten(
        self: *CheckpointScheduler,
        flush_callback: ?FlushCallback,
        meta_callback: ?MetaCallback,
    ) errors.MonolithError!bool {
        self.manager.recordWritten();

        const records = self.manager.getRecordsSinceCheckpoint();
        if (records >= self.interval_records) {
            _ = try self.manager.checkpoint(flush_callback, meta_callback);
            return true;
        }
        return false;
    }
};

// Tests

test "CheckpointManager init" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_ckpt_init") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_ckpt_init") catch {};

    var wal = try WALWriter.init(allocator, "/tmp/test_ckpt_init", .none, wal_writer.DEFAULT_SEGMENT_SIZE);
    defer wal.deinit();

    var manager = CheckpointManager.init(allocator, &wal);
    defer manager.deinit();

    try std.testing.expectEqual(INVALID_LSN, manager.getLastCheckpointLSN());
    try std.testing.expectEqual(@as(u64, 0), manager.getRecordsSinceCheckpoint());
}

test "CheckpointManager needsCheckpoint" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_ckpt_needs") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_ckpt_needs") catch {};

    var wal = try WALWriter.init(allocator, "/tmp/test_ckpt_needs", .none, wal_writer.DEFAULT_SEGMENT_SIZE);
    defer wal.deinit();

    var manager = CheckpointManager.init(allocator, &wal);
    defer manager.deinit();

    // Configure with low threshold
    manager.configure(.{ .min_records = 5, .min_wal_size = 1024 * 1024 });

    try std.testing.expect(!manager.needsCheckpoint());

    // Add some records
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        manager.recordWritten();
    }

    try std.testing.expect(manager.needsCheckpoint());
}

test "CheckpointManager force checkpoint" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_ckpt_force") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_ckpt_force") catch {};

    var wal = try WALWriter.init(allocator, "/tmp/test_ckpt_force", .none, wal_writer.DEFAULT_SEGMENT_SIZE);
    defer wal.deinit();

    var manager = CheckpointManager.init(allocator, &wal);
    defer manager.deinit();

    manager.configure(.{ .force = true });
    try std.testing.expect(manager.needsCheckpoint());
}

test "CheckpointManager checkpoint" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_ckpt_do") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_ckpt_do") catch {};

    var wal = try WALWriter.init(allocator, "/tmp/test_ckpt_do", .none, wal_writer.DEFAULT_SEGMENT_SIZE);
    defer wal.deinit();

    var manager = CheckpointManager.init(allocator, &wal);
    defer manager.deinit();

    // Write some records
    _ = try wal.writeBegin(1);
    manager.recordWritten();
    _ = try wal.writeInsert(1, "key", "value");
    manager.recordWritten();
    _ = try wal.writeCommit(1);
    manager.recordWritten();

    try std.testing.expectEqual(@as(u64, 3), manager.getRecordsSinceCheckpoint());

    // Perform checkpoint
    const checkpoint_lsn = try manager.checkpoint(null, null);

    try std.testing.expect(checkpoint_lsn > 0);
    try std.testing.expectEqual(checkpoint_lsn, manager.getLastCheckpointLSN());
    try std.testing.expectEqual(@as(u64, 0), manager.getRecordsSinceCheckpoint());
}

test "CheckpointManager with callbacks" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_ckpt_cb") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_ckpt_cb") catch {};

    var wal = try WALWriter.init(allocator, "/tmp/test_ckpt_cb", .none, wal_writer.DEFAULT_SEGMENT_SIZE);
    defer wal.deinit();

    var manager = CheckpointManager.init(allocator, &wal);
    defer manager.deinit();

    // Track callback invocations
    var flush_called = false;
    var meta_lsn: LSN = 0;

    const flush_cb = FlushCallback{
        .context = @ptrCast(&flush_called),
        .flushFn = struct {
            fn flush(ctx: *anyopaque) errors.MonolithError!void {
                const called: *bool = @ptrCast(@alignCast(ctx));
                called.* = true;
            }
        }.flush,
    };

    const meta_cb = MetaCallback{
        .context = @ptrCast(&meta_lsn),
        .updateFn = struct {
            fn update(ctx: *anyopaque, lsn: LSN) errors.MonolithError!void {
                const stored: *LSN = @ptrCast(@alignCast(ctx));
                stored.* = lsn;
            }
        }.update,
    };

    const checkpoint_lsn = try manager.checkpoint(flush_cb, meta_cb);

    try std.testing.expect(flush_called);
    try std.testing.expectEqual(checkpoint_lsn, meta_lsn);
}

test "CheckpointScheduler" {
    const allocator = std.testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_ckpt_sched") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_ckpt_sched") catch {};

    var wal = try WALWriter.init(allocator, "/tmp/test_ckpt_sched", .none, wal_writer.DEFAULT_SEGMENT_SIZE);
    defer wal.deinit();

    var manager = CheckpointManager.init(allocator, &wal);
    defer manager.deinit();

    var scheduler = CheckpointScheduler.init(&manager, 5);

    // Write 4 records - should not checkpoint
    var i: u32 = 0;
    while (i < 4) : (i += 1) {
        const triggered = try scheduler.onRecordWritten(null, null);
        try std.testing.expect(!triggered);
    }

    // 5th record should trigger checkpoint
    const triggered = try scheduler.onRecordWritten(null, null);
    try std.testing.expect(triggered);

    // Records count should be reset
    try std.testing.expectEqual(@as(u64, 0), manager.getRecordsSinceCheckpoint());
}

test "CheckpointResult" {
    const result = CheckpointResult{
        .checkpoint_lsn = 100,
        .segments_deleted = 3,
    };

    try std.testing.expectEqual(@as(LSN, 100), result.checkpoint_lsn);
    try std.testing.expectEqual(@as(u64, 3), result.segments_deleted);
}

test "CheckpointConfig defaults" {
    const config = CheckpointConfig{};

    try std.testing.expectEqual(@as(u32, 1000), config.min_records);
    try std.testing.expectEqual(@as(u64, 16 * 1024 * 1024), config.min_wal_size);
    try std.testing.expect(!config.force);
}
