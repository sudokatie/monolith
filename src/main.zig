const std = @import("std");

// Core types and errors
pub const types = @import("core/types.zig");
pub const errors = @import("core/errors.zig");

// Storage components
pub const page = @import("storage/page.zig");
pub const file = @import("storage/file.zig");
pub const meta = @import("storage/meta.zig");
pub const freelist = @import("storage/freelist.zig");
pub const buffer = @import("storage/buffer.zig");
pub const btree_node = @import("storage/btree_node.zig");
pub const btree = @import("storage/btree.zig");

// WAL components
pub const wal_record = @import("wal/record.zig");
pub const wal_writer = @import("wal/writer.zig");
pub const wal_recovery = @import("wal/recovery.zig");
pub const wal_checkpoint = @import("wal/checkpoint.zig");

// Transaction components
pub const txn_manager = @import("txn/manager.zig");
pub const txn_mvcc = @import("txn/mvcc.zig");

// Database API
pub const db = @import("db.zig");

// Re-export commonly used types
pub const PageId = types.PageId;
pub const TransactionId = types.TransactionId;
pub const LSN = types.LSN;
pub const PageType = types.PageType;
pub const TxnState = types.TxnState;
pub const SyncMode = types.SyncMode;
pub const IsolationLevel = types.IsolationLevel;
pub const Config = types.Config;
pub const Key = types.Key;
pub const Value = types.Value;
pub const KeyValue = types.KeyValue;
pub const Ordering = types.Ordering;

// Re-export errors
pub const Error = errors.Error;
pub const MonolithError = errors.MonolithError;

// Constants
pub const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
pub const INVALID_TXN_ID = types.INVALID_TXN_ID;
pub const INVALID_LSN = types.INVALID_LSN;
pub const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;
pub const DB_MAGIC = types.DB_MAGIC;
pub const DB_VERSION = types.DB_VERSION;

// Utility functions
pub const compareKeys = types.compareKeys;
pub const formatError = errors.formatError;

test {
    // Run all tests in submodules
    std.testing.refAllDecls(@This());
}

test "library exports" {
    const testing = std.testing;

    // Test that types are properly exported
    const page_id: PageId = 42;
    try testing.expect(page_id == 42);

    const txn_id: TransactionId = 1;
    try testing.expect(txn_id == 1);

    const config = Config{};
    try testing.expect(config.page_size == DEFAULT_PAGE_SIZE);

    // Test key comparison
    try testing.expect(compareKeys("a", "b") == .less);
}
