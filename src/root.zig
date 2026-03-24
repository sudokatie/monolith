//! Monolith - Embedded key-value store in Zig
//!
//! ACID transactions, MVCC, crash recovery.

// Core types
pub const types = @import("core/types.zig");
pub const errors = @import("core/errors.zig");

// Storage layer
pub const page = @import("storage/page.zig");
pub const file = @import("storage/file.zig");
pub const meta = @import("storage/meta.zig");
pub const freelist = @import("storage/freelist.zig");
pub const buffer = @import("storage/buffer.zig");
pub const btree_node = @import("storage/btree_node.zig");
pub const btree = @import("storage/btree.zig");

// Write-ahead log
pub const wal_record = @import("wal/record.zig");
pub const wal_writer = @import("wal/writer.zig");
pub const wal_recovery = @import("wal/recovery.zig");
pub const wal_checkpoint = @import("wal/checkpoint.zig");

// Transaction layer
pub const txn_manager = @import("txn/manager.zig");
pub const txn_mvcc = @import("txn/mvcc.zig");
pub const txn_lock = @import("txn/lock.zig");

// Database API
pub const db = @import("db.zig");

// Convenience re-exports
pub const DB = db.DB;
pub const DBConfig = db.DBConfig;
pub const DBTransaction = db.DBTransaction;
pub const DBIterator = db.DBIterator;
pub const Snapshot = db.Snapshot;
pub const DBStats = db.DBStats;

pub const Key = types.Key;
pub const Value = types.Value;
pub const SyncMode = types.SyncMode;
pub const IsolationLevel = types.IsolationLevel;

pub const Error = errors.Error;

test {
    @import("std").testing.refAllDecls(@This());
}
