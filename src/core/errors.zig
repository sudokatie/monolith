const std = @import("std");

/// Errors that can occur in monolith operations
pub const Error = error{
    /// Database file is corrupted
    Corrupted,
    /// Page checksum mismatch
    ChecksumMismatch,
    /// Invalid page type
    InvalidPageType,
    /// Page not found in file
    PageNotFound,
    /// Key not found in database
    KeyNotFound,
    /// Key already exists (for insert-only operations)
    KeyExists,
    /// Transaction conflict detected
    TransactionConflict,
    /// Transaction has already committed or aborted
    TransactionInactive,
    /// Deadlock detected
    Deadlock,
    /// Lock timeout
    LockTimeout,
    /// Buffer pool is full
    BufferPoolFull,
    /// No free pages available
    OutOfSpace,
    /// Invalid configuration
    InvalidConfig,
    /// Database is not open
    NotOpen,
    /// Database is already open
    AlreadyOpen,
    /// WAL is corrupted
    WALCorrupted,
    /// Invalid WAL record
    InvalidWALRecord,
    /// Recovery failed
    RecoveryFailed,
    /// Invalid magic number
    InvalidMagic,
    /// Version mismatch
    VersionMismatch,
    /// Page size mismatch
    PageSizeMismatch,
    /// Iterator exhausted
    EndOfIterator,
    /// Operation would block
    WouldBlock,
    /// Read-only transaction cannot write
    ReadOnlyTransaction,
    /// Value too large for page
    ValueTooLarge,
    /// Key too large
    KeyTooLarge,
};

/// Combined error set with std errors
pub const MonolithError = Error || std.fs.File.OpenError || std.fs.File.ReadError || std.fs.File.WriteError || std.mem.Allocator.Error;

/// Format an error for display
pub fn formatError(err: MonolithError) []const u8 {
    return switch (err) {
        Error.Corrupted => "database file is corrupted",
        Error.ChecksumMismatch => "page checksum mismatch",
        Error.InvalidPageType => "invalid page type",
        Error.PageNotFound => "page not found",
        Error.KeyNotFound => "key not found",
        Error.KeyExists => "key already exists",
        Error.TransactionConflict => "transaction conflict",
        Error.TransactionInactive => "transaction is not active",
        Error.Deadlock => "deadlock detected",
        Error.LockTimeout => "lock timeout",
        Error.BufferPoolFull => "buffer pool is full",
        Error.OutOfSpace => "no free pages available",
        Error.InvalidConfig => "invalid configuration",
        Error.NotOpen => "database is not open",
        Error.AlreadyOpen => "database is already open",
        Error.WALCorrupted => "write-ahead log is corrupted",
        Error.InvalidWALRecord => "invalid WAL record",
        Error.RecoveryFailed => "recovery failed",
        Error.InvalidMagic => "invalid database magic number",
        Error.VersionMismatch => "database version mismatch",
        Error.PageSizeMismatch => "page size mismatch",
        Error.EndOfIterator => "end of iterator",
        Error.WouldBlock => "operation would block",
        Error.ReadOnlyTransaction => "read-only transaction cannot write",
        Error.ValueTooLarge => "value too large",
        Error.KeyTooLarge => "key too large",
        else => "unknown error",
    };
}

test "formatError" {
    const testing = std.testing;

    const msg = formatError(Error.KeyNotFound);
    try testing.expectEqualStrings("key not found", msg);
}

test "error types" {
    // Just verify error types compile
    const err: MonolithError = Error.Corrupted;
    // Use the error in a meaningful way
    const msg = formatError(err);
    const testing = std.testing;
    try testing.expectEqualStrings("database file is corrupted", msg);
}
