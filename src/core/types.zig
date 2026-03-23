const std = @import("std");

/// Page identifier - uniquely identifies a page in the database file
pub const PageId = u64;

/// Transaction identifier - uniquely identifies a transaction
pub const TransactionId = u64;

/// Log sequence number - monotonically increasing, identifies WAL position
pub const LSN = u64;

/// Invalid page ID constant
pub const INVALID_PAGE_ID: PageId = std.math.maxInt(PageId);

/// Invalid transaction ID constant
pub const INVALID_TXN_ID: TransactionId = 0;

/// Invalid LSN constant
pub const INVALID_LSN: LSN = 0;

/// Page size in bytes (4KB default)
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Magic number for database file
pub const DB_MAGIC: u32 = 0x4D4F4E4F; // "MONO"

/// Current database format version
pub const DB_VERSION: u32 = 1;

/// Key type - variable-length byte slice
pub const Key = []const u8;

/// Value type - variable-length byte slice
pub const Value = []const u8;

/// Key-value pair
pub const KeyValue = struct {
    key: Key,
    value: Value,

    pub fn init(key: Key, value: Value) KeyValue {
        return .{ .key = key, .value = value };
    }
};

/// Page type enumeration
pub const PageType = enum(u8) {
    /// Invalid/uninitialized page
    invalid = 0,
    /// Database metadata page
    meta = 1,
    /// Free list page
    freelist = 2,
    /// B+ tree internal node
    internal = 3,
    /// B+ tree leaf node
    leaf = 4,
    /// Overflow page for large values
    overflow = 5,
};

/// Transaction state
pub const TxnState = enum(u8) {
    /// Transaction is active
    active = 0,
    /// Transaction has committed
    committed = 1,
    /// Transaction has aborted
    aborted = 2,
};

/// Sync mode for durability
pub const SyncMode = enum(u8) {
    /// Sync after every write
    sync = 0,
    /// Sync in batches
    batch = 1,
    /// No sync (test only)
    none = 2,
};

/// Isolation level for transactions
pub const IsolationLevel = enum(u8) {
    /// Read committed - see latest committed values
    read_committed = 0,
    /// Repeatable read - snapshot at transaction start
    repeatable_read = 1,
    /// Serializable - full isolation
    serializable = 2,
};

/// Database configuration
pub const Config = struct {
    /// Page size in bytes
    page_size: usize = DEFAULT_PAGE_SIZE,
    /// Buffer pool size in bytes
    cache_size: usize = 64 * 1024 * 1024, // 64MB default
    /// Sync mode for durability
    sync_mode: SyncMode = .sync,
    /// Default isolation level
    isolation_level: IsolationLevel = .read_committed,
    /// WAL segment size in bytes
    wal_segment_size: usize = 16 * 1024 * 1024, // 16MB default
    /// Checkpoint interval in number of transactions
    checkpoint_interval: u32 = 1000,
};

/// Comparison result for keys
pub const Ordering = enum {
    less,
    equal,
    greater,
};

/// Compare two keys lexicographically
pub fn compareKeys(a: Key, b: Key) Ordering {
    const min_len = @min(a.len, b.len);
    for (a[0..min_len], b[0..min_len]) |ca, cb| {
        if (ca < cb) return .less;
        if (ca > cb) return .greater;
    }
    if (a.len < b.len) return .less;
    if (a.len > b.len) return .greater;
    return .equal;
}

test "compareKeys" {
    const testing = std.testing;

    try testing.expectEqual(Ordering.equal, compareKeys("abc", "abc"));
    try testing.expectEqual(Ordering.less, compareKeys("abc", "abd"));
    try testing.expectEqual(Ordering.greater, compareKeys("abd", "abc"));
    try testing.expectEqual(Ordering.less, compareKeys("ab", "abc"));
    try testing.expectEqual(Ordering.greater, compareKeys("abc", "ab"));
    try testing.expectEqual(Ordering.equal, compareKeys("", ""));
    try testing.expectEqual(Ordering.less, compareKeys("", "a"));
}

test "constants" {
    const testing = std.testing;

    try testing.expect(INVALID_PAGE_ID == std.math.maxInt(PageId));
    try testing.expect(INVALID_TXN_ID == 0);
    try testing.expect(INVALID_LSN == 0);
    try testing.expect(DEFAULT_PAGE_SIZE == 4096);
    try testing.expect(DB_MAGIC == 0x4D4F4E4F);
}

test "Config defaults" {
    const config = Config{};

    const testing = std.testing;
    try testing.expect(config.page_size == DEFAULT_PAGE_SIZE);
    try testing.expect(config.cache_size == 64 * 1024 * 1024);
    try testing.expect(config.sync_mode == .sync);
}
