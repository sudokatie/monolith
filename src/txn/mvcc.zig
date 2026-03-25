const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");

const TransactionId = types.TransactionId;
const Key = types.Key;
const Value = types.Value;
const INVALID_TXN_ID = types.INVALID_TXN_ID;

/// Version chain entry for MVCC
pub const Version = struct {
    /// Transaction that created this version
    created_by: TransactionId,
    /// Transaction that deleted this version (INVALID_TXN_ID if live)
    deleted_by: TransactionId,
    /// Creation timestamp
    begin_ts: u64,
    /// Deletion timestamp (max if not deleted)
    end_ts: u64,
    /// Value data (owned)
    value: []u8,
    /// Next older version
    next: ?*Version,

    /// Check if this version is visible to a transaction
    pub fn isVisibleTo(self: *const Version, read_ts: u64) bool {
        return self.begin_ts <= read_ts and read_ts < self.end_ts;
    }
};

/// MVCC version manager for a single key
pub const VersionChain = struct {
    /// Key (owned)
    key: []u8,
    /// Head of version chain (newest)
    head: ?*Version,
    /// Allocator
    allocator: std.mem.Allocator,

    /// Initialize empty version chain
    pub fn init(allocator: std.mem.Allocator, key: Key) !VersionChain {
        const key_copy = try allocator.alloc(u8, key.len);
        @memcpy(key_copy, key);

        return .{
            .key = key_copy,
            .allocator = allocator,
            .head = null,
        };
    }

    /// Free all resources
    pub fn deinit(self: *VersionChain) void {
        var current = self.head;
        while (current) |v| {
            const next = v.next;
            self.allocator.free(v.value);
            self.allocator.destroy(v);
            current = next;
        }
        self.allocator.free(self.key);
    }

    /// Add a new version
    pub fn addVersion(self: *VersionChain, txn_id: TransactionId, ts: u64, value: Value) !void {
        const version = try self.allocator.create(Version);
        version.* = .{
            .created_by = txn_id,
            .deleted_by = INVALID_TXN_ID,
            .begin_ts = ts,
            .end_ts = std.math.maxInt(u64),
            .value = try self.allocator.alloc(u8, value.len),
            .next = self.head,
        };
        @memcpy(version.value, value);

        // Mark previous head as deleted
        if (self.head) |old_head| {
            old_head.deleted_by = txn_id;
            old_head.end_ts = ts;
        }

        self.head = version;
    }

    /// Mark current version as deleted
    pub fn markDeleted(self: *VersionChain, txn_id: TransactionId, ts: u64) void {
        if (self.head) |head| {
            head.deleted_by = txn_id;
            head.end_ts = ts;
        }
    }

    /// Get version visible to transaction
    pub fn getVisible(self: *const VersionChain, read_ts: u64) ?Value {
        var current = self.head;
        while (current) |v| {
            if (v.isVisibleTo(read_ts)) {
                return v.value;
            }
            current = v.next;
        }
        return null;
    }

    /// Check if key exists (any visible version)
    pub fn exists(self: *const VersionChain, read_ts: u64) bool {
        return self.getVisible(read_ts) != null;
    }

    /// Garbage collect old versions
    pub fn gc(self: *VersionChain, oldest_active_ts: u64) void {
        // Keep versions that might be visible to any active transaction
        var prev: ?*Version = null;
        var current = self.head;

        while (current) |v| {
            const next = v.next;

            // If this version's end_ts is before oldest active, and there's a newer version,
            // this version can be removed
            if (v.end_ts < oldest_active_ts and prev != null) {
                if (prev) |p| {
                    p.next = next;
                }
                self.allocator.free(v.value);
                self.allocator.destroy(v);
            } else {
                prev = v;
            }

            current = next;
        }
    }

    /// Get version count (for stats/debugging)
    pub fn versionCount(self: *const VersionChain) usize {
        var count: usize = 0;
        var current = self.head;
        while (current) |v| {
            count += 1;
            current = v.next;
        }
        return count;
    }
};

/// MVCC store - manages version chains for all keys
pub const MVCCStore = struct {
    /// Key to version chain mapping
    chains: std.StringHashMap(*VersionChain),
    /// Allocator
    allocator: std.mem.Allocator,
    /// Current timestamp
    current_ts: u64,
    /// Mutex for thread safety
    mutex: std.Thread.Mutex,

    /// Initialize MVCC store
    pub fn init(allocator: std.mem.Allocator) MVCCStore {
        return .{
            .chains = std.StringHashMap(*VersionChain).init(allocator),
            .allocator = allocator,
            .current_ts = 1,
            .mutex = .{},
        };
    }

    /// Free resources
    pub fn deinit(self: *MVCCStore) void {
        var iter = self.chains.valueIterator();
        while (iter.next()) |chain_ptr| {
            chain_ptr.*.deinit();
            self.allocator.destroy(chain_ptr.*);
        }
        self.chains.deinit();
    }

    /// Get or create version chain for key
    fn getOrCreateChain(self: *MVCCStore, key: Key) !*VersionChain {
        if (self.chains.get(key)) |chain| {
            return chain;
        }

        const chain = try self.allocator.create(VersionChain);
        chain.* = try VersionChain.init(self.allocator, key);
        try self.chains.put(chain.key, chain);
        return chain;
    }

    /// Put a value (creates new version)
    pub fn put(self: *MVCCStore, txn_id: TransactionId, key: Key, value: Value) !u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const ts = self.current_ts;
        self.current_ts += 1;

        const chain = try self.getOrCreateChain(key);
        try chain.addVersion(txn_id, ts, value);

        return ts;
    }

    /// Get a value visible to timestamp
    pub fn get(self: *MVCCStore, key: Key, read_ts: u64) ?Value {
        self.mutex.lock();
        defer self.mutex.unlock();

        const chain = self.chains.get(key) orelse return null;
        return chain.getVisible(read_ts);
    }

    /// Delete a key (marks current version as deleted)
    pub fn delete(self: *MVCCStore, txn_id: TransactionId, key: Key) !u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const ts = self.current_ts;
        self.current_ts += 1;

        if (self.chains.get(key)) |chain| {
            chain.markDeleted(txn_id, ts);
        }

        return ts;
    }

    /// Check if key exists at timestamp
    pub fn exists(self: *MVCCStore, key: Key, read_ts: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const chain = self.chains.get(key) orelse return false;
        return chain.exists(read_ts);
    }

    /// Garbage collect old versions
    pub fn gc(self: *MVCCStore, oldest_active_ts: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var iter = self.chains.valueIterator();
        while (iter.next()) |chain_ptr| {
            chain_ptr.*.gc(oldest_active_ts);
        }
    }

    /// Get current timestamp
    pub fn getTimestamp(self: *MVCCStore) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.current_ts;
    }

    /// Get key count
    pub fn keyCount(self: *const MVCCStore) usize {
        return self.chains.count();
    }
};

// Tests

test "Version visibility" {
    const v = Version{
        .created_by = 1,
        .deleted_by = INVALID_TXN_ID,
        .begin_ts = 5,
        .end_ts = 10,
        .value = undefined,
        .next = null,
    };

    try std.testing.expect(!v.isVisibleTo(4));
    try std.testing.expect(v.isVisibleTo(5));
    try std.testing.expect(v.isVisibleTo(9));
    try std.testing.expect(!v.isVisibleTo(10));
    try std.testing.expect(!v.isVisibleTo(15));
}

test "VersionChain basic" {
    const allocator = std.testing.allocator;

    var chain = try VersionChain.init(allocator, "key1");
    defer chain.deinit();

    // Add version
    try chain.addVersion(1, 5, "value1");
    try std.testing.expectEqual(@as(usize, 1), chain.versionCount());

    // Get visible
    const v = chain.getVisible(5);
    try std.testing.expect(v != null);
    try std.testing.expectEqualStrings("value1", v.?);

    // Not visible before creation
    try std.testing.expectEqual(@as(?Value, null), chain.getVisible(4));
}

test "VersionChain multiple versions" {
    const allocator = std.testing.allocator;

    var chain = try VersionChain.init(allocator, "key1");
    defer chain.deinit();

    // Add first version at ts=1
    try chain.addVersion(1, 1, "v1");

    // Add second version at ts=5 (deletes first)
    try chain.addVersion(2, 5, "v2");

    try std.testing.expectEqual(@as(usize, 2), chain.versionCount());

    // ts=1 sees v1
    try std.testing.expectEqualStrings("v1", chain.getVisible(1).?);

    // ts=5 sees v2
    try std.testing.expectEqualStrings("v2", chain.getVisible(5).?);

    // ts=3 sees v1
    try std.testing.expectEqualStrings("v1", chain.getVisible(3).?);
}

test "VersionChain delete" {
    const allocator = std.testing.allocator;

    var chain = try VersionChain.init(allocator, "key1");
    defer chain.deinit();

    try chain.addVersion(1, 1, "value");
    chain.markDeleted(2, 5);

    // Visible before delete
    try std.testing.expect(chain.exists(3));

    // Not visible after delete
    try std.testing.expect(!chain.exists(5));
}

test "MVCCStore put and get" {
    const allocator = std.testing.allocator;

    var store = MVCCStore.init(allocator);
    defer store.deinit();

    const ts1 = try store.put(1, "key1", "value1");
    const ts2 = try store.put(1, "key2", "value2");

    try std.testing.expect(ts2 > ts1);

    const v1 = store.get("key1", ts1);
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualStrings("value1", v1.?);

    const v2 = store.get("key2", ts2);
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("value2", v2.?);

    // key2 not visible at ts1
    try std.testing.expectEqual(@as(?Value, null), store.get("key2", ts1));
}

test "MVCCStore update" {
    const allocator = std.testing.allocator;

    var store = MVCCStore.init(allocator);
    defer store.deinit();

    const ts1 = try store.put(1, "key", "v1");
    const ts2 = try store.put(2, "key", "v2");

    // Old timestamp sees old value
    try std.testing.expectEqualStrings("v1", store.get("key", ts1).?);

    // New timestamp sees new value
    try std.testing.expectEqualStrings("v2", store.get("key", ts2).?);
}

test "MVCCStore delete" {
    const allocator = std.testing.allocator;

    var store = MVCCStore.init(allocator);
    defer store.deinit();

    const ts1 = try store.put(1, "key", "value");
    const ts2 = try store.delete(2, "key");

    // Before delete: exists
    try std.testing.expect(store.exists("key", ts1));

    // After delete: gone
    try std.testing.expect(!store.exists("key", ts2));
}

test "MVCCStore gc" {
    const allocator = std.testing.allocator;

    var store = MVCCStore.init(allocator);
    defer store.deinit();

    _ = try store.put(1, "key", "v1");
    _ = try store.put(2, "key", "v2");
    _ = try store.put(3, "key", "v3");

    const chain = store.chains.get("key").?;
    try std.testing.expectEqual(@as(usize, 3), chain.versionCount());

    // GC with oldest_active_ts = 10 should keep some versions
    store.gc(10);

    // Latest version should still be there
    try std.testing.expect(store.get("key", 100) != null);
}
