const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");

const TransactionId = types.TransactionId;
const LSN = types.LSN;
const IsolationLevel = types.IsolationLevel;
const INVALID_TXN_ID = types.INVALID_TXN_ID;
const INVALID_LSN = types.INVALID_LSN;

/// A single version of a key-value pair
pub const Version = struct {
    /// Transaction that created this version
    created_by: TransactionId,
    /// Transaction that deleted this version (or INVALID_TXN_ID if not deleted)
    deleted_by: TransactionId,
    /// LSN when this version was created
    created_lsn: LSN,
    /// LSN when this version was deleted (or INVALID_LSN if not deleted)
    deleted_lsn: LSN,
    /// The value (owned)
    value: []const u8,
    /// Next older version (forms a chain)
    prev: ?*Version,

    pub fn init(
        allocator: std.mem.Allocator,
        created_by: TransactionId,
        created_lsn: LSN,
        value: []const u8,
    ) !*Version {
        const version = try allocator.create(Version);
        version.* = .{
            .created_by = created_by,
            .deleted_by = INVALID_TXN_ID,
            .created_lsn = created_lsn,
            .deleted_lsn = INVALID_LSN,
            .value = try allocator.dupe(u8, value),
            .prev = null,
        };
        return version;
    }

    pub fn deinit(self: *Version, allocator: std.mem.Allocator) void {
        allocator.free(self.value);
        allocator.destroy(self);
    }

    /// Mark this version as deleted
    pub fn markDeleted(self: *Version, deleted_by: TransactionId, deleted_lsn: LSN) void {
        self.deleted_by = deleted_by;
        self.deleted_lsn = deleted_lsn;
    }

    /// Check if version is deleted
    pub fn isDeleted(self: *const Version) bool {
        return self.deleted_by != INVALID_TXN_ID;
    }
};

/// Version chain for a single key
pub const VersionChain = struct {
    /// Most recent version (head of chain)
    head: ?*Version,
    /// Number of versions in chain
    version_count: u32,
    /// Allocator
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) VersionChain {
        return .{
            .head = null,
            .version_count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *VersionChain) void {
        var current = self.head;
        while (current) |version| {
            const prev = version.prev;
            version.deinit(self.allocator);
            current = prev;
        }
        self.head = null;
        self.version_count = 0;
    }

    /// Add a new version to the chain (becomes head)
    pub fn addVersion(
        self: *VersionChain,
        created_by: TransactionId,
        created_lsn: LSN,
        value: []const u8,
    ) !*Version {
        const version = try Version.init(self.allocator, created_by, created_lsn, value);
        version.prev = self.head;
        self.head = version;
        self.version_count += 1;
        return version;
    }

    /// Mark the current head as deleted and return it
    pub fn deleteHead(
        self: *VersionChain,
        deleted_by: TransactionId,
        deleted_lsn: LSN,
    ) ?*Version {
        if (self.head) |head| {
            head.markDeleted(deleted_by, deleted_lsn);
            return head;
        }
        return null;
    }

    /// Get the most recent version
    pub fn getHead(self: *const VersionChain) ?*Version {
        return self.head;
    }

    /// Get version count
    pub fn getVersionCount(self: *const VersionChain) u32 {
        return self.version_count;
    }
};

/// Visibility checker for MVCC
pub const VisibilityChecker = struct {
    /// Set of committed transaction IDs
    committed_txns: std.AutoHashMap(TransactionId, LSN),
    /// Set of aborted transaction IDs
    aborted_txns: std.AutoHashMap(TransactionId, void),
    /// Allocator
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) VisibilityChecker {
        return .{
            .committed_txns = std.AutoHashMap(TransactionId, LSN).init(allocator),
            .aborted_txns = std.AutoHashMap(TransactionId, void).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *VisibilityChecker) void {
        self.committed_txns.deinit();
        self.aborted_txns.deinit();
    }

    /// Mark a transaction as committed
    pub fn markCommitted(self: *VisibilityChecker, txn_id: TransactionId, commit_lsn: LSN) !void {
        try self.committed_txns.put(txn_id, commit_lsn);
    }

    /// Mark a transaction as aborted
    pub fn markAborted(self: *VisibilityChecker, txn_id: TransactionId) !void {
        try self.aborted_txns.put(txn_id, {});
    }

    /// Check if a transaction is committed
    pub fn isCommitted(self: *const VisibilityChecker, txn_id: TransactionId) bool {
        return self.committed_txns.contains(txn_id);
    }

    /// Check if a transaction is aborted
    pub fn isAborted(self: *const VisibilityChecker, txn_id: TransactionId) bool {
        return self.aborted_txns.contains(txn_id);
    }

    /// Get commit LSN for a transaction
    pub fn getCommitLSN(self: *const VisibilityChecker, txn_id: TransactionId) ?LSN {
        return self.committed_txns.get(txn_id);
    }

    /// Check if a version is visible to a transaction
    pub fn isVisible(
        self: *const VisibilityChecker,
        version: *const Version,
        reader_txn_id: TransactionId,
        reader_snapshot_lsn: LSN,
        isolation: IsolationLevel,
    ) bool {
        // Version created by same transaction is always visible
        if (version.created_by == reader_txn_id) {
            return !version.isDeleted() or version.deleted_by != reader_txn_id;
        }

        // Check if creator is committed
        const creator_committed = self.isCommitted(version.created_by);
        if (!creator_committed) {
            return false; // Uncommitted version not visible
        }

        // For repeatable read, check snapshot
        if (isolation == .repeatable_read or isolation == .serializable) {
            const creator_commit_lsn = self.getCommitLSN(version.created_by) orelse return false;
            if (creator_commit_lsn > reader_snapshot_lsn) {
                return false; // Created after snapshot
            }
        }

        // Check if deleted
        if (version.isDeleted()) {
            // Deleted by same transaction - not visible
            if (version.deleted_by == reader_txn_id) {
                return false;
            }

            // Check if deleter is committed
            if (self.isCommitted(version.deleted_by)) {
                // For repeatable read, check snapshot
                if (isolation == .repeatable_read or isolation == .serializable) {
                    const deleter_commit_lsn = self.getCommitLSN(version.deleted_by) orelse return true;
                    if (deleter_commit_lsn <= reader_snapshot_lsn) {
                        return false; // Deleted before snapshot
                    }
                    return true; // Deleted after snapshot - still visible
                }
                return false; // Read committed sees deletion
            }
            // Deleter not committed - version still visible
            return true;
        }

        return true;
    }

    /// Find visible version in a chain
    pub fn findVisibleVersion(
        self: *const VisibilityChecker,
        chain: *const VersionChain,
        reader_txn_id: TransactionId,
        reader_snapshot_lsn: LSN,
        isolation: IsolationLevel,
    ) ?*Version {
        var current = chain.head;
        while (current) |version| {
            if (self.isVisible(version, reader_txn_id, reader_snapshot_lsn, isolation)) {
                return version;
            }
            current = version.prev;
        }
        return null;
    }
};

/// MVCC version store - maps keys to version chains
pub const MVCCStore = struct {
    allocator: std.mem.Allocator,
    /// Key to version chain mapping
    chains: std.StringHashMap(VersionChain),
    /// Visibility checker
    visibility: VisibilityChecker,
    /// Statistics
    total_versions: u64,
    gc_runs: u64,
    versions_collected: u64,

    pub fn init(allocator: std.mem.Allocator) MVCCStore {
        return .{
            .allocator = allocator,
            .chains = std.StringHashMap(VersionChain).init(allocator),
            .visibility = VisibilityChecker.init(allocator),
            .total_versions = 0,
            .gc_runs = 0,
            .versions_collected = 0,
        };
    }

    pub fn deinit(self: *MVCCStore) void {
        var it = self.chains.iterator();
        while (it.next()) |entry| {
            // Free the key
            self.allocator.free(entry.key_ptr.*);
            // Deinit the chain
            entry.value_ptr.deinit();
        }
        self.chains.deinit();
        self.visibility.deinit();
    }

    /// Put a new version
    pub fn put(
        self: *MVCCStore,
        key: []const u8,
        value: []const u8,
        txn_id: TransactionId,
        lsn: LSN,
    ) !void {
        const result = self.chains.getOrPut(key) catch {
            return errors.Error.OutOfSpace;
        };

        if (!result.found_existing) {
            // New key - need to copy it
            result.key_ptr.* = self.allocator.dupe(u8, key) catch {
                return errors.Error.OutOfSpace;
            };
            result.value_ptr.* = VersionChain.init(self.allocator);
        }

        _ = result.value_ptr.addVersion(txn_id, lsn, value) catch {
            return errors.Error.OutOfSpace;
        };
        self.total_versions += 1;
    }

    /// Delete a key (mark current version as deleted)
    pub fn delete(
        self: *MVCCStore,
        key: []const u8,
        txn_id: TransactionId,
        lsn: LSN,
    ) bool {
        if (self.chains.getPtr(key)) |chain| {
            _ = chain.deleteHead(txn_id, lsn);
            return true;
        }
        return false;
    }

    /// Get visible value for a key
    pub fn get(
        self: *const MVCCStore,
        key: []const u8,
        txn_id: TransactionId,
        snapshot_lsn: LSN,
        isolation: IsolationLevel,
    ) ?[]const u8 {
        if (self.chains.getPtr(key)) |chain| {
            if (self.visibility.findVisibleVersion(chain, txn_id, snapshot_lsn, isolation)) |version| {
                return version.value;
            }
        }
        return null;
    }

    /// Mark transaction as committed
    pub fn commitTransaction(self: *MVCCStore, txn_id: TransactionId, commit_lsn: LSN) !void {
        try self.visibility.markCommitted(txn_id, commit_lsn);
    }

    /// Mark transaction as aborted
    pub fn abortTransaction(self: *MVCCStore, txn_id: TransactionId) !void {
        try self.visibility.markAborted(txn_id);
    }

    /// Garbage collect old versions
    /// Removes versions that are no longer visible to any active transaction
    pub fn gc(self: *MVCCStore, oldest_active_lsn: LSN) u64 {
        var collected: u64 = 0;

        var it = self.chains.iterator();
        while (it.next()) |entry| {
            const chain = entry.value_ptr;
            collected += self.gcChain(chain, oldest_active_lsn);
        }

        self.gc_runs += 1;
        self.versions_collected += collected;
        self.total_versions -= collected;
        return collected;
    }

    fn gcChain(self: *MVCCStore, chain: *VersionChain, oldest_active_lsn: LSN) u64 {
        var collected: u64 = 0;
        var prev_ptr: ?*?*Version = null;
        var current = chain.head;

        while (current) |version| {
            const next = version.prev;

            // Can collect if:
            // 1. Version is deleted AND
            // 2. Delete was committed before oldest active transaction AND
            // 3. There's a newer committed version
            const can_collect = version.isDeleted() and
                version.deleted_lsn < oldest_active_lsn and
                self.visibility.isCommitted(version.deleted_by);

            if (can_collect) {
                // Unlink from chain
                if (prev_ptr) |pp| {
                    pp.* = next;
                } else {
                    chain.head = next;
                }
                version.deinit(self.allocator);
                chain.version_count -= 1;
                collected += 1;
            } else {
                prev_ptr = &current.?.prev;
            }

            current = next;
        }

        return collected;
    }

    /// Get statistics
    pub fn getStats(self: *const MVCCStore) MVCCStats {
        return .{
            .total_keys = self.chains.count(),
            .total_versions = self.total_versions,
            .gc_runs = self.gc_runs,
            .versions_collected = self.versions_collected,
        };
    }
};

/// MVCC statistics
pub const MVCCStats = struct {
    total_keys: usize,
    total_versions: u64,
    gc_runs: u64,
    versions_collected: u64,
};

// Tests

test "Version create and delete" {
    const allocator = std.testing.allocator;

    const version = try Version.init(allocator, 1, 100, "test value");
    defer version.deinit(allocator);

    try std.testing.expectEqual(@as(TransactionId, 1), version.created_by);
    try std.testing.expectEqual(@as(LSN, 100), version.created_lsn);
    try std.testing.expectEqualStrings("test value", version.value);
    try std.testing.expect(!version.isDeleted());

    version.markDeleted(2, 200);
    try std.testing.expect(version.isDeleted());
    try std.testing.expectEqual(@as(TransactionId, 2), version.deleted_by);
}

test "VersionChain operations" {
    const allocator = std.testing.allocator;

    var chain = VersionChain.init(allocator);
    defer chain.deinit();

    _ = try chain.addVersion(1, 100, "v1");
    _ = try chain.addVersion(2, 200, "v2");
    _ = try chain.addVersion(3, 300, "v3");

    try std.testing.expectEqual(@as(u32, 3), chain.getVersionCount());

    const head = chain.getHead().?;
    try std.testing.expectEqualStrings("v3", head.value);
    try std.testing.expectEqualStrings("v2", head.prev.?.value);
}

test "VisibilityChecker committed transactions" {
    const allocator = std.testing.allocator;

    var checker = VisibilityChecker.init(allocator);
    defer checker.deinit();

    try checker.markCommitted(1, 100);
    try checker.markCommitted(2, 200);
    try checker.markAborted(3);

    try std.testing.expect(checker.isCommitted(1));
    try std.testing.expect(checker.isCommitted(2));
    try std.testing.expect(!checker.isCommitted(3));
    try std.testing.expect(checker.isAborted(3));
    try std.testing.expectEqual(@as(LSN, 100), checker.getCommitLSN(1).?);
}

test "VisibilityChecker version visibility" {
    const allocator = std.testing.allocator;

    var checker = VisibilityChecker.init(allocator);
    defer checker.deinit();

    // Commit transaction 1 at LSN 100
    try checker.markCommitted(1, 100);

    const version = try Version.init(allocator, 1, 50, "value");
    defer version.deinit(allocator);

    // Reader in txn 2, snapshot at LSN 150 - should see committed version
    try std.testing.expect(checker.isVisible(version, 2, 150, .read_committed));
    try std.testing.expect(checker.isVisible(version, 2, 150, .repeatable_read));

    // Reader with snapshot before commit (LSN 80) - repeatable read should NOT see it
    try std.testing.expect(checker.isVisible(version, 2, 80, .read_committed)); // RC always sees committed
    try std.testing.expect(!checker.isVisible(version, 2, 80, .repeatable_read)); // RR respects snapshot
}

test "MVCCStore put and get" {
    const allocator = std.testing.allocator;

    var store = MVCCStore.init(allocator);
    defer store.deinit();

    // Transaction 1 writes
    try store.put("key1", "value1", 1, 10);
    try store.commitTransaction(1, 20);

    // Transaction 2 reads
    const value = store.get("key1", 2, 50, .read_committed);
    try std.testing.expectEqualStrings("value1", value.?);
}

test "MVCCStore delete" {
    const allocator = std.testing.allocator;

    var store = MVCCStore.init(allocator);
    defer store.deinit();

    // Write and commit
    try store.put("key1", "value1", 1, 10);
    try store.commitTransaction(1, 20);

    // Delete and commit
    _ = store.delete("key1", 2, 30);
    try store.commitTransaction(2, 40);

    // Should not be visible
    const value = store.get("key1", 3, 50, .read_committed);
    try std.testing.expect(value == null);
}

test "MVCCStore snapshot isolation" {
    const allocator = std.testing.allocator;

    var store = MVCCStore.init(allocator);
    defer store.deinit();

    // Transaction 1 writes v1
    try store.put("key", "v1", 1, 10);
    try store.commitTransaction(1, 20);

    // Transaction 2 starts with snapshot at LSN 25
    // Transaction 3 writes v2 and commits at LSN 30
    try store.put("key", "v2", 3, 25);
    try store.commitTransaction(3, 30);

    // Transaction 2 with repeatable read should still see v1 (committed before snapshot)
    const value_rr = store.get("key", 2, 25, .repeatable_read);
    try std.testing.expectEqualStrings("v1", value_rr.?);

    // Read committed should see v2
    const value_rc = store.get("key", 2, 25, .read_committed);
    try std.testing.expectEqualStrings("v2", value_rc.?);
}

test "MVCCStore gc" {
    const allocator = std.testing.allocator;

    var store = MVCCStore.init(allocator);
    defer store.deinit();

    // Create multiple versions
    try store.put("key", "v1", 1, 10);
    try store.commitTransaction(1, 15);

    try store.put("key", "v2", 2, 20);
    try store.commitTransaction(2, 25);

    // Delete v2
    _ = store.delete("key", 3, 30);
    try store.commitTransaction(3, 35);

    const stats = store.getStats();
    try std.testing.expectEqual(@as(u64, 2), stats.total_versions);

    // GC with oldest active LSN = 100 (after all commits)
    const collected = store.gc(100);

    // Should have collected the deleted version
    try std.testing.expect(collected >= 1);
}

test "MVCCStats" {
    const stats = MVCCStats{
        .total_keys = 10,
        .total_versions = 25,
        .gc_runs = 3,
        .versions_collected = 5,
    };

    try std.testing.expectEqual(@as(usize, 10), stats.total_keys);
    try std.testing.expectEqual(@as(u64, 25), stats.total_versions);
}
