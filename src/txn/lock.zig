const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");

const TransactionId = types.TransactionId;
const PageId = types.PageId;
const INVALID_TXN_ID = types.INVALID_TXN_ID;

/// Lock mode
pub const LockMode = enum {
    /// Shared lock (read)
    shared,
    /// Exclusive lock (write)
    exclusive,
};

/// Lock request status
pub const LockStatus = enum {
    /// Lock granted
    granted,
    /// Lock waiting
    waiting,
    /// Lock denied (timeout or conflict)
    denied,
};

/// A single lock holder
const LockHolder = struct {
    txn_id: TransactionId,
    mode: LockMode,
};

/// Lock entry for a resource
const LockEntry = struct {
    /// Current holders of the lock
    holders: std.ArrayList(LockHolder),
    /// Waiting requests
    waiters: std.ArrayList(LockHolder),
    /// Allocator
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) LockEntry {
        return .{
            .holders = .{},
            .waiters = .{},
            .allocator = allocator,
        };
    }

    fn deinit(self: *LockEntry) void {
        self.holders.deinit(self.allocator);
        self.waiters.deinit(self.allocator);
    }

    /// Check if lock can be granted
    fn canGrant(self: *const LockEntry, mode: LockMode, txn_id: TransactionId) bool {
        // If no holders, always grant
        if (self.holders.items.len == 0) return true;

        // Check existing holders
        for (self.holders.items) |holder| {
            // Same transaction can upgrade
            if (holder.txn_id == txn_id) continue;

            // Shared locks are compatible with each other
            if (mode == .shared and holder.mode == .shared) continue;

            // Any exclusive lock conflicts
            return false;
        }

        return true;
    }

    /// Check if transaction already holds this lock
    fn isHeldBy(self: *const LockEntry, txn_id: TransactionId) bool {
        for (self.holders.items) |holder| {
            if (holder.txn_id == txn_id) return true;
        }
        return false;
    }

    /// Get lock mode held by transaction
    fn getModeHeldBy(self: *const LockEntry, txn_id: TransactionId) ?LockMode {
        for (self.holders.items) |holder| {
            if (holder.txn_id == txn_id) return holder.mode;
        }
        return null;
    }

    /// Add a holder
    fn addHolder(self: *LockEntry, txn_id: TransactionId, mode: LockMode) !void {
        // Check for upgrade
        for (self.holders.items) |*holder| {
            if (holder.txn_id == txn_id) {
                // Upgrade to exclusive if needed
                if (mode == .exclusive) {
                    holder.mode = .exclusive;
                }
                return;
            }
        }
        try self.holders.append(self.allocator, .{ .txn_id = txn_id, .mode = mode });
    }

    /// Remove a holder
    fn removeHolder(self: *LockEntry, txn_id: TransactionId) bool {
        for (self.holders.items, 0..) |holder, i| {
            if (holder.txn_id == txn_id) {
                _ = self.holders.orderedRemove(i);
                return true;
            }
        }
        return false;
    }

    /// Add a waiter
    fn addWaiter(self: *LockEntry, txn_id: TransactionId, mode: LockMode) !void {
        try self.waiters.append(self.allocator, .{ .txn_id = txn_id, .mode = mode });
    }

    /// Remove a waiter
    fn removeWaiter(self: *LockEntry, txn_id: TransactionId) void {
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            if (self.waiters.items[i].txn_id == txn_id) {
                _ = self.waiters.orderedRemove(i);
            } else {
                i += 1;
            }
        }
    }

    /// Try to grant waiting locks
    fn grantWaiters(self: *LockEntry) u32 {
        var granted: u32 = 0;
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const waiter = self.waiters.items[i];
            if (self.canGrant(waiter.mode, waiter.txn_id)) {
                self.addHolder(waiter.txn_id, waiter.mode) catch {
                    i += 1;
                    continue;
                };
                _ = self.waiters.orderedRemove(i);
                granted += 1;
            } else {
                i += 1;
            }
        }
        return granted;
    }
};

/// Lock manager for concurrency control
pub const LockManager = struct {
    allocator: std.mem.Allocator,
    /// Page locks: page_id -> lock entry
    page_locks: std.AutoHashMap(PageId, LockEntry),
    /// Key locks: key hash -> lock entry
    key_locks: std.AutoHashMap(u64, LockEntry),
    /// Default timeout in nanoseconds
    default_timeout_ns: u64,
    /// Statistics
    locks_acquired: u64,
    locks_released: u64,
    lock_waits: u64,
    lock_timeouts: u64,

    pub fn init(allocator: std.mem.Allocator) LockManager {
        return .{
            .allocator = allocator,
            .page_locks = std.AutoHashMap(PageId, LockEntry).init(allocator),
            .key_locks = std.AutoHashMap(u64, LockEntry).init(allocator),
            .default_timeout_ns = 5 * std.time.ns_per_s, // 5 seconds
            .locks_acquired = 0,
            .locks_released = 0,
            .lock_waits = 0,
            .lock_timeouts = 0,
        };
    }

    pub fn deinit(self: *LockManager) void {
        var page_it = self.page_locks.iterator();
        while (page_it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.page_locks.deinit();

        var key_it = self.key_locks.iterator();
        while (key_it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.key_locks.deinit();
    }

    /// Set default timeout
    pub fn setTimeout(self: *LockManager, timeout_ns: u64) void {
        self.default_timeout_ns = timeout_ns;
    }

    /// Acquire a page lock
    pub fn lockPage(
        self: *LockManager,
        page_id: PageId,
        txn_id: TransactionId,
        mode: LockMode,
    ) errors.MonolithError!LockStatus {
        const result = self.page_locks.getOrPut(page_id) catch {
            return errors.Error.OutOfSpace;
        };

        if (!result.found_existing) {
            result.value_ptr.* = LockEntry.init(self.allocator);
        }

        return self.acquireLock(result.value_ptr, txn_id, mode);
    }

    /// Release a page lock
    pub fn unlockPage(
        self: *LockManager,
        page_id: PageId,
        txn_id: TransactionId,
    ) bool {
        if (self.page_locks.getPtr(page_id)) |entry| {
            if (entry.removeHolder(txn_id)) {
                self.locks_released += 1;
                _ = entry.grantWaiters();
                return true;
            }
        }
        return false;
    }

    /// Acquire a key lock
    pub fn lockKey(
        self: *LockManager,
        key: []const u8,
        txn_id: TransactionId,
        mode: LockMode,
    ) errors.MonolithError!LockStatus {
        const key_hash = std.hash.Wyhash.hash(0, key);

        const result = self.key_locks.getOrPut(key_hash) catch {
            return errors.Error.OutOfSpace;
        };

        if (!result.found_existing) {
            result.value_ptr.* = LockEntry.init(self.allocator);
        }

        return self.acquireLock(result.value_ptr, txn_id, mode);
    }

    /// Release a key lock
    pub fn unlockKey(
        self: *LockManager,
        key: []const u8,
        txn_id: TransactionId,
    ) bool {
        const key_hash = std.hash.Wyhash.hash(0, key);

        if (self.key_locks.getPtr(key_hash)) |entry| {
            if (entry.removeHolder(txn_id)) {
                self.locks_released += 1;
                _ = entry.grantWaiters();
                return true;
            }
        }
        return false;
    }

    /// Release all locks held by a transaction
    pub fn releaseAll(self: *LockManager, txn_id: TransactionId) u32 {
        var released: u32 = 0;

        // Release page locks
        var page_it = self.page_locks.iterator();
        while (page_it.next()) |entry| {
            if (entry.value_ptr.removeHolder(txn_id)) {
                released += 1;
                _ = entry.value_ptr.grantWaiters();
            }
            entry.value_ptr.removeWaiter(txn_id);
        }

        // Release key locks
        var key_it = self.key_locks.iterator();
        while (key_it.next()) |entry| {
            if (entry.value_ptr.removeHolder(txn_id)) {
                released += 1;
                _ = entry.value_ptr.grantWaiters();
            }
            entry.value_ptr.removeWaiter(txn_id);
        }

        self.locks_released += released;
        return released;
    }

    /// Try to acquire a lock
    fn acquireLock(
        self: *LockManager,
        entry: *LockEntry,
        txn_id: TransactionId,
        mode: LockMode,
    ) errors.MonolithError!LockStatus {
        // Already held?
        if (entry.isHeldBy(txn_id)) {
            const current_mode = entry.getModeHeldBy(txn_id).?;
            if (mode == .shared or current_mode == .exclusive) {
                return .granted; // Already have sufficient lock
            }
            // Need upgrade
        }

        // Can grant immediately?
        if (entry.canGrant(mode, txn_id)) {
            entry.addHolder(txn_id, mode) catch {
                return errors.Error.OutOfSpace;
            };
            self.locks_acquired += 1;
            return .granted;
        }

        // Would need to wait
        self.lock_waits += 1;
        return .waiting;
    }

    /// Try to acquire page lock with timeout-based waiting
    pub fn tryLockPageWithWait(
        self: *LockManager,
        page_id: PageId,
        txn_id: TransactionId,
        mode: LockMode,
    ) errors.MonolithError!LockStatus {
        return self.lockWithTimeout(page_id, null, txn_id, mode, self.default_timeout_ns);
    }

    /// Try to acquire key lock with timeout-based waiting
    pub fn tryLockKeyWithWait(
        self: *LockManager,
        key: []const u8,
        txn_id: TransactionId,
        mode: LockMode,
    ) errors.MonolithError!LockStatus {
        const key_hash = std.hash.Wyhash.hash(0, key);
        return self.lockWithTimeout(null, key_hash, txn_id, mode, self.default_timeout_ns);
    }

    /// Internal: acquire lock with timeout
    fn lockWithTimeout(
        self: *LockManager,
        page_id: ?PageId,
        key_hash: ?u64,
        txn_id: TransactionId,
        mode: LockMode,
        timeout_ns: u64,
    ) errors.MonolithError!LockStatus {
        const start_time = std.time.nanoTimestamp();
        const deadline = start_time + @as(i128, timeout_ns);

        // Spin interval starts small and grows
        var spin_ns: u64 = 1000; // 1 microsecond
        const max_spin_ns: u64 = 10_000_000; // 10 milliseconds

        while (true) {
            // Try to acquire lock
            const status = if (page_id) |pid|
                try self.lockPage(pid, txn_id, mode)
            else if (key_hash) |kh|
                try self.lockKeyHash(kh, txn_id, mode)
            else
                return .denied;

            if (status == .granted) {
                return .granted;
            }

            // Check timeout
            const now = std.time.nanoTimestamp();
            if (now >= deadline) {
                self.lock_timeouts += 1;
                return .denied;
            }

            // Sleep with exponential backoff
            std.time.sleep(spin_ns);
            spin_ns = @min(spin_ns * 2, max_spin_ns);
        }
    }

    /// Lock a key by its hash (internal)
    fn lockKeyHash(
        self: *LockManager,
        key_hash: u64,
        txn_id: TransactionId,
        mode: LockMode,
    ) errors.MonolithError!LockStatus {
        const result = self.key_locks.getOrPut(key_hash) catch {
            return errors.Error.OutOfSpace;
        };

        if (!result.found_existing) {
            result.value_ptr.* = LockEntry.init(self.allocator);
        }

        return self.acquireLock(result.value_ptr, txn_id, mode);
    }

    /// Get lock statistics
    pub fn getStats(self: *const LockManager) LockStats {
        return .{
            .acquired = self.locks_acquired,
            .released = self.locks_released,
            .waits = self.lock_waits,
            .timeouts = self.lock_timeouts,
            .page_locks = self.page_locks.count(),
            .key_locks = self.key_locks.count(),
        };
    }

    /// Check if transaction holds a lock
    pub fn holdsPageLock(self: *const LockManager, page_id: PageId, txn_id: TransactionId) bool {
        if (self.page_locks.getPtr(page_id)) |entry| {
            return entry.isHeldBy(txn_id);
        }
        return false;
    }

    /// Check if transaction holds a key lock
    pub fn holdsKeyLock(self: *const LockManager, key: []const u8, txn_id: TransactionId) bool {
        const key_hash = std.hash.Wyhash.hash(0, key);
        if (self.key_locks.getPtr(key_hash)) |entry| {
            return entry.isHeldBy(txn_id);
        }
        return false;
    }
};

/// Lock statistics
pub const LockStats = struct {
    acquired: u64,
    released: u64,
    waits: u64,
    timeouts: u64,
    page_locks: usize,
    key_locks: usize,
};

// Tests

test "LockManager init" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator);
    defer manager.deinit();

    const stats = manager.getStats();
    try std.testing.expectEqual(@as(u64, 0), stats.acquired);
}

test "LockManager page lock" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator);
    defer manager.deinit();

    // Acquire exclusive lock
    const status = try manager.lockPage(1, 100, .exclusive);
    try std.testing.expectEqual(LockStatus.granted, status);
    try std.testing.expect(manager.holdsPageLock(1, 100));

    // Release
    try std.testing.expect(manager.unlockPage(1, 100));
    try std.testing.expect(!manager.holdsPageLock(1, 100));
}

test "LockManager shared locks compatible" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator);
    defer manager.deinit();

    // Multiple shared locks should be compatible
    const s1 = try manager.lockPage(1, 100, .shared);
    const s2 = try manager.lockPage(1, 101, .shared);
    const s3 = try manager.lockPage(1, 102, .shared);

    try std.testing.expectEqual(LockStatus.granted, s1);
    try std.testing.expectEqual(LockStatus.granted, s2);
    try std.testing.expectEqual(LockStatus.granted, s3);
}

test "LockManager exclusive blocks" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator);
    defer manager.deinit();

    // Acquire exclusive lock
    const x1 = try manager.lockPage(1, 100, .exclusive);
    try std.testing.expectEqual(LockStatus.granted, x1);

    // Another exclusive should wait
    const x2 = try manager.lockPage(1, 101, .exclusive);
    try std.testing.expectEqual(LockStatus.waiting, x2);

    // Shared should also wait
    const s1 = try manager.lockPage(1, 102, .shared);
    try std.testing.expectEqual(LockStatus.waiting, s1);
}

test "LockManager key lock" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator);
    defer manager.deinit();

    const status = try manager.lockKey("mykey", 100, .shared);
    try std.testing.expectEqual(LockStatus.granted, status);
    try std.testing.expect(manager.holdsKeyLock("mykey", 100));

    try std.testing.expect(manager.unlockKey("mykey", 100));
}

test "LockManager release all" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator);
    defer manager.deinit();

    // Acquire multiple locks
    _ = try manager.lockPage(1, 100, .exclusive);
    _ = try manager.lockPage(2, 100, .shared);
    _ = try manager.lockKey("k1", 100, .exclusive);
    _ = try manager.lockKey("k2", 100, .shared);

    // Release all for txn 100
    const released = manager.releaseAll(100);
    try std.testing.expectEqual(@as(u32, 4), released);
}

test "LockManager reentrant" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator);
    defer manager.deinit();

    // Same transaction can acquire same lock multiple times
    const s1 = try manager.lockPage(1, 100, .shared);
    const s2 = try manager.lockPage(1, 100, .shared);

    try std.testing.expectEqual(LockStatus.granted, s1);
    try std.testing.expectEqual(LockStatus.granted, s2);
}

test "LockManager upgrade" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator);
    defer manager.deinit();

    // Start with shared
    const s1 = try manager.lockPage(1, 100, .shared);
    try std.testing.expectEqual(LockStatus.granted, s1);

    // Upgrade to exclusive (same txn, no other holders)
    const x1 = try manager.lockPage(1, 100, .exclusive);
    try std.testing.expectEqual(LockStatus.granted, x1);
}

test "LockStats" {
    const stats = LockStats{
        .acquired = 100,
        .released = 80,
        .waits = 10,
        .timeouts = 2,
        .page_locks = 50,
        .key_locks = 30,
    };

    try std.testing.expectEqual(@as(u64, 100), stats.acquired);
    try std.testing.expectEqual(@as(usize, 50), stats.page_locks);
}
