const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");

const TransactionId = types.TransactionId;
const Key = types.Key;
const INVALID_TXN_ID = types.INVALID_TXN_ID;

/// Lock mode
pub const LockMode = enum {
    /// Shared lock (read)
    shared,
    /// Exclusive lock (write)
    exclusive,
};

/// Lock request
pub const LockRequest = struct {
    txn_id: TransactionId,
    mode: LockMode,
    granted: bool,
};

/// Lock entry for a single key
pub const LockEntry = struct {
    /// Key (owned copy)
    key: []u8,
    /// Lock mode currently held
    mode: ?LockMode,
    /// Transactions holding the lock
    holders: std.ArrayListUnmanaged(TransactionId),
    /// Waiting requests queue
    waiters: std.ArrayListUnmanaged(LockRequest),
    /// Allocator
    allocator: std.mem.Allocator,

    /// Initialize lock entry
    pub fn init(allocator: std.mem.Allocator, key: Key) !LockEntry {
        const key_copy = try allocator.alloc(u8, key.len);
        @memcpy(key_copy, key);

        return .{
            .key = key_copy,
            .mode = null,
            .holders = .{},
            .waiters = .{},
            .allocator = allocator,
        };
    }

    /// Free resources
    pub fn deinit(self: *LockEntry) void {
        self.allocator.free(self.key);
        self.holders.deinit(self.allocator);
        self.waiters.deinit(self.allocator);
    }

    /// Check if lock can be granted
    pub fn canGrant(self: *const LockEntry, mode: LockMode) bool {
        if (self.mode == null) return true;

        return switch (mode) {
            .shared => self.mode.? == .shared,
            .exclusive => false,
        };
    }

    /// Try to acquire lock
    pub fn acquire(self: *LockEntry, txn_id: TransactionId, mode: LockMode) !bool {
        // Check if this transaction already holds the lock
        for (self.holders.items) |holder| {
            if (holder == txn_id) {
                // Already holding - check for upgrade
                if (mode == .exclusive and self.mode.? == .shared) {
                    // Can only upgrade if we're the only holder
                    if (self.holders.items.len == 1) {
                        self.mode = .exclusive;
                        return true;
                    }
                    // Need to wait for upgrade
                    try self.waiters.append(self.allocator, .{
                        .txn_id = txn_id,
                        .mode = mode,
                        .granted = false,
                    });
                    return false;
                }
                return true;
            }
        }

        // Check if we can grant immediately
        if (self.canGrant(mode)) {
            try self.holders.append(self.allocator, txn_id);
            self.mode = mode;
            return true;
        }

        // Add to wait queue
        try self.waiters.append(self.allocator, .{
            .txn_id = txn_id,
            .mode = mode,
            .granted = false,
        });
        return false;
    }

    /// Release lock
    pub fn release(self: *LockEntry, txn_id: TransactionId) void {
        // Remove from holders
        var i: usize = 0;
        while (i < self.holders.items.len) {
            if (self.holders.items[i] == txn_id) {
                _ = self.holders.orderedRemove(i);
            } else {
                i += 1;
            }
        }

        // If no more holders, clear mode and try to grant waiters
        if (self.holders.items.len == 0) {
            self.mode = null;
            self.grantWaiters() catch {};
        }
    }

    /// Grant locks to waiting transactions
    fn grantWaiters(self: *LockEntry) !void {
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const req = &self.waiters.items[i];
            if (!req.granted and self.canGrant(req.mode)) {
                try self.holders.append(self.allocator, req.txn_id);
                self.mode = req.mode;
                req.granted = true;

                // If exclusive, stop granting
                if (req.mode == .exclusive) break;
            }
            i += 1;
        }

        // Remove granted requests
        i = 0;
        while (i < self.waiters.items.len) {
            if (self.waiters.items[i].granted) {
                _ = self.waiters.orderedRemove(i);
            } else {
                i += 1;
            }
        }
    }

    /// Check if transaction holds the lock
    pub fn isHeldBy(self: *const LockEntry, txn_id: TransactionId) bool {
        for (self.holders.items) |holder| {
            if (holder == txn_id) return true;
        }
        return false;
    }
};

/// Lock manager
pub const LockManager = struct {
    /// Key to lock entry mapping
    locks: std.StringHashMap(*LockEntry),
    /// Transaction to locks mapping (for cleanup)
    txn_locks: std.AutoHashMap(TransactionId, std.ArrayListUnmanaged([]const u8)),
    /// Allocator
    allocator: std.mem.Allocator,
    /// Mutex for thread safety
    mutex: std.Thread.Mutex,
    /// Lock timeout in milliseconds
    timeout_ms: u64,

    /// Initialize lock manager
    pub fn init(allocator: std.mem.Allocator, timeout_ms: u64) LockManager {
        return .{
            .locks = std.StringHashMap(*LockEntry).init(allocator),
            .txn_locks = std.AutoHashMap(TransactionId, std.ArrayListUnmanaged([]const u8)).init(allocator),
            .allocator = allocator,
            .mutex = .{},
            .timeout_ms = timeout_ms,
        };
    }

    /// Free resources
    pub fn deinit(self: *LockManager) void {
        var iter = self.locks.valueIterator();
        while (iter.next()) |entry_ptr| {
            entry_ptr.*.deinit();
            self.allocator.destroy(entry_ptr.*);
        }
        self.locks.deinit();

        var txn_iter = self.txn_locks.valueIterator();
        while (txn_iter.next()) |list_ptr| {
            list_ptr.deinit(self.allocator);
        }
        self.txn_locks.deinit();
    }

    /// Get or create lock entry
    fn getOrCreateEntry(self: *LockManager, key: Key) !*LockEntry {
        if (self.locks.get(key)) |entry| {
            return entry;
        }

        const entry = try self.allocator.create(LockEntry);
        entry.* = try LockEntry.init(self.allocator, key);
        try self.locks.put(entry.key, entry);
        return entry;
    }

    /// Acquire a lock (blocking with timeout)
    pub fn acquire(self: *LockManager, txn_id: TransactionId, key: Key, mode: LockMode) !void {
        self.mutex.lock();

        const entry = try self.getOrCreateEntry(key);
        const granted = try entry.acquire(txn_id, mode);

        if (granted) {
            try self.recordLock(txn_id, key);
            self.mutex.unlock();
            return;
        }

        self.mutex.unlock();

        // Wait for lock
        const start = std.time.milliTimestamp();
        while (std.time.milliTimestamp() - start < @as(i64, @intCast(self.timeout_ms))) {
            std.Thread.sleep(1_000_000); // 1ms

            self.mutex.lock();
            // Check if we got the lock
            if (entry.isHeldBy(txn_id)) {
                try self.recordLock(txn_id, key);
                self.mutex.unlock();
                return;
            }
            self.mutex.unlock();
        }

        // Timeout - remove from waiters
        self.mutex.lock();
        defer self.mutex.unlock();

        var i: usize = 0;
        while (i < entry.waiters.items.len) {
            if (entry.waiters.items[i].txn_id == txn_id) {
                _ = entry.waiters.orderedRemove(i);
            } else {
                i += 1;
            }
        }

        return errors.Error.LockTimeout;
    }

    /// Try to acquire lock without waiting
    pub fn tryAcquire(self: *LockManager, txn_id: TransactionId, key: Key, mode: LockMode) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = try self.getOrCreateEntry(key);

        // Only grant if immediately available
        if (entry.canGrant(mode)) {
            try entry.holders.append(entry.allocator, txn_id);
            entry.mode = mode;
            try self.recordLock(txn_id, key);
            return true;
        }

        return false;
    }

    /// Release a specific lock
    pub fn release(self: *LockManager, txn_id: TransactionId, key: Key) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.locks.get(key)) |entry| {
            entry.release(txn_id);
        }
    }

    /// Release all locks for a transaction
    pub fn releaseAll(self: *LockManager, txn_id: TransactionId) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.txn_locks.getPtr(txn_id)) |list| {
            for (list.items) |key| {
                if (self.locks.get(key)) |entry| {
                    entry.release(txn_id);
                }
            }
            list.deinit(self.allocator);
            _ = self.txn_locks.remove(txn_id);
        }
    }

    /// Record that a transaction holds a lock
    fn recordLock(self: *LockManager, txn_id: TransactionId, key: Key) !void {
        const gop = try self.txn_locks.getOrPut(txn_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        }
        try gop.value_ptr.append(self.allocator, key);
    }

    /// Check for deadlock (simple cycle detection)
    pub fn hasDeadlock(self: *LockManager, txn_id: TransactionId, key: Key) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Simple wait-for graph cycle detection
        var visited = std.AutoHashMap(TransactionId, void).init(self.allocator);
        defer visited.deinit();

        return self.detectCycle(txn_id, key, &visited);
    }

    fn detectCycle(self: *LockManager, start_txn: TransactionId, key: Key, visited: *std.AutoHashMap(TransactionId, void)) bool {
        if (visited.contains(start_txn)) {
            return true;
        }
        visited.put(start_txn, {}) catch return false;

        const entry = self.locks.get(key) orelse return false;

        // Check if any holder is waiting for a lock we hold
        for (entry.holders.items) |holder| {
            if (holder == start_txn) continue;

            // Check what this holder is waiting for
            if (self.txn_locks.get(holder)) |held_keys| {
                for (held_keys.items) |held_key| {
                    if (self.detectCycle(holder, held_key, visited)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// Get lock stats
    pub fn stats(self: *const LockManager) LockStats {
        return .{
            .total_locks = self.locks.count(),
        };
    }
};

/// Lock statistics
pub const LockStats = struct {
    total_locks: usize,
};

// Tests

test "LockEntry shared lock" {
    const allocator = std.testing.allocator;

    var entry = try LockEntry.init(allocator, "key");
    defer entry.deinit();

    // First shared lock granted
    try std.testing.expect(try entry.acquire(1, .shared));
    try std.testing.expectEqual(LockMode.shared, entry.mode.?);

    // Second shared lock granted (compatible)
    try std.testing.expect(try entry.acquire(2, .shared));
    try std.testing.expectEqual(@as(usize, 2), entry.holders.items.len);
}

test "LockEntry exclusive lock" {
    const allocator = std.testing.allocator;

    var entry = try LockEntry.init(allocator, "key");
    defer entry.deinit();

    // First exclusive lock granted
    try std.testing.expect(try entry.acquire(1, .exclusive));
    try std.testing.expectEqual(LockMode.exclusive, entry.mode.?);

    // Second exclusive lock blocked
    try std.testing.expect(!try entry.acquire(2, .exclusive));
    try std.testing.expectEqual(@as(usize, 1), entry.waiters.items.len);
}

test "LockEntry shared blocked by exclusive" {
    const allocator = std.testing.allocator;

    var entry = try LockEntry.init(allocator, "key");
    defer entry.deinit();

    // Exclusive lock first
    try std.testing.expect(try entry.acquire(1, .exclusive));

    // Shared lock blocked
    try std.testing.expect(!try entry.acquire(2, .shared));
}

test "LockEntry release grants waiters" {
    const allocator = std.testing.allocator;

    var entry = try LockEntry.init(allocator, "key");
    defer entry.deinit();

    // Exclusive lock
    try std.testing.expect(try entry.acquire(1, .exclusive));

    // Waiter
    try std.testing.expect(!try entry.acquire(2, .shared));

    // Release exclusive
    entry.release(1);

    // Waiter should be granted
    try std.testing.expect(entry.isHeldBy(2));
}

test "LockManager basic" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator, 100);
    defer manager.deinit();

    // Acquire shared lock
    try manager.acquire(1, "key", .shared);

    // Release
    manager.release(1, "key");
}

test "LockManager tryAcquire" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator, 100);
    defer manager.deinit();

    // First exclusive succeeds
    try std.testing.expect(try manager.tryAcquire(1, "key", .exclusive));

    // Second fails immediately
    try std.testing.expect(!try manager.tryAcquire(2, "key", .shared));

    manager.releaseAll(1);
}

test "LockManager releaseAll" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator, 100);
    defer manager.deinit();

    try manager.acquire(1, "key1", .shared);
    try manager.acquire(1, "key2", .exclusive);

    // Release all for txn 1
    manager.releaseAll(1);

    // Now other txn can acquire
    try std.testing.expect(try manager.tryAcquire(2, "key1", .exclusive));
    try std.testing.expect(try manager.tryAcquire(2, "key2", .exclusive));

    manager.releaseAll(2);
}

test "LockManager stats" {
    const allocator = std.testing.allocator;

    var manager = LockManager.init(allocator, 100);
    defer manager.deinit();

    try std.testing.expectEqual(@as(usize, 0), manager.stats().total_locks);

    try manager.acquire(1, "key1", .shared);
    try std.testing.expectEqual(@as(usize, 1), manager.stats().total_locks);

    try manager.acquire(1, "key2", .shared);
    try std.testing.expectEqual(@as(usize, 2), manager.stats().total_locks);

    manager.releaseAll(1);
}
