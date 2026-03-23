const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const page_mod = @import("page.zig");

const PageId = types.PageId;
const PageType = types.PageType;
const Key = types.Key;
const Value = types.Value;
const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;
const PAGE_HEADER_SIZE = page_mod.PAGE_HEADER_SIZE;
const CHECKSUM_SIZE = page_mod.CHECKSUM_SIZE;
const Ordering = types.Ordering;
const compareKeys = types.compareKeys;

/// B+ tree node header size
/// Layout after page header:
///   0:     is_leaf (u8, 1 = leaf, 0 = internal)
///   1-2:   key_count (u16)
///   3-10:  right_sibling (PageId, for leaves only)
///   11-12: reserved
pub const NODE_HEADER_SIZE: usize = 13;

/// Slot entry for variable-length keys/values
/// Each slot points to key/value data in the page
pub const Slot = struct {
    /// Offset to key data from start of data area
    key_offset: u16,
    /// Length of key
    key_len: u16,
    /// For leaf: offset to value data; for internal: child page ID (as u16 index into children array)
    value_offset: u16,
    /// For leaf: length of value; for internal: unused
    value_len: u16,
};

pub const SLOT_SIZE: usize = @sizeOf(Slot);

/// Calculate max keys for a node (rough estimate for overflow detection)
pub fn maxKeysPerNode(page_size: usize) usize {
    // Usable space after headers
    const usable = page_size - PAGE_HEADER_SIZE - NODE_HEADER_SIZE - CHECKSUM_SIZE;
    // Assume average key+value of 32 bytes + slot overhead
    return usable / (32 + SLOT_SIZE);
}

/// B+ tree node (wraps a page buffer)
pub const BTreeNode = struct {
    /// Underlying buffer
    buffer: []u8,
    /// Page size
    page_size: usize,
    /// Is this a leaf node?
    is_leaf: bool,
    /// Number of keys in node
    key_count: u16,
    /// Right sibling page ID (leaves only)
    right_sibling: PageId,
    /// Free space pointer (end of used data area, grows downward)
    free_space: u16,
    /// Allocator for temporary operations
    allocator: std.mem.Allocator,

    /// Data area start (after page header + node header)
    const DATA_START: usize = PAGE_HEADER_SIZE + NODE_HEADER_SIZE;

    /// Max children we reserve space for in internal nodes
    const MAX_CHILDREN: usize = 256;

    /// Initialize a new empty node
    pub fn init(allocator: std.mem.Allocator, buffer: []u8, is_leaf: bool) BTreeNode {
        const page_size = buffer.len;

        // Initialize page header
        const page_type: PageType = if (is_leaf) .leaf else .internal;
        _ = page_mod.Page.init(buffer, INVALID_PAGE_ID, page_type);

        // Initialize node header
        const header_start = PAGE_HEADER_SIZE;
        buffer[header_start] = if (is_leaf) 1 else 0;
        buffer[header_start + 1] = 0; // key_count low
        buffer[header_start + 2] = 0; // key_count high
        @memcpy(buffer[header_start + 3 .. header_start + 11], std.mem.asBytes(&INVALID_PAGE_ID));

        // For internal nodes, reserve space at the end for children pointers
        const free_space: u16 = if (is_leaf)
            @intCast(page_size - CHECKSUM_SIZE)
        else
            @intCast(page_size - CHECKSUM_SIZE - MAX_CHILDREN * @sizeOf(PageId));

        return .{
            .buffer = buffer,
            .page_size = page_size,
            .is_leaf = is_leaf,
            .key_count = 0,
            .right_sibling = INVALID_PAGE_ID,
            .free_space = free_space,
            .allocator = allocator,
        };
    }

    /// Load an existing node from buffer
    pub fn load(allocator: std.mem.Allocator, buffer: []u8) !BTreeNode {
        const page_size = buffer.len;
        const header_start = PAGE_HEADER_SIZE;

        const is_leaf = buffer[header_start] == 1;
        const key_count = std.mem.bytesToValue(u16, buffer[header_start + 1 .. header_start + 3]);
        const right_sibling = std.mem.bytesToValue(PageId, buffer[header_start + 3 .. header_start + 11]);

        // Calculate free space by finding lowest used offset
        var free_space: u16 = if (is_leaf)
            @intCast(page_size - CHECKSUM_SIZE)
        else
            @intCast(page_size - CHECKSUM_SIZE - MAX_CHILDREN * @sizeOf(PageId));

        const slot_area_start = DATA_START;

        for (0..key_count) |i| {
            const slot = getSlotAt(buffer, slot_area_start, i);
            if (slot.key_offset < free_space) {
                free_space = slot.key_offset;
            }
            if (is_leaf and slot.value_offset < free_space) {
                free_space = slot.value_offset;
            }
        }

        return .{
            .buffer = buffer,
            .page_size = page_size,
            .is_leaf = is_leaf,
            .key_count = key_count,
            .right_sibling = right_sibling,
            .free_space = free_space,
            .allocator = allocator,
        };
    }

    /// Write node header back to buffer
    pub fn writeHeader(self: *BTreeNode) void {
        const header_start = PAGE_HEADER_SIZE;
        self.buffer[header_start] = if (self.is_leaf) 1 else 0;
        @memcpy(self.buffer[header_start + 1 .. header_start + 3], std.mem.asBytes(&self.key_count));
        @memcpy(self.buffer[header_start + 3 .. header_start + 11], std.mem.asBytes(&self.right_sibling));
    }

    /// Get slot at index
    fn getSlotAt(buffer: []const u8, slot_area_start: usize, index: usize) Slot {
        const slot_offset = slot_area_start + index * SLOT_SIZE;
        return .{
            .key_offset = std.mem.bytesToValue(u16, buffer[slot_offset .. slot_offset + 2]),
            .key_len = std.mem.bytesToValue(u16, buffer[slot_offset + 2 .. slot_offset + 4]),
            .value_offset = std.mem.bytesToValue(u16, buffer[slot_offset + 4 .. slot_offset + 6]),
            .value_len = std.mem.bytesToValue(u16, buffer[slot_offset + 6 .. slot_offset + 8]),
        };
    }

    /// Set slot at index
    fn setSlotAt(buffer: []u8, slot_area_start: usize, index: usize, slot: Slot) void {
        const slot_offset = slot_area_start + index * SLOT_SIZE;
        @memcpy(buffer[slot_offset .. slot_offset + 2], std.mem.asBytes(&slot.key_offset));
        @memcpy(buffer[slot_offset + 2 .. slot_offset + 4], std.mem.asBytes(&slot.key_len));
        @memcpy(buffer[slot_offset + 4 .. slot_offset + 6], std.mem.asBytes(&slot.value_offset));
        @memcpy(buffer[slot_offset + 6 .. slot_offset + 8], std.mem.asBytes(&slot.value_len));
    }

    /// Get key at index
    pub fn getKey(self: *const BTreeNode, index: usize) ?Key {
        if (index >= self.key_count) return null;
        const slot = getSlotAt(self.buffer, DATA_START, index);
        return self.buffer[slot.key_offset .. slot.key_offset + slot.key_len];
    }

    /// Get value at index (leaf nodes only)
    pub fn getValue(self: *const BTreeNode, index: usize) ?Value {
        if (!self.is_leaf or index >= self.key_count) return null;
        const slot = getSlotAt(self.buffer, DATA_START, index);
        return self.buffer[slot.value_offset .. slot.value_offset + slot.value_len];
    }

    /// Fixed offset for children array (stored at end of usable area, before checksum)
    /// Children grow backward from this point
    fn childrenBaseOffset(self: *const BTreeNode) usize {
        // Reserve space at end for max children (before checksum)
        // Store children in reverse order from end
        return self.page_size - CHECKSUM_SIZE - @sizeOf(PageId);
    }

    /// Get child page ID at index (internal nodes only)
    /// Internal nodes have n+1 children for n keys
    pub fn getChild(self: *const BTreeNode, index: usize) ?PageId {
        if (self.is_leaf) return null;
        if (index > self.key_count) return null;

        // Children stored backward from end of page (before checksum)
        const child_offset = self.childrenBaseOffset() - index * @sizeOf(PageId);
        return std.mem.bytesToValue(PageId, self.buffer[child_offset .. child_offset + 8]);
    }

    /// Set child page ID at index
    pub fn setChild(self: *BTreeNode, index: usize, child_id: PageId) void {
        if (self.is_leaf) return;
        const child_offset = self.childrenBaseOffset() - index * @sizeOf(PageId);
        @memcpy(self.buffer[child_offset .. child_offset + 8], std.mem.asBytes(&child_id));
    }

    /// Binary search for key, returns index where key should be
    pub fn searchKey(self: *const BTreeNode, key: Key) SearchResult {
        if (self.key_count == 0) {
            return .{ .index = 0, .found = false };
        }

        var low: usize = 0;
        var high: usize = self.key_count;

        while (low < high) {
            const mid = low + (high - low) / 2;
            const mid_key = self.getKey(mid) orelse return .{ .index = low, .found = false };

            switch (compareKeys(key, mid_key)) {
                .less => high = mid,
                .greater => low = mid + 1,
                .equal => return .{ .index = mid, .found = true },
            }
        }

        return .{ .index = low, .found = false };
    }

    /// Check if node would overflow with new key-value
    pub fn wouldOverflow(self: *const BTreeNode, key_len: usize, value_len: usize) bool {
        // Space needed: slot + key data + value data
        const space_needed = SLOT_SIZE + key_len + value_len;

        // Available space: free_space - (slot area end)
        const slot_area_end = DATA_START + (self.key_count + 1) * SLOT_SIZE;
        if (self.is_leaf) {
            // Leaf: just slots
            if (self.free_space <= slot_area_end) return true;
            return (self.free_space - slot_area_end) < space_needed;
        } else {
            // Internal: slots + children pointers
            const children_size = (self.key_count + 2) * @sizeOf(PageId);
            const min_free = slot_area_end + children_size;
            if (self.free_space <= min_free) return true;
            return (self.free_space - min_free) < space_needed;
        }
    }

    /// Insert key-value at position (for leaf nodes)
    pub fn insertAt(self: *BTreeNode, index: usize, key: Key, value: Value) !void {
        if (!self.is_leaf) return errors.Error.InvalidPageType;
        if (self.wouldOverflow(key.len, value.len)) return errors.Error.ValueTooLarge;

        // Shift existing slots to make room
        if (index < self.key_count) {
            var i: usize = self.key_count;
            while (i > index) : (i -= 1) {
                const src_slot = getSlotAt(self.buffer, DATA_START, i - 1);
                setSlotAt(self.buffer, DATA_START, i, src_slot);
            }
        }

        // Allocate space for key and value (grow downward from free_space)
        self.free_space -= @intCast(value.len);
        const value_offset = self.free_space;
        @memcpy(self.buffer[value_offset .. value_offset + value.len], value);

        self.free_space -= @intCast(key.len);
        const key_offset = self.free_space;
        @memcpy(self.buffer[key_offset .. key_offset + key.len], key);

        // Write new slot
        setSlotAt(self.buffer, DATA_START, index, .{
            .key_offset = @intCast(key_offset),
            .key_len = @intCast(key.len),
            .value_offset = @intCast(value_offset),
            .value_len = @intCast(value.len),
        });

        self.key_count += 1;
        self.writeHeader();
    }

    /// Insert key and child pointer at position (for internal nodes)
    pub fn insertKeyChild(self: *BTreeNode, index: usize, key: Key, right_child: PageId) !void {
        if (self.is_leaf) return errors.Error.InvalidPageType;
        if (self.wouldOverflow(key.len, 0)) return errors.Error.ValueTooLarge;

        // Shift existing slots
        if (index < self.key_count) {
            var i: usize = self.key_count;
            while (i > index) : (i -= 1) {
                const src_slot = getSlotAt(self.buffer, DATA_START, i - 1);
                setSlotAt(self.buffer, DATA_START, i, src_slot);
            }
        }

        // Shift children to make room for new right child at index+1
        // Children are stored backward from end, so shift means moving to lower indices
        var i: usize = self.key_count + 1; // Will have n+2 children after insert
        while (i > index + 1) : (i -= 1) {
            const prev_child = self.getChild(i - 1) orelse continue;
            self.setChild(i, prev_child);
        }

        // Allocate space for key
        self.free_space -= @intCast(key.len);
        const key_offset = self.free_space;
        @memcpy(self.buffer[key_offset .. key_offset + key.len], key);

        // Write new slot
        setSlotAt(self.buffer, DATA_START, index, .{
            .key_offset = @intCast(key_offset),
            .key_len = @intCast(key.len),
            .value_offset = 0,
            .value_len = 0,
        });

        self.key_count += 1;

        // Set the new right child
        self.setChild(index + 1, right_child);

        self.writeHeader();
    }

    /// Check if node is at least half full
    pub fn isHalfFull(self: *const BTreeNode) bool {
        const max_keys = maxKeysPerNode(self.page_size);
        return self.key_count >= max_keys / 2;
    }
};

/// Binary search result
pub const SearchResult = struct {
    index: usize,
    found: bool,
};

// Tests

test "BTreeNode leaf init" {
    const allocator = std.testing.allocator;
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    const node = BTreeNode.init(allocator, &buffer, true);

    try std.testing.expect(node.is_leaf);
    try std.testing.expectEqual(@as(u16, 0), node.key_count);
    try std.testing.expectEqual(INVALID_PAGE_ID, node.right_sibling);
}

test "BTreeNode internal init" {
    const allocator = std.testing.allocator;
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    const node = BTreeNode.init(allocator, &buffer, false);

    try std.testing.expect(!node.is_leaf);
    try std.testing.expectEqual(@as(u16, 0), node.key_count);
}

test "BTreeNode leaf insert and search" {
    const allocator = std.testing.allocator;
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    var node = BTreeNode.init(allocator, &buffer, true);

    // Insert some key-values
    try node.insertAt(0, "banana", "yellow");
    try node.insertAt(0, "apple", "red");
    try node.insertAt(2, "cherry", "red");

    try std.testing.expectEqual(@as(u16, 3), node.key_count);

    // Search
    const r1 = node.searchKey("apple");
    try std.testing.expect(r1.found);
    try std.testing.expectEqual(@as(usize, 0), r1.index);

    const r2 = node.searchKey("banana");
    try std.testing.expect(r2.found);
    try std.testing.expectEqual(@as(usize, 1), r2.index);

    const r3 = node.searchKey("cherry");
    try std.testing.expect(r3.found);
    try std.testing.expectEqual(@as(usize, 2), r3.index);

    const r4 = node.searchKey("date");
    try std.testing.expect(!r4.found);
    try std.testing.expectEqual(@as(usize, 3), r4.index);

    // Get values
    try std.testing.expectEqualStrings("red", node.getValue(0).?);
    try std.testing.expectEqualStrings("yellow", node.getValue(1).?);
}

test "BTreeNode load" {
    const allocator = std.testing.allocator;
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    // Create and populate
    var node1 = BTreeNode.init(allocator, &buffer, true);
    try node1.insertAt(0, "key1", "val1");
    try node1.insertAt(1, "key2", "val2");

    // Load from same buffer
    var node2 = try BTreeNode.load(allocator, &buffer);

    try std.testing.expect(node2.is_leaf);
    try std.testing.expectEqual(@as(u16, 2), node2.key_count);
    try std.testing.expectEqualStrings("key1", node2.getKey(0).?);
    try std.testing.expectEqualStrings("val1", node2.getValue(0).?);
}

test "BTreeNode overflow detection" {
    const allocator = std.testing.allocator;
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    const node = BTreeNode.init(allocator, &buffer, true);

    // Should not overflow for small inserts
    try std.testing.expect(!node.wouldOverflow(10, 10));

    // Should overflow for huge value
    try std.testing.expect(node.wouldOverflow(10, DEFAULT_PAGE_SIZE));
}

test "BTreeNode binary search edge cases" {
    const allocator = std.testing.allocator;
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    var node = BTreeNode.init(allocator, &buffer, true);

    // Empty node
    const r1 = node.searchKey("anything");
    try std.testing.expect(!r1.found);
    try std.testing.expectEqual(@as(usize, 0), r1.index);

    // Single key
    try node.insertAt(0, "middle", "val");

    const r2 = node.searchKey("aaa");
    try std.testing.expect(!r2.found);
    try std.testing.expectEqual(@as(usize, 0), r2.index);

    const r3 = node.searchKey("zzz");
    try std.testing.expect(!r3.found);
    try std.testing.expectEqual(@as(usize, 1), r3.index);

    const r4 = node.searchKey("middle");
    try std.testing.expect(r4.found);
    try std.testing.expectEqual(@as(usize, 0), r4.index);
}

test "BTreeNode internal with children" {
    const allocator = std.testing.allocator;
    var buffer: [DEFAULT_PAGE_SIZE]u8 = undefined;

    var node = BTreeNode.init(allocator, &buffer, false);

    // Set initial left child
    node.setChild(0, 100);

    // Insert key with right child
    try node.insertKeyChild(0, "split_key", 200);

    try std.testing.expectEqual(@as(u16, 1), node.key_count);
    try std.testing.expectEqualStrings("split_key", node.getKey(0).?);
    try std.testing.expectEqual(@as(?PageId, 100), node.getChild(0));
    try std.testing.expectEqual(@as(?PageId, 200), node.getChild(1));
}

test "maxKeysPerNode" {
    const max = maxKeysPerNode(DEFAULT_PAGE_SIZE);
    // Should be reasonable (not 0, not huge)
    try std.testing.expect(max > 10);
    try std.testing.expect(max < 500);
}
