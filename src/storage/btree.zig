const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const btree_node = @import("btree_node.zig");
const buffer_mod = @import("buffer.zig");
const freelist_mod = @import("freelist.zig");
const page_mod = @import("page.zig");

const PageId = types.PageId;
const PageType = types.PageType;
const Key = types.Key;
const Value = types.Value;
const KeyValue = types.KeyValue;
const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;
const Ordering = types.Ordering;
const compareKeys = types.compareKeys;

const BTreeNode = btree_node.BTreeNode;
const SearchResult = btree_node.SearchResult;
const BufferPool = buffer_mod.BufferPool;
const BufferFrame = buffer_mod.BufferFrame;
const Freelist = freelist_mod.Freelist;

/// Minimum fill factor for splits
const MIN_KEYS_AFTER_SPLIT: usize = 2;

/// B+ tree implementation
pub const BTree = struct {
    /// Buffer pool for page access
    buffer_pool: *BufferPool,
    /// Free list for page allocation
    freelist: *Freelist,
    /// Root page ID
    root_page_id: PageId,
    /// Page size
    page_size: usize,
    /// Allocator
    allocator: std.mem.Allocator,

    /// Initialize B+ tree
    pub fn init(
        allocator: std.mem.Allocator,
        buffer_pool: *BufferPool,
        freelist: *Freelist,
        root_page_id: PageId,
    ) BTree {
        return .{
            .buffer_pool = buffer_pool,
            .freelist = freelist,
            .root_page_id = root_page_id,
            .page_size = buffer_pool.page_size,
            .allocator = allocator,
        };
    }

    /// Search for a key, returns value if found
    pub fn search(self: *BTree, key: Key) !?Value {
        if (self.root_page_id == INVALID_PAGE_ID) {
            return null;
        }

        var current_page_id = self.root_page_id;

        while (true) {
            const frame = try self.buffer_pool.fetchPage(current_page_id);
            defer self.buffer_pool.unpinPage(current_page_id, false);

            var node = try BTreeNode.load(self.allocator, frame.buffer);
            const result = node.searchKey(key);

            if (node.is_leaf) {
                if (result.found) {
                    // Copy value to owned memory
                    const val = node.getValue(result.index) orelse return null;
                    const copy = try self.allocator.alloc(u8, val.len);
                    @memcpy(copy, val);
                    return copy;
                }
                return null;
            } else {
                // Internal node - descend to appropriate child
                const child_idx = if (result.found) result.index + 1 else result.index;
                current_page_id = node.getChild(child_idx) orelse return errors.Error.Corrupted;
            }
        }
    }

    /// Insert a key-value pair
    pub fn insert(self: *BTree, key: Key, value: Value) !void {
        // If tree is empty, create root
        if (self.root_page_id == INVALID_PAGE_ID) {
            const new_page_id = try self.allocatePage();
            const frame = try self.buffer_pool.newPage(new_page_id, .leaf);
            defer self.buffer_pool.unpinPage(new_page_id, true);

            var node = BTreeNode.init(self.allocator, frame.buffer, true);
            try node.insertAt(0, key, value);

            self.root_page_id = new_page_id;
            return;
        }

        // Try to insert, handling splits
        const split_info = try self.insertRecursive(self.root_page_id, key, value);

        // If root split, create new root
        if (split_info) |info| {
            const new_root_id = try self.allocatePage();
            const frame = try self.buffer_pool.newPage(new_root_id, .internal);
            defer self.buffer_pool.unpinPage(new_root_id, true);

            var new_root = BTreeNode.init(self.allocator, frame.buffer, false);
            new_root.setChild(0, self.root_page_id);
            try new_root.insertKeyChild(0, info.key, info.right_page_id);

            self.allocator.free(info.key);
            self.root_page_id = new_root_id;
        }
    }

    /// Split info returned when a node splits
    const SplitInfo = struct {
        key: []u8, // Owned, caller must free
        right_page_id: PageId,
    };

    /// Recursive insert, returns split info if node split
    fn insertRecursive(self: *BTree, page_id: PageId, key: Key, value: Value) !?SplitInfo {
        const frame = try self.buffer_pool.fetchPage(page_id);
        var node = try BTreeNode.load(self.allocator, frame.buffer);
        const result = node.searchKey(key);

        if (node.is_leaf) {
            // Leaf node - insert or update
            if (result.found) {
                // Key exists - update value (delete and reinsert)
                self.buffer_pool.unpinPage(page_id, false);
                try self.delete(key);
                return self.insertRecursive(page_id, key, value);
            }

            // Check if we need to split
            if (node.wouldOverflow(key.len, value.len)) {
                // Need to split
                const split = try self.splitLeaf(&node, page_id, result.index, key, value);
                self.buffer_pool.unpinPage(page_id, true);
                return split;
            }

            // Simple insert
            try node.insertAt(result.index, key, value);
            self.buffer_pool.unpinPage(page_id, true);
            return null;
        } else {
            // Internal node - recurse to child
            const child_idx = if (result.found) result.index + 1 else result.index;
            const child_id = node.getChild(child_idx) orelse {
                self.buffer_pool.unpinPage(page_id, false);
                return errors.Error.Corrupted;
            };
            self.buffer_pool.unpinPage(page_id, false);

            const child_split = try self.insertRecursive(child_id, key, value);

            if (child_split) |info| {
                // Child split - insert separator key
                const frame2 = try self.buffer_pool.fetchPage(page_id);
                var node2 = try BTreeNode.load(self.allocator, frame2.buffer);
                const insert_idx = node2.searchKey(info.key).index;

                if (node2.wouldOverflow(info.key.len, 0)) {
                    // Need to split internal node
                    const split = try self.splitInternal(&node2, page_id, insert_idx, info.key, info.right_page_id);
                    self.allocator.free(info.key);
                    self.buffer_pool.unpinPage(page_id, true);
                    return split;
                }

                try node2.insertKeyChild(insert_idx, info.key, info.right_page_id);
                self.allocator.free(info.key);
                self.buffer_pool.unpinPage(page_id, true);
            }

            return null;
        }
    }

    /// Split a leaf node
    fn splitLeaf(self: *BTree, node: *BTreeNode, _: PageId, insert_idx: usize, key: Key, value: Value) !SplitInfo {
        // Collect all keys and values including new one
        const total = node.key_count + 1;
        var keys = try self.allocator.alloc([]u8, total);
        defer {
            for (keys) |k| self.allocator.free(k);
            self.allocator.free(keys);
        }
        var values = try self.allocator.alloc([]u8, total);
        defer {
            for (values) |v| self.allocator.free(v);
            self.allocator.free(values);
        }

        var j: usize = 0;
        for (0..total) |i| {
            if (i == insert_idx) {
                keys[i] = try self.allocator.alloc(u8, key.len);
                @memcpy(keys[i], key);
                values[i] = try self.allocator.alloc(u8, value.len);
                @memcpy(values[i], value);
            } else {
                const k = node.getKey(j) orelse break;
                const v = node.getValue(j) orelse break;
                keys[i] = try self.allocator.alloc(u8, k.len);
                @memcpy(keys[i], k);
                values[i] = try self.allocator.alloc(u8, v.len);
                @memcpy(values[i], v);
                j += 1;
            }
        }

        // Split point
        const mid = total / 2;

        // Create new right leaf
        const right_page_id = try self.allocatePage();
        const right_frame = try self.buffer_pool.newPage(right_page_id, .leaf);
        defer self.buffer_pool.unpinPage(right_page_id, true);

        var right_node = BTreeNode.init(self.allocator, right_frame.buffer, true);
        right_node.right_sibling = node.right_sibling;

        // Rewrite left node
        node.* = BTreeNode.init(self.allocator, node.buffer, true);
        node.right_sibling = right_page_id;

        // Distribute keys
        for (0..mid) |i| {
            try node.insertAt(i, keys[i], values[i]);
        }
        for (mid..total) |i| {
            try right_node.insertAt(i - mid, keys[i], values[i]);
        }

        // Separator key is first key of right node (copy it)
        const sep_key = try self.allocator.alloc(u8, keys[mid].len);
        @memcpy(sep_key, keys[mid]);

        return .{
            .key = sep_key,
            .right_page_id = right_page_id,
        };
    }

    /// Split an internal node
    fn splitInternal(self: *BTree, node: *BTreeNode, page_id: PageId, insert_idx: usize, key: Key, right_child: PageId) !SplitInfo {
        _ = page_id;

        // Collect all keys and children
        const total_keys = node.key_count + 1;
        const total_children = total_keys + 1;

        var keys = try self.allocator.alloc([]u8, total_keys);
        defer {
            for (keys) |k| self.allocator.free(k);
            self.allocator.free(keys);
        }
        var children = try self.allocator.alloc(PageId, total_children);
        defer self.allocator.free(children);

        // Copy existing keys/children, inserting new ones
        var ki: usize = 0;
        var ci: usize = 0;
        for (0..total_keys) |i| {
            if (i == insert_idx) {
                keys[i] = try self.allocator.alloc(u8, key.len);
                @memcpy(keys[i], key);
            } else {
                const k = node.getKey(ki) orelse break;
                keys[i] = try self.allocator.alloc(u8, k.len);
                @memcpy(keys[i], k);
                ki += 1;
            }
        }

        for (0..total_children) |i| {
            if (i == insert_idx + 1) {
                children[i] = right_child;
            } else {
                children[i] = node.getChild(ci) orelse INVALID_PAGE_ID;
                ci += 1;
            }
        }

        // Split point - middle key goes up
        const mid = total_keys / 2;

        // Create new right internal node
        const right_page_id = try self.allocatePage();
        const right_frame = try self.buffer_pool.newPage(right_page_id, .internal);
        defer self.buffer_pool.unpinPage(right_page_id, true);

        var right_node = BTreeNode.init(self.allocator, right_frame.buffer, false);

        // Rewrite left node
        const old_buffer = node.buffer;
        node.* = BTreeNode.init(self.allocator, old_buffer, false);

        // Distribute: left gets [0..mid), right gets [mid+1..total_keys)
        // Key at mid goes up as separator
        node.setChild(0, children[0]);
        for (0..mid) |i| {
            try node.insertKeyChild(i, keys[i], children[i + 1]);
        }

        right_node.setChild(0, children[mid + 1]);
        for (mid + 1..total_keys) |i| {
            try right_node.insertKeyChild(i - mid - 1, keys[i], children[i + 1]);
        }

        // Separator key
        const sep_key = try self.allocator.alloc(u8, keys[mid].len);
        @memcpy(sep_key, keys[mid]);

        return .{
            .key = sep_key,
            .right_page_id = right_page_id,
        };
    }

    /// Delete a key
    pub fn delete(self: *BTree, key: Key) !void {
        if (self.root_page_id == INVALID_PAGE_ID) {
            return errors.Error.KeyNotFound;
        }

        const deleted = try self.deleteRecursive(self.root_page_id, key);
        if (!deleted) {
            return errors.Error.KeyNotFound;
        }

        // Check if root is now empty internal node
        const frame = try self.buffer_pool.fetchPage(self.root_page_id);
        defer self.buffer_pool.unpinPage(self.root_page_id, false);

        const node = try BTreeNode.load(self.allocator, frame.buffer);
        if (!node.is_leaf and node.key_count == 0) {
            // Replace root with its only child
            const old_root = self.root_page_id;
            self.root_page_id = node.getChild(0) orelse INVALID_PAGE_ID;
            try self.freelist.free(old_root);
        }
    }

    /// Recursive delete, returns true if key was found and deleted
    fn deleteRecursive(self: *BTree, page_id: PageId, key: Key) !bool {
        const frame = try self.buffer_pool.fetchPage(page_id);
        var node = try BTreeNode.load(self.allocator, frame.buffer);
        const result = node.searchKey(key);

        if (node.is_leaf) {
            if (!result.found) {
                self.buffer_pool.unpinPage(page_id, false);
                return false;
            }

            // Delete by shifting
            try self.deleteFromLeaf(&node, result.index);
            self.buffer_pool.unpinPage(page_id, true);
            return true;
        } else {
            // Internal node
            const child_idx = if (result.found) result.index + 1 else result.index;
            const child_id = node.getChild(child_idx) orelse {
                self.buffer_pool.unpinPage(page_id, false);
                return errors.Error.Corrupted;
            };
            self.buffer_pool.unpinPage(page_id, false);

            return self.deleteRecursive(child_id, key);
        }
    }

    /// Delete entry from leaf at index
    fn deleteFromLeaf(self: *BTree, node: *BTreeNode, index: usize) !void {
        // Rebuild node without the deleted entry
        const count = node.key_count;
        if (count == 0) return;

        var keys = try self.allocator.alloc([]u8, count - 1);
        defer {
            for (keys) |k| self.allocator.free(k);
            self.allocator.free(keys);
        }
        var values = try self.allocator.alloc([]u8, count - 1);
        defer {
            for (values) |v| self.allocator.free(v);
            self.allocator.free(values);
        }

        var j: usize = 0;
        for (0..count) |i| {
            if (i == index) continue;
            const k = node.getKey(i) orelse continue;
            const v = node.getValue(i) orelse continue;
            keys[j] = try self.allocator.alloc(u8, k.len);
            @memcpy(keys[j], k);
            values[j] = try self.allocator.alloc(u8, v.len);
            @memcpy(values[j], v);
            j += 1;
        }

        const old_sibling = node.right_sibling;
        node.* = BTreeNode.init(self.allocator, node.buffer, true);
        node.right_sibling = old_sibling;

        for (0..j) |i| {
            try node.insertAt(i, keys[i], values[i]);
        }
    }

    /// Range scan iterator
    pub fn range(self: *BTree, start_key: ?Key, end_key: ?Key) !RangeIterator {
        return RangeIterator.init(self, start_key, end_key);
    }

    /// Allocate a new page (from freelist or file growth)
    fn allocatePage(self: *BTree) !PageId {
        if (self.freelist.allocate()) |page_id| {
            return page_id;
        }
        return self.buffer_pool.file.allocatePage();
    }

    /// Get root page ID
    pub fn getRootPageId(self: *const BTree) PageId {
        return self.root_page_id;
    }
};

/// Range scan iterator
pub const RangeIterator = struct {
    tree: *BTree,
    current_page_id: PageId,
    current_index: usize,
    end_key: ?[]u8,
    exhausted: bool,

    pub fn init(tree: *BTree, start_key: ?Key, end_key: ?Key) !RangeIterator {
        var iter = RangeIterator{
            .tree = tree,
            .current_page_id = INVALID_PAGE_ID,
            .current_index = 0,
            .end_key = null,
            .exhausted = false,
        };

        // Copy end key if provided
        if (end_key) |ek| {
            iter.end_key = try tree.allocator.alloc(u8, ek.len);
            @memcpy(iter.end_key.?, ek);
        }

        // Find starting leaf
        if (tree.root_page_id == INVALID_PAGE_ID) {
            iter.exhausted = true;
            return iter;
        }

        var current = tree.root_page_id;
        while (true) {
            const frame = try tree.buffer_pool.fetchPage(current);
            defer tree.buffer_pool.unpinPage(current, false);

            var node = try BTreeNode.load(tree.allocator, frame.buffer);

            if (node.is_leaf) {
                iter.current_page_id = current;
                if (start_key) |sk| {
                    const result = node.searchKey(sk);
                    iter.current_index = result.index;
                } else {
                    iter.current_index = 0;
                }

                // Check if we're past the end
                if (iter.current_index >= node.key_count) {
                    iter.current_page_id = node.right_sibling;
                    iter.current_index = 0;
                }
                break;
            } else {
                const child_idx = if (start_key) |sk| blk: {
                    const result = node.searchKey(sk);
                    break :blk if (result.found) result.index + 1 else result.index;
                } else 0;

                current = node.getChild(child_idx) orelse {
                    iter.exhausted = true;
                    break;
                };
            }
        }

        if (iter.current_page_id == INVALID_PAGE_ID) {
            iter.exhausted = true;
        }

        return iter;
    }

    pub fn deinit(self: *RangeIterator) void {
        if (self.end_key) |ek| {
            self.tree.allocator.free(ek);
        }
    }

    /// Get next key-value pair (caller owns returned memory)
    pub fn next(self: *RangeIterator) !?KeyValue {
        if (self.exhausted or self.current_page_id == INVALID_PAGE_ID) {
            return null;
        }

        const frame = try self.tree.buffer_pool.fetchPage(self.current_page_id);
        defer self.tree.buffer_pool.unpinPage(self.current_page_id, false);

        var node = try BTreeNode.load(self.tree.allocator, frame.buffer);

        if (self.current_index >= node.key_count) {
            // Move to next leaf
            self.current_page_id = node.right_sibling;
            self.current_index = 0;

            if (self.current_page_id == INVALID_PAGE_ID) {
                self.exhausted = true;
                return null;
            }

            return self.next();
        }

        const key = node.getKey(self.current_index) orelse {
            self.exhausted = true;
            return null;
        };

        // Check end key
        if (self.end_key) |ek| {
            if (compareKeys(key, ek) != .less) {
                self.exhausted = true;
                return null;
            }
        }

        const value = node.getValue(self.current_index) orelse {
            self.exhausted = true;
            return null;
        };

        // Copy to owned memory
        const key_copy = try self.tree.allocator.alloc(u8, key.len);
        @memcpy(key_copy, key);
        const value_copy = try self.tree.allocator.alloc(u8, value.len);
        @memcpy(value_copy, value);

        self.current_index += 1;

        return KeyValue.init(key_copy, value_copy);
    }
};

// Tests

test "BTree empty search" {
    const allocator = std.testing.allocator;
    const path = "test_btree_empty.db";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var file = try @import("file.zig").File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 10);
    defer pool.deinit();

    var freelist = try Freelist.init(allocator, &file, INVALID_PAGE_ID);
    defer freelist.deinit();

    var tree = BTree.init(allocator, &pool, &freelist, INVALID_PAGE_ID);

    const result = try tree.search("nonexistent");
    try std.testing.expectEqual(@as(?Value, null), result);
}

test "BTree insert and search" {
    const allocator = std.testing.allocator;
    const path = "test_btree_insert.db";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var file = try @import("file.zig").File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer file.close();

    // Allocate meta pages
    _ = try file.allocatePage();
    _ = try file.allocatePage();

    var pool = try BufferPool.init(allocator, &file, 10);
    defer pool.deinit();

    var freelist = try Freelist.init(allocator, &file, INVALID_PAGE_ID);
    defer freelist.deinit();

    var tree = BTree.init(allocator, &pool, &freelist, INVALID_PAGE_ID);

    // Insert
    try tree.insert("key1", "value1");
    try tree.insert("key2", "value2");
    try tree.insert("key3", "value3");

    // Search
    const v1 = try tree.search("key1");
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualStrings("value1", v1.?);
    allocator.free(v1.?);

    const v2 = try tree.search("key2");
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("value2", v2.?);
    allocator.free(v2.?);

    const v3 = try tree.search("key3");
    try std.testing.expect(v3 != null);
    try std.testing.expectEqualStrings("value3", v3.?);
    allocator.free(v3.?);

    // Not found
    const v4 = try tree.search("key4");
    try std.testing.expectEqual(@as(?Value, null), v4);
}

test "BTree delete" {
    const allocator = std.testing.allocator;
    const path = "test_btree_delete.db";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var file = try @import("file.zig").File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer file.close();

    _ = try file.allocatePage();
    _ = try file.allocatePage();

    var pool = try BufferPool.init(allocator, &file, 10);
    defer pool.deinit();

    var freelist = try Freelist.init(allocator, &file, INVALID_PAGE_ID);
    defer freelist.deinit();

    var tree = BTree.init(allocator, &pool, &freelist, INVALID_PAGE_ID);

    try tree.insert("a", "1");
    try tree.insert("b", "2");
    try tree.insert("c", "3");

    // Delete middle key
    try tree.delete("b");

    const va = try tree.search("a");
    try std.testing.expect(va != null);
    allocator.free(va.?);

    const vb = try tree.search("b");
    try std.testing.expectEqual(@as(?Value, null), vb);

    const vc = try tree.search("c");
    try std.testing.expect(vc != null);
    allocator.free(vc.?);
}

test "BTree range scan" {
    const allocator = std.testing.allocator;
    const path = "test_btree_range.db";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var file = try @import("file.zig").File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer file.close();

    _ = try file.allocatePage();
    _ = try file.allocatePage();

    var pool = try BufferPool.init(allocator, &file, 10);
    defer pool.deinit();

    var freelist = try Freelist.init(allocator, &file, INVALID_PAGE_ID);
    defer freelist.deinit();

    var tree = BTree.init(allocator, &pool, &freelist, INVALID_PAGE_ID);

    try tree.insert("a", "1");
    try tree.insert("b", "2");
    try tree.insert("c", "3");
    try tree.insert("d", "4");

    // Range [b, d)
    var iter = try tree.range("b", "d");
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |kv| {
        allocator.free(kv.key);
        allocator.free(kv.value);
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 2), count);
}

test "BTree many inserts" {
    const allocator = std.testing.allocator;
    const path = "test_btree_many.db";

    std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path) catch {};

    var file = try @import("file.zig").File.open(allocator, path, DEFAULT_PAGE_SIZE);
    defer file.close();

    _ = try file.allocatePage();
    _ = try file.allocatePage();

    var pool = try BufferPool.init(allocator, &file, 100);
    defer pool.deinit();

    var freelist = try Freelist.init(allocator, &file, INVALID_PAGE_ID);
    defer freelist.deinit();

    var tree = BTree.init(allocator, &pool, &freelist, INVALID_PAGE_ID);

    // Insert many keys to trigger splits
    var key_buf: [16]u8 = undefined;
    var val_buf: [16]u8 = undefined;

    for (0..50) |i| {
        const key_len = std.fmt.bufPrint(&key_buf, "key{d:04}", .{i}) catch continue;
        const val_len = std.fmt.bufPrint(&val_buf, "val{d:04}", .{i}) catch continue;
        try tree.insert(key_buf[0..key_len.len], val_buf[0..val_len.len]);
    }

    // Verify all are findable
    for (0..50) |i| {
        const key_len = std.fmt.bufPrint(&key_buf, "key{d:04}", .{i}) catch continue;
        const result = try tree.search(key_buf[0..key_len.len]);
        try std.testing.expect(result != null);
        allocator.free(result.?);
    }
}
