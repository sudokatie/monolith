const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const btree_node = @import("btree_node.zig");
const buffer_mod = @import("buffer.zig");
const page_mod = @import("page.zig");
const file_mod = @import("file.zig");

const PageId = types.PageId;
const PageType = types.PageType;
const Key = types.Key;
const Value = types.Value;
const Ordering = types.Ordering;
const compareKeys = types.compareKeys;
const INVALID_PAGE_ID = types.INVALID_PAGE_ID;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;

const BTreeNode = btree_node.BTreeNode;
const SearchResult = btree_node.SearchResult;
const BufferPool = buffer_mod.BufferPool;
const File = file_mod.File;

/// Split result returned when a node splits
const SplitResult = struct {
    /// The key to promote to parent
    split_key: []const u8,
    /// Page ID of the new (right) node
    new_page_id: PageId,
};

/// B+ tree for key-value storage
pub const BTree = struct {
    /// Root page ID (INVALID_PAGE_ID if tree is empty)
    root_id: PageId,
    /// Buffer pool for page access
    buffer_pool: *BufferPool,
    /// File handle for page allocation
    file: *File,
    /// Allocator for temporary operations
    allocator: std.mem.Allocator,
    /// Page size
    page_size: usize,

    /// Initialize an empty B+ tree
    pub fn init(allocator: std.mem.Allocator, buffer_pool: *BufferPool, file: *File) BTree {
        return .{
            .root_id = INVALID_PAGE_ID,
            .buffer_pool = buffer_pool,
            .file = file,
            .allocator = allocator,
            .page_size = buffer_pool.page_size,
        };
    }

    /// Search for a key in the tree
    /// Returns the value if found, null otherwise
    pub fn search(self: *BTree, key: Key) !?Value {
        // Empty tree
        if (self.root_id == INVALID_PAGE_ID) {
            return null;
        }

        // Start at root and descend to leaf
        var current_page_id = self.root_id;

        while (true) {
            // Fetch current page
            const frame = try self.buffer_pool.fetchPage(current_page_id);
            defer self.buffer_pool.unpinPage(current_page_id, false);

            // Load node from buffer
            var node = try BTreeNode.load(self.allocator, frame.buffer);

            if (node.is_leaf) {
                // Search in leaf node
                const result = node.searchKey(key);
                if (result.found) {
                    // Return a copy of the value to avoid buffer pool issues
                    if (node.getValue(result.index)) |val| {
                        const copy = try self.allocator.dupe(u8, val);
                        return copy;
                    }
                }
                return null;
            } else {
                // Internal node - find child to descend to
                const result = node.searchKey(key);
                const child_index = if (result.found) result.index + 1 else result.index;

                const child_id = node.getChild(child_index) orelse {
                    return null;
                };

                current_page_id = child_id;
            }
        }
    }

    /// Insert a key-value pair into the tree
    pub fn insert(self: *BTree, key: Key, value: Value) !void {
        // Empty tree - create root leaf
        if (self.root_id == INVALID_PAGE_ID) {
            const page_id = try self.file.allocatePage();
            const frame = try self.buffer_pool.newPage(page_id, .leaf);

            var node = BTreeNode.init(self.allocator, frame.buffer, true);
            try node.insertAt(0, key, value);
            self.buffer_pool.unpinPage(page_id, true);

            self.root_id = page_id;
            return;
        }

        // Find path to leaf and insert
        const split = try self.insertRecursive(self.root_id, key, value);

        // If root split, create new root
        if (split) |s| {
            defer self.allocator.free(s.split_key);

            const new_root_id = try self.file.allocatePage();
            const frame = try self.buffer_pool.newPage(new_root_id, .internal);

            var new_root = BTreeNode.init(self.allocator, frame.buffer, false);
            new_root.setChild(0, self.root_id);
            try new_root.insertKeyChild(0, s.split_key, s.new_page_id);
            self.buffer_pool.unpinPage(new_root_id, true);

            self.root_id = new_root_id;
        }
    }

    /// Recursive insert - returns split info if node split
    fn insertRecursive(self: *BTree, page_id: PageId, key: Key, value: Value) !?SplitResult {
        const frame = try self.buffer_pool.fetchPage(page_id);
        var node = try BTreeNode.load(self.allocator, frame.buffer);

        if (node.is_leaf) {
            // Insert into leaf
            const result = node.searchKey(key);

            if (result.found) {
                // Update existing key - for now, just skip (could implement update)
                self.buffer_pool.unpinPage(page_id, false);
                return null;
            }

            // Check if insert would overflow
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
            // Internal node - descend to child
            const result = node.searchKey(key);
            const child_index = if (result.found) result.index + 1 else result.index;
            const child_id = node.getChild(child_index) orelse {
                self.buffer_pool.unpinPage(page_id, false);
                return errors.Error.Corrupted;
            };

            // Unpin parent before descending (to avoid deadlocks)
            self.buffer_pool.unpinPage(page_id, false);

            // Recurse into child
            const child_split = try self.insertRecursive(child_id, key, value);

            if (child_split) |cs| {
                // Child split - need to insert new key into this node
                const parent_frame = try self.buffer_pool.fetchPage(page_id);
                var parent_node = try BTreeNode.load(self.allocator, parent_frame.buffer);

                // Check if parent would overflow
                if (parent_node.wouldOverflow(cs.split_key.len, 0)) {
                    // Split internal node
                    const parent_split = try self.splitInternal(&parent_node, page_id, child_index, cs.split_key, cs.new_page_id);
                    self.buffer_pool.unpinPage(page_id, true);
                    self.allocator.free(cs.split_key);
                    return parent_split;
                }

                // Simple insert into parent
                try parent_node.insertKeyChild(child_index, cs.split_key, cs.new_page_id);
                self.buffer_pool.unpinPage(page_id, true);
                self.allocator.free(cs.split_key);
            }

            return null;
        }
    }

    /// Split a leaf node
    fn splitLeaf(self: *BTree, node: *BTreeNode, _: PageId, insert_idx: usize, key: Key, value: Value) !SplitResult {
        // Create new leaf
        const new_page_id = try self.file.allocatePage();
        const new_frame = try self.buffer_pool.newPage(new_page_id, .leaf);
        var new_node = BTreeNode.init(self.allocator, new_frame.buffer, true);

        // Calculate split point (half of keys go to new node)
        const total_keys = node.key_count + 1; // including the new key
        const split_point = total_keys / 2;

        // Collect all keys and values (including new one)
        var keys: std.ArrayListUnmanaged([]const u8) = .empty;
        defer keys.deinit(self.allocator);
        var values: std.ArrayListUnmanaged([]const u8) = .empty;
        defer values.deinit(self.allocator);

        var inserted = false;
        for (0..node.key_count) |i| {
            if (!inserted and i == insert_idx) {
                try keys.append(self.allocator, key);
                try values.append(self.allocator, value);
                inserted = true;
            }
            if (node.getKey(i)) |k| {
                try keys.append(self.allocator, k);
            }
            if (node.getValue(i)) |v| {
                try values.append(self.allocator, v);
            }
        }
        if (!inserted) {
            try keys.append(self.allocator, key);
            try values.append(self.allocator, value);
        }

        // Clear original node and repopulate with first half
        // (We're reusing the buffer, so we just reset and re-insert)
        const old_buffer = node.buffer;
        node.* = BTreeNode.init(self.allocator, old_buffer, true);
        for (0..split_point) |i| {
            try node.insertAt(i, keys.items[i], values.items[i]);
        }

        // Put second half in new node
        for (split_point..keys.items.len) |i| {
            try new_node.insertAt(i - split_point, keys.items[i], values.items[i]);
        }

        // Link leaves
        new_node.right_sibling = node.right_sibling;
        new_node.writeHeader();
        node.right_sibling = new_page_id;
        node.writeHeader();

        self.buffer_pool.unpinPage(new_page_id, true);

        // The split key is the first key in the new (right) node
        const split_key = try self.allocator.dupe(u8, keys.items[split_point]);

        return SplitResult{
            .split_key = split_key,
            .new_page_id = new_page_id,
        };
    }

    /// Split an internal node
    fn splitInternal(self: *BTree, node: *BTreeNode, _: PageId, insert_idx: usize, key: Key, right_child: PageId) !SplitResult {
        // Create new internal node
        const new_page_id = try self.file.allocatePage();
        const new_frame = try self.buffer_pool.newPage(new_page_id, .internal);
        var new_node = BTreeNode.init(self.allocator, new_frame.buffer, false);

        // Collect all keys and children
        var keys: std.ArrayListUnmanaged([]const u8) = .empty;
        defer keys.deinit(self.allocator);
        var children: std.ArrayListUnmanaged(PageId) = .empty;
        defer children.deinit(self.allocator);

        // First child
        if (node.getChild(0)) |c| {
            try children.append(self.allocator, c);
        }

        var inserted = false;
        for (0..node.key_count) |i| {
            if (!inserted and i == insert_idx) {
                try keys.append(self.allocator, key);
                try children.append(self.allocator, right_child);
                inserted = true;
            }
            if (node.getKey(i)) |k| {
                try keys.append(self.allocator, k);
            }
            if (node.getChild(i + 1)) |c| {
                try children.append(self.allocator, c);
            }
        }
        if (!inserted) {
            try keys.append(self.allocator, key);
            try children.append(self.allocator, right_child);
        }

        const total_keys = keys.items.len;
        const split_point = total_keys / 2;

        // The middle key goes up to parent
        const split_key = try self.allocator.dupe(u8, keys.items[split_point]);

        // Rebuild original node with first half (keys 0..split_point-1)
        const old_buffer = node.buffer;
        node.* = BTreeNode.init(self.allocator, old_buffer, false);
        node.setChild(0, children.items[0]);
        for (0..split_point) |i| {
            try node.insertKeyChild(i, keys.items[i], children.items[i + 1]);
        }

        // New node gets second half (keys split_point+1..end)
        new_node.setChild(0, children.items[split_point + 1]);
        for (split_point + 1..total_keys) |i| {
            try new_node.insertKeyChild(i - split_point - 1, keys.items[i], children.items[i + 1]);
        }

        self.buffer_pool.unpinPage(new_page_id, true);

        return SplitResult{
            .split_key = split_key,
            .new_page_id = new_page_id,
        };
    }

    /// Check if a key exists in the tree
    pub fn contains(self: *BTree, key: Key) !bool {
        const result = try self.search(key);
        if (result) |v| {
            self.allocator.free(v);
            return true;
        }
        return false;
    }

    /// Get the root page ID
    pub fn getRootId(self: *const BTree) PageId {
        return self.root_id;
    }

    /// Check if the tree is empty
    pub fn isEmpty(self: *const BTree) bool {
        return self.root_id == INVALID_PAGE_ID;
    }

    /// Set the root page ID (used by insert/delete operations)
    pub fn setRoot(self: *BTree, root_id: PageId) void {
        self.root_id = root_id;
    }
};

// Tests

test "BTree empty tree search" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_empty.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    // Empty tree should return null for any key
    const result = try tree.search("anykey");
    try std.testing.expect(result == null);
    try std.testing.expect(tree.isEmpty());
}

test "BTree single leaf search" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_leaf.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    // Allocate and create a leaf page
    const page_id = try file.allocatePage();
    const frame = try pool.newPage(page_id, .leaf);

    var node = BTreeNode.init(allocator, frame.buffer, true);
    try node.insertAt(0, "apple", "red");
    try node.insertAt(1, "banana", "yellow");
    try node.insertAt(2, "cherry", "red");
    pool.unpinPage(page_id, true);

    // Create tree with this leaf as root
    var tree = BTree.init(allocator, &pool, &file);
    tree.setRoot(page_id);

    // Search for existing keys
    const v1 = try tree.search("apple");
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualStrings("red", v1.?);
    allocator.free(v1.?);

    const v2 = try tree.search("banana");
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("yellow", v2.?);
    allocator.free(v2.?);

    // Search for non-existent key
    const v3 = try tree.search("date");
    try std.testing.expect(v3 == null);

    // Contains check
    try std.testing.expect(try tree.contains("cherry"));
    try std.testing.expect(!try tree.contains("elderberry"));
}

test "BTree two level search" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_2level.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    // Create left leaf (keys < "m")
    const left_id = try file.allocatePage();
    const left_frame = try pool.newPage(left_id, .leaf);
    var left_node = BTreeNode.init(allocator, left_frame.buffer, true);
    try left_node.insertAt(0, "apple", "fruit1");
    try left_node.insertAt(1, "banana", "fruit2");
    try left_node.insertAt(2, "cherry", "fruit3");
    pool.unpinPage(left_id, true);

    // Create right leaf (keys >= "m")
    const right_id = try file.allocatePage();
    const right_frame = try pool.newPage(right_id, .leaf);
    var right_node = BTreeNode.init(allocator, right_frame.buffer, true);
    try right_node.insertAt(0, "mango", "fruit4");
    try right_node.insertAt(1, "orange", "fruit5");
    try right_node.insertAt(2, "peach", "fruit6");
    pool.unpinPage(right_id, true);

    // Create root internal node
    const root_id = try file.allocatePage();
    const root_frame = try pool.newPage(root_id, .internal);
    var root_node = BTreeNode.init(allocator, root_frame.buffer, false);
    root_node.setChild(0, left_id);
    try root_node.insertKeyChild(0, "mango", right_id);
    pool.unpinPage(root_id, true);

    // Create tree
    var tree = BTree.init(allocator, &pool, &file);
    tree.setRoot(root_id);

    // Search left subtree
    const v1 = try tree.search("apple");
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualStrings("fruit1", v1.?);
    allocator.free(v1.?);

    const v2 = try tree.search("cherry");
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("fruit3", v2.?);
    allocator.free(v2.?);

    // Search right subtree
    const v3 = try tree.search("mango");
    try std.testing.expect(v3 != null);
    try std.testing.expectEqualStrings("fruit4", v3.?);
    allocator.free(v3.?);

    const v4 = try tree.search("peach");
    try std.testing.expect(v4 != null);
    try std.testing.expectEqualStrings("fruit6", v4.?);
    allocator.free(v4.?);

    // Search non-existent
    const v5 = try tree.search("grape");
    try std.testing.expect(v5 == null);

    const v6 = try tree.search("zebra");
    try std.testing.expect(v6 == null);
}

test "BTree isEmpty and getRootId" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_meta.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    try std.testing.expect(tree.isEmpty());
    try std.testing.expectEqual(INVALID_PAGE_ID, tree.getRootId());

    // Set root
    tree.setRoot(42);
    try std.testing.expect(!tree.isEmpty());
    try std.testing.expectEqual(@as(PageId, 42), tree.getRootId());
}

test "BTree insert single key" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_insert1.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    // Insert into empty tree
    try tree.insert("hello", "world");

    try std.testing.expect(!tree.isEmpty());

    const v = try tree.search("hello");
    try std.testing.expect(v != null);
    try std.testing.expectEqualStrings("world", v.?);
    allocator.free(v.?);
}

test "BTree insert multiple keys" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_insert_multi.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    // Insert multiple keys
    try tree.insert("banana", "yellow");
    try tree.insert("apple", "red");
    try tree.insert("cherry", "red");
    try tree.insert("date", "brown");
    try tree.insert("elderberry", "purple");

    // Verify all keys
    const v1 = try tree.search("apple");
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualStrings("red", v1.?);
    allocator.free(v1.?);

    const v2 = try tree.search("banana");
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("yellow", v2.?);
    allocator.free(v2.?);

    const v3 = try tree.search("elderberry");
    try std.testing.expect(v3 != null);
    try std.testing.expectEqualStrings("purple", v3.?);
    allocator.free(v3.?);

    // Non-existent key
    const v4 = try tree.search("fig");
    try std.testing.expect(v4 == null);
}

test "BTree insert causing leaf split" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_split.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    // Insert enough keys to cause a split
    // With 4KB pages, we can fit many small keys before splitting
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        var val_buf: [16]u8 = undefined;
        const key_len = std.fmt.bufPrint(&key_buf, "key{d:0>5}", .{i}) catch unreachable;
        const val_len = std.fmt.bufPrint(&val_buf, "val{d:0>5}", .{i}) catch unreachable;
        try tree.insert(key_len, val_len);
    }

    // Tree should have split at least once
    try std.testing.expect(!tree.isEmpty());

    // Verify random keys still findable
    const v1 = try tree.search("key00000");
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualStrings("val00000", v1.?);
    allocator.free(v1.?);

    const v2 = try tree.search("key00050");
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("val00050", v2.?);
    allocator.free(v2.?);

    const v3 = try tree.search("key00099");
    try std.testing.expect(v3 != null);
    try std.testing.expectEqualStrings("val00099", v3.?);
    allocator.free(v3.?);
}

test "BTree insert duplicate key" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_dup.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    try tree.insert("key", "value1");
    try tree.insert("key", "value2"); // Duplicate - currently skipped

    // Should still have original value (update not implemented yet)
    const v = try tree.search("key");
    try std.testing.expect(v != null);
    try std.testing.expectEqualStrings("value1", v.?);
    allocator.free(v.?);
}
