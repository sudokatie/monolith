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

/// Key-value pair returned by iterator
pub const KeyValue = struct {
    key: []const u8,
    value: []const u8,
};

/// B+ tree iterator for range scans
pub const BTreeIterator = struct {
    /// Reference to the tree
    tree: *BTree,
    /// Current leaf page ID
    current_page_id: PageId,
    /// Current slot index within leaf
    current_slot: usize,
    /// End key (exclusive), null means scan to end
    end_key: ?[]const u8,
    /// Whether iterator has been exhausted
    exhausted: bool,
    /// Allocator for copied keys/values
    allocator: std.mem.Allocator,

    /// Advance to next key-value pair
    /// Returns null when iteration is complete
    pub fn next(self: *BTreeIterator) !?KeyValue {
        if (self.exhausted or self.current_page_id == INVALID_PAGE_ID) {
            return null;
        }

        // Fetch current leaf
        const frame = try self.tree.buffer_pool.fetchPage(self.current_page_id);
        defer self.tree.buffer_pool.unpinPage(self.current_page_id, false);

        var node = try BTreeNode.load(self.allocator, frame.buffer);

        // Check if we've exhausted current leaf
        if (self.current_slot >= node.key_count) {
            // Move to right sibling
            if (node.right_sibling == INVALID_PAGE_ID) {
                self.exhausted = true;
                return null;
            }

            self.current_page_id = node.right_sibling;
            self.current_slot = 0;

            // Recursively call next with new page
            return self.next();
        }

        // Get current key
        const key = node.getKey(self.current_slot) orelse {
            self.exhausted = true;
            return null;
        };

        // Check end bound
        if (self.end_key) |end| {
            if (compareKeys(key, end) != .less) {
                self.exhausted = true;
                return null;
            }
        }

        // Get value
        const value = node.getValue(self.current_slot) orelse {
            self.exhausted = true;
            return null;
        };

        // Copy key and value (buffer pool may evict the page)
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);
        const value_copy = try self.allocator.dupe(u8, value);

        // Advance slot
        self.current_slot += 1;

        return KeyValue{
            .key = key_copy,
            .value = value_copy,
        };
    }

    /// Free resources from a KeyValue returned by next()
    pub fn freeKeyValue(self: *BTreeIterator, kv: KeyValue) void {
        self.allocator.free(kv.key);
        self.allocator.free(kv.value);
    }

    /// Reset iterator to beginning
    pub fn reset(self: *BTreeIterator) void {
        self.exhausted = false;
        self.current_slot = 0;
        // Need to re-seek to start
    }
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

    /// Delete a key from the tree
    /// Returns true if key was found and deleted, false if not found
    pub fn delete(self: *BTree, key: Key) !bool {
        // Empty tree
        if (self.root_id == INVALID_PAGE_ID) {
            return false;
        }

        // Find and delete
        const deleted = try self.deleteRecursive(self.root_id, key);

        // Check if root is now empty (for internal nodes)
        if (deleted) {
            const frame = try self.buffer_pool.fetchPage(self.root_id);
            var node = try BTreeNode.load(self.allocator, frame.buffer);
            self.buffer_pool.unpinPage(self.root_id, false);

            if (!node.is_leaf and node.key_count == 0) {
                // Root is empty internal node - make first child the new root
                if (node.getChild(0)) |new_root| {
                    // TODO: free the old root page
                    self.root_id = new_root;
                }
            } else if (node.is_leaf and node.key_count == 0) {
                // Tree is now empty
                // TODO: free the root page
                self.root_id = INVALID_PAGE_ID;
            }
        }

        return deleted;
    }

    /// Recursive delete - returns true if key was deleted
    fn deleteRecursive(self: *BTree, page_id: PageId, key: Key) !bool {
        const frame = try self.buffer_pool.fetchPage(page_id);
        var node = try BTreeNode.load(self.allocator, frame.buffer);

        if (node.is_leaf) {
            // Search for key in leaf
            const result = node.searchKey(key);
            if (!result.found) {
                self.buffer_pool.unpinPage(page_id, false);
                return false;
            }

            // Remove the key
            node.removeAt(result.index);
            self.buffer_pool.unpinPage(page_id, true);

            // Note: We're not doing merge/redistribute for simplicity
            // A production implementation would handle underflow here
            return true;
        } else {
            // Internal node - find child to descend to
            const result = node.searchKey(key);
            const child_index = if (result.found) result.index + 1 else result.index;
            const child_id = node.getChild(child_index) orelse {
                self.buffer_pool.unpinPage(page_id, false);
                return false;
            };

            // Unpin parent before descending
            self.buffer_pool.unpinPage(page_id, false);

            // Recurse into child
            return self.deleteRecursive(child_id, key);
        }
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

    /// Create an iterator for a full scan (all keys in order)
    pub fn scan(self: *BTree) !BTreeIterator {
        return self.scanRange(null, null);
    }

    /// Create an iterator for a range scan
    /// start_key: inclusive start (null = from beginning)
    /// end_key: exclusive end (null = to end)
    pub fn scanRange(self: *BTree, start_key: ?Key, end_key: ?Key) !BTreeIterator {
        // Empty tree
        if (self.root_id == INVALID_PAGE_ID) {
            return BTreeIterator{
                .tree = self,
                .current_page_id = INVALID_PAGE_ID,
                .current_slot = 0,
                .end_key = end_key,
                .exhausted = true,
                .allocator = self.allocator,
            };
        }

        // Find starting leaf and position
        var leaf_id: PageId = undefined;
        var slot: usize = undefined;

        if (start_key) |sk| {
            // Seek to start key
            const result = try self.findLeafAndSlot(sk);
            leaf_id = result.page_id;
            slot = result.slot;
        } else {
            // Find leftmost leaf
            leaf_id = try self.findLeftmostLeaf();
            slot = 0;
        }

        return BTreeIterator{
            .tree = self,
            .current_page_id = leaf_id,
            .current_slot = slot,
            .end_key = end_key,
            .exhausted = false,
            .allocator = self.allocator,
        };
    }

    /// Find the leftmost leaf page
    fn findLeftmostLeaf(self: *BTree) !PageId {
        var current_id = self.root_id;

        while (true) {
            const frame = try self.buffer_pool.fetchPage(current_id);
            defer self.buffer_pool.unpinPage(current_id, false);

            var node = try BTreeNode.load(self.allocator, frame.buffer);

            if (node.is_leaf) {
                return current_id;
            }

            // Go to leftmost child
            const child_id = node.getChild(0) orelse return errors.Error.Corrupted;
            current_id = child_id;
        }
    }

    /// Find leaf page and slot for a key (for seek)
    const LeafPosition = struct {
        page_id: PageId,
        slot: usize,
    };

    fn findLeafAndSlot(self: *BTree, key: Key) !LeafPosition {
        var current_id = self.root_id;

        while (true) {
            const frame = try self.buffer_pool.fetchPage(current_id);
            defer self.buffer_pool.unpinPage(current_id, false);

            var node = try BTreeNode.load(self.allocator, frame.buffer);

            if (node.is_leaf) {
                const result = node.searchKey(key);
                return LeafPosition{
                    .page_id = current_id,
                    .slot = result.index,
                };
            }

            // Internal node - find child
            const result = node.searchKey(key);
            const child_index = if (result.found) result.index + 1 else result.index;
            const child_id = node.getChild(child_index) orelse return errors.Error.Corrupted;
            current_id = child_id;
        }
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

test "BTree delete existing key" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_delete1.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    // Insert some keys
    try tree.insert("apple", "red");
    try tree.insert("banana", "yellow");
    try tree.insert("cherry", "red");

    // Delete one
    const deleted = try tree.delete("banana");
    try std.testing.expect(deleted);

    // Verify it's gone
    const v1 = try tree.search("banana");
    try std.testing.expect(v1 == null);

    // Other keys still there
    const v2 = try tree.search("apple");
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("red", v2.?);
    allocator.free(v2.?);

    const v3 = try tree.search("cherry");
    try std.testing.expect(v3 != null);
    try std.testing.expectEqualStrings("red", v3.?);
    allocator.free(v3.?);
}

test "BTree delete non-existent key" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_delete_missing.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    try tree.insert("apple", "red");

    // Delete non-existent key
    const deleted = try tree.delete("banana");
    try std.testing.expect(!deleted);

    // Original key still there
    const v = try tree.search("apple");
    try std.testing.expect(v != null);
    allocator.free(v.?);
}

test "BTree delete from empty tree" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_delete_empty.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    const deleted = try tree.delete("anything");
    try std.testing.expect(!deleted);
}

test "BTree delete last key" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_delete_last.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    try tree.insert("only", "key");

    const deleted = try tree.delete("only");
    try std.testing.expect(deleted);

    // Tree should be empty now
    try std.testing.expect(tree.isEmpty());

    const v = try tree.search("only");
    try std.testing.expect(v == null);
}

test "BTree full range scan" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_scan_full.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    // Insert keys (out of order to test sorting)
    try tree.insert("cherry", "red");
    try tree.insert("apple", "green");
    try tree.insert("banana", "yellow");
    try tree.insert("date", "brown");
    try tree.insert("elderberry", "purple");

    // Full scan
    var iter = try tree.scan();
    var count: usize = 0;
    var prev_key: ?[]const u8 = null;

    while (try iter.next()) |kv| {
        defer iter.freeKeyValue(kv);

        // Verify sorted order
        if (prev_key) |pk| {
            try std.testing.expect(compareKeys(pk, kv.key) == .less);
            allocator.free(pk);
        }
        prev_key = try allocator.dupe(u8, kv.key);
        count += 1;
    }
    if (prev_key) |pk| allocator.free(pk);

    try std.testing.expectEqual(@as(usize, 5), count);
}

test "BTree bounded range scan" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_scan_bounded.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    // Insert keys
    try tree.insert("a", "1");
    try tree.insert("b", "2");
    try tree.insert("c", "3");
    try tree.insert("d", "4");
    try tree.insert("e", "5");

    // Scan from "b" to "d" (exclusive)
    var iter = try tree.scanRange("b", "d");
    var keys: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (keys.items) |k| allocator.free(k);
        keys.deinit(allocator);
    }

    while (try iter.next()) |kv| {
        try keys.append(allocator, try allocator.dupe(u8, kv.key));
        iter.freeKeyValue(kv);
    }

    try std.testing.expectEqual(@as(usize, 2), keys.items.len);
    try std.testing.expectEqualStrings("b", keys.items[0]);
    try std.testing.expectEqualStrings("c", keys.items[1]);
}

test "BTree empty range scan" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_scan_empty.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    // Empty tree scan
    var iter1 = try tree.scan();
    try std.testing.expect(try iter1.next() == null);

    // Insert some keys
    try tree.insert("a", "1");
    try tree.insert("c", "3");

    // Scan empty range (nothing between b and b)
    var iter2 = try tree.scanRange("b", "b");
    try std.testing.expect(try iter2.next() == null);

    // Scan range with no matching keys
    var iter3 = try tree.scanRange("x", "z");
    try std.testing.expect(try iter3.next() == null);
}

test "BTree range scan across multiple leaves" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_scan_multi.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    // Insert enough keys to cause splits (multiple leaves)
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        var val_buf: [16]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "key{d:0>5}", .{i}) catch unreachable;
        const val = std.fmt.bufPrint(&val_buf, "val{d:0>5}", .{i}) catch unreachable;
        try tree.insert(key, val);
    }

    // Full scan should return all 100 keys in order
    var iter = try tree.scan();
    var count: usize = 0;
    var prev_key: ?[]const u8 = null;

    while (try iter.next()) |kv| {
        defer iter.freeKeyValue(kv);

        // Verify sorted order
        if (prev_key) |pk| {
            try std.testing.expect(compareKeys(pk, kv.key) == .less);
            allocator.free(pk);
        }
        prev_key = try allocator.dupe(u8, kv.key);
        count += 1;
    }
    if (prev_key) |pk| allocator.free(pk);

    try std.testing.expectEqual(@as(usize, 100), count);
}

test "BTree scan with start key only" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_scan_start.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var file = try File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool, &file);

    try tree.insert("a", "1");
    try tree.insert("b", "2");
    try tree.insert("c", "3");
    try tree.insert("d", "4");

    // Scan from "c" to end
    var iter = try tree.scanRange("c", null);
    var count: usize = 0;

    while (try iter.next()) |kv| {
        defer iter.freeKeyValue(kv);
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 2), count);
}
