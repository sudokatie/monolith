const std = @import("std");
const types = @import("../core/types.zig");
const errors = @import("../core/errors.zig");
const btree_node = @import("btree_node.zig");
const buffer_mod = @import("buffer.zig");
const page_mod = @import("page.zig");

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

/// B+ tree for key-value storage
pub const BTree = struct {
    /// Root page ID (INVALID_PAGE_ID if tree is empty)
    root_id: PageId,
    /// Buffer pool for page access
    buffer_pool: *BufferPool,
    /// Allocator for temporary operations
    allocator: std.mem.Allocator,
    /// Page size
    page_size: usize,

    /// Initialize an empty B+ tree
    pub fn init(allocator: std.mem.Allocator, buffer_pool: *BufferPool) BTree {
        return .{
            .root_id = INVALID_PAGE_ID,
            .buffer_pool = buffer_pool,
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

                // In internal node, key at index i separates children i and i+1
                // If found: go right (index + 1)
                // If not found: go to index (which is where key should be)
                const child_index = if (result.found) result.index + 1 else result.index;

                const child_id = node.getChild(child_index) orelse {
                    return null;
                };

                current_page_id = child_id;
            }
        }
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

    const file_mod = @import("file.zig");
    var file = try file_mod.File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool);

    // Empty tree should return null for any key
    const result = try tree.search("anykey");
    try std.testing.expect(result == null);
    try std.testing.expect(tree.isEmpty());
}

test "BTree single leaf search" {
    const allocator = std.testing.allocator;

    const test_path = "test_btree_leaf.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    const file_mod = @import("file.zig");
    var file = try file_mod.File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
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
    var tree = BTree.init(allocator, &pool);
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

    const file_mod = @import("file.zig");
    var file = try file_mod.File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
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
    var tree = BTree.init(allocator, &pool);
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

    const file_mod = @import("file.zig");
    var file = try file_mod.File.open(allocator, test_path, DEFAULT_PAGE_SIZE);
    defer file.close();

    var pool = try BufferPool.init(allocator, &file, 16);
    defer pool.deinit();

    var tree = BTree.init(allocator, &pool);

    try std.testing.expect(tree.isEmpty());
    try std.testing.expectEqual(INVALID_PAGE_ID, tree.getRootId());

    // Set root
    tree.setRoot(42);
    try std.testing.expect(!tree.isEmpty());
    try std.testing.expectEqual(@as(PageId, 42), tree.getRootId());
}
