const std = @import("std");
const db_mod = @import("db.zig");
const types = @import("core/types.zig");

const Database = db_mod.Database;
const Config = types.Config;

/// Benchmark configuration
pub const BenchConfig = struct {
    /// Number of operations
    num_ops: usize = 10000,
    /// Key size in bytes
    key_size: usize = 16,
    /// Value size in bytes
    value_size: usize = 100,
    /// Read/write ratio (0.0 = all writes, 1.0 = all reads)
    read_ratio: f64 = 0.5,
    /// Database page size
    page_size: usize = types.DEFAULT_PAGE_SIZE,
    /// Buffer pool size
    cache_size: usize = 16 * 1024 * 1024, // 16MB
};

/// Benchmark results
pub const BenchResult = struct {
    /// Total operations
    ops: usize,
    /// Elapsed time in nanoseconds
    elapsed_ns: u64,
    /// Operations per second
    ops_per_sec: f64,
    /// Average latency in microseconds
    avg_latency_us: f64,
    /// Throughput in MB/s
    throughput_mb_s: f64,
};

/// Run insert benchmark
pub fn benchInsert(allocator: std.mem.Allocator, config: BenchConfig) !BenchResult {
    const path = "bench_insert.db";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("bench_insert.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("bench_insert.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, .{
        .page_size = config.page_size,
        .cache_size = config.cache_size,
        .sync_mode = .none, // No sync for benchmarks
    });
    defer db.close();

    const key_buf = try allocator.alloc(u8, config.key_size);
    defer allocator.free(key_buf);
    const value_buf = try allocator.alloc(u8, config.value_size);
    defer allocator.free(value_buf);

    // Fill value with random data
    var prng = std.Random.DefaultPrng.init(12345);
    prng.fill(value_buf);

    const start = std.time.nanoTimestamp();

    for (0..config.num_ops) |i| {
        // Generate key
        _ = std.fmt.bufPrint(key_buf, "{d:0>16}", .{i}) catch continue;
        try db.put(key_buf, value_buf);
    }

    const end = std.time.nanoTimestamp();
    const elapsed_ns: u64 = @intCast(end - start);

    const data_size = config.num_ops * (config.key_size + config.value_size);
    const elapsed_sec = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;

    return .{
        .ops = config.num_ops,
        .elapsed_ns = elapsed_ns,
        .ops_per_sec = @as(f64, @floatFromInt(config.num_ops)) / elapsed_sec,
        .avg_latency_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(config.num_ops)) / 1000.0,
        .throughput_mb_s = @as(f64, @floatFromInt(data_size)) / elapsed_sec / 1_000_000.0,
    };
}

/// Run read benchmark
pub fn benchRead(allocator: std.mem.Allocator, config: BenchConfig) !BenchResult {
    const path = "bench_read.db";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("bench_read.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("bench_read.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, .{
        .page_size = config.page_size,
        .cache_size = config.cache_size,
        .sync_mode = .none,
    });
    defer db.close();

    const key_buf = try allocator.alloc(u8, config.key_size);
    defer allocator.free(key_buf);
    const value_buf = try allocator.alloc(u8, config.value_size);
    defer allocator.free(value_buf);

    // Populate database first
    for (0..config.num_ops) |i| {
        _ = std.fmt.bufPrint(key_buf, "{d:0>16}", .{i}) catch continue;
        try db.put(key_buf, value_buf);
    }

    const start = std.time.nanoTimestamp();

    for (0..config.num_ops) |i| {
        _ = std.fmt.bufPrint(key_buf, "{d:0>16}", .{i}) catch continue;
        if (try db.get(key_buf)) |val| {
            allocator.free(val);
        }
    }

    const end = std.time.nanoTimestamp();
    const elapsed_ns: u64 = @intCast(end - start);

    const data_size = config.num_ops * (config.key_size + config.value_size);
    const elapsed_sec = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;

    return .{
        .ops = config.num_ops,
        .elapsed_ns = elapsed_ns,
        .ops_per_sec = @as(f64, @floatFromInt(config.num_ops)) / elapsed_sec,
        .avg_latency_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(config.num_ops)) / 1000.0,
        .throughput_mb_s = @as(f64, @floatFromInt(data_size)) / elapsed_sec / 1_000_000.0,
    };
}

/// Run mixed workload benchmark
pub fn benchMixed(allocator: std.mem.Allocator, config: BenchConfig) !BenchResult {
    const path = "bench_mixed.db";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("bench_mixed.db.wal") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("bench_mixed.db.wal") catch {};
    }

    const db = try Database.open(allocator, path, .{
        .page_size = config.page_size,
        .cache_size = config.cache_size,
        .sync_mode = .none,
    });
    defer db.close();

    const key_buf = try allocator.alloc(u8, config.key_size);
    defer allocator.free(key_buf);
    const value_buf = try allocator.alloc(u8, config.value_size);
    defer allocator.free(value_buf);

    var prng = std.Random.DefaultPrng.init(12345);

    // Pre-populate half the keys
    for (0..config.num_ops / 2) |i| {
        _ = std.fmt.bufPrint(key_buf, "{d:0>16}", .{i}) catch continue;
        try db.put(key_buf, value_buf);
    }

    const start = std.time.nanoTimestamp();

    var reads: usize = 0;
    var writes: usize = 0;

    for (0..config.num_ops) |_| {
        const key_idx = prng.random().uintLessThan(usize, config.num_ops);
        _ = std.fmt.bufPrint(key_buf, "{d:0>16}", .{key_idx}) catch continue;

        if (prng.random().float(f64) < config.read_ratio) {
            // Read
            if (try db.get(key_buf)) |val| {
                allocator.free(val);
            }
            reads += 1;
        } else {
            // Write
            try db.put(key_buf, value_buf);
            writes += 1;
        }
    }

    const end = std.time.nanoTimestamp();
    const elapsed_ns: u64 = @intCast(end - start);

    const data_size = config.num_ops * (config.key_size + config.value_size);
    const elapsed_sec = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;

    return .{
        .ops = config.num_ops,
        .elapsed_ns = elapsed_ns,
        .ops_per_sec = @as(f64, @floatFromInt(config.num_ops)) / elapsed_sec,
        .avg_latency_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(config.num_ops)) / 1000.0,
        .throughput_mb_s = @as(f64, @floatFromInt(data_size)) / elapsed_sec / 1_000_000.0,
    };
}

/// Print benchmark results
pub fn printResult(name: []const u8, result: BenchResult) void {
    std.debug.print("\n{s}:\n", .{name});
    std.debug.print("  Operations:    {d}\n", .{result.ops});
    std.debug.print("  Time:          {d:.2} ms\n", .{@as(f64, @floatFromInt(result.elapsed_ns)) / 1_000_000.0});
    std.debug.print("  Ops/sec:       {d:.0}\n", .{result.ops_per_sec});
    std.debug.print("  Avg latency:   {d:.2} us\n", .{result.avg_latency_us});
    std.debug.print("  Throughput:    {d:.2} MB/s\n", .{result.throughput_mb_s});
}

/// Run all benchmarks
pub fn runAll(allocator: std.mem.Allocator) !void {
    std.debug.print("=== Monolith Benchmarks ===\n", .{});

    const config = BenchConfig{
        .num_ops = 10000,
        .key_size = 16,
        .value_size = 100,
    };

    const insert_result = try benchInsert(allocator, config);
    printResult("Sequential Insert", insert_result);

    const read_result = try benchRead(allocator, config);
    printResult("Sequential Read", read_result);

    const mixed_result = try benchMixed(allocator, config);
    printResult("Mixed Workload (50/50)", mixed_result);

    std.debug.print("\n=== Done ===\n", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try runAll(allocator);
}

// Tests

test "bench insert small" {
    const allocator = std.testing.allocator;

    const result = try benchInsert(allocator, .{
        .num_ops = 100,
        .key_size = 8,
        .value_size = 32,
    });

    try std.testing.expectEqual(@as(usize, 100), result.ops);
    try std.testing.expect(result.ops_per_sec > 0);
}

test "bench read small" {
    const allocator = std.testing.allocator;

    const result = try benchRead(allocator, .{
        .num_ops = 100,
        .key_size = 8,
        .value_size = 32,
    });

    try std.testing.expectEqual(@as(usize, 100), result.ops);
    try std.testing.expect(result.ops_per_sec > 0);
}

test "bench mixed small" {
    const allocator = std.testing.allocator;

    const result = try benchMixed(allocator, .{
        .num_ops = 100,
        .key_size = 8,
        .value_size = 32,
        .read_ratio = 0.8,
    });

    try std.testing.expectEqual(@as(usize, 100), result.ops);
    try std.testing.expect(result.ops_per_sec > 0);
}
