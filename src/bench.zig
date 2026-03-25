const std = @import("std");
const monolith = @import("monolith");
const DB = monolith.DB;
const DBConfig = monolith.DBConfig;

const BENCH_DIR = "/tmp/monolith_bench";

// Simple print function for Zig 0.15
fn print(comptime fmt: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    const str = std.fmt.bufPrint(&buf, fmt, args) catch return;
    _ = std.posix.write(1, str) catch {};
}

fn cleanup() void {
    std.fs.cwd().deleteTree(BENCH_DIR) catch {};
}

fn formatDuration(ns: u64) [32]u8 {
    var buf: [32]u8 = undefined;
    @memset(&buf, 0);
    if (ns < 1000) {
        _ = std.fmt.bufPrint(&buf, "{d}ns", .{ns}) catch {};
    } else if (ns < 1_000_000) {
        _ = std.fmt.bufPrint(&buf, "{d:.2}us", .{@as(f64, @floatFromInt(ns)) / 1000.0}) catch {};
    } else if (ns < 1_000_000_000) {
        _ = std.fmt.bufPrint(&buf, "{d:.2}ms", .{@as(f64, @floatFromInt(ns)) / 1_000_000.0}) catch {};
    } else {
        _ = std.fmt.bufPrint(&buf, "{d:.2}s", .{@as(f64, @floatFromInt(ns)) / 1_000_000_000.0}) catch {};
    }
    return buf;
}

fn formatOpsPerSec(ops: u64, ns: u64) [32]u8 {
    var buf: [32]u8 = undefined;
    @memset(&buf, 0);
    if (ns == 0) {
        _ = std.fmt.bufPrint(&buf, "inf ops/sec", .{}) catch {};
    } else {
        const ops_per_sec = @as(f64, @floatFromInt(ops)) * 1_000_000_000.0 / @as(f64, @floatFromInt(ns));
        if (ops_per_sec >= 1_000_000) {
            _ = std.fmt.bufPrint(&buf, "{d:.2}M ops/sec", .{ops_per_sec / 1_000_000.0}) catch {};
        } else if (ops_per_sec >= 1_000) {
            _ = std.fmt.bufPrint(&buf, "{d:.2}K ops/sec", .{ops_per_sec / 1_000.0}) catch {};
        } else {
            _ = std.fmt.bufPrint(&buf, "{d:.0} ops/sec", .{ops_per_sec}) catch {};
        }
    }
    return buf;
}

fn printResult(name: []const u8, ops: u64, ns: u64) void {
    const duration = formatDuration(ns / ops);
    const throughput = formatOpsPerSec(ops, ns);
    
    print("{s:<30} {d:>10} ops  {s:>12}  {s:>16}\n", .{
        name,
        ops,
        std.mem.sliceTo(&duration, 0),
        std.mem.sliceTo(&throughput, 0),
    });
}

fn benchSequentialWrites(allocator: std.mem.Allocator, config: DBConfig, count: u64) !u64 {
    cleanup();
    defer cleanup();

    const db = try DB.open(allocator, BENCH_DIR, config);
    defer db.close();

    var key_buf: [32]u8 = undefined;
    const value = "benchmark_value_data_012345678901234567890123456789";

    var timer = try std.time.Timer.start();

    var i: u64 = 0;
    while (i < count) : (i += 1) {
        const key = std.fmt.bufPrint(&key_buf, "key_{d:0>16}", .{i}) catch unreachable;
        try db.put(key, value);
    }

    return timer.read();
}

fn benchSequentialReads(allocator: std.mem.Allocator, config: DBConfig, count: u64) !u64 {
    cleanup();
    defer cleanup();

    const db = try DB.open(allocator, BENCH_DIR, config);
    defer db.close();

    // Populate first
    var key_buf: [32]u8 = undefined;
    const value = "benchmark_value_data_012345678901234567890123456789";

    var i: u64 = 0;
    while (i < count) : (i += 1) {
        const key = std.fmt.bufPrint(&key_buf, "key_{d:0>16}", .{i}) catch unreachable;
        try db.put(key, value);
    }

    // Now benchmark reads
    var timer = try std.time.Timer.start();

    i = 0;
    while (i < count) : (i += 1) {
        const key = std.fmt.bufPrint(&key_buf, "key_{d:0>16}", .{i}) catch unreachable;
        _ = try db.get(key);
    }

    return timer.read();
}

fn benchRandomReads(allocator: std.mem.Allocator, config: DBConfig, count: u64) !u64 {
    cleanup();
    defer cleanup();

    const db = try DB.open(allocator, BENCH_DIR, config);
    defer db.close();

    // Populate first
    var key_buf: [32]u8 = undefined;
    const value = "benchmark_value_data_012345678901234567890123456789";

    var i: u64 = 0;
    while (i < count) : (i += 1) {
        const key = std.fmt.bufPrint(&key_buf, "key_{d:0>16}", .{i}) catch unreachable;
        try db.put(key, value);
    }

    // Now benchmark random reads
    var rng = std.Random.DefaultPrng.init(42);
    var timer = try std.time.Timer.start();

    i = 0;
    while (i < count) : (i += 1) {
        const idx = rng.random().intRangeAtMost(u64, 0, count - 1);
        const key = std.fmt.bufPrint(&key_buf, "key_{d:0>16}", .{idx}) catch unreachable;
        _ = try db.get(key);
    }

    return timer.read();
}

fn benchTransactions(allocator: std.mem.Allocator, config: DBConfig, count: u64) !u64 {
    cleanup();
    defer cleanup();

    const db = try DB.open(allocator, BENCH_DIR, config);
    defer db.close();

    var key_buf: [32]u8 = undefined;
    const value = "txn_value";

    var timer = try std.time.Timer.start();

    var i: u64 = 0;
    while (i < count) : (i += 1) {
        var txn = try db.begin();

        // Do 10 operations per transaction
        var j: u64 = 0;
        while (j < 10) : (j += 1) {
            const key = std.fmt.bufPrint(&key_buf, "txn_key_{d:0>8}_{d}", .{ i, j }) catch unreachable;
            try txn.put(key, value);
        }

        try txn.commit();
        txn.deinit();
    }

    return timer.read();
}

fn benchRangeScan(allocator: std.mem.Allocator, config: DBConfig, count: u64) !u64 {
    cleanup();
    defer cleanup();

    const db = try DB.open(allocator, BENCH_DIR, config);
    defer db.close();

    // Populate
    var key_buf: [32]u8 = undefined;
    const value = "range_value";

    var i: u64 = 0;
    while (i < count) : (i += 1) {
        const key = std.fmt.bufPrint(&key_buf, "rng_{d:0>16}", .{i}) catch unreachable;
        try db.put(key, value);
    }

    // Benchmark full scan
    var timer = try std.time.Timer.start();

    var iter = try db.range(null, null);
    defer iter.close();

    var scanned: u64 = 0;
    while (true) {
        const entry = iter.next() catch break;
        if (entry == null) break;
        iter.freeEntry(entry.?);
        scanned += 1;
    }

    const elapsed = timer.read();

    if (scanned != count) {
        
        print("  WARNING: scanned {d} but expected {d}\n", .{ scanned, count });
    }

    return elapsed;
}

fn benchBatchedWrites(allocator: std.mem.Allocator, count: u64) !u64 {
    cleanup();
    defer cleanup();

    // Use batch sync mode for higher throughput
    const db = try DB.open(allocator, BENCH_DIR, .{
        .sync_mode = .none, // No fsync for max speed
        .enable_wal = false,
    });
    defer db.close();

    var key_buf: [32]u8 = undefined;
    const value = "batch_value_data_0123456789";

    var timer = try std.time.Timer.start();

    var i: u64 = 0;
    while (i < count) : (i += 1) {
        const key = std.fmt.bufPrint(&key_buf, "bat_{d:0>16}", .{i}) catch unreachable;
        try db.put(key, value);
    }

    return timer.read();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    

    print("\n", .{});
    print("Monolith Benchmarks\n", .{});
    print("===================\n\n", .{});

    const small_count: u64 = 10_000;
    const large_count: u64 = 100_000;

    // Default config (sync mode, WAL enabled)
    const sync_config = DBConfig{
        .sync_mode = .sync,
        .enable_wal = true,
    };

    // No-sync config for comparison
    const nosync_config = DBConfig{
        .sync_mode = .none,
        .enable_wal = false,
    };

    print("{s:<30} {s:>10}  {s:>12}  {s:>16}\n", .{ "Benchmark", "Ops", "Latency", "Throughput" });
    print("{s}\n", .{"-" ** 72});

    // Sequential writes (sync)
    var ns = try benchSequentialWrites(allocator, sync_config, small_count);
    printResult("seq write (sync)", small_count, ns);

    // Sequential writes (no sync)
    ns = try benchSequentialWrites(allocator, nosync_config, large_count);
    printResult("seq write (no sync)", large_count, ns);

    // Batched writes
    ns = try benchBatchedWrites(allocator, large_count);
    printResult("batched write", large_count, ns);

    // Sequential reads
    ns = try benchSequentialReads(allocator, nosync_config, large_count);
    printResult("seq read", large_count, ns);

    // Random reads
    ns = try benchRandomReads(allocator, nosync_config, large_count);
    printResult("random read", large_count, ns);

    // Transactions
    ns = try benchTransactions(allocator, nosync_config, small_count);
    printResult("transactions (10 ops each)", small_count, ns);

    // Range scan
    ns = try benchRangeScan(allocator, nosync_config, large_count);
    printResult("range scan", large_count, ns);

    print("\n", .{});
}
