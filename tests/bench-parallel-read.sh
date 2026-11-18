#!/usr/bin/env bash
#
# bench-parallel-read.sh — M4 parallel read benchmark
#
# Verifies ADR-006: reads scale with cluster size because each node reads
# its own local blocks (after replication + startup repair).
#
# Tests:
#   1. Single reader on node 1 (baseline)
#   2. Three parallel readers on nodes 1, 2, 3 (expected ~3x)
#   3. Ten parallel readers on node 1 (parallel I/O on one node)
#
# Every dd is wrapped in `timeout` so a slow/stuck read fails instead of
# hanging forever. Cleanup runs even on failure (trap EXIT).
#
set -uo pipefail

FFS=./target/release/ffs
SIZE_MB="${SIZE_MB:-200}"
BLOCK_SIZE_MB="${BLOCK_SIZE_MB:-4}"
READ_TIMEOUT="${READ_TIMEOUT:-60}"

if [ ! -x "$FFS" ]; then
    echo "FATAL: $FFS not found. Run: cargo build --release"
    exit 1
fi

cleanup() {
    pkill -9 -f 'target/release/ffs' 2>/dev/null || true
    for i in 1 2 3; do
        timeout 5 fusermount3 -u /tmp/mnt$i 2>/dev/null || true
    done
    sleep 1
    rm -rf /tmp/node1 /tmp/node2 /tmp/node3 /tmp/mnt1 /tmp/mnt2 /tmp/mnt3
    rm -f /tmp/bench-source.bin
}
trap cleanup EXIT

cleanup
mkdir -p /tmp/mnt1 /tmp/mnt2 /tmp/mnt3
export RUST_LOG=warn

echo "=== M4 Parallel Read Benchmark ==="
echo "File size: ${SIZE_MB} MB"
echo "Block size: ${BLOCK_SIZE_MB} MB"
echo "Per-read timeout: ${READ_TIMEOUT}s"
echo ""

# ---------------------------------------------------------------
# Prepare source file
# ---------------------------------------------------------------
echo "Preparing ${SIZE_MB} MB test file..."
dd if=/dev/urandom of=/tmp/bench-source.bin bs=1M count=$SIZE_MB 2>/dev/null

# ---------------------------------------------------------------
# Start cluster
# ---------------------------------------------------------------
echo "Starting 3-node cluster..."

$FFS --node-id 1 --peers "2=127.0.0.1:7002,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7001 --mount /tmp/mnt1 --wal-dir /tmp/node1 > /tmp/node1.log 2>&1 &
$FFS --node-id 2 --peers "1=127.0.0.1:7001,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7002 --mount /tmp/mnt2 --wal-dir /tmp/node2 > /tmp/node2.log 2>&1 &
$FFS --node-id 3 --peers "1=127.0.0.1:7001,2=127.0.0.1:7002" \
  --raft-addr 127.0.0.1:7003 --mount /tmp/mnt3 --wal-dir /tmp/node3 > /tmp/node3.log 2>&1 &

# Wait for all mounts.
echo "Waiting for mounts..."
MOUNTS_OK=0
for _ in $(seq 1 100); do
    ALL=1
    for i in 1 2 3; do
        mountpoint -q /tmp/mnt$i || ALL=0
    done
    [ "$ALL" == "1" ] && { MOUNTS_OK=1; break; }
    sleep 0.1
done

if [ "$MOUNTS_OK" == "0" ]; then
    echo "FATAL: not all mounts active"
    for i in 1 2 3; do
        echo "=== node $i ==="
        tail -20 /tmp/node$i.log
    done
    exit 1
fi
echo "✓ All 3 mounts active"

# ---------------------------------------------------------------
# Detect leader
# ---------------------------------------------------------------
echo "Detecting leader..."
LEADER=""
for _ in $(seq 1 50); do
    for i in 1 2 3; do
        if echo p > /tmp/mnt$i/.probe 2>/dev/null; then
            LEADER=$i
            rm -f /tmp/mnt$i/.probe
            break 2
        fi
    done
    sleep 0.3
done

if [ -z "$LEADER" ]; then
    echo "FATAL: no leader after 15s"
    exit 1
fi
echo "✓ Leader: node $LEADER"

# ---------------------------------------------------------------
# Upload test file
# ---------------------------------------------------------------
echo ""
echo "Uploading ${SIZE_MB} MB to cluster..."
dd if=/tmp/bench-source.bin of=/tmp/mnt$LEADER/bench.bin bs=${BLOCK_SIZE_MB}M conv=fsync 2>/dev/null

# ---------------------------------------------------------------
# Wait for replication + startup repair
# ---------------------------------------------------------------
echo "Waiting for replication + repair (up to 30s)..."
sleep 5

EXPECTED_BLOCKS=$(( (SIZE_MB + BLOCK_SIZE_MB - 1) / BLOCK_SIZE_MB ))

for attempt in $(seq 1 30); do
    ALL_OK=1
    for i in 1 2 3; do
        COUNT=$(ls /tmp/node$i/data/*.block 2>/dev/null | wc -l)
        if [ "$COUNT" -lt "$EXPECTED_BLOCKS" ]; then
            ALL_OK=0
            break
        fi
    done
    [ "$ALL_OK" == "1" ] && break
    sleep 1
done

echo "Block counts (expected ~${EXPECTED_BLOCKS}):"
for i in 1 2 3; do
    echo "  node $i: $(ls /tmp/node$i/data/*.block 2>/dev/null | wc -l) blocks"
done

# ---------------------------------------------------------------
# Helper: measure time in ms
# ---------------------------------------------------------------
now_ms() {
    date +%s%3N
}

calc_mbps() {
    awk "BEGIN { printf \"%.1f\", $1 / ($2 / 1000.0) }"
}

# ---------------------------------------------------------------
# Warm-up: read file fully from each node so page cache is hot
# ---------------------------------------------------------------
echo ""
echo "Warming page cache on all 3 nodes..."
for i in 1 2 3; do
    timeout $READ_TIMEOUT dd if=/tmp/mnt$i/bench.bin of=/dev/null bs=${BLOCK_SIZE_MB}M 2>/dev/null
done
sleep 1

# ---------------------------------------------------------------
# Test 1: Single reader (node 1)
# ---------------------------------------------------------------
echo ""
echo "=== Test 1: Single reader (node 1) ==="

S=$(now_ms)
if ! timeout $READ_TIMEOUT dd if=/tmp/mnt1/bench.bin of=/dev/null bs=${BLOCK_SIZE_MB}M 2>/dev/null; then
    echo "  FAIL: single read timed out after ${READ_TIMEOUT}s"
    exit 1
fi
E=$(now_ms)
T1_MS=$((E - S))
T1_MBPS=$(calc_mbps $SIZE_MB $T1_MS)
echo "  ${T1_MS}ms (${T1_MBPS} MB/s)"

# ---------------------------------------------------------------
# Test 2: 3 parallel readers (one per node)
# ---------------------------------------------------------------
echo ""
echo "=== Test 2: 3 parallel readers (nodes 1, 2, 3) ==="

# Pre-check: each node can read independently
for i in 1 2 3; do
    if ! timeout $READ_TIMEOUT dd if=/tmp/mnt$i/bench.bin of=/dev/null bs=${BLOCK_SIZE_MB}M 2>/dev/null; then
        echo "  FAIL: node $i pre-check timed out"
        exit 1
    fi
done
sleep 1

S=$(now_ms)
PIDS=()
for i in 1 2 3; do
    timeout $READ_TIMEOUT dd if=/tmp/mnt$i/bench.bin of=/dev/null bs=${BLOCK_SIZE_MB}M 2>/dev/null &
    PIDS+=($!)
done
FAILED=0
for pid in "${PIDS[@]}"; do
    wait "$pid" || FAILED=1
done
E=$(now_ms)
T3_MS=$((E - S))

if [ "$FAILED" == "1" ]; then
    echo "  FAIL: one or more parallel reads timed out"
    exit 1
fi

TOTAL_MB=$((SIZE_MB * 3))
T3_MBPS=$(calc_mbps $TOTAL_MB $T3_MS)
SPEEDUP=$(awk "BEGIN { printf \"%.2f\", $T3_MBPS / $T1_MBPS }")
echo "  ${T3_MS}ms total, ${T3_MBPS} MB/s aggregate"
echo "  Speedup vs single-reader: ${SPEEDUP}x (ideal: 3.0x)"

# ---------------------------------------------------------------
# Test 3: 10 parallel readers on node 1
# ---------------------------------------------------------------
echo ""
echo "=== Test 3: 10 parallel readers (all on node 1) ==="

echo "Pre-check: single read on node 1..."
S=$(now_ms)
if ! timeout $READ_TIMEOUT dd if=/tmp/mnt1/bench.bin of=/dev/null bs=${BLOCK_SIZE_MB}M 2>/dev/null; then
    echo "  WARN: pre-check timed out (>${READ_TIMEOUT}s) — skipping Test 3"
    T10_MBPS="SKIPPED"
    T10_MS=0
else
    E=$(now_ms)
    PRECHECK_MS=$((E - S))
    echo "  Pre-check: ${PRECHECK_MS}ms"

    if [ $PRECHECK_MS -lt 30000 ]; then
        sleep 1
        S=$(now_ms)
        PIDS=()
        for _ in $(seq 1 10); do
            timeout $READ_TIMEOUT dd if=/tmp/mnt1/bench.bin of=/dev/null bs=${BLOCK_SIZE_MB}M 2>/dev/null &
            PIDS+=($!)
        done
        FAILED=0
        for pid in "${PIDS[@]}"; do
            wait "$pid" || FAILED=1
        done
        E=$(now_ms)
        T10_MS=$((E - S))

        if [ "$FAILED" == "1" ]; then
            echo "  WARN: one or more parallel reads timed out"
            T10_MBPS="FAILED"
        else
            TOTAL_MB_10=$((SIZE_MB * 10))
            T10_MBPS=$(calc_mbps $TOTAL_MB_10 $T10_MS)
            echo "  ${T10_MS}ms total, ${T10_MBPS} MB/s aggregate"
        fi
    else
        echo "  Pre-check too slow (>30s) — skipping parallel test"
        T10_MBPS="SKIPPED"
    fi
fi

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
echo ""
echo "========================================"
echo "  BENCHMARK SUMMARY"
echo "========================================"
echo "  File size:                        ${SIZE_MB} MB"
echo "  Single reader (node 1):           ${T1_MBPS} MB/s (${T1_MS}ms)"
echo "  3 parallel (one per node):        ${T3_MBPS} MB/s (${SPEEDUP}x speedup)"
echo "  10 parallel (all on node 1):      ${T10_MBPS} MB/s"
echo "========================================"

# ---------------------------------------------------------------
# Failure counters
# ---------------------------------------------------------------
echo ""
echo "Failure counters (leader node $LEADER):"
echo "  replicate timeouts: $(grep -c 'replicate to.*timed out' /tmp/node$LEADER.log 2>/dev/null || echo 0)"
echo "  write failures:     $(grep -c 'write failed' /tmp/node$LEADER.log 2>/dev/null || echo 0)"
echo "  fetch failures:     $(grep -c 'fetch_block.*timeout\|fetch_block.*failed' /tmp/node$LEADER.log 2>/dev/null || echo 0)"

# ---------------------------------------------------------------
# Replication lag: block counts consistency
# ---------------------------------------------------------------
echo ""
echo "Replication consistency:"
C1=$(ls /tmp/node1/data/*.block 2>/dev/null | wc -l)
C2=$(ls /tmp/node2/data/*.block 2>/dev/null | wc -l)
C3=$(ls /tmp/node3/data/*.block 2>/dev/null | wc -l)
echo "  node 1: $C1 blocks"
echo "  node 2: $C2 blocks"
echo "  node 3: $C3 blocks"

MAX=$C1
[ $C2 -gt $MAX ] && MAX=$C2
[ $C3 -gt $MAX ] && MAX=$C3
MIN=$C1
[ $C2 -lt $MIN ] && MIN=$C2
[ $C3 -lt $MIN ] && MIN=$C3
if [ $((MAX - MIN)) -le 2 ]; then
    echo "  ✓ Consistent (diff ≤ 2 blocks)"
else
    echo "  ⚠ Lag: $((MAX - MIN)) blocks difference"
fi

echo ""
echo "Done."