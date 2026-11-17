#!/usr/bin/env bash
#
# e2e-chunking.sh — M3 validation
#
# Verifies chunked block storage with fire-and-forget replication:
# - Write 20 MB using dd bs=4M (5 blocks, not 320 like `cp` would)
# - Verify all 3 nodes have the same content (with retries for async replication)
# - Append 8 MB
# - Truncate to 0
# - Check block counts and failure counters
#
set -euo pipefail

FFS=./target/release/ffs

# Verify binary exists.
if [ ! -x "$FFS" ]; then
    echo "FATAL: $FFS nije executable. Pokreni: cargo build --release"
    exit 1
fi

cleanup() {
    pkill -9 -f 'target/release/ffs' 2>/dev/null || true
    pkill -9 -f 'target/debug/ffs' 2>/dev/null || true
    for i in 1 2 3; do fusermount3 -u /tmp/mnt$i 2>/dev/null || true; done
    sleep 2
    rm -rf /tmp/node1 /tmp/node2 /tmp/node3 /tmp/mnt1 /tmp/mnt2 /tmp/mnt3
    rm -f /tmp/big.bin /tmp/append.bin
}

cleanup
mkdir -p /tmp/mnt1 /tmp/mnt2 /tmp/mnt3
export RUST_LOG=info

# ---------- Start cluster -----------------------------------------------

echo "Starting 3-node cluster..."

$FFS --node-id 1 --peers "2=127.0.0.1:7002,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7001 --mount /tmp/mnt1 --wal-dir /tmp/node1 > /tmp/node1.log 2>&1 &
$FFS --node-id 2 --peers "1=127.0.0.1:7001,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7002 --mount /tmp/mnt2 --wal-dir /tmp/node2 > /tmp/node2.log 2>&1 &
$FFS --node-id 3 --peers "1=127.0.0.1:7001,2=127.0.0.1:7002" \
  --raft-addr 127.0.0.1:7003 --mount /tmp/mnt3 --wal-dir /tmp/node3 > /tmp/node3.log 2>&1 &

# Wait for all 3 mounts to become active.
echo "Waiting for mounts..."
MOUNTS_OK=0
for _ in $(seq 1 100); do
    ALL=1
    for i in 1 2 3; do
        if ! mountpoint -q /tmp/mnt$i; then ALL=0; fi
    done
    if [ "$ALL" == "1" ]; then MOUNTS_OK=1; break; fi
    sleep 0.1
done

if [ "$MOUNTS_OK" == "0" ]; then
    echo "FATAL: nisu svi mount-ovi aktivni"
    for i in 1 2 3; do
        echo "=== node $i ==="
        tail -20 /tmp/node$i.log
    done
    cleanup
    exit 1
fi
echo "✓ All 3 mounts active"

# ---------- Detect leader -----------------------------------------------

echo "Detecting leader..."
LEADER=""
for _ in $(seq 1 50); do
    for i in 1 2 3; do
        if echo p > /tmp/mnt$i/p 2>/dev/null; then
            LEADER=$i
            rm -f /tmp/mnt$i/p
            break 2
        fi
    done
    sleep 0.3
done
if [ -z "$LEADER" ]; then
    echo "FATAL: no leader after 15s"
    cleanup
    exit 1
fi
echo "✓ Leader: node $LEADER"

# ---------- Write 20 MB using dd bs=4M ----------------------------------
# Use dd with bs=4M (= BLOCK_SIZE) instead of `cp` (which uses 64KB buffer).
# This produces 5 FUSE writes (= 5 blocks) instead of ~320, avoiding
# 60x more Raft proposes and keeping each FUSE write under the kernel's
# 1s per-write timeout.

echo ""
echo "=== Write 20 MB (dd bs=4M) ==="
dd if=/dev/urandom of=/tmp/big.bin bs=1M count=20 2>/dev/null
time dd if=/tmp/big.bin of=/tmp/mnt$LEADER/big.bin bs=4M conv=fsync 2>&1 | tail -1

# ---------- Wait for async replication ----------------------------------

echo "Waiting for replication..."
for _ in $(seq 1 50); do
    ALL=1
    for i in 1 2 3; do
        if [ ! -f /tmp/mnt$i/big.bin ]; then ALL=0; fi
    done
    if [ "$ALL" == "1" ]; then break; fi
    sleep 0.2
done

# ---------- Verify sizes ------------------------------------------------

EXPECTED=$(stat -c %s /tmp/big.bin)
for i in 1 2 3; do
    ACTUAL=$(stat -c %s /tmp/mnt$i/big.bin 2>/dev/null || echo MISSING)
    if [ "$EXPECTED" != "$ACTUAL" ]; then
        echo "FAIL: size mismatch on node $i: $ACTUAL vs $EXPECTED"
        exit 1
    fi
    echo "✓ node $i size $ACTUAL"
done

# ---------- Verify content (with retry for async replication) -----------

EXPECTED_HASH=$(sha256sum /tmp/big.bin | cut -d' ' -f1)
for i in 1 2 3; do
    MATCH=0
    for _ in $(seq 1 50); do
        ACTUAL_HASH=$(sha256sum /tmp/mnt$i/big.bin 2>/dev/null | cut -d' ' -f1 || echo "")
        if [ "$EXPECTED_HASH" == "$ACTUAL_HASH" ]; then MATCH=1; break; fi
        sleep 0.2
    done
    if [ "$MATCH" == "0" ]; then
        echo "FAIL: content mismatch on node $i after 10s"
        exit 1
    fi
    echo "✓ node $i hash matches"
done

# ---------- Append 8 MB using dd oflag=append ---------------------------

echo ""
echo "=== Append 8 MB (dd bs=4M oflag=append) ==="
dd if=/dev/urandom of=/tmp/append.bin bs=1M count=8 2>/dev/null
time dd if=/tmp/append.bin of=/tmp/mnt$LEADER/big.bin bs=4M oflag=append conv=notrunc,fsync 2>&1 | tail -1

# Give async replication + manifest commit time.
sleep 2

NEW_EXPECTED=$((EXPECTED + 8*1024*1024))
for i in 1 2 3; do
    ACTUAL=$(stat -c %s /tmp/mnt$i/big.bin 2>/dev/null || echo MISSING)
    if [ "$NEW_EXPECTED" != "$ACTUAL" ]; then
        echo "FAIL: size after append on node $i: $ACTUAL vs $NEW_EXPECTED"
        exit 1
    fi
    echo "✓ node $i size after append $ACTUAL"
done

# ---------- Truncate to 0 -----------------------------------------------

echo ""
echo "=== Truncate to 0 ==="
truncate -s 0 /tmp/mnt$LEADER/big.bin
sleep 2

for i in 1 2 3; do
    ACTUAL=$(stat -c %s /tmp/mnt$i/big.bin 2>/dev/null || echo MISSING)
    if [ "$ACTUAL" != "0" ]; then
        echo "FAIL: size after truncate on node $i: $ACTUAL"
        exit 1
    fi
    echo "✓ node $i size 0"
done

# ---------- Block counts ------------------------------------------------

echo ""
echo "=== Block counts ==="
for i in 1 2 3; do
    echo "  node $i: $(ls /tmp/node$i/data/*.block 2>/dev/null | wc -l) blocks"
done

# ---------- Failure counters --------------------------------------------

echo ""
echo "=== Failure counters (node $LEADER) ==="
echo "  replicate timeouts: $(grep -c 'replicate to.*timed out' /tmp/node$LEADER.log 2>/dev/null || echo 0)"
echo "  leadership lost:    $(grep -c 'leadership lost' /tmp/node$LEADER.log 2>/dev/null || echo 0)"
echo "  write failures:     $(grep -c 'write failed' /tmp/node$LEADER.log 2>/dev/null || echo 0)"

# ---------- Done --------------------------------------------------------

echo ""
echo "=================================="
echo "✓ ALL CHUNKING TESTS PASSED"
echo "=================================="

cleanup