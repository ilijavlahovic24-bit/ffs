#!/usr/bin/env bash
set -euo pipefail

FFS=./target/release/ffs

# Verify binary exists first.
if [ ! -x "$FFS" ]; then
    echo "FATAL: $FFS ne postoji ili nije executable"
    echo "Pokreni: cargo build --release"
    exit 1
fi

cleanup() {
    pkill -9 -f 'target/release/ffs' 2>/dev/null || true
    for i in 1 2 3; do fusermount3 -u /tmp/mnt$i 2>/dev/null || true; done
    sleep 2
    rm -rf /tmp/node1 /tmp/node2 /tmp/node3 /tmp/mnt1 /tmp/mnt2 /tmp/mnt3
}

cleanup
mkdir -p /tmp/mnt1 /tmp/mnt2 /tmp/mnt3

$FFS --node-id 1 --peers "2=127.0.0.1:7002,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7001 --mount /tmp/mnt1 --wal-dir /tmp/node1 > /tmp/n1.log 2>&1 &
$FFS --node-id 2 --peers "1=127.0.0.1:7001,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7002 --mount /tmp/mnt2 --wal-dir /tmp/node2 > /tmp/n2.log 2>&1 &
$FFS --node-id 3 --peers "1=127.0.0.1:7001,2=127.0.0.1:7002" \
  --raft-addr 127.0.0.1:7003 --mount /tmp/mnt3 --wal-dir /tmp/node3 > /tmp/n3.log 2>&1 &

# Wait for all 3 mounts to be active.
echo "Waiting for mounts..."
MOUNTS_OK=1
for _ in $(seq 1 100); do
    ALL=1
    for i in 1 2 3; do
        if ! mountpoint -q /tmp/mnt$i; then ALL=0; fi
    done
    if [ "$ALL" == "1" ]; then MOUNTS_OK=1; break; fi
    MOUNTS_OK=0
    sleep 0.1
done

if [ "$MOUNTS_OK" == "0" ]; then
    echo "FATAL: nisu svi mount-ovi aktivni"
    for i in 1 2 3; do
        echo "=== node $i mount: $(mountpoint -q /tmp/mnt$i && echo OK || echo FAIL) ==="
        echo "=== node $i log (last 20) ==="
        tail -20 /tmp/n$i.log
    done
    exit 1
fi
echo "✓ All 3 mounts active"

echo ""
echo "Waiting for leader (up to 15s)..."
LEADER=""
for _ in $(seq 1 50); do
    for i in 1 2 3; do
        if echo "probe" > /tmp/mnt$i/probe.txt 2>/dev/null; then
            LEADER=$i
            rm -f /tmp/mnt$i/probe.txt
            break 2
        fi
    done
    sleep 0.3
done
if [ -z "$LEADER" ]; then
    echo "FATAL: no leader"
    exit 1
fi
echo "✓ Leader: node $LEADER"
FOLLOWER=$([ "$LEADER" -eq 1 ] && echo 2 || echo 1)

echo ""
echo "=== Small file ==="
echo "hello world" > /tmp/mnt1/small.txt
sleep 1
cat /tmp/mnt1/small.txt

echo ""
echo "=== 1 MB write ==="
dd if=/dev/urandom of=/tmp/1mb.bin bs=1M count=1 2>/dev/null
time cp /tmp/1mb.bin /tmp/mnt1/1mb.bin

echo "Waiting for replication..."
for _ in $(seq 1 50); do
    if [ -f /tmp/mnt$FOLLOWER/1mb.bin ]; then break; fi
    sleep 0.2
done
if cmp -s /tmp/1mb.bin /tmp/mnt$FOLLOWER/1mb.bin; then
    echo "✓ 1 MB replicated and matches"
else
    echo "WARN: 1 MB not yet on follower"
fi

echo ""
echo "=== 4 MB write ==="
dd if=/dev/urandom of=/tmp/4mb.bin bs=1M count=4 2>/dev/null
time cp /tmp/4mb.bin /tmp/mnt1/4mb.bin

sleep 2
if cmp -s /tmp/4mb.bin /tmp/mnt$FOLLOWER/4mb.bin; then
    echo "✓ 4 MB replicated and matches"
else
    echo "WARN: 4 MB not yet on follower"
fi

echo ""
echo "=== 20 MB write ==="
dd if=/dev/urandom of=/tmp/20mb.bin bs=1M count=20 2>/dev/null
time cp /tmp/20mb.bin /tmp/mnt1/20mb.bin

sleep 3
if cmp -s /tmp/20mb.bin /tmp/mnt$FOLLOWER/20mb.bin; then
    echo "✓ 20 MB replicated and matches"
else
    echo "WARN: 20 MB not yet on follower"
fi

echo ""
echo "=== 4 MB append ==="
dd if=/dev/urandom of=/tmp/append.bin bs=1M count=4 2>/dev/null
BEFORE=$(stat -c %s /tmp/mnt1/20mb.bin)
time cat /tmp/append.bin >> /tmp/mnt1/20mb.bin
AFTER=$(stat -c %s /tmp/mnt1/20mb.bin)
echo "Size before: $BEFORE, after: $AFTER"
EXPECTED=$((BEFORE + 4*1024*1024))
if [ "$AFTER" == "$EXPECTED" ]; then echo "✓ append size OK"; else echo "FAIL"; fi

echo ""
echo "=== Truncate ==="
truncate -s 0 /tmp/mnt1/20mb.bin
sleep 1
SIZE=$(stat -c %s /tmp/mnt1/20mb.bin)
if [ "$SIZE" == "0" ]; then echo "✓ truncated"; else echo "FAIL: size $SIZE"; fi

echo ""
echo "=== Block counts ==="
for i in 1 2 3; do
    echo "  node $i: $(ls /tmp/node$i/data/*.block 2>/dev/null | wc -l) blocks"
done

echo ""
echo "=== Timing breakdown (node $LEADER) ==="
grep -E "block write|background replicate|manifest update" /tmp/n$LEADER.log | tail -20

echo ""
echo "=== Failure counters ==="
echo "  replicate timeouts: $(grep -c 'replicate to.*timed out' /tmp/n$LEADER.log || true)"
echo "  leadership lost:    $(grep -c 'leadership lost' /tmp/n$LEADER.log || true)"
echo "  write failures:     $(grep -c 'write failed' /tmp/n$LEADER.log || true)"

echo ""
echo "=== Cleanup ==="
cleanup
echo "DONE"