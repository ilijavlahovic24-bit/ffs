#!/usr/bin/env bash
set -euo pipefail

FFS=./target/debug/ffs

cleanup() {
    pkill -f 'target/debug/ffs' 2>/dev/null || true
    for i in 1 2 3; do fusermount3 -u /tmp/mnt$i 2>/dev/null || true; done
    sleep 1
    rm -rf /tmp/node1 /tmp/node2 /tmp/node3 /tmp/mnt1 /tmp/mnt2 /tmp/mnt3
}

cleanup
mkdir -p /tmp/mnt1 /tmp/mnt2 /tmp/mnt3
export RUST_LOG=info

$FFS --node-id 1 --peers "2=127.0.0.1:7002,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7001 --mount /tmp/mnt1 --wal-dir /tmp/node1 > /tmp/node1.log 2>&1 &
$FFS --node-id 2 --peers "1=127.0.0.1:7001,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7002 --mount /tmp/mnt2 --wal-dir /tmp/node2 > /tmp/node2.log 2>&1 &
$FFS --node-id 3 --peers "1=127.0.0.1:7001,2=127.0.0.1:7002" \
  --raft-addr 127.0.0.1:7003 --mount /tmp/mnt3 --wal-dir /tmp/node3 > /tmp/node3.log 2>&1 &
sleep 3

# Detect leader
LEADER=""
for i in 1 2 3; do
    if echo p > /tmp/mnt$i/p 2>/dev/null; then LEADER=$i; rm -f /tmp/mnt$i/p; break; fi
done
[ -z "$LEADER" ] && { echo "FAIL: no leader"; exit 1; }
echo "Leader: node $LEADER"

# Write 20 MB (5 blocks of 4 MiB each).
echo "Writing 20 MB..."
dd if=/dev/urandom of=/tmp/big.bin bs=1M count=20 2>/dev/null
cp /tmp/big.bin /tmp/mnt$LEADER/big.bin

# Verify sizes match.
EXPECTED=$(stat -c %s /tmp/big.bin)
for i in 1 2 3; do
    ACTUAL=$(stat -c %s /tmp/mnt$i/big.bin)
    if [ "$EXPECTED" != "$ACTUAL" ]; then
        echo "FAIL: size mismatch on node $i: $ACTUAL vs $EXPECTED"
        exit 1
    fi
    echo "✓ node $i size $ACTUAL"
done

# Verify content matches (hash).
EXPECTED_HASH=$(sha256sum /tmp/big.bin | cut -d' ' -f1)
for i in 1 2 3; do
    ACTUAL_HASH=$(sha256sum /tmp/mnt$i/big.bin | cut -d' ' -f1)
    if [ "$EXPECTED_HASH" != "$ACTUAL_HASH" ]; then
        echo "FAIL: content mismatch on node $i"
        exit 1
    fi
    echo "✓ node $i hash matches"
done

# Append 8 MB.
echo "Appending 8 MB..."
dd if=/dev/urandom of=/tmp/append.bin bs=1M count=8 2>/dev/null
cat /tmp/append.bin >> /tmp/mnt$LEADER/big.bin

NEW_EXPECTED=$((EXPECTED + 8*1024*1024))
for i in 1 2 3; do
    ACTUAL=$(stat -c %s /tmp/mnt$i/big.bin)
    if [ "$NEW_EXPECTED" != "$ACTUAL" ]; then
        echo "FAIL: size after append on node $i: $ACTUAL vs $NEW_EXPECTED"
        exit 1
    fi
    echo "✓ node $i size after append $ACTUAL"
done

# Truncate to 0.
echo "Truncating..."
truncate -s 0 /tmp/mnt$LEADER/big.bin
for i in 1 2 3; do
    ACTUAL=$(stat -c %s /tmp/mnt$i/big.bin)
    if [ "$ACTUAL" != "0" ]; then
        echo "FAIL: size after truncate on node $i: $ACTUAL"
        exit 1
    fi
    echo "✓ node $i size 0"
done

echo ""
echo "ALL CHUNKING TESTS PASSED"
cleanup