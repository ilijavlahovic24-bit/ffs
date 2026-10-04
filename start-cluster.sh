#!/usr/bin/env bash
set -e

FFS=./target/debug/ffs

pkill -f 'target/debug/ffs' || true
for i in 1 2 3; do fusermount3 -u /tmp/mnt$i 2>/dev/null || true; done
sleep 1
rm -rf /tmp/node1 /tmp/node2 /tmp/node3 /tmp/mnt1 /tmp/mnt2 /tmp/mnt3
mkdir -p /tmp/mnt1 /tmp/mnt2 /tmp/mnt3

export RUST_LOG=info

# Start all three nearly simultaneously — no long delay between them,
# so leader election happens with all three alive.
setsid $FFS --node-id 1 \
  --peers "2=127.0.0.1:7002,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7001 \
  --mount /tmp/mnt1 \
  --wal-dir /tmp/node1 > /tmp/node1.log 2>&1 &

setsid $FFS --node-id 2 \
  --peers "1=127.0.0.1:7001,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7002 \
  --mount /tmp/mnt2 \
  --wal-dir /tmp/node2 > /tmp/node2.log 2>&1 &

setsid $FFS --node-id 3 \
  --peers "1=127.0.0.1:7001,2=127.0.0.1:7002" \
  --raft-addr 127.0.0.1:7003 \
  --mount /tmp/mnt3 \
  --wal-dir /tmp/node3 > /tmp/node3.log 2>&1 &

sleep 3

echo "=== ps ==="
ps aux | grep 'target/debug/ffs' | grep -v grep | wc -l

echo "=== leader ==="
grep "became leader" /tmp/node*.log

echo "=== elections ==="
for i in 1 2 3; do echo "node$i: $(grep -c 'starting election' /tmp/node$i.log)"; done