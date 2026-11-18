#!/usr/bin/env bash
#
# bench-checkpoint.sh — checkpoint write throughput, replication check and
# durability across a leader failure (kill -9).
#
# Steps:
#   1. Start a 3-node cluster, wait (patiently) for a leader
#   2. Write a checkpoint on the leader (dd conv=fsync) and report MB/s
#   3. Verify the checkpoint is byte-identical when read from every node
#   4. kill -9 the leader, wait for a new one, verify the checkpoint is still
#      readable and identical on the surviving nodes (KILL_LEADER=0 to skip)
#
# All results are printed to the terminal; nothing is written to result
# files. (Daemon logs /tmp/node*.log are only used to count warnings, and
# /tmp/ckpt.bin is the temporary source data. Both are removed/overwritten.)
#
# Env:
#   CKPT_SIZE_MB    checkpoint size in MB                (default 200)
#   LEADER_WAIT_S   max seconds to wait for a leader      (default 60)
#   REPLICA_WAIT_S  max seconds to wait for replicas      (default 30)
#   READ_TIMEOUT    max seconds for one verification read (default 60)
#   KILL_LEADER     1 = run the leader-failure test       (default 1)
#   RUST_LOG        daemon log level                      (default warn)
#
# NOTE: a Raft election currently takes ~20 s after start-up, so the leader
# wait must be generous. The time to the first leader is printed below.
#
set -uo pipefail

FFS=./target/release/ffs
SIZE_MB="${CKPT_SIZE_MB:-200}"
LEADER_WAIT_S="${LEADER_WAIT_S:-60}"
REPLICA_WAIT_S="${REPLICA_WAIT_S:-30}"
READ_TIMEOUT="${READ_TIMEOUT:-60}"
KILL_LEADER="${KILL_LEADER:-1}"
export RUST_LOG="${RUST_LOG:-warn}"

CKPT_SRC=/tmp/ckpt.bin
LEADER=""
FAILS=0
declare -a NODE_PID

if [ ! -x "$FFS" ]; then
    echo "FATAL: $FFS not found. Run: cargo build --release"
    exit 1
fi

# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------
cleanup() {
    # SIGTERM first so the daemons can unmount cleanly; SIGKILL as fallback.
    pkill -TERM -f 'target/release/ffs' 2>/dev/null || true
    for _ in $(seq 1 20); do
        pgrep -f 'target/release/ffs' >/dev/null 2>&1 || break
        sleep 0.25
    done
    pkill -9 -f 'target/release/ffs' 2>/dev/null || true
    for i in 1 2 3; do
        timeout 5 fusermount3 -u /tmp/mnt$i 2>/dev/null || true
    done
    sleep 1
    rm -rf /tmp/node1 /tmp/node2 /tmp/node3 /tmp/mnt1 /tmp/mnt2 /tmp/mnt3
    rm -f "$CKPT_SRC"
}
trap cleanup EXIT

now_ms() {
    date +%s%3N
}

calc_mbps() {
    awk "BEGIN { printf \"%.1f\", $1 / ($2 / 1000.0) }"
}

block_count() {
    find "/tmp/node$1/data" -maxdepth 1 -name '*.block' 2>/dev/null | wc -l
}

# Probe-write on each given node's mount; print the first node that accepts
# the write (= the leader). Followers answer "Read-only file system".
# `timeout` protects against a dead/stuck FUSE mount.
find_leader() {
    local i
    for i in "$@"; do
        if timeout 5 bash -c 'echo p > "$1"' _ "/tmp/mnt$i/.probe" 2>/dev/null; then
            rm -f "/tmp/mnt$i/.probe" 2>/dev/null
            echo "$i"
            return 0
        fi
    done
    return 1
}

# wait_for_leader SECONDS NODE...   -> sets global LEADER
wait_for_leader() {
    local limit="$1" start=$SECONDS found
    shift
    while [ $((SECONDS - start)) -lt "$limit" ]; do
        if found=$(find_leader "$@"); then
            LEADER="$found"
            return 0
        fi
        sleep 0.5
    done
    return 1
}

# wait_for_size PATH BYTES SECONDS
wait_for_size() {
    local path="$1" size="$2" limit="$3" start=$SECONDS
    while [ $((SECONDS - start)) -lt "$limit" ]; do
        [ "$(stat -c %s "$path" 2>/dev/null || echo -1)" == "$size" ] && return 0
        sleep 0.3
    done
    return 1
}

verify_node() {   # verify_node NODE  -> prints PASS/FAIL line, bumps FAILS
    local i="$1" path="/tmp/mnt$i/ckpt.bin" t0 t1 hash
    t0=$(now_ms)
    if ! wait_for_size "$path" "$SRC_SIZE" "$REPLICA_WAIT_S"; then
        echo "  ✗ node $i: file missing or wrong size after ${REPLICA_WAIT_S}s"
        FAILS=$((FAILS + 1))
        return
    fi
    if ! hash=$(timeout "$READ_TIMEOUT" sha256sum "$path" 2>/dev/null | cut -d' ' -f1) || [ -z "$hash" ]; then
        echo "  ✗ node $i: read failed or timed out (${READ_TIMEOUT}s)"
        FAILS=$((FAILS + 1))
        return
    fi
    t1=$(now_ms)
    if [ "$hash" == "$SRC_HASH" ]; then
        echo "  ✓ node $i: identical ($((t1 - t0)) ms)"
    else
        echo "  ✗ node $i: CONTENT MISMATCH (sha256 ${hash:0:12}…)"
        FAILS=$((FAILS + 1))
    fi
}

# ---------------------------------------------------------------
# Start cluster
# ---------------------------------------------------------------
cleanup
mkdir -p /tmp/mnt1 /tmp/mnt2 /tmp/mnt3

echo "=== FerumFS checkpoint benchmark ==="
echo "Checkpoint size: ${SIZE_MB} MB, leader wait: ${LEADER_WAIT_S}s, RUST_LOG=$RUST_LOG"
echo ""

echo "Starting 3-node cluster..."
CLUSTER_START=$SECONDS

$FFS --node-id 1 --peers "2=127.0.0.1:7002,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7001 --mount /tmp/mnt1 --wal-dir /tmp/node1 > /tmp/node1.log 2>&1 &
NODE_PID[1]=$!
$FFS --node-id 2 --peers "1=127.0.0.1:7001,3=127.0.0.1:7003" \
  --raft-addr 127.0.0.1:7002 --mount /tmp/mnt2 --wal-dir /tmp/node2 > /tmp/node2.log 2>&1 &
NODE_PID[2]=$!
$FFS --node-id 3 --peers "1=127.0.0.1:7001,2=127.0.0.1:7002" \
  --raft-addr 127.0.0.1:7003 --mount /tmp/mnt3 --wal-dir /tmp/node3 > /tmp/node3.log 2>&1 &
NODE_PID[3]=$!

# Wait for all mounts (up to 30s).
echo "Waiting for mounts..."
MOUNTS_OK=0
for _ in $(seq 1 300); do
    ALL=1
    for i in 1 2 3; do
        mountpoint -q /tmp/mnt$i 2>/dev/null || ALL=0
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

# Wait for a leader. Elections are slow right now (~20 s), so poll instead
# of sleeping a fixed time and giving up early.
echo "Waiting for a leader (up to ${LEADER_WAIT_S}s)..."
if ! wait_for_leader "$LEADER_WAIT_S" 1 2 3; then
    echo "FAIL: no leader after ${LEADER_WAIT_S}s"
    for i in 1 2 3; do
        echo "=== node $i (last 20 lines) ==="
        tail -20 /tmp/node$i.log
    done
    exit 1
fi
echo "✓ Leader: node $LEADER ($((SECONDS - CLUSTER_START))s after start)"
if [ $((SECONDS - CLUSTER_START)) -gt 5 ]; then
    echo "  ⚠ Leader election took more than 5 s; expected well under 1 s."
fi

# ---------------------------------------------------------------
# Test 1: checkpoint write throughput
# ---------------------------------------------------------------
echo ""
echo "=== Test 1: checkpoint write (${SIZE_MB} MB, fsync) ==="
dd if=/dev/urandom of="$CKPT_SRC" bs=1M count="$SIZE_MB" 2>/dev/null
SRC_SIZE=$(stat -c %s "$CKPT_SRC")
SRC_HASH=$(sha256sum "$CKPT_SRC" | cut -d' ' -f1)

S=$(now_ms)
if ! dd if="$CKPT_SRC" of="/tmp/mnt$LEADER/ckpt.bin" bs=4M conv=fsync 2>/dev/null; then
    echo "  FAIL: write to leader (node $LEADER) failed"
    tail -10 "/tmp/node$LEADER.log"
    exit 1
fi
E=$(now_ms)
TOTAL_MS=$((E - S))
echo "  Write + fsync: ${TOTAL_MS} ms ($(calc_mbps "$SIZE_MB" "$TOTAL_MS") MB/s)"

# ---------------------------------------------------------------
# Test 2: replication (identical content from every node)
# ---------------------------------------------------------------
echo ""
echo "=== Test 2: replication (sha256 from every node) ==="
for i in 1 2 3; do
    verify_node "$i"
done

echo ""
echo "Block counts:"
for i in 1 2 3; do
    echo "  node $i: $(block_count $i) blocks"
done

# ---------------------------------------------------------------
# Test 3: durability across leader failure
# ---------------------------------------------------------------
if [ "$KILL_LEADER" == "1" ]; then
    echo ""
    echo "=== Test 3: kill -9 the leader (node $LEADER), verify on survivors ==="

    OLD_LEADER="$LEADER"
    SURVIVORS=()
    for i in 1 2 3; do
        [ "$i" != "$OLD_LEADER" ] && SURVIVORS+=("$i")
    done

    kill -9 "${NODE_PID[$OLD_LEADER]}" 2>/dev/null
    wait "${NODE_PID[$OLD_LEADER]}" 2>/dev/null
    echo "  Killed node $OLD_LEADER (pid ${NODE_PID[$OLD_LEADER]})"

    FAILOVER_START=$SECONDS
    if wait_for_leader "$LEADER_WAIT_S" "${SURVIVORS[@]}"; then
        echo "  ✓ New leader: node $LEADER (failover took $((SECONDS - FAILOVER_START))s)"
        if timeout 10 bash -c 'echo after > "$1"' _ "/tmp/mnt$LEADER/after-failover.txt" 2>/dev/null; then
            echo "  ✓ New leader accepts writes"
        else
            echo "  ✗ New leader rejected a write"
            FAILS=$((FAILS + 1))
        fi
    else
        echo "  ✗ No new leader after ${LEADER_WAIT_S}s"
        FAILS=$((FAILS + 1))
    fi

    echo "  Verifying checkpoint on survivors:"
    for i in "${SURVIVORS[@]}"; do
        verify_node "$i"
    done
fi

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
echo ""
echo "========================================"
echo "  SUMMARY"
echo "========================================"
echo "  Checkpoint:  ${SIZE_MB} MB, write+fsync ${TOTAL_MS} ms ($(calc_mbps "$SIZE_MB" "$TOTAL_MS") MB/s)"
echo "  Warnings in daemon logs:"
for i in 1 2 3; do
    echo "    node $i: $(grep -c 'WARN' /tmp/node$i.log 2>/dev/null || true) WARN, $(grep -c 'ERROR' /tmp/node$i.log 2>/dev/null || true) ERROR"
done
if [ "$FAILS" -eq 0 ]; then
    echo "  Result:      PASS"
else
    echo "  Result:      FAIL ($FAILS check(s) failed)"
fi
echo "========================================"

[ "$FAILS" -eq 0 ]