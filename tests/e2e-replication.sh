#!/usr/bin/env bash
#
# e2e-replication.sh — M2 validation
#
# Starts a 3-node cluster, verifies that data written on the leader is
# present on all nodes, kills the leader, verifies that data survives on
# the remaining nodes, and verifies that the restarted leader catches up.
#
# Exit 0 on success, non-zero on failure with diagnostics.
#
set -euo pipefail

FFS="${FFS:-./target/debug/ffs}"
LOG_DIR="${LOG_DIR:-/tmp}"
DATA_DIR_PREFIX="${DATA_DIR_PREFIX:-/tmp/node}"
MNT_PREFIX="${MNT_PREFIX:-/tmp/mnt}"

cleanup() {
    pkill -f 'target/debug/ffs' 2>/dev/null || true
    for i in 1 2 3; do
        fusermount3 -u "${MNT_PREFIX}$i" 2>/dev/null || true
    done
    sleep 0.5
    for i in 1 2 3; do
        rm -rf "${MNT_PREFIX}$i" "${DATA_DIR_PREFIX}$i"
    done
}

diagnostics() {
    echo ""
    echo "========== DIAGNOSTICS =========="
    echo ""
    echo "--- Process state ---"
    ps aux | grep 'target/debug/ffs' | grep -v grep || echo "  (no ffs processes)"
    echo ""
    echo "--- Mount state ---"
    mount | grep "${MNT_PREFIX}" || echo "  (no mounts)"
    echo ""
    for i in 1 2 3; do
        echo "--- Node $i log (last 20) ---"
        tail -20 "${LOG_DIR}/node${i}.log" 2>/dev/null || echo "  (no log)"
        echo ""
        echo "--- Node $i block count ---"
        ls "${DATA_DIR_PREFIX}${i}/data/" 2>/dev/null | wc -l || echo "  0"
        echo ""
    done
    echo "================================="
}

fail() {
    echo ""
    echo "FAIL: $*"
    diagnostics
    cleanup
    exit 1
}

# ---------------------------------------------------------------------------
# 1. Setup
# ---------------------------------------------------------------------------

echo "=== M2 Data Replication Test ==="
echo ""

cleanup
for i in 1 2 3; do
    mkdir -p "${MNT_PREFIX}$i"
done

export RUST_LOG=info

# ---------------------------------------------------------------------------
# 2. Start cluster
# ---------------------------------------------------------------------------

echo "Starting 3-node cluster..."

"$FFS" --node-id 1 \
    --peers "2=127.0.0.1:7002,3=127.0.0.1:7003" \
    --raft-addr 127.0.0.1:7001 \
    --mount "${MNT_PREFIX}1" \
    --wal-dir "${DATA_DIR_PREFIX}1" \
    > "${LOG_DIR}/node1.log" 2>&1 &

"$FFS" --node-id 2 \
    --peers "1=127.0.0.1:7001,3=127.0.0.1:7003" \
    --raft-addr 127.0.0.1:7002 \
    --mount "${MNT_PREFIX}2" \
    --wal-dir "${DATA_DIR_PREFIX}2" \
    > "${LOG_DIR}/node2.log" 2>&1 &

"$FFS" --node-id 3 \
    --peers "1=127.0.0.1:7001,2=127.0.0.1:7002" \
    --raft-addr 127.0.0.1:7003 \
    --mount "${MNT_PREFIX}3" \
    --wal-dir "${DATA_DIR_PREFIX}3" \
    > "${LOG_DIR}/node3.log" 2>&1 &

# Wait for all three mounts.
for i in 1 2 3; do
    for _ in $(seq 1 50); do
        if mountpoint -q "${MNT_PREFIX}$i" 2>/dev/null; then
            break
        fi
        sleep 0.1
    done
    if ! mountpoint -q "${MNT_PREFIX}$i" 2>/dev/null; then
        fail "node $i mount did not become active"
    fi
done
echo "✓ All 3 nodes mounted"
sleep 2

# ---------------------------------------------------------------------------
# 3. Detect leader
# ---------------------------------------------------------------------------

echo "Detecting leader..."

LEADER=""
for _ in $(seq 1 20); do
    for i in 1 2 3; do
        if echo "probe" > "${MNT_PREFIX}$i/probe.txt" 2>/dev/null; then
            LEADER=$i
            rm -f "${MNT_PREFIX}$i/probe.txt"
            break 2
        fi
    done
    sleep 0.3
done

[ -z "$LEADER" ] && fail "no leader detected after 6s"
echo "✓ Leader: node $LEADER"

# ---------------------------------------------------------------------------
# 4. Write data and verify replication
# ---------------------------------------------------------------------------

CONTENT="data-rep-$(date +%s%N)"
echo ""
echo "Writing '$CONTENT' on node $LEADER..."
if ! echo "$CONTENT" > "${MNT_PREFIX}$LEADER/data.txt"; then
    fail "write on leader failed"
fi

# Give replication time to settle.
sleep 1

echo "Verifying data on all nodes..."
for i in 1 2 3; do
    ACTUAL=$(cat "${MNT_PREFIX}$i/data.txt" 2>/dev/null || echo "MISSING")
    if [ "$ACTUAL" != "$CONTENT" ]; then
        fail "node $i has '$ACTUAL', expected '$CONTENT'"
    fi
    echo "✓ node $i has data"
done

# ---------------------------------------------------------------------------
# 5. Kill leader
# ---------------------------------------------------------------------------

echo ""
echo "Killing leader node $LEADER..."

LEADER_PID=$(pgrep -f "target/debug/ffs --node-id $LEADER " | head -1)
if [ -z "$LEADER_PID" ]; then
    fail "could not find leader process"
fi
echo "  PID: $LEADER_PID"
kill -9 "$LEADER_PID"

# FUSE cleanup — kill -9 leaves a stale mount that blocks the restarted
# node from remounting. Unmount it explicitly.
sleep 1
fusermount3 -u "${MNT_PREFIX}$LEADER" 2>/dev/null || true
rm -rf "${MNT_PREFIX}$LEADER"
mkdir -p "${MNT_PREFIX}$LEADER"
sleep 1

# ---------------------------------------------------------------------------
# 6. Verify survivors still have data
# ---------------------------------------------------------------------------

echo ""
echo "Verifying data survived on remaining nodes..."
for i in 1 2 3; do
    [ "$i" == "$LEADER" ] && continue
    ACTUAL=$(cat "${MNT_PREFIX}$i/data.txt" 2>/dev/null || echo "MISSING")
    if [ "$ACTUAL" != "$CONTENT" ]; then
        fail "after kill, node $i has '$ACTUAL', expected '$CONTENT'"
    fi
    echo "✓ node $i still has data"
done

# ---------------------------------------------------------------------------
# 7. Detect new leader and verify write capability
# ---------------------------------------------------------------------------

echo ""
echo "Detecting new leader..."

NEW_LEADER=""
for _ in $(seq 1 30); do
    for i in 1 2 3; do
        [ "$i" == "$LEADER" ] && continue
        if echo "after-failover" > "${MNT_PREFIX}$i/new.txt" 2>/dev/null; then
            NEW_LEADER=$i
            break 2
        fi
    done
    sleep 0.3
done

[ -z "$NEW_LEADER" ] && fail "no new leader after 9s"
echo "✓ New leader: node $NEW_LEADER"

# Give data replication a moment.
sleep 1

# ---------------------------------------------------------------------------
# 8. Restart old leader and verify catch-up
# ---------------------------------------------------------------------------

echo ""
echo "Restarting node $LEADER..."

case $LEADER in
    1) PEERS_ARG="2=127.0.0.1:7002,3=127.0.0.1:7003" ;;
    2) PEERS_ARG="1=127.0.0.1:7001,3=127.0.0.1:7003" ;;
    3) PEERS_ARG="1=127.0.0.1:7001,2=127.0.0.1:7002" ;;
esac

"$FFS" --node-id "$LEADER" \
    --peers "$PEERS_ARG" \
    --raft-addr "127.0.0.1:$((7000 + LEADER))" \
    --mount "${MNT_PREFIX}$LEADER" \
    --wal-dir "${DATA_DIR_PREFIX}$LEADER" \
    > "${LOG_DIR}/node${LEADER}.log" 2>&1 &

# Wait for mount.
for _ in $(seq 1 50); do
    if mountpoint -q "${MNT_PREFIX}$LEADER" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if ! mountpoint -q "${MNT_PREFIX}$LEADER" 2>/dev/null; then
    fail "restarted node $LEADER did not mount"
fi
echo "  Mounted"

# Wait for metadata replication to catch up, then verify the new file.
echo "Waiting for node $LEADER to catch up..."
CAUGHT_UP=0
for attempt in $(seq 1 10); do
    sleep 1
    ACTUAL=$(cat "${MNT_PREFIX}$LEADER/new.txt" 2>/dev/null || echo "MISSING")
    if [ "$ACTUAL" == "after-failover" ]; then
        echo "✓ Restarted node $LEADER has new metadata (attempt $attempt)"
        CAUGHT_UP=1
        break
    fi
done
[ "$CAUGHT_UP" == "0" ] && fail "node $LEADER did not catch up within 10s"

# Also verify the original data is intact on the restarted node.
ACTUAL=$(cat "${MNT_PREFIX}$LEADER/data.txt" 2>/dev/null || echo "MISSING")
if [ "$ACTUAL" != "$CONTENT" ]; then
    fail "restarted node $LEADER has '$ACTUAL' for data.txt, expected '$CONTENT'"
fi
echo "✓ Restarted node $LEADER has original data intact"

# ---------------------------------------------------------------------------
# 9. Cross-node reads
# ---------------------------------------------------------------------------

echo ""
echo "Verifying cross-node reads..."
for i in 1 2 3; do
    ACTUAL=$(cat "${MNT_PREFIX}$i/data.txt" 2>/dev/null || echo "MISSING")
    if [ "$ACTUAL" != "$CONTENT" ]; then
        fail "cross-node read: node $i has '$ACTUAL'"
    fi
done
echo "✓ All nodes consistent"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------

echo ""
echo "=================================="
echo "✓ ALL DATA REPLICATION TESTS PASSED"
echo "=================================="

cleanup
exit 0