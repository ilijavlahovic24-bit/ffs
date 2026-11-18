#!/usr/bin/env bash
#
# diag.sh — FerumFS diagnostics. Everything is printed to the terminal;
# nothing is saved to result files.
#
# Run it from the repo root BEFORE cleaning up the cluster, so the state
# of a failed run is preserved:
#   ./diag.sh 2>&1 | tee /dev/null        # or just ./diag.sh and copy the output
#
# Env:
#   NODE_LOG_DIR     where node<N>.log live            (default /tmp)
#   CHECKPOINT_SH    script to show the head of        (default tests/bench-checkpoint.sh)
#   WRITE_COUNT      number of 1 MiB writes in test    (default 20)
#
set -uo pipefail

NODE_LOG_DIR="${NODE_LOG_DIR:-/tmp}"
CHECKPOINT_SH="${CHECKPOINT_SH:-tests/bench-checkpoint.sh}"
WRITE_COUNT="${WRITE_COUNT:-20}"

section() {
    echo ""
    echo "=============================================="
    echo "  $1"
    echo "=============================================="
}

# ---------------------------------------------------------------
# Part 1: cluster state (why is there no leader / read-only fs?)
# ---------------------------------------------------------------
section "1.1 ffs processes"
pgrep -af 'target/release/ffs' || echo "(none running)"

section "1.2 listening ports 7001-7003"
ss -ltnp 2>/dev/null | grep -E ':700[123]' || echo "(nothing listening)"

section "1.3 FUSE mounts"
mount | grep -i fuse || echo "(no FUSE mounts)"

section "1.4 node directories"
for i in 1 2 3; do
    echo "--- /tmp/node$i ---"
    ls -la "/tmp/node$i" 2>&1 | head -12
done

section "1.5 node logs (last 20 lines each)"
for i in 1 2 3; do
    echo "--- $NODE_LOG_DIR/node$i.log ---"
    tail -20 "$NODE_LOG_DIR/node$i.log" 2>&1
done

section "1.6 head of $CHECKPOINT_SH (lines 1-45)"
if [ -f "$CHECKPOINT_SH" ]; then
    sed -n '1,45p' "$CHECKPOINT_SH"
else
    echo "(not found: $CHECKPOINT_SH)"
fi

section "1.7 where NotLeader / EROFS appear in the source"
grep -rnE "NotLeader|EROFS" --include=*.rs . 2>/dev/null | grep -v '/target/' | head -25 \
    || echo "(no matches)"

# ---------------------------------------------------------------
# Part 2: write latency (fsync cost vs Raft manifest commit)
# ---------------------------------------------------------------
section "2.1 raw disk fsync cost (100 x 1 MiB, oflag=dsync)"
FSYNC_FILE=$(mktemp /tmp/fsync-test.XXXXXX)
dd if=/dev/zero of="$FSYNC_FILE" bs=1M count=100 oflag=dsync 2>&1 | tail -1
rm -f "$FSYNC_FILE"

section "2.2 leader detection (the mount that accepts writes)"
LEADER=""
for i in 1 2 3; do
    if mountpoint -q "/tmp/mnt$i" 2>/dev/null; then
        if { echo p > "/tmp/mnt$i/.diag-probe"; } 2>/dev/null; then
            LEADER=$i
            rm -f "/tmp/mnt$i/.diag-probe"
            break
        else
            echo "  node $i: write rejected (follower, or no leader)"
        fi
    else
        echo "  node $i: not mounted"
    fi
done

if [ -z "$LEADER" ]; then
    echo "No mount accepts writes -> no leader. Part 2.3 skipped."
    echo "Check 1.5 (logs) and 1.7 (does NotLeader map to EROFS?)."
    exit 0
fi
echo "Leader mount: /tmp/mnt$LEADER (node $LEADER)"

section "2.3 write test on the leader: ${WRITE_COUNT} x 1 MiB"
TESTFILE="/tmp/mnt$LEADER/.diag-write-test"
trap 'rm -f "$TESTFILE"' EXIT

echo "--- oflag=direct ---"
if ! dd if=/dev/zero of="$TESTFILE" bs=1M count="$WRITE_COUNT" oflag=direct 2>&1 | tail -1; then
    echo "(direct write failed)"
fi
rm -f "$TESTFILE"

echo "--- conv=fsync (fallback if direct was rejected) ---"
dd if=/dev/zero of="$TESTFILE" bs=1M count="$WRITE_COUNT" conv=fsync 2>&1 | tail -1
rm -f "$TESTFILE"

section "2.4 timing lines from the leader log (last 10 each)"
LOG="$NODE_LOG_DIR/node$LEADER.log"
echo "--- local block write ---"
grep "local block write" "$LOG" 2>/dev/null | tail -10
echo "--- manifest update (raft) ---"
grep "manifest update" "$LOG" 2>/dev/null | tail -10
if ! grep -qE "local block write|manifest update" "$LOG" 2>/dev/null; then
    echo ""
    echo "No timing lines found. The cluster was started with RUST_LOG=warn."
    echo "Restart it with RUST_LOG=info (original code) or RUST_LOG=debug"
    echo "(rewritten code) and run this script again."
fi

echo ""
echo "Done. Copy everything above."