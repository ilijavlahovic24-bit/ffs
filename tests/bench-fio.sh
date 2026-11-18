#!/usr/bin/env bash
#
# FerumFS fio benchmarks.
#
# Limits of the current VFS (Phase 10 is not implemented yet):
#   - only append or full overwrite from offset 0 is supported
#   - truncate only to 0
# so the write test is a SINGLE pass (no --time_based, which would wrap
# around and overwrite), fallocate is disabled, and random write is skipped.
#
# All results are printed to the terminal; nothing is written to result
# files (the only file created is the fio test file on the mount, which is
# removed on exit).
#
# Env:
#   FERUMFS_MOUNT  mount point            (default /tmp/mnt1)
#   FIO_SIZE       size per job           (default 512M)
#   FIO_RUNTIME    seconds for read tests (default 30)
#   FIO_DIRECT     1 = O_DIRECT, 0 = page cache (default 1; use 0 if the
#                  mount rejects O_DIRECT with EINVAL)
#
set -uo pipefail   # no -e: failures are handled per test

MOUNT="${FERUMFS_MOUNT:-/tmp/mnt1}"
SIZE="${FIO_SIZE:-512M}"
RUNTIME="${FIO_RUNTIME:-30}"
DIRECT="${FIO_DIRECT:-1}"
TESTFILE="$MOUNT/fio_test_file"

if ! command -v fio >/dev/null 2>&1; then
    echo "ERROR: fio is not installed." >&2
    echo "Install: sudo apt install fio" >&2
    exit 1
fi

if ! mountpoint -q "$MOUNT"; then
    echo "ERROR: $MOUNT is not a FerumFS mount." >&2
    echo "Start a cluster and mount first." >&2
    exit 1
fi

trap 'rm -f "$TESTFILE"' EXIT
rm -f "$TESTFILE"

echo "=== FerumFS fio benchmarks ==="
echo "Mount: $MOUNT"
echo "Size per job: $SIZE, read runtime: ${RUNTIME}s, direct: $DIRECT, cores: $(nproc)"
echo ""

# run_test NAME RW BS IODEPTH NUMJOBS [extra fio args...]
run_test() {
    local name="$1" rw="$2" bs="$3" iodepth="$4" numjobs="$5"
    shift 5

    echo "--- $name ---"

    # fio output is captured in a variable, not in a file.
    local out rc
    out=$(fio --name="$name" \
        --filename="$TESTFILE" \
        --size="$SIZE" \
        --bs="$bs" \
        --rw="$rw" \
        --ioengine=libaio \
        --iodepth="$iodepth" \
        --numjobs="$numjobs" \
        --direct="$DIRECT" \
        --fallocate=none \
        --group_reporting=1 \
        "$@" 2>&1)
    rc=$?

    if [ $rc -ne 0 ]; then
        echo "  FAIL (fio rc=$rc), last lines:"
        echo "$out" | tail -8 | sed 's/^/    /'
    else
        echo "$out" | grep -E "(read|write):|lat \(" | head -6
    fi
    echo ""
}

# --- Sequential write (checkpoint-like): ONE pass, append only -------
run_test "seq-write-1m" write 1M 1 1

# --- Sequential read (training dataset) ------------------------------
# Reads may be time_based: wrapping to offset 0 is fine for reading.
run_test "seq-read-1m" read 1M 1 1 --time_based --runtime="$RUNTIME"

# --- Sequential read, high QD (multi-worker dataset loader) ----------
run_test "seq-read-1m-qd8" read 1M 8 4 --time_based --runtime="$RUNTIME"

# --- Random read (small-file workload / read amplification probe) ----
# With 4 MB blocks and no range reads, each 4k read pulls a whole block.
run_test "rand-read-4k" randread 4k 32 4 --time_based --runtime="$RUNTIME"

# --- Random write: NOT supported until Phase 10 ----------------------
# run_test "rand-write-4k" randwrite 4k 32 4 --time_based --runtime="$RUNTIME"
echo "(rand-write-4k skipped: random write is Phase 10)"

echo ""
echo "=== Benchmark complete ==="
