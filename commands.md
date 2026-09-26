1. Cleanup (if previous test left state behind)
# Kill any running fuse process
pkill -f 'target/debug/fuse' || true

# Unmount if still mounted
fusermount3 -u /tmp/mnt || true

# Clean mount point and storage
rm -rf /tmp/mnt/*
rm -f  /tmp/ferumfs.wal
rm -rf /tmp/ferumfs-tmp /tmp/ferumfs-data

# Verify everything is clean
mount | grep /tmp/mnt
ls -la /tmp/mnt
ls -la /tmp/ferumfs-data 2>/dev/null || echo "data dir does not exist (OK)"

2. Prepare directories


mkdir -p /tmp/mnt
mkdir -p /tmp/ferumfs-tmp
mkdir -p /tmp/ferumfs-data

3. Build
   
cd ~/FerumFS
cargo build -p fuse

4. Start FUSE

~/FerumFS/target/debug/fuse /tmp/mnt


5. Verify mount

mount | grep /tmp/mnt
stat -f /tmp/mnt


6. Test operations

ls /tmp/mnt
echo hello > /tmp/mnt/a.txt
cat /tmp/mnt/a.txt
ls -la /tmp/mnt
mkdir /tmp/mnt/dir
ls /tmp/mnt
rm /tmp/mnt/a.txt
rmdir /tmp/mnt/dir
ls /tmp/mnt

7. Verify data dir (files do NOT live in /tmp/mnt)

ls -la /tmp/ferumfs-data/
cat /tmp/ferumfs-data/2

8. Stop
# In Terminal 1: Ctrl+C

# In Terminal 2:
fusermount3 -u /tmp/mnt
mount | grep /tmp/mnt    # must be empty