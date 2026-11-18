# U drugom terminalu, dok test visi:
echo "=== Procesi ==="
ps aux | grep 'target/release/ffs' | grep -v grep | wc -l
ps aux | grep 'dd ' | grep -v grep | wc -l

echo "=== Blocked dd procesi ==="
for pid in $(pgrep -f '^dd '); do
    echo "dd $pid: $(cat /proc/$pid/wchan 2>/dev/null || echo gone)"
done

echo "=== node1 log (zadnjih 30) ==="
tail -30 /tmp/node1.log

echo "=== node2 log (zadnjih 30) ==="
tail -30 /tmp/node2.log

echo "=== Da li cat radi na mnt1? ==="
timeout 5 cat /tmp/mnt1/bench.bin | head -c 100 | wc -c