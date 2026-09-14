#!/bin/sh
# One client. NAME=a holds a lock, NAME=b challenges it from another host.
# MODE=nfs mounts the export. MODE=local uses a shared volume, as a control.
set -eu

if [ "$MODE" = nfs ]; then
    mkdir -p /mnt/target
    mount -t nfs -o "$MOUNT_OPTS" nfs:/ /mnt/target
    TARGET=/mnt/target
else
    TARGET=/local
fi
LOCK="$TARGET/lockfile"
OUT="/results/$MODE.$NAME.log"

# The name and magic number `network_filesystem` reads through statfs. This
# checks the detection table against a real mount.
stat -f -c '%T %t' "$TARGET" > "/results/$MODE.$NAME.fstype"

# What the kernel really mounted. It drops options it does not honour: NFSv4
# ignores local_lock, for instance, because its locking is part of the protocol.
awk -v t="$TARGET" '$2 == t {print $3, $4}' /proc/self/mounts \
    > "/results/$MODE.$NAME.mount"

: > "$LOCK"

wait_until() {
    while [ "$(date +%s)" -lt "$1" ]; do sleep 0.05; done
}

if [ "$NAME" = a ]; then
    wait_until "$START"
    flock -x "$LOCK" -c "
      date +holder_acquired=%s.%N
      sleep $HOLD_SECONDS
      date +holder_released=%s.%N
    " > "$OUT"
else
    # Two seconds into the holder's window, so it certainly holds the lock.
    wait_until "$(( START + 2 ))"

    if err=$(flock -n -x "$LOCK" -c 'true' 2>&1); then
        echo "nonblocking=acquired" > "$OUT"
    else
        # flock(1) exits 1 when the lock is held. Anything else is a failure,
        # and its message is the interesting part.
        if [ $? = 1 ] && [ -z "$err" ]; then
            echo "nonblocking=refused" > "$OUT"
        else
            echo "nonblocking=error" > "$OUT"
            echo "nonblocking_error=\"$err\"" >> "$OUT"
        fi
    fi

    # Now the blocking form, the one `File::lock` uses. The holder releases
    # while this waits, so a healthy filesystem hands the lock over.
    start=$(date +%s.%N)
    if err=$(flock -w 30 -x "$LOCK" -c 'true' 2>&1); then
        echo "blocking=acquired" >> "$OUT"
    else
        echo "blocking=failed" >> "$OUT"
        echo "blocking_error=\"${err:-timeout}\"" >> "$OUT"
    fi
    echo "blocking_seconds=$(echo "$(date +%s.%N) - $start" | bc)" >> "$OUT"
fi

[ "$MODE" = nfs ] && umount /mnt/target
exit 0
