# Known issues

## Incorrect Memory Usage in `htop`

If you're using `htop`, you might notice it showing very high memory usage for Erigon. This is because Erigon's internal database, MDBX, uses MemoryMap, allowing the operating system to manage all read, write, and cache operations ([linux](https://linux-kernel-labs.github.io/refs/heads/master/labs/memory_mapping.html), [windows](https://docs.microsoft.com/en-us/windows/win32/memory/file-mapping)).

The **`RES`** (Resident) column in `htop` combines the memory used by the application and the OS's page cache for that application. This can be misleading because the OS automatically frees the page cache whenever it needs memory for other processes. So, even if `htop` shows 90% memory usage, a large portion of that is just the OS page cache, and you might still be able to run other applications without issue.

### Tools for Accurate Memory Usage

For a more accurate view of Erigon's memory usage, use one of these tools:

* **`vmmap -summary PID`**: Use this command to see a detailed breakdown. The **`MALLOC ZONE`** and **`REGION TYPE`** sections will show you the memory used by the application and the size of the OS page cache separately.
* **Prometheus Dashboard**: This provides a graphical representation of the Go application's memory usage, excluding the OS page cache. You can run it with `make prometheus` and access it in your browser at `localhost:3000` with the credentials `admin/admin`.
* **`cat /proc/<PID>/smaps`**: This command gives a raw text file of the memory maps, which can be useful for detailed analysis.

Erigon typically uses about **4 GB of RAM** during a genesis sync and about **1 GB** during normal operation. The OS page cache, however, can use an unlimited amount of memory.

> **Warning:** Running multiple Erigon instances on the same machine can cause a performance hit due to concurrent disk access. This is especially true during a genesis sync, where the "Blocks Execution stage" performs many random reads. It is not recommended to run multiple genesis syncs on the same disk. Once the initial sync is complete, running multiple instances on the same disk is generally fine.

## `htop` shows incorrect memory usage

Erigon's internal DB (MDBX) using `MemoryMap` - when OS does manage all `read, write, cache` operations instead of Application ([linux](https://linux-kernel-labs.github.io/refs/heads/master/labs/memory_mapping.html), [windows](https://docs.microsoft.com/en-us/windows/win32/memory/file-mapping)) `htop` on column `res` shows memory of "App + OS used to hold page cache for given App", but it's not informative, because if `htop` says that app using 90% of memory you still can run 3 more instances of app on the same machine - because most of that `90%` is "OS pages cache".
OS automatically frees this cache any time it needs memory. Smaller "page cache size" may not impact performance of Erigon at all.

Next tools show correct memory usage of Erigon:

- `vmmap -summary PID | grep -i "Physical footprint"`. Without `grep` you can see details
    - `section MALLOC ZONE column Resident Size` shows App memory usage, `section REGION TYPE column Resident Size`
      shows OS pages cache size.
- `Prometheus` dashboard shows memory of Go app without OS pages cache (`make prometheus`, open in
  browser `localhost:3000`, credentials `admin/admin`)
- `cat /proc/<PID>/smaps`

Erigon uses ~4Gb of RAM during genesis sync and ~1Gb during normal work. OS pages cache can utilize unlimited amount of memory.

**Warning:** Multiple instances of Erigon on same machine will touch Disk concurrently, it impacts performance - one of main Erigon optimizations: "reduce Disk random access". "Blocks Execution stage" still does many random reads - this is reason why it's slowest stage. We do not recommend running multiple genesis syncs on same Disk. If genesis sync passed, then it's fine to run multiple Erigon instances on same Disk.

## Cloud Network Drives

Cloud network drives, like gp3, aren't ideal for Erigon's block execution. This is because they are designed for parallel and batch database workloads, which contrasts with Erigon's use of non-parallel, blocking I/O for block execution. This mismatch leads to poor performance. See also issue [#1516]( https://github.com/erigontech/erigon/issues/1516#issuecomment-811958891).

### Improving Performance

To improve performance, focus on **reducing disk latency**, not on increasing throughput or IOPS. There are several ways to do this:

* **Use latency-critical cloud drives** or attached NVMe storage, especially for the initial sync.
* **Increase RAM**. More RAM helps buffer data and reduces the need for frequent disk access.
* If you have sufficient RAM, you can set the environment variable **`ERIGON_SNAPSHOT_MADV_RND=false`**. This setting can improve performance by altering how Erigon handles memory.
* Use the flag **`--db.pagesize=64kb`** to decrease database fragmentation and improve I/O efficiency.
* Consider using **Erigon3**, which is designed to download most of the history, making it less sensitive to disk latency.

### Background File System Features Can Hurt Performance

Certain file system features, such as `autodefrag` on btrfs, can drastically increase write I/O. This can lead to a significant performance hit, sometimes as much as a 100x increase in disk activity.

### Gnome Tracker Can Interfere with Erigon

[Gnome Tracker](https://wiki.gnome.org/Attic/Tracker) is known to detect and shut down mining processes, which can cause Erigon to crash. If you're running Erigon, it's a good idea to disable Gnome Tracker to prevent this.

### The `--mount` Option and BuildKit Errors

If you're encountering a BuildKit error when starting Erigon, it's likely due to an issue with the `--mount` option in your command. To fix this, you can use the following command to start Erigon with `docker-compose`:

```sh
XDG_DATA_HOME=/preferred/data/folder DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 make docker-compose
```

### Erigon Crashes from Kernel Allocation Limits

If Erigon crashes with a **`cannot allocate memory`** error, your kernel's memory allocation limits might be too low. You can resolve this by adding the following lines to `/etc/sysctl.conf` or a new `.conf` file in `/etc/sysctl.d/`:

```
vm.overcommit_memory = 1 
vm.max_map_count = 16777216 
```