---
description: >-
  A list of currently recognized bugs, workarounds, and ongoing development
  priorities.
---

# Known Issues

### Incorrect Memory Usage in `htop` <a href="#incorrect-memory-usage-in-htop" id="incorrect-memory-usage-in-htop"></a>

If you're using `htop`, you might notice it showing very high memory usage for Erigon. This is because Erigon's internal database, MDBX, uses MemoryMap, allowing the operating system to manage all read, write, and cache operations ([linux](https://linux-kernel-labs.github.io/refs/heads/master/labs/memory_mapping.html), [windows](https://docs.microsoft.com/en-us/windows/win32/memory/file-mapping)).

The **`RES`** (Resident) column in `htop` combines the memory used by the application and the OS's page cache for that application. This can be misleading because the OS automatically frees the page cache whenever it needs memory for other processes. So, even if `htop` shows 90% memory usage, a large portion of that is just the OS page cache, and you might still be able to run other applications without issue.

#### Tools for Accurate Memory Usage <a href="#tools-for-accurate-memory-usage" id="tools-for-accurate-memory-usage"></a>

For a more accurate view of Erigon's memory usage, use one of these tools:

* **`vmmap -summary PID`**: Use this command to see a detailed breakdown. The **`MALLOC ZONE`** and **`REGION TYPE`** sections will show you the memory used by the application and the size of the OS page cache separately.
* **Prometheus Dashboard**: This provides a graphical representation of the Go application's memory usage, excluding the OS page cache. You can run it with `make prometheus` and access it in your browser at `localhost:3000` with the credentials `admin/admin`.
* **`cat /proc/<PID>/smaps`**: This command gives a raw text file of the memory maps, which can be useful for detailed analysis.

Erigon typically uses about **4 GB of RAM** during a genesis sync and about **1 GB** during normal operation. The OS page cache, however, can use an unlimited amount of memory.

{% hint style="warning" %}
**Warning:** Running multiple Erigon instances on the same machine can cause a performance hit due to concurrent disk access. This is especially true during a genesis sync, where the "Blocks Execution stage" performs many random reads. It is not recommended to run multiple genesis syncs on the same disk. Once the initial sync is complete, running multiple instances on the same disk is generally fine.
{% endhint %}

### Cloud Network Drives <a href="#cloud-network-drives" id="cloud-network-drives"></a>

Cloud network drives, like gp3, aren't ideal for Erigon's block execution. This is because they are designed for parallel and batch database workloads, which contrasts with Erigon's use of non-parallel, blocking I/O for block execution. This mismatch leads to poor performance. See also issue [#1516](https://github.com/erigontech/erigon/issues/1516#issuecomment-811958891).

#### Improving Performance <a href="#improving-performance" id="improving-performance"></a>

Erigon3 is designed to download most of the history, making it less sensitive to disk latency. To improve performance, focus on **reducing disk latency**, not on increasing throughput or IOPS. There are several ways to do this:

* **Use latency-critical cloud drives** or attached NVMe storage, especially for the initial sync.
* **Increase RAM**. More RAM helps buffer data and reduces the need for frequent disk access.
* If you have sufficient RAM, you can set the environment variable **`ERIGON_SNAPSHOT_MADV_RND=false`**. This setting can improve performance by altering how Erigon handles memory.
* Use the flag **`--db.pagesize=64kb`** to decrease database fragmentation and improve I/O efficiency.

#### Background File System Features Can Hurt Performance <a href="#background-file-system-features-can-hurt-performance" id="background-file-system-features-can-hurt-performance"></a>

Certain file system features, such as `autodefrag` on btrfs, can drastically increase write I/O. This can lead to a significant performance hit, sometimes as much as a 100x increase in disk activity.

#### Gnome Tracker Can Interfere with Erigon <a href="#gnome-tracker-can-interfere-with-erigon" id="gnome-tracker-can-interfere-with-erigon"></a>

[Gnome Tracker](https://wiki.gnome.org/Attic/Tracker) is known to detect and shut down mining processes, which can cause Erigon to crash. If you're running Erigon, it's a good idea to disable Gnome Tracker to prevent this.

#### The `--mount` Option and BuildKit Errors <a href="#the-mount-option-and-buildkit-errors" id="the-mount-option-and-buildkit-errors"></a>

If you're encountering a BuildKit error when starting Erigon, it's likely due to an issue with the `--mount` option in your command. To fix this, you can use the following command to start Erigon with `docker-compose`:

{% code overflow="wrap" %}
```bash
XDG_DATA_HOME=/preferred/data/folder DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 make docker-compose
```
{% endcode %}

#### Erigon Crashes from Kernel Allocation Limits <a href="#erigon-crashes-from-kernel-allocation-limits" id="erigon-crashes-from-kernel-allocation-limits"></a>

If Erigon crashes with a **`cannot allocate memory`** error, your kernel's memory allocation limits might be too low. You can resolve this by adding the following lines to `/etc/sysctl.conf` or a new `.conf` file in `/etc/sysctl.d/`:

```bash
vm.overcommit_memory = 1 
vm.max_map_count = 16777216 
```
