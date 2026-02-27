package datasource

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// SystemStats holds a point-in-time snapshot of system resource usage.
// All size fields are in bytes; callers format for display.
type SystemStats struct {
	CPUPercent uint64 // overall CPU usage 0-100
	NumCPU     int    // runtime.NumCPU()
	MemUsed    uint64 // used RAM bytes (Total - Available)
	MemTotal   uint64 // total RAM bytes
	GoHeap     uint64 // Go runtime heap in-use bytes
	DiskUsed   uint64 // used disk bytes on datadir filesystem
	DiskTotal  uint64 // total disk bytes on datadir filesystem
	DiskIOPS_R uint64 // completed read I/Os since boot (absolute counter)
	DiskIOPS_W uint64 // completed write I/Os since boot (absolute counter)
}

// cpuTimes holds cumulative CPU jiffies from /proc/stat.
type cpuTimes struct {
	idle  uint64
	total uint64
}

// SystemCollector gathers system metrics. It keeps the previous /proc/stat
// CPU sample so it can compute a percentage delta between calls.
type SystemCollector struct {
	datadir string
	prevCPU cpuTimes
}

// SystemPollInterval is how often system stats are refreshed.
const SystemPollInterval = 5 * time.Second

// NewSystemCollector creates a collector. datadir is used for disk usage via Statfs.
func NewSystemCollector(datadir string) *SystemCollector {
	c := &SystemCollector{datadir: datadir}
	c.prevCPU = readCPUTimes() // prime the first sample
	return c
}

// CollectSystemStats gathers CPU, memory, disk, and Go heap metrics.
// Reads /proc/stat and /proc/meminfo (Linux); on other platforms the
// CPU and host-memory fields will be zero.
func (c *SystemCollector) CollectSystemStats() SystemStats {
	stats := SystemStats{
		NumCPU: runtime.NumCPU(),
	}

	// CPU from /proc/stat delta
	cur := readCPUTimes()
	totalDelta := cur.total - c.prevCPU.total
	idleDelta := cur.idle - c.prevCPU.idle
	if totalDelta > 0 {
		stats.CPUPercent = (totalDelta - idleDelta) * 100 / totalDelta
	}
	c.prevCPU = cur

	// Go heap
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	stats.GoHeap = m.HeapInuse

	// RAM from /proc/meminfo
	memTotalKB, memAvailKB := readMemInfo()
	stats.MemTotal = memTotalKB * 1024
	if memTotalKB > memAvailKB {
		stats.MemUsed = (memTotalKB - memAvailKB) * 1024
	}

	// Disk usage via syscall.Statfs
	var sfs syscall.Statfs_t
	if err := syscall.Statfs(c.datadir, &sfs); err == nil {
		bsize := uint64(sfs.Bsize)
		stats.DiskTotal = sfs.Blocks * bsize
		free := sfs.Bavail * bsize // available to non-root
		if stats.DiskTotal > free {
			stats.DiskUsed = stats.DiskTotal - free
		}
	}

	// Disk I/O counters from /proc/diskstats
	stats.DiskIOPS_R, stats.DiskIOPS_W = readDiskIO()

	return stats
}

// readCPUTimes parses the aggregate "cpu" line from /proc/stat.
func readCPUTimes() cpuTimes {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuTimes{}
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			return cpuTimes{}
		}
		// fields: cpu user nice system idle iowait irq softirq steal guest guest_nice
		var vals [10]uint64
		var total uint64
		for i := 1; i < len(fields) && i <= 10; i++ {
			v, _ := strconv.ParseUint(fields[i], 10, 64)
			vals[i-1] = v
			total += v
		}
		return cpuTimes{idle: vals[3], total: total}
	}
	return cpuTimes{}
}

// readMemInfo returns (MemTotal, MemAvailable) in kB from /proc/meminfo.
func readMemInfo() (total, avail uint64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "MemTotal:"):
			total = parseMemInfoValue(line)
		case strings.HasPrefix(line, "MemAvailable:"):
			avail = parseMemInfoValue(line)
		}
		if total > 0 && avail > 0 {
			break
		}
	}
	return total, avail
}

func parseMemInfoValue(line string) uint64 {
	// Format: "MemTotal:       8024028 kB"
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	v, _ := strconv.ParseUint(fields[1], 10, 64)
	return v
}

// readDiskIO reads /proc/diskstats and sums completed reads (field 4) and
// writes (field 8) across whole-disk devices. These are absolute counters
// since boot.
func readDiskIO() (reads, writes uint64) {
	f, err := os.Open("/proc/diskstats")
	if err != nil {
		return 0, 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 11 {
			continue
		}
		name := fields[2]
		if isPartition(name) {
			continue
		}
		r, _ := strconv.ParseUint(fields[3], 10, 64)
		w, _ := strconv.ParseUint(fields[7], 10, 64)
		reads += r
		writes += w
	}
	return reads, writes
}

// isPartition returns true if the device name looks like a partition (sda1, nvme0n1p1)
// rather than a whole disk (sda, nvme0n1).
func isPartition(name string) bool {
	if len(name) == 0 {
		return false
	}
	// nvme partitions: nvme0n1p1 — contains "p" after "n<digit>"
	if strings.HasPrefix(name, "nvme") {
		idx := strings.LastIndex(name, "n")
		if idx >= 0 {
			after := name[idx+1:]
			if strings.Contains(after, "p") {
				return true
			}
		}
		return false
	}
	// Traditional: sda is disk, sda1 is partition; skip loop/ram/dm
	last := name[len(name)-1]
	return last >= '0' && last <= '9'
}

// DiskIOPS holds computed per-second I/O rates.
type DiskIOPS struct {
	Read  uint64
	Write uint64
}

// DiskIOPSTracker computes IOPS from absolute /proc/diskstats counters.
type DiskIOPSTracker struct {
	prevR    uint64
	prevW    uint64
	prevTime time.Time
	first    bool
}

// NewDiskIOPSTracker creates a new tracker.
func NewDiskIOPSTracker() *DiskIOPSTracker {
	return &DiskIOPSTracker{first: true}
}

// Update takes absolute read/write counters and returns per-second IOPS.
func (t *DiskIOPSTracker) Update(absR, absW uint64) DiskIOPS {
	now := time.Now()
	if t.first {
		t.prevR = absR
		t.prevW = absW
		t.prevTime = now
		t.first = false
		return DiskIOPS{}
	}
	dt := now.Sub(t.prevTime).Seconds()
	if dt <= 0 {
		return DiskIOPS{}
	}
	iops := DiskIOPS{
		Read:  uint64(float64(absR-t.prevR) / dt),
		Write: uint64(float64(absW-t.prevW) / dt),
	}
	t.prevR = absR
	t.prevW = absW
	t.prevTime = now
	return iops
}

// FormatBytes formats a byte count into a human-readable string.
func FormatBytes(b uint64) string {
	const (
		_GiB = 1024 * 1024 * 1024
		_TiB = 1024 * _GiB
	)
	switch {
	case b >= _TiB:
		return fmt.Sprintf("%.1f TiB", float64(b)/float64(_TiB))
	case b >= _GiB:
		return fmt.Sprintf("%.1f GiB", float64(b)/float64(_GiB))
	default:
		return fmt.Sprintf("%.0f MiB", float64(b)/float64(1024*1024))
	}
}
