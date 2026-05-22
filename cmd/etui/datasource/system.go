package datasource

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"time"

	pscpu "github.com/shirou/gopsutil/v4/cpu"
	psdisk "github.com/shirou/gopsutil/v4/disk"
	psmem "github.com/shirou/gopsutil/v4/mem"
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

// SystemCollector gathers system metrics for the local host.
type SystemCollector struct {
	datadir string
}

// SystemPollInterval is how often system stats are refreshed.
const SystemPollInterval = 5 * time.Second

// NewSystemCollector creates a collector. datadir is used for filesystem usage.
func NewSystemCollector(datadir string) *SystemCollector {
	return &SystemCollector{datadir: datadir}
}

// CollectSystemStats gathers CPU, memory, disk, and Go heap metrics.
// Host metrics are collected via gopsutil so the widget works on Linux, macOS,
// and Windows.
func (c *SystemCollector) CollectSystemStats() SystemStats {
	stats := SystemStats{
		NumCPU: runtime.NumCPU(),
	}

	if cpuPercent, err := pscpu.Percent(0, false); err == nil && len(cpuPercent) > 0 {
		stats.CPUPercent = clampPercent(cpuPercent[0])
	}

	// Go heap
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	stats.GoHeap = m.HeapInuse

	if vm, err := psmem.VirtualMemory(); err == nil {
		stats.MemTotal = vm.Total
		if vm.Available <= vm.Total {
			stats.MemUsed = vm.Total - vm.Available
		} else {
			stats.MemUsed = vm.Used
		}
	}

	if usage, err := psdisk.Usage(c.datadir); err == nil {
		stats.DiskTotal = usage.Total
		stats.DiskUsed = usage.Used
	}

	stats.DiskIOPS_R, stats.DiskIOPS_W = readDiskIO()

	return stats
}

// readDiskIO sums absolute read/write operation counters across visible
// physical disks.
func readDiskIO() (reads, writes uint64) {
	counters, err := psdisk.IOCounters()
	if err != nil {
		return 0, 0
	}
	for name, counter := range counters {
		if !shouldCountDisk(name) {
			continue
		}
		reads += counter.ReadCount
		writes += counter.WriteCount
	}
	return reads, writes
}

func clampPercent(v float64) uint64 {
	switch {
	case math.IsNaN(v), v < 0:
		return 0
	case v > 100:
		return 100
	default:
		return uint64(v + 0.5)
	}
}

func shouldCountDisk(name string) bool {
	switch runtime.GOOS {
	case "linux":
		return !isLinuxPartition(name)
	case "darwin":
		return !isDarwinPartition(name)
	case "windows":
		return isWindowsFixedDrive(name)
	default:
		return name != ""
	}
}

// isLinuxPartition returns true if the device name looks like a partition
// (sda1, nvme0n1p1) rather than a whole disk (sda, nvme0n1).
func isLinuxPartition(name string) bool {
	if len(name) == 0 {
		return false
	}
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
	last := name[len(name)-1]
	return last >= '0' && last <= '9'
}

func isDarwinPartition(name string) bool {
	if !strings.HasPrefix(name, "disk") {
		return false
	}
	suffix := strings.TrimPrefix(name, "disk")
	return strings.Contains(suffix, "s")
}

func isWindowsFixedDrive(name string) bool {
	return len(name) == 2 && name[1] == ':'
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
