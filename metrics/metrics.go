// Go port of Coda Hale's Metrics library
//
// <https://github.com/rcrowley/go-metrics>
//
// Coda Hale's original work: <https://github.com/codahale/metrics>
package metrics

import (
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/shirou/gopsutil/v3/process"
)

// Enabled is checked by the constructor functions for all of the
// standard metrics. If it is true, the metric returned is a stub.
//
// This global kill-switch helps quantify the observer effect and makes
// for less cluttered pprof profiles.
var Enabled = false

// EnabledExpensive is a soft-flag meant for external packages to check if costly
// metrics gathering is allowed or not. The goal is to separate standard metrics
// for health monitoring and debug metrics that might impact runtime performance.
var EnabledExpensive = false

// enablerFlags is the CLI flag names to use to enable metrics collections.
var enablerFlags = []string{"metrics"}

// expensiveEnablerFlags is the CLI flag names to use to enable metrics collections.
var expensiveEnablerFlags = []string{"metrics.expensive"}

// Init enables or disables the metrics system. Since we need this to run before
// any other code gets to create meters and timers, we'll actually do an ugly hack
// and peek into the command line args for the metrics flag.
func init() {
	for _, arg := range os.Args {
		flag := strings.TrimLeft(arg, "-")

		for _, enabler := range enablerFlags {
			if !Enabled && flag == enabler {
				log.Info("Enabling metrics collection")
				Enabled = true
			}
		}
		for _, enabler := range expensiveEnablerFlags {
			if !EnabledExpensive && flag == enabler {
				log.Info("Enabling expensive metrics collection")
				EnabledExpensive = true
			}
		}
	}
}

// CollectProcessMetrics periodically collects various metrics about the running
// process.
func CollectProcessMetrics(refresh time.Duration) {
	// Short circuit if the metrics system is disabled
	if !Enabled {
		return
	}
	refreshFreq := int64(refresh / time.Second)

	// Create the various data collectors
	cpuStats := make([]*CPUStats, 2)
	memstats := make([]*runtime.MemStats, 2)
	diskstats := make([]*DiskStats, 2)
	for i := 0; i < len(memstats); i++ {
		cpuStats[i] = new(CPUStats)
		memstats[i] = new(runtime.MemStats)
		diskstats[i] = new(DiskStats)
	}
	// Define the various metrics to collect
	var (
		cpuSysLoad    = GetOrRegisterGauge("system/cpu/sysload", DefaultRegistry)
		cpuSysWait    = GetOrRegisterGauge("system/cpu/syswait", DefaultRegistry)
		cpuProcLoad   = GetOrRegisterGauge("system/cpu/procload", DefaultRegistry)
		cpuThreads    = GetOrRegisterGauge("system/cpu/threads", DefaultRegistry)
		cpuGoroutines = GetOrRegisterGauge("system/cpu/goroutines", DefaultRegistry)

		memPauses = GetOrRegisterMeter("system/memory/pauses", DefaultRegistry)
		memAllocs = GetOrRegisterMeter("system/memory/allocs", DefaultRegistry)
		memFrees  = GetOrRegisterMeter("system/memory/frees", DefaultRegistry)
		memHeld   = GetOrRegisterGauge("system/memory/held", DefaultRegistry)
		memUsed   = GetOrRegisterGauge("system/memory/used", DefaultRegistry)

		diskReads      = GetOrRegisterMeter("system/disk/readcount", DefaultRegistry)
		diskReadBytes  = GetOrRegisterMeter("system/disk/readbytes", DefaultRegistry)
		diskWrites     = GetOrRegisterMeter("system/disk/writecount", DefaultRegistry)
		diskWriteBytes = GetOrRegisterMeter("system/disk/writebytes", DefaultRegistry)

		// copy from prometheus client
		goGoroutines = GetOrRegisterGauge("go/goroutines", DefaultRegistry)
		goThreads    = GetOrRegisterGauge("go/threads", DefaultRegistry)

		//struct rusage {
		//	struct timeval ru_utime; /* user CPU time used */
		//	struct timeval ru_stime; /* system CPU time used */
		//	long   ru_maxrss;        /* maximum resident set size (kilobytes) */
		//	long   ru_minflt;        /* page reclaims (soft page faults) */
		//	long   ru_majflt;        /* page faults (hard page faults) */
		//	long   ru_inblock;       /* block input operations */
		//	long   ru_oublock;       /* block output operations */
		//	long   ru_nvcsw;         /* voluntary context switches */
		//	long   ru_nivcsw;        /* involuntary context switches */
		//};
		ruMaxrss   = GetOrRegisterGauge("ru/maxrss", DefaultRegistry)
		ruMinflt   = GetOrRegisterGauge("ru/minflt", DefaultRegistry)
		ruMajflt   = GetOrRegisterGauge("ru/majflt", DefaultRegistry)
		ruInblock  = GetOrRegisterGauge("ru/inblock", DefaultRegistry)
		ruOutblock = GetOrRegisterGauge("ru/outblock", DefaultRegistry)
		ruNvcsw    = GetOrRegisterGauge("ru/nvcsw", DefaultRegistry)
		ruNivcsw   = GetOrRegisterGauge("ru/nivcsw", DefaultRegistry)

		memRSS    = GetOrRegisterGauge("mem/rss", DefaultRegistry)
		memVMS    = GetOrRegisterGauge("mem/vms", DefaultRegistry)
		memHVM    = GetOrRegisterGauge("mem/hvm", DefaultRegistry)
		memData   = GetOrRegisterGauge("mem/data", DefaultRegistry)
		memStack  = GetOrRegisterGauge("mem/stack", DefaultRegistry)
		memLocked = GetOrRegisterGauge("mem/locked", DefaultRegistry)
		memSwap   = GetOrRegisterGauge("mem/swap", DefaultRegistry)
	)

	p, _ := process.NewProcess(int32(os.Getpid()))
	if p == nil {
		return
	}

	// Iterate loading the different stats and updating the meters
	for i := 1; ; i++ {
		location1 := i % 2
		location2 := (i - 1) % 2

		//ReadCPUStats(cpuStats[location1])
		mi, _ := p.Times()
		ReadCPUStats2(mi, cpuStats[location1])
		cpuSysLoad.Update((cpuStats[location1].GlobalTime - cpuStats[location2].GlobalTime) / refreshFreq)
		cpuSysWait.Update((cpuStats[location1].GlobalWait - cpuStats[location2].GlobalWait) / refreshFreq)
		cpuProcLoad.Update((cpuStats[location1].LocalTime - cpuStats[location2].LocalTime) / refreshFreq)
		cpuThreads.Update(int64(threadCreateProfile.Count()))
		cpuGoroutines.Update(int64(runtime.NumGoroutine()))

		//vm, _ := mem.VirtualMemory()
		//sw, _ := mem.SwapMemory()
		//mi, _ := p.CPUPercent()

		// getrusage(2)
		ruMaxrss.Update(cpuStats[location1].Usage.Maxrss)
		ruInblock.Update(cpuStats[location1].Usage.Inblock)
		ruOutblock.Update(cpuStats[location1].Usage.Oublock)
		//mi, _ := p.MemoryInfoEx()
		//if m, _ := p.MemoryMaps(true); m != nil && len(*m) > 0 {
		//	mm := (*m)[0]
		//}
		if m, _ := p.MemoryInfo(); m != nil {
			memRSS.Update(int64(m.RSS))
			memVMS.Update(int64(m.VMS))
			memHVM.Update(int64(m.HWM))
			memData.Update(int64(m.Data))
			memStack.Update(int64(m.Stack))
			memLocked.Update(int64(m.Locked))
			memSwap.Update(int64(m.Swap))
		}
		if pf, _ := p.PageFaults(); pf != nil {
			ruMinflt.Update(int64(pf.MinorFaults))
			ruMajflt.Update(int64(pf.MajorFaults))
		}
		if cs, _ := p.NumCtxSwitches(); cs != nil {
			ruNvcsw.Update(cs.Voluntary)
			ruNivcsw.Update(cs.Involuntary)
		}

		runtime.ReadMemStats(memstats[location1])
		memPauses.Mark(int64(memstats[location1].PauseTotalNs - memstats[location2].PauseTotalNs))
		memAllocs.Mark(int64(memstats[location1].Mallocs - memstats[location2].Mallocs))
		memFrees.Mark(int64(memstats[location1].Frees - memstats[location2].Frees))
		memHeld.Update(int64(memstats[location1].HeapSys - memstats[location1].HeapReleased))
		memUsed.Update(int64(memstats[location1].Alloc))

		if io, _ := p.IOCounters(); io != nil {
			diskReads.Mark(int64(io.ReadCount))
			diskWrites.Mark(int64(io.WriteCount))
			diskReadBytes.Mark(int64(io.ReadBytes))
			diskWriteBytes.Mark(int64(io.WriteBytes))
		}
		//if ReadDiskStats(diskstats[location1]) == nil {
		//diskReadBytes.Mark(diskstats[location1].ReadBytes - diskstats[location2].ReadBytes)
		//diskWriteBytes.Mark(diskstats[location1].WriteBytes - diskstats[location2].WriteBytes)
		//diskReadBytesCounter.Inc(diskstats[location1].ReadBytes - diskstats[location2].ReadBytes)
		//diskWriteBytesCounter.Inc(diskstats[location1].WriteBytes - diskstats[location2].WriteBytes)
		//}

		goGoroutines.Update(int64(runtime.NumGoroutine()))
		n, _ := runtime.ThreadCreateProfile(nil)
		goThreads.Update(int64(n))

		time.Sleep(refresh)
	}
}
