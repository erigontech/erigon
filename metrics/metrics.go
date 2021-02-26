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

	// Create the various data collectors
	cpuStats := &CPUStats{}
	memstats := &runtime.MemStats{}
	diskstats := &DiskStats{}

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

		diskReads             = GetOrRegisterMeter("system/disk/readcount", DefaultRegistry)
		diskReadBytes         = GetOrRegisterMeter("system/disk/readdata", DefaultRegistry)
		diskReadBytesCounter  = GetOrRegisterCounter("system/disk/readbytes", DefaultRegistry)
		diskWrites            = GetOrRegisterMeter("system/disk/writecount", DefaultRegistry)
		diskWriteBytes        = GetOrRegisterMeter("system/disk/writedata", DefaultRegistry)
		diskWriteBytesCounter = GetOrRegisterCounter("system/disk/writebytes", DefaultRegistry)

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
	)

	// Iterate loading the different stats and updating the meters
	for {
		//location1 := i % 2
		//location2 := (i - 1) % 2

		ReadCPUStats(cpuStats)
		cpuSysLoad.Update(cpuStats.GlobalTime)
		cpuSysWait.Update(cpuStats.GlobalWait)
		cpuProcLoad.Update(cpuStats.LocalTime)
		cpuThreads.Update(int64(threadCreateProfile.Count()))
		cpuGoroutines.Update(int64(runtime.NumGoroutine()))

		// getrusage(2)
		ruMaxrss.Update(cpuStats.Usage.Maxrss)
		ruMinflt.Update(cpuStats.Usage.Minflt)
		ruMajflt.Update(cpuStats.Usage.Majflt)
		ruInblock.Update(cpuStats.Usage.Inblock)
		ruOutblock.Update(cpuStats.Usage.Oublock)
		ruNvcsw.Update(cpuStats.Usage.Nvcsw)
		ruNivcsw.Update(cpuStats.Usage.Nivcsw)

		memstats.PauseTotalNs = 0
		memstats.Mallocs = 0
		memstats.Frees = 0
		memstats.HeapSys = 0
		memstats.Alloc = 0
		runtime.ReadMemStats(memstats)
		memPauses.Mark(int64(memstats.PauseTotalNs))
		memAllocs.Mark(int64(memstats.Mallocs))
		memFrees.Mark(int64(memstats.Frees))
		memHeld.Update(int64(memstats.HeapSys))
		memUsed.Update(int64(memstats.Alloc))

		diskstats.ReadCount = 0
		diskstats.ReadBytes = 0
		diskstats.WriteCount = 0
		diskstats.WriteBytes = 0
		if ReadDiskStats(diskstats) == nil {
			diskReads.Mark(diskstats.ReadCount)
			diskReadBytes.Mark(diskstats.ReadBytes)
			diskWrites.Mark(diskstats.WriteCount)
			diskWriteBytes.Mark(diskstats.WriteBytes)

			diskReadBytesCounter.Inc(diskstats.ReadBytes)
			diskWriteBytesCounter.Inc(diskstats.WriteBytes)
		}

		goGoroutines.Update(int64(runtime.NumGoroutine()))
		n, _ := runtime.ThreadCreateProfile(nil)
		goThreads.Update(int64(n))

		time.Sleep(refresh)
	}
}
