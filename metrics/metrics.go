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
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/log"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
)

// Enabled is checked by the constructor functions for all of the
// standard metrics. If it is true, the metric returned is a stub.
//
// This global kill-switch helps quantify the observer effect and makes
// for less cluttered pprof profiles.
var Enabled = false

// callbacks - storing list of callbacks as type []func()
// use metrics.AddCallback to add your function to metrics collection loop (to avoid multiple goroutines collecting metrics)
var callbacks atomic.Value

func init() {
	callbacks.Store([]func(){})
}
func AddCallback(collect func()) {
	list := callbacks.Load().([]func())
	list = append(list, collect)
	callbacks.Store(list)
}

func getCallbacks() []func() {
	return callbacks.Load().([]func())
}

// Calling Load method

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
	for i := 0; i < len(cpuStats); i++ {
		cpuStats[i] = new(CPUStats)
		memstats[i] = new(runtime.MemStats)
		diskstats[i] = new(DiskStats)
	}
	// Define the various metrics to collect
	var (
		cpuSysLoad    = GetOrRegisterGauge("system/cpu/sysload", DefaultRegistry)
		cpuSysWait    = GetOrRegisterGauge("system/cpu/syswait", DefaultRegistry)
		cpuThreads    = GetOrRegisterGauge("system/cpu/threads", DefaultRegistry)
		cpuGoroutines = GetOrRegisterGauge("system/cpu/goroutines", DefaultRegistry)

		// disabled because of performance impact and because this info exists in logs
		memPauses = GetOrRegisterMeter("system/memory/pauses", DefaultRegistry)
		memAllocs = GetOrRegisterMeter("system/memory/allocs", DefaultRegistry)
		memFrees  = GetOrRegisterMeter("system/memory/frees", DefaultRegistry)
		memHeld   = GetOrRegisterGauge("system/memory/held", DefaultRegistry)
		memUsed   = GetOrRegisterGauge("system/memory/used", DefaultRegistry)

		diskReadBytes  = GetOrRegisterMeter("system/disk/readbytes", DefaultRegistry)
		diskWriteBytes = GetOrRegisterMeter("system/disk/writebytes", DefaultRegistry)

		// copy from prometheus client
		goGoroutines = GetOrRegisterGauge("go/goroutines", DefaultRegistry)
		goThreads    = GetOrRegisterGauge("go/threads", DefaultRegistry)

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

		vmemTotal     = GetOrRegisterGauge("vmem/total", DefaultRegistry)
		vmemAvailable = GetOrRegisterGauge("vmem/available", DefaultRegistry)
		vmemUsed      = GetOrRegisterGauge("vmem/used", DefaultRegistry)
		vmemBuffers   = GetOrRegisterGauge("vmem/buffers", DefaultRegistry)
		vmemCached    = GetOrRegisterGauge("vmem/cached", DefaultRegistry)
		vmemWriteBack = GetOrRegisterGauge("vmem/writeback", DefaultRegistry)
		vmemDirty     = GetOrRegisterGauge("vmem/dirty", DefaultRegistry)
		vmemShared    = GetOrRegisterGauge("vmem/shared", DefaultRegistry)
		vmemMapped    = GetOrRegisterGauge("vmem/mapped", DefaultRegistry)
	)

	p, _ := process.NewProcess(int32(os.Getpid()))
	if p == nil {
		return
	}

	// Iterate loading the different stats and updating the meters
	for i := 1; ; i++ {
		time.Sleep(refresh)
		location1 := i % 2
		location2 := (i - 1) % 2

		ReadCPUStats(p, cpuStats[location1])
		cpuSysLoad.Update((cpuStats[location1].GlobalTime - cpuStats[location2].GlobalTime) / refreshFreq)
		cpuSysWait.Update((cpuStats[location1].GlobalWait - cpuStats[location2].GlobalWait) / refreshFreq)

		inblock, outblokc, nvcsw, nivcsw := getRUsage(p)
		ruInblock.Update(inblock)
		ruOutblock.Update(outblokc)
		ruNvcsw.Update(nvcsw)
		ruNivcsw.Update(nivcsw)

		cpuThreads.Update(int64(threadCreateProfile.Count()))
		cpuGoroutines.Update(int64(runtime.NumGoroutine()))

		if m, _ := mem.VirtualMemory(); m != nil {
			vmemTotal.Update(int64(m.Total))
			vmemAvailable.Update(int64(m.Available))
			vmemUsed.Update(int64(m.Used))
			vmemBuffers.Update(int64(m.Buffers))
			vmemCached.Update(int64(m.Cached))
			vmemWriteBack.Update(int64(m.WriteBack))
			vmemDirty.Update(int64(m.Dirty))
			vmemShared.Update(int64(m.Shared))
			vmemMapped.Update(int64(m.Mapped))

			//Slab           uint64 `json:"slab"`
			//Sreclaimable   uint64 `json:"sreclaimable"`
			//Sunreclaim     uint64 `json:"sunreclaim"`
			//PageTables     uint64 `json:"pageTables"`
			//SwapCached     uint64 `json:"swapCached"`
			//CommitLimit    uint64 `json:"commitLimit"`
			//CommittedAS    uint64 `json:"committedAS"`
			//HighTotal      uint64 `json:"highTotal"`
			//HighFree       uint64 `json:"highFree"`
			//LowTotal       uint64 `json:"lowTotal"`
			//LowFree        uint64 `json:"lowFree"`
			//SwapTotal      uint64 `json:"swapTotal"`
			//SwapFree       uint64 `json:"swapFree"`
			//VmallocTotal   uint64 `json:"vmallocTotal"`
			//VmallocUsed    uint64 `json:"vmallocUsed"`
			//VmallocChunk   uint64 `json:"vmallocChunk"`
		}
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

		runtime.ReadMemStats(memstats[location1])
		memPauses.Mark(int64(memstats[location1].PauseTotalNs - memstats[location2].PauseTotalNs))
		memAllocs.Mark(int64(memstats[location1].Mallocs - memstats[location2].Mallocs))
		memFrees.Mark(int64(memstats[location1].Frees - memstats[location2].Frees))
		memHeld.Update(int64(memstats[location1].HeapSys - memstats[location1].HeapReleased))
		memUsed.Update(int64(memstats[location1].Alloc))

		if io, _ := p.IOCounters(); io != nil {
			diskstats[location1].ReadBytes = int64(io.ReadBytes)
			diskstats[location1].WriteBytes = int64(io.WriteBytes)
			diskReadBytes.Mark(diskstats[location1].ReadBytes - diskstats[location2].ReadBytes)
			diskWriteBytes.Mark(diskstats[location1].WriteBytes - diskstats[location2].WriteBytes)
		}
		goGoroutines.Update(int64(runtime.NumGoroutine()))
		n, _ := runtime.ThreadCreateProfile(nil)
		goThreads.Update(int64(n))

		for _, cb := range getCallbacks() {
			cb()
		}
	}
}
