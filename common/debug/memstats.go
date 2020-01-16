package debug

import (
	"fmt"
	"runtime"
)

func PrintMemStats(short bool) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	if short {
		fmt.Printf("HeapInuse: %vMb\n", ByteToMb(m.HeapInuse))
	} else {
		fmt.Printf("HeapInuse: %vMb, Alloc: %vMb, TotalAlloc: %vMb, Sys: %vMb, NumGC: %v, PauseNs: %d\n", ByteToMb(m.HeapInuse), ByteToMb(m.Alloc), ByteToMb(m.TotalAlloc), ByteToMb(m.Sys), m.NumGC, m.PauseNs[(m.NumGC+255)%256])
	}
}

func ByteToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
