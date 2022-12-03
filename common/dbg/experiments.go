package dbg

import (
	"os"
	"runtime"
)

var doMemstat = true

func init() {
	_, ok := os.LookupEnv("NO_MEMSTAT")
	if ok {
		doMemstat = false
	}
}

func DoMemStat() bool { return doMemstat }
func ReadMemStats(m *runtime.MemStats) {
	if doMemstat {
		runtime.ReadMemStats(m)
	}
}
