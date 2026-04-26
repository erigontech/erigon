// Instrumentation: SD-FINAL event emitted when an account destroyed at tx end
// still has a non-zero balance. Mirrors the vm package's SDBurnLog setup but
// kept separate to avoid an import cycle.
package state

import (
	"io"
	"log"
	"os"
	"sync"
)

var (
	sdBurnOnce sync.Once
	sdBurnLog  *log.Logger
)

const sdBurnDefaultPath = "/tmp/sd_burns.log"

func sdBurnInit() {
	path := os.Getenv("SD_BURN_LOG")
	if path == "" {
		path = sdBurnDefaultPath
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		sdBurnLog = log.New(io.Discard, "", 0)
		return
	}
	sdBurnLog = log.New(f, "", 0)
}

// sdBurnLogf writes one line to the SD-FINAL log.
func sdBurnLogf(format string, args ...any) {
	sdBurnOnce.Do(sdBurnInit)
	sdBurnLog.Printf(format, args...)
}
