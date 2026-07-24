package commitment

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// Diagnostic-only: when WARMUP_DUMP_FILE is set, record every (hashedKey,
// startDepth) fed to the warmuper during a real run, so the exact key workload
// can be replayed against a standalone warmuper harness. Not for production use.
var (
	warmDumpPath = os.Getenv("WARMUP_DUMP_FILE")
	warmDumpMu   sync.Mutex
	warmDumpW    *bufio.Writer
	warmDumpOnce sync.Once
	warmDumpN    atomic.Uint64
)

func warmDumpKey(hashedKey []byte, startDepth int) {
	if warmDumpPath == "" {
		return
	}
	warmDumpOnce.Do(func() {
		f, err := os.Create(warmDumpPath)
		if err != nil {
			warmDumpPath = ""
			return
		}
		warmDumpW = bufio.NewWriterSize(f, 4<<20)
	})
	if warmDumpW == nil {
		return
	}
	warmDumpMu.Lock()
	fmt.Fprintf(warmDumpW, "%x %d\n", hashedKey, startDepth)
	warmDumpMu.Unlock()
	if warmDumpN.Add(1)%(1<<16) == 0 {
		warmDumpFlush()
	}
}

func warmDumpFlush() {
	if warmDumpW == nil {
		return
	}
	warmDumpMu.Lock()
	_ = warmDumpW.Flush()
	warmDumpMu.Unlock()
}
