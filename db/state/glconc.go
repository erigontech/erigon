package state

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

// GLCONC=true samples how many goroutines are concurrently inside the
// commitment-domain getLatest (i.e. reading cold branch nodes during the fold).
// Used to confirm whether the commitment fold is serial (~1) or parallel.
var glConcEnabled = dbg.EnvBool("GLCONC", false)

var (
	glInflight atomic.Int64
	glMaxWin   atomic.Int64
	glTotal    atomic.Int64
	glOnce     sync.Once
)

func glStartSampler() {
	glOnce.Do(func() {
		go func() {
			for {
				time.Sleep(250 * time.Millisecond)
				log.Info("[GLCONC] commitment getLatest",
					"inflight", glInflight.Load(),
					"max250ms", glMaxWin.Swap(0),
					"total", glTotal.Load())
			}
		}()
	})
}

func glEnter() {
	glStartSampler()
	glTotal.Add(1)
	n := glInflight.Add(1)
	for {
		m := glMaxWin.Load()
		if n <= m || glMaxWin.CompareAndSwap(m, n) {
			break
		}
	}
}

func glExit() { glInflight.Add(-1) }
