package execctx

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/cache"
)

// CodeHashForAddr exposes the unexported codeHashForAddr for tests in the
// external test package (which cannot import db/state to build a SharedDomains
// internally without an import cycle).
func (sd *SharedDomains) CodeHashForAddr(tx kv.TemporalTx, addr []byte, txNum uint64) []byte {
	return sd.codeHashForAddr(tx, sd.cacheViewsFor(tx).state, addr)
}

// SetStateCacheForTest attaches a cache unconditionally, bypassing the
// USE_STATE_CACHE env gate that SetStateCache honors. Cache-behavior tests use
// it so they always exercise the cache instead of skipping when the env is off
// — without mutating the process-global flag (which would race t.Parallel tests).
func (sd *SharedDomains) SetStateCacheForTest(sc *cache.StateCache) {
	if !sd.clearStateCache {
		sd.stateCache = sc
	}
	if sd.baseStateVersionKnown {
		sd.cachePublisher = sc.Publisher()
		sd.cachePublisher.Initialize(sd.baseStateVersion)
	}
}

func (sd *SharedDomains) SetStateCacheReaderForTest(sc *cache.StateCache) {
	if !sd.clearStateCache {
		sd.stateCache = sc
	}
}
