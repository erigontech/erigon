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

// SetStateCacheForTest attaches canonical cache capability without the
// USE_STATE_CACHE gate used by SetCanonicalStateCache and SetStateCacheReader.
// It avoids changing the process-wide flag in parallel tests.
func (sd *SharedDomains) SetStateCacheForTest(sc *cache.StateCache) {
	if !sd.clearExecutionCaches {
		sd.stateCache = sc
	}
	if sd.baseStateVersionKnown {
		sd.statePublisher = sc.Publisher()
		sd.statePublisher.Initialize(sd.baseCacheGenerations.state)
	}
}

func (sd *SharedDomains) SetStateCacheReaderForTest(sc *cache.StateCache) {
	if !sd.clearExecutionCaches {
		sd.stateCache = sc
	}
}
