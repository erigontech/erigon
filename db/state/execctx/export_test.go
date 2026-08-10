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

// SetCanonicalCachesForTest attaches canonical cache capability without the
// USE_STATE_CACHE gate used by SetCanonicalCaches and SetStateCacheReader.
// It avoids changing the process-wide flag in parallel tests.
func (sd *SharedDomains) SetCanonicalCachesForTest(sc *cache.StateCache) {
	sd.setCanonicalCaches(sc)
}

func (sd *SharedDomains) SetStateCacheReaderForTest(sc *cache.StateCache) {
	sd.setStateCacheReader(sc)
}

func (sd *SharedDomains) CachePublishersEnabledForTest() (state, branch bool) {
	return sd.statePublisher.Enabled(), sd.branchPublisher.Enabled()
}
