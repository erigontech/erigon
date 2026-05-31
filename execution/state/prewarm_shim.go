package state

import (
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types"
)

// PrewarmBlockStateCacheFromBAL is a vio-restructure shim — no-op. The
// BAL-driven prewarm lives in a separate landing; the typed-vio shape
// compiles without it.
func PrewarmBlockStateCacheFromBAL(_ *BlockStateCache, _ types.BlockAccessList, _ *cache.StateCache) {
}
