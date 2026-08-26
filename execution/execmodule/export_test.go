package execmodule

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// RecordSealedForTest records a sealed header in the accept-by-seal set (sealedByHash), so tests can exercise
// ValidateChain's accept-by-lookup path without driving the full marker seal. Test-only (compiled only under
// _test.go); it does not widen the production API.
func (e *ExecModule) RecordSealedForTest(h *types.Header) {
	e.pendingBoundaryMu.Lock()
	if e.sealedByHash == nil {
		e.sealedByHash = make(map[common.Hash]*types.Header)
	}
	e.sealedByHash[h.Hash()] = h
	e.pendingBoundaryMu.Unlock()
}
