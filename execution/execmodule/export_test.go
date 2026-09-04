package execmodule

import (
	"context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// RecordSealedForTest records a sealed header in the accept-by-seal set (sealedByHash), so tests can exercise
// ValidateChain's accept-by-lookup path without driving the full marker seal. Test-only (compiled only under
// _test.go); it does not widen the production API.
func (e *ExecModule) RecordSealedForTest(h *types.Header) {
	e.pendingBlockMu.Lock()
	if e.sealedByHash == nil {
		e.sealedByHash = make(map[common.Hash]*types.Header)
	}
	e.sealedByHash[h.Hash()] = h
	e.pendingBlockMu.Unlock()
}

// AccumulateFlashblockForTest drives one accumulation round with an explicit restore flag, so tests can
// exercise the re-open path (restore=true, body replayed verbatim) against the accumulate path
// (restore=false, body filtered) without reproducing the timing that makes them differ in production.
func (e *ExecModule) AccumulateFlashblockForTest(ctx context.Context, in FlashblockInputs, txs [][]byte, restore bool) (*types.RawBody, common.Hash, ValidationResult, error) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return nil, common.Hash{}, ValidationResult{}, err
	}
	defer e.semaphore.Release(1)
	return e.accumulateFlashblockLocked(ctx, in, txs, restore)
}

// ResetFlashBodyForTest clears the in-progress body and its dedup records while leaving the pre-exec
// generation in place. That is the state a re-open lands in when the abandon does not leave a clean parent
// generation active: no memory of what was accepted, and a live generation that already applied it.
func (e *ExecModule) ResetFlashBodyForTest(num uint64) {
	e.flash.mu.Lock()
	e.flash.resetLocked(num)
	e.flash.mu.Unlock()
}
