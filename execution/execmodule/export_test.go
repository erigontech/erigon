package execmodule

import (
	"context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
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

// FlashBodyForTest returns the in-progress block's accumulated tx RLPs, so a test can assert what a failed
// or abandoned round left behind. Test-only.
func (e *ExecModule) FlashBodyForTest() [][]byte { return e.flashBodyCopy() }

// ModuleContextTxnSequenceForTest returns the kv.EthTx sequence as the committed DB holds it and as the module
// context's block overlay holds it, so a test can see whether a canonical insert allocated transaction ids in the
// module context. ok is false when there is no module context yet.
func (e *ExecModule) ModuleContextTxnSequenceForTest(ctx context.Context) (committed, moduleContext uint64, ok bool, err error) {
	if err = e.semaphore.Acquire(ctx, 1); err != nil {
		return 0, 0, false, err
	}
	defer e.semaphore.Release(1)
	sd := e.currentContext
	if sd == nil || sd.BlockOverlay() == nil {
		return 0, 0, false, nil
	}
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, 0, false, err
	}
	defer roTx.Rollback()
	if committed, err = roTx.ReadSequence(kv.EthTx); err != nil {
		return 0, 0, false, err
	}
	if moduleContext, err = sd.BlockOverlay().ReadSequence(kv.EthTx); err != nil {
		return 0, 0, false, err
	}
	return committed, moduleContext, true, nil
}
