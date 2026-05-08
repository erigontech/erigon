package stagedsync

import (
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

// blockValidator runs BlockPostValidation in a goroutine for a SINGLE block.
// One instance per block — no shared state across blocks. The previous
// shared-across-blocks validator carried v.err and v.wg between blocks,
// which was prone to leaking wg counters on early-return and to "sticky
// errors" propagating from one block's failure into the next. A fresh
// instance per block eliminates both classes of bug.
//
// The shape mirrors the commitmentCalculator: the validator owns its own
// goroutine and delivers its result through a buffered channel. Wait()
// is the single point of synchronization.
type blockValidator struct {
	done chan error // buffered(1); written once, then re-stuffed on each Wait
}

// newBlockValidator starts validation in a goroutine. The caller must
// eventually call Wait() to surface the result and to drain the goroutine.
func newBlockValidator(blockGasUsed, blobGasUsed uint64, checkReceipts, checkBloom bool, receipts types.Receipts,
	header *types.Header, txns types.Transactions,
	chainConfig *chain.Config, logger log.Logger) *blockValidator {
	bv := &blockValidator{done: make(chan error, 1)}
	go func() {
		bv.done <- protocol.BlockPostValidation(blockGasUsed, blobGasUsed, checkReceipts, checkBloom, receipts, header, txns, chainConfig, logger)
	}()
	return bv
}

// Wait blocks until validation completes and returns the (wrapped) error,
// or nil. Safe to call on a nil receiver (no-op) and safe to call multiple
// times — the result is re-stuffed into the channel after each read.
func (bv *blockValidator) Wait() error {
	if bv == nil {
		return nil
	}
	err := <-bv.done
	bv.done <- err
	if err != nil {
		return fmt.Errorf("%w, %w", rules.ErrInvalidBlock, err)
	}
	return nil
}
