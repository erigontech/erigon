package stagedsync

import (
	"fmt"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/ethutils"
)

type blockValidator struct {
	done chan error // buffered(1); written once, then re-stuffed on each Wait
}

func newBlockValidator(engine rules.Engine, blockGasUsed, blobGasUsed uint64, checkReceipts, checkBloom bool, receipts types.Receipts,
	header *types.Header, txns types.Transactions,
	chainConfig *chain.Config, logger log.Logger) *blockValidator {
	bv := &blockValidator{done: make(chan error, 1)}
	go func() {
		bv.done <- validateBlockPostExecution(engine, chainConfig, header, blockGasUsed, blobGasUsed, checkReceipts, checkBloom, receipts, txns, logger)
	}()
	return bv
}

func validateBlockPostExecution(engine rules.Engine, chainConfig *chain.Config, header *types.Header,
	gasUsed, blobGasUsed uint64, checkReceipts, checkBloom bool,
	receipts types.Receipts, txns types.Transactions, logger log.Logger) error {
	err := engine.ValidateBlockPostExecution(chainConfig, header, gasUsed, blobGasUsed, checkReceipts, checkBloom, receipts, txns, logger)
	switch {
	case err != nil && dbg.LogHashMismatchReason():
		ethutils.LogReceipts(log.LvlWarn, "post-execution validation failed", receipts, txns, chainConfig, header, logger)
	case err == nil && dbg.TraceLogs && dbg.TraceBlock(header.Number.Uint64()):
		ethutils.LogReceipts(log.LvlInfo, "trace logs", receipts, txns, chainConfig, header, logger)
	}
	return err
}

// Safe on nil receiver and idempotent (re-stuffs the result after each read).
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
