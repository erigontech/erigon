package stagedsync

import (
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

// BlockPostExecutionValidator validates block state after execution (gas used, receipts, etc.)
type BlockPostExecutionValidator interface {
	Process(blockGasUsed, blobGasUsed uint64, checkReceipts bool, receipts types.Receipts,
		header *types.Header, isMining bool, txns types.Transactions,
		chainConfig *chain.Config, logger log.Logger) error
	Wait() error
}

// blockPostExecutionValidator is a synchronous implementation of BlockPostExecutionValidator
type blockPostExecutionValidator struct{}

func newBlockPostExecutionValidator() BlockPostExecutionValidator {
	return &blockPostExecutionValidator{}
}

func (v *blockPostExecutionValidator) Process(blockGasUsed, blobGasUsed uint64, checkReceipts bool, receipts types.Receipts,
	header *types.Header, isMining bool, txns types.Transactions,
	chainConfig *chain.Config, logger log.Logger) error {
	return protocol.BlockPostValidation(blockGasUsed, blobGasUsed, checkReceipts, receipts, header, isMining, txns, chainConfig, logger)
}

func (v *blockPostExecutionValidator) Wait() error {
	return nil
}

// parallelBlockPostExecutionValidator is an asynchronous implementation of BlockPostExecutionValidator.
// Process() is non-blocking and spawns a goroutine, Wait() blocks until all validations complete.
type parallelBlockPostExecutionValidator struct {
	mu  sync.Mutex
	wg  sync.WaitGroup
	err error
}

func newParallelBlockPostExecutionValidator() BlockPostExecutionValidator {
	return &parallelBlockPostExecutionValidator{}
}

func (v *parallelBlockPostExecutionValidator) Process(blockGasUsed, blobGasUsed uint64, checkReceipts bool, receipts types.Receipts,
	header *types.Header, isMining bool, txns types.Transactions,
	chainConfig *chain.Config, logger log.Logger) error {
	v.wg.Add(1)
	v.mu.Lock()
	if v.err != nil {
		v.mu.Unlock()
		return v.err
	}
	v.mu.Unlock()
	go func() {
		defer v.wg.Done()
		if err := protocol.BlockPostValidation(blockGasUsed, blobGasUsed, checkReceipts, receipts, header, isMining, txns, chainConfig, logger); err != nil {
			v.mu.Lock()
			if v.err == nil {
				v.err = err
			}
			v.mu.Unlock()
		}
	}()
	return nil
}

func (v *parallelBlockPostExecutionValidator) Wait() error {
	v.wg.Wait()
	if v.err != nil {
		return fmt.Errorf("%w, %w", rules.ErrInvalidBlock, v.err)
	}
	return nil
}
