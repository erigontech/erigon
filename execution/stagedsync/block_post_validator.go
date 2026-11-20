package stagedsync

import (
	"context"
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
	digest *types.ReceiptDigest, header *types.Header, txns types.Transactions,
	chainConfig *chain.Config, logger log.Logger,
) *blockValidator {
	bv := &blockValidator{done: make(chan error, 1)}
	go func() {
		bv.done <- validateBlockPostExecution(engine, chainConfig, header, blockGasUsed, blobGasUsed, checkReceipts, checkBloom, receipts, digest, txns, logger)
	}()
	return bv
}

func validateBlockPostExecution(engine rules.Engine, chainConfig *chain.Config, header *types.Header,
	gasUsed, blobGasUsed uint64, checkReceipts, checkBloom bool,
	receipts types.Receipts, digest *types.ReceiptDigest, txns types.Transactions, logger log.Logger,
) error {
	err := engine.ValidateBlockPostExecution(chainConfig, header, gasUsed, blobGasUsed, checkReceipts, checkBloom, receipts, digest, txns, logger)
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

type receiptPipeline struct {
	in     chan *types.Receipt
	done   chan struct{}
	digest *types.ReceiptDigest
}

func newReceiptPipeline(ctx context.Context, txCount int) *receiptPipeline {
	p := &receiptPipeline{in: make(chan *types.Receipt, txCount), done: make(chan struct{})}
	go p.run(ctx, txCount)
	return p
}

func (p *receiptPipeline) run(ctx context.Context, txCount int) {
	defer close(p.done)
	var bloom types.Bloom
	receipts := make(types.Receipts, 0, txCount)
	stream := types.NewListHashStream()
	for {
		select {
		case <-ctx.Done():
			return
		case r, ok := <-p.in:
			if !ok {
				if ctx.Err() == nil {
					p.digest = &types.ReceiptDigest{Bloom: bloom, Root: stream.Hash()}
				}
				return
			}
			r.Bloom = types.CreateBloom(types.Receipts{r})
			bloom.Or(&r.Bloom)
			receipts = append(receipts, r)
			stream.Update(receipts)
		}
	}
}

func (p *receiptPipeline) feed(r *types.Receipt) {
	select {
	case p.in <- r:
	case <-p.done:
	}
}

func (p *receiptPipeline) join() *types.ReceiptDigest {
	close(p.in)
	<-p.done
	return p.digest
}

func (pe *parallelExecutor) receiptChecks(blockNum uint64) (checkReceipts, checkBloom bool) {
	checkBloom = !pe.cfg.vmConfig.StatelessExec && !pe.cfg.vmConfig.NoReceipts
	return checkBloom && pe.cfg.chainConfig.IsByzantium(blockNum), checkBloom
}
