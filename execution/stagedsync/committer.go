package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
)

// commitmentResult is the outcome of a single per-block commitment computation.
type commitmentResult struct {
	blockNum uint64
	txNum    uint64
	rootHash []byte
	err      error
}

// commitmentCalculator receives the same txResult/blockResult stream as the
// apply loop (via a fan-out channel). For each txResult it accumulates the
// TouchKey updates. At each blockResult boundary it computes the trie root
// from sd.mem and publishes the result.
type commitmentCalculator struct {
	doms      *execctx.SharedDomains
	db        kv.TemporalRoDB
	logPrefix string

	// in receives the same applyResult stream as the apply loop.
	in chan applyResult
	// out publishes per-block commitment roots.
	out chan commitmentResult

	wg   sync.WaitGroup
	done chan struct{}
}

func newCommitmentCalculator(
	doms *execctx.SharedDomains,
	db kv.TemporalRoDB,
	logPrefix string,
	in chan applyResult,
	out chan commitmentResult,
) *commitmentCalculator {
	return &commitmentCalculator{
		doms:      doms,
		db:        db,
		logPrefix: logPrefix,
		in:        in,
		out:       out,
		done:      make(chan struct{}),
	}
}

func (cc *commitmentCalculator) Start(ctx context.Context) {
	cc.wg.Add(1)
	go cc.loop(ctx)
}

func (cc *commitmentCalculator) Stop() {
	close(cc.done)
	cc.wg.Wait()
}

func (cc *commitmentCalculator) loop(ctx context.Context) {
	defer cc.wg.Done()

	for {
		select {
		case result, ok := <-cc.in:
			if !ok {
				return
			}
			switch result := result.(type) {
			case *txResult:
				// Per-TX: the TouchKey calls happen inside DomainPut
				// (called by ApplyStateWrites in the execLoop).
				// The commitment context accumulates them automatically.
				// Nothing to do here — the touches are already recorded
				// in the SharedDomains commitment context.
				_ = result

			case *blockResult:
				// Block boundary: compute commitment from accumulated touches.
				if dbg.DiscardCommitment() {
					continue
				}
				cc.computeAndPublish(ctx, result)
			}

		case <-ctx.Done():
			return
		case <-cc.done:
			return
		}
	}
}

func (cc *commitmentCalculator) computeAndPublish(ctx context.Context, br *blockResult) {
	roTx, err := cc.db.BeginTemporalRo(ctx)
	if err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: br.BlockNum,
			txNum:    br.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: open roTx: %w", err),
		})
		return
	}
	defer roTx.Rollback()

	rh, err := cc.doms.ComputeCommitment(ctx, roTx, true, br.BlockNum, br.lastTxNum, cc.logPrefix, nil)
	if err != nil {
		cc.publish(ctx, commitmentResult{
			blockNum: br.BlockNum,
			txNum:    br.lastTxNum,
			err:      fmt.Errorf("commitmentCalculator: %w", err),
		})
		return
	}

	r := commitmentResult{
		blockNum: br.BlockNum,
		txNum:    br.lastTxNum,
		rootHash: rh,
	}

	// Check against expected root
	if !bytes.Equal(rh, br.StateRoot.Bytes()) {
		r.err = fmt.Errorf("%w: block %d root %x expected %x", ErrWrongTrieRoot, br.BlockNum, rh, br.StateRoot)
	}

	cc.publish(ctx, r)
}

func (cc *commitmentCalculator) publish(ctx context.Context, r commitmentResult) {
	select {
	case cc.out <- r:
	case <-ctx.Done():
	case <-cc.done:
	}
}

// collectCommitmentResult drains one result from the commitment calculator.
// Returns nil on context cancellation.
func collectCommitmentResult(ctx context.Context, out chan commitmentResult) *commitmentResult {
	select {
	case r := <-out:
		return &r
	case <-ctx.Done():
		return nil
	}
}

// Helper for progress logging during commitment.
var _ = commitment.CommitProgress{} // keep import
var _ = common.Hash{}               // keep import
