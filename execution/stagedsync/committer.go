package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
)

// commitmentResult is the outcome of a single per-block commitment computation.
type commitmentResult struct {
	blockNum uint64
	txNum    uint64
	rootHash []byte
	err      error
}

// commitmentCalculator receives the same txResult/blockResult stream as the
// apply loop (via a fan-out channel). For each txResult it accumulates key
// touches from the writes. At each blockResult boundary it computes the trie
// root and publishes the result.
//
// The calculator owns its own commitment.Updates buffer. Writes from the
// execLoop flow through VersionedWrites.TouchUpdates() which calls
// TouchPlainKeyDirect() — no serialization round-trip.
//
// It reads values from sd.mem (shared with the execLoop) via GetLatest.
// The execLoop's Flush writes to sd.mem before sending the blockResult,
// so by the time the calculator receives the blockResult, sd.mem has the
// correct block-boundary state.
type commitmentCalculator struct {
	doms      *execctx.SharedDomains
	db        kv.TemporalRoDB
	logPrefix string

	// updates is the calculator's OWN buffer — never shared with the
	// execLoop or apply loop. Only this goroutine reads/writes it.
	updates *commitment.Updates

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
	// Create the calculator's own Updates buffer matching the sdCtx's mode.
	sdCtxUpdates := doms.GetCommitmentContext().GetUpdates()
	calcUpdates := sdCtxUpdates.NewEmpty()

	return &commitmentCalculator{
		doms:      doms,
		db:        db,
		logPrefix: logPrefix,
		updates:   calcUpdates,
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
		// Check done FIRST — prioritize shutdown over processing.
		select {
		case <-cc.done:
			return
		default:
		}

		select {
		case result, ok := <-cc.in:
			if !ok {
				return
			}
			switch r := result.(type) {
			case *txResult:
				// Accumulate key touches in the calculator's OWN buffer.
				// This buffer is never accessed by the execLoop or apply loop.
				if len(r.writes) > 0 {
					state.VersionedWrites(r.writes).TouchUpdates(cc.updates)
				}

			case *blockResult:
				// Block boundary: just accumulate — don't compute per-block.
				// The apply loop will request computation at batch boundaries
				// via drainBeforeExit. Per-block commitment would fill
				// the rootResults channel and deadlock the pipeline.
				// TODO: add explicit compute-and-drain coordination.
				_ = r
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

	// Install the calculator's accumulated touches into sdCtx for
	// ComputeCommitment to process. After processing, install a fresh
	// empty buffer. This is safe because inline TouchKey is disabled —
	// no other goroutine accesses sdCtx.updates.
	sdCtx := cc.doms.GetCommitmentContext()
	sdCtx.SetUpdates(cc.updates)
	cc.updates = cc.updates.NewEmpty()

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

	// Check against expected root from the block header.
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

// Keep imports used.
var _ = commitment.CommitProgress{}
var _ = common.Hash{}
