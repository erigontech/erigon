package stagedsync

import (
	"context"
	"fmt"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// coinbaseFoldCheck reconstructs the coinbase balance in the apply loop, absolute
// and base-anchored: at each block it reads the block-start coinbase from the
// shared domain (base = the value apply committed for block N-1), then for each tx
// accumulates the traveling per-tx tips and asserts base + Σtips exactly equals the
// coinbase balance write calcFees materialized. This is the value apply must
// SYNTHESIZE once the fold moves off exec (step 5b), so proving it here de-risks the
// apply half without changing behaviour. baseReader reads sd.mem live; onTx runs
// before the tx's own writes are applied, so the first tx of a block reads N-1's
// coinbase. A direct EVM coinbase write is a checkpoint reset (base + Σtips no longer
// holds), so checking stops for the rest of that block — mirrors coinbaseVecCrossCheck.
type coinbaseFoldCheck struct {
	baseReader state.StateReader
	block      uint64
	base       uint256.Int
	started    bool
	stopped    bool
	sumTips    uint256.Int
}

func (c *coinbaseFoldCheck) onTx(r *txResult) {
	if r.coinbase.IsNil() || c.baseReader == nil {
		return
	}
	if !c.started || r.blockNum != c.block {
		c.block = r.blockNum
		c.started = true
		c.stopped = false
		c.sumTips = uint256.Int{}
		c.base = uint256.Int{}
		if acc, err := c.baseReader.ReadAccountData(r.coinbase); err == nil && acc != nil {
			c.base = acc.Balance
		}
	}
	if c.stopped {
		return
	}
	if r.coinbaseDirect {
		c.stopped = true
		return
	}
	c.sumTips.Add(&c.sumTips, &r.feeTipped)
	w, ok := coinbaseBalanceWrite(r.writes, r.coinbase)
	if !ok {
		return
	}
	var want uint256.Int
	want.Add(&c.base, &c.sumTips)
	if want != w {
		panic(fmt.Sprintf("coinbaseApplyCheck mismatch block=%d txNum=%d want=%s got=%s base=%s sumTips=%s",
			r.blockNum, r.txNum, want.String(), w.String(), c.base.String(), c.sumTips.String()))
	}
}

// coinbaseBalanceWrite returns the coinbase's balance write in this tx's write set,
// if present. WriteSetView exposes only iterators, so scan the balance writes.
func coinbaseBalanceWrite(writes state.WriteSetView, coinbase accounts.Address) (uint256.Int, bool) {
	if writes == nil {
		return uint256.Int{}, false
	}
	for addr, bw := range writes.Balances() {
		if addr == coinbase {
			return bw.Val, true
		}
	}
	return uint256.Int{}, false
}

// ResultConsumer is one logical stage of the apply pipeline. The pipeline drives
// consumers in registration order per streamed result and owns their lifecycle;
// a consumer implements only its own per-result work (plus optional Open/Close)
// and declares whether its output feeds the next block's read base.
//
// FeedsReadBase marks the consumer whose applied frontier gates the next block's
// read base (the domain writer): once its writes reach the shared domain the
// block's prev-block map can be dropped. Every other consumer is a pure sink that
// never gates a read. Recorded here as the seam later steps (backpressure, a
// concurrent log writer) wire; the pipeline itself drives all consumers alike.
type ResultConsumer interface {
	Name() string
	FeedsReadBase() bool
	Open(ctx context.Context) error
	OnTxResult(ctx context.Context, r *txResult) error
	OnBlockEnd(ctx context.Context, r *blockResult) error
	Close(cause error) error
}

// consumerPipeline drives an ordered set of ResultConsumers within the apply
// goroutine (shape A: logical split, single writer of the shared domain). It owns
// delivery order and lifecycle; per-consumer per-result work is the consumer's.
type consumerPipeline struct {
	consumers []ResultConsumer
}

func (p *consumerPipeline) open(ctx context.Context) error {
	for _, c := range p.consumers {
		if err := c.Open(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *consumerPipeline) onTx(ctx context.Context, r *txResult) error {
	for _, c := range p.consumers {
		if err := c.OnTxResult(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (p *consumerPipeline) onBlock(ctx context.Context, r *blockResult) error {
	for _, c := range p.consumers {
		if err := c.OnBlockEnd(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// closeAll closes consumers in reverse registration order and returns the first
// close error, mirroring the registry's commit-before-apply reverse-close rule.
func (p *consumerPipeline) closeAll(cause error) error {
	var first error
	for _, c := range slices.Backward(p.consumers) {
		if err := c.Close(cause); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// domainWriter applies each tx's state writes (accounts, storage, code) to the
// shared domain. It feeds the next block's read base: N+1 resolves these once the
// block's prev-block map is dropped. blockCache=nil writes the domain directly;
// the versionMap composes each tx's base.
type domainWriter struct {
	pe   *parallelExecutor
	rwTx kv.TemporalRwTx
}

func (w *domainWriter) Name() string               { return "domain" }
func (w *domainWriter) FeedsReadBase() bool        { return true }
func (w *domainWriter) Open(context.Context) error { return nil }
func (w *domainWriter) OnTxResult(ctx context.Context, r *txResult) error {
	return w.pe.rs.ApplyStateWrites(ctx, w.rwTx, r.blockNum, r.txNum, r.writes, nil, r.rules, nil)
}
func (w *domainWriter) OnBlockEnd(context.Context, *blockResult) error { return nil }
func (w *domainWriter) Close(error) error                              { return nil }

// logWriter applies each tx's per-tx indexes (receipts, logs, trace indices) to
// the shared domain. It is a pure sink: logs are never read back during
// execution, so it never gates a read. Finalize results carry no indexes.
type logWriter struct {
	pe   *parallelExecutor
	rwTx kv.TemporalRwTx
}

func (w *logWriter) Name() string               { return "log" }
func (w *logWriter) FeedsReadBase() bool        { return false }
func (w *logWriter) Open(context.Context) error { return nil }
func (w *logWriter) OnTxResult(_ context.Context, r *txResult) error {
	if r.isFinalize {
		return nil
	}
	return w.pe.rs.ApplyTxIndexes(w.rwTx, r.txNum, r.receipt, r.cumulativeBlobGasUsed, r.logs, r.traceFroms, r.traceTos)
}
func (w *logWriter) OnBlockEnd(context.Context, *blockResult) error { return nil }
func (w *logWriter) Close(error) error                              { return nil }
