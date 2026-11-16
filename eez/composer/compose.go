// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package composer

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/prove"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

// defaultMaxTxs bounds the held transactions one Sync block takes.
const defaultMaxTxs = 50

// CrossChain is what a composer composes cross-chain effects with.
type CrossChain struct {
	Pool *HeldPool
	// L1 reads L1's state at a block, L1Config is its chain configuration,
	// L1Hash returns its canonical block hashes, Manager is EEZ on it.
	L1       l1.States
	L1Config *chain.Config
	L1Hash   func(ctx context.Context, number uint64) (common.Hash, error)
	Manager  common.Address
	// L2 runs read on what the block after parent executes on; Header
	// returns the header the sequencer's Sync block on parent gets, roots
	// aside.
	L2       func(ctx context.Context, parent *types.Header, read func(PreState) error) error
	Header   func(parent *types.Header) (*types.Header, error)
	L2Config *chain.Config
	Engine   rules.Engine
	Sync     protocol.SyncTxContext
	// MaxTxs bounds the held transactions one Sync block takes.
	MaxTxs int
}

// CrossChain has steady slots carry the held transactions' effects.
func (c *Composer) CrossChain(x *CrossChain) {
	if x.MaxTxs <= 0 {
		x.MaxTxs = defaultMaxTxs
	}
	c.x = x
}

// effects is a Sync block's composition: the held transactions it settles,
// their deliveries and its transactions.
type effects struct {
	held       []*Held
	deliveries []*Delivery
	txs        []types.Transaction
	// anchor is the L1 block the transactions were simulated on, the parent
	// of the one the bundle aims at.
	anchor common.Hash
}

// compose simulates held transactions on L1 at block target-1 and delivers
// their calls on a draft of the Sync block on parent. A poison transaction
// is evicted with the rest of its nonce chain; a transient failure or a
// full block returns the rest to the pool. Nil when none composes.
func (c *Composer) compose(ctx context.Context, parent *types.Header, target uint64) *effects {
	held := c.x.Pool.PopN(c.x.MaxTxs)
	if len(held) == 0 {
		return nil
	}
	fx := &effects{}
	rest := held
	err := func() error {
		var err error
		if fx.anchor, err = c.x.L1Hash(ctx, target-1); err != nil {
			return err
		}
		return c.x.L1.ViewState(ctx, fx.anchor, func(anchor *types.Header, reader state.StateReader) error {
			blockHash := func(n uint64) (common.Hash, error) { return c.x.L1Hash(ctx, n) }
			src := NewSource(c.x.L1Config, anchor, reader, blockHash, c.x.Manager, protocol.MainnetRollupID)
			return c.x.L2(ctx, parent, func(pre PreState) error {
				header, err := c.x.Header(parent)
				if err != nil {
					return err
				}
				draft, err := NewDraft(c.x.L2Config, c.x.Engine, header, pre)
				if err != nil {
					return err
				}
				var evicted []*Held
				for len(rest) > 0 {
					h := rest[0]
					if slices.ContainsFunc(evicted, func(e *Held) bool { return e.chainKey() == h.chainKey() && h.Nonce > e.Nonce }) {
						rest = rest[1:]
						continue
					}
					tx, err := types.DecodeTransaction(h.Raw)
					if err != nil {
						return err
					}
					ds, err := NewDeliveries(c.x.Sync, draft)
					if err != nil {
						return err
					}
					if _, err := src.Run(tx, ds.Dispatch); err != nil {
						if !IsPoison(err) {
							return err
						}
						c.logger.Warn("[eez] held transaction evicted: it cannot compose", "event", "eez.composer.poison",
							"hash", h.Hash, "sender", h.Sender, "nonce", h.Nonce, "err", err)
						c.x.Pool.Evict(h.Sender, h.Direction, h.Nonce)
						evicted = append(evicted, h)
						rest = rest[1:]
						continue
					}
					draft = ds.fork
					fx.held = append(fx.held, h)
					fx.deliveries = append(fx.deliveries, ds.Delivered...)
					rest = rest[1:]
				}
				fx.txs = draft.Txs()
				return nil
			})
		})
	}()
	if err != nil {
		c.logger.Warn("[eez] composition stopped, the rest waits for the next slot", "event", "eez.composer.compose.stopped",
			"composed", len(fx.held), "waiting", len(rest), "err", err)
	}
	c.x.Pool.PushFront(rest)
	if len(fx.held) == 0 {
		return nil
	}
	return fx
}

// postEffects settles e, the composed Sync block sync: its candidates
// from its witness, the live batch over its window, proven in time for the
// slot; the bundle carries the posting, then the held transactions as the
// triggers that consume its inbound entries. A failure fails the batch, so
// recovery replaces the Sync block and requeues them.
func (c *Composer) postEffects(ctx context.Context, e emission, sync *types.Header) {
	err := c.settleEffects(ctx, e, sync)
	if err == nil {
		return
	}
	c.logger.Warn("[eez] composed Sync block not posted, it will be replaced", "event", "eez.composer.effects.failed",
		"sync", e.sync, "err", err)
	watch := c.ledger.Begin(Posted[*Held]{Height: e.sync, Terminal: sync, Txs: e.fx.held})
	c.ledger.Report(e.sync, common.Hash{}, watch, Failed, true)
}

func (c *Composer) settleEffects(ctx context.Context, e emission, sync *types.Header) error {
	block, err := c.block(ctx, e.sync)
	if err != nil {
		return err
	}
	if block.Hash() != sync.Hash() || !sameTxs(block, e.fx.txs) {
		return fmt.Errorf("committed block %d is not the composed one", e.sync)
	}
	_, w, err := c.windowBlock(ctx, e.sync)
	if err != nil {
		return err
	}
	candidates, err := Candidates(c.x.L2Config, c.x.Engine, block, &stateless.Witness{State: w.State, Codes: w.Codes, Headers: w.Headers}, PairEnds(block.Transactions()))
	if err != nil {
		return err
	}
	blocks, err := c.blocks(ctx, e.cursor, e.sync)
	if err != nil {
		return err
	}
	fx := Effects{}
	for _, d := range e.fx.deliveries {
		fx.Inbound = append(fx.Inbound, *d.Sidecar)
	}
	batch, err := c.cfg.Batcher.Batch(blocks[0].ParentHash(), blocks, fx, candidates, true)
	if err != nil {
		return err
	}
	gas, err := PostBatchGas(batch)
	if err != nil {
		return err
	}
	if gas > c.cfg.MaxGas {
		return fmt.Errorf("%w: %d over %d", errOverBudget, gas, c.cfg.MaxGas)
	}
	anchor, err := c.anchor(ctx, blocks[0])
	if err != nil {
		return err
	}
	start := time.Unix(int64(e.target.Time), 0).Add(-c.cfg.Slack - c.cfg.Proof)
	w2 := &prove.Window{
		RollupID: c.cfg.Batcher.RollupID, Batch: batch, Anchor: anchor, From: e.cursor + 1, To: e.sync, Block: c.windowBlock,
		L1Parent: e.fx.anchor, Time: sync.Time,
	}
	sig, err := c.prover.ProveBy(ctx, w2, start, c.cfg.Proof)
	if err != nil {
		return fmt.Errorf("prove %d..%d: %w", e.cursor+1, e.sync, err)
	}
	batch.Proofs = [][]byte{sig}
	input, err := protocol.EncodePostAndVerifyBatch(batch)
	if err != nil {
		return err
	}
	sctx, cancel := context.WithTimeout(ctx, c.cfg.L1Read)
	tx, err := c.poster.Sign(sctx, input, c.cfg.MaxGas)
	cancel()
	if err != nil {
		return err
	}
	bundle := []types.Transaction{tx}
	for _, h := range e.fx.held {
		trigger, err := types.DecodeTransaction(h.Raw)
		if err != nil {
			return err
		}
		bundle = append(bundle, trigger)
	}
	c.send(e.cursor, sync, batch, bundle, e.target, e.fx.held)
	return nil
}

// sameTxs reports whether block carries exactly txs.
func sameTxs(block *types.Block, txs []types.Transaction) bool {
	got := block.Transactions()
	if len(got) != len(txs) {
		return false
	}
	for i := range txs {
		if got[i].Hash() != txs[i].Hash() {
			return false
		}
	}
	return true
}
