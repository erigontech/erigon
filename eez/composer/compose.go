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
	"errors"
	"fmt"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// maxDeferrals is how many compositions a held transaction waits out for a
// predecessor sent elsewhere to land on L1, or for a window that leaves it
// room.
const maxDeferrals = 32

// errNoRoom defers a transaction only a lighter window leaves room for.
var errNoRoom = errors.New("no room behind the window")

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
	Header   func(parent *types.Header, headers rules.ChainHeaderReader) (*types.Header, error)
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
// their calls on a draft of the Sync block on parent, while the block and
// the gas of the posting settling the window from cursor hold them. A poison
// transaction, or one an empty block or a posting of the Sync block alone
// cannot hold, is evicted with the rest of its nonce chain; a transient
// failure or a full block or posting returns the rest to the pool. Up to
// maxDeferrals compositions, a transaction whose nonce is ahead of its
// sender's on L1 waits with the rest of its chain, out of the drain's count,
// and so does one only a lighter window has room for, in the count, while
// others behind it compose. Nil when none composes.
func (c *Composer) compose(ctx context.Context, cursor uint64, parent *types.Header, target uint64) *effects {
	rest := c.x.Pool.PopN(c.x.MaxTxs)
	if len(rest) == 0 {
		return nil
	}
	fx := &effects{}
	// unroomed counts those deferred after a simulation: they take from the
	// drain as the others do.
	popped, unroomed := len(rest), 0
	var evicted, deferred []*Held
	err := func() error {
		// Read ahead of the state views, which hold the chain's read
		// transactions.
		window, err := c.blocks(ctx, cursor, parent.Number.Uint64())
		if err != nil {
			return err
		}
		if fx.anchor, err = c.x.L1Hash(ctx, target-1); err != nil {
			return err
		}
		return c.x.L1.ViewState(ctx, fx.anchor, func(anchor *types.Header, reader state.StateReader) error {
			blockHash := func(n uint64) (common.Hash, error) { return c.x.L1Hash(ctx, n) }
			src := NewSource(c.x.L1Config, anchor, reader, blockHash, c.x.Manager, protocol.MainnetRollupID)
			return c.x.L2(ctx, parent, func(pre PreState) error {
				header, err := c.x.Header(parent, pre.Headers)
				if err != nil {
					return err
				}
				draft, err := NewDraft(c.x.L2Config, c.x.Engine, header, pre)
				if err != nil {
					return err
				}
				// The posting carries the window's blocks before any effect; the
				// bundle's block may have 1/1024 less gas than the anchor.
				block := anchor.GasLimit - anchor.GasLimit/1024
				sync := types.NewBlockWithHeader(header, nil)
				gas, err := windowBudget(c.cfg.Batcher, c.cfg.MaxGas, block, append(window, sync))
				if err != nil {
					return err
				}
				// The lightest window a posting of effects settles.
				alone, err := windowBudget(c.cfg.Batcher, c.cfg.MaxGas, block, []*types.Block{sync})
				if err != nil {
					return err
				}
				evict := func(h *Held, err error) {
					c.logger.Warn("[eez] held transaction evicted: it cannot compose", "event", "eez.composer.poison",
						"hash", h.Hash, "sender", h.Sender, "nonce", h.Nonce, "err", err)
					c.x.Pool.Evict(h.Sender, h.Direction, h.Nonce)
					evicted = append(evicted, h)
				}
				for {
					if len(rest) == 0 {
						// Those waiting on L1 or on the one ahead leave the drain room
						// for others.
						want := c.x.MaxTxs - (popped - len(deferred) + unroomed)
						if want <= 0 {
							break
						}
						if rest = c.x.Pool.PopN(want); len(rest) == 0 {
							break
						}
						popped += len(rest)
					}
					h := rest[0]
					if slices.ContainsFunc(evicted, func(e *Held) bool { return e.chainKey() == h.chainKey() && h.Nonce > e.Nonce }) {
						rest = rest[1:]
						continue
					}
					if slices.ContainsFunc(deferred, func(d *Held) bool { return d.chainKey() == h.chainKey() && h.Nonce > d.Nonce }) {
						// It waits on the one of its chain ahead.
						deferred = append(deferred, h)
						rest = rest[1:]
						continue
					}
					onChain, err := src.Nonce(h.Sender)
					if err != nil {
						return err
					}
					switch {
					case h.Nonce < onChain:
						c.logger.Info("[eez] held transaction dropped: its nonce is spent", "event", "eez.composer.stale",
							"hash", h.Hash, "sender", h.Sender, "nonce", h.Nonce, "onChain", onChain)
						c.x.Pool.Release([]*Held{h})
						rest = rest[1:]
						continue
					case h.Nonce > onChain:
						// Admitted on a pending nonce: a predecessor sent elsewhere
						// has yet to land.
						if h.Deferred++; h.Deferred >= maxDeferrals {
							evict(h, Poison(fmt.Errorf("nonce %d still ahead of the sender's %d on L1", h.Nonce, onChain)))
						} else {
							if h.Deferred == 1 {
								c.logger.Info("[eez] held transaction waits: its nonce is ahead of the sender's on L1", "event", "eez.composer.nonce_gap",
									"hash", h.Hash, "sender", h.Sender, "nonce", h.Nonce, "onChain", onChain)
							}
							deferred = append(deferred, h)
						}
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
					var cost l1Cost
					_, err = src.Run(tx, ds.Dispatch, func([]*Call) error {
						var err error
						if cost, err = inboundCost(ds.Delivered); err != nil {
							return err
						}
						cost.trigger = tx.GetGasLimit()
						switch {
						case gas.fits(cost):
							return nil
						case !alone.fits(cost):
							return Poison(fmt.Errorf("alone: %w", errOverBudget))
						case len(fx.held) > 0:
							return errOverBudget
						}
						if h.Deferred++; h.Deferred >= maxDeferrals {
							return Poison(fmt.Errorf("no window left room in %d compositions: %w", maxDeferrals, errOverBudget))
						}
						return errNoRoom
					})
					if errors.Is(err, errNoRoom) {
						deferred = append(deferred, h)
						unroomed++
						rest = rest[1:]
						continue
					}
					if errors.Is(err, ErrFull) && len(fx.held) == 0 {
						err = Poison(fmt.Errorf("alone: %w", err))
					}
					if err != nil {
						if !IsPoison(err) {
							return err
						}
						evict(h, err)
						rest = rest[1:]
						continue
					}
					gas.take(cost)
					h.Deferred = 0
					draft = ds.fork
					fx.held = append(fx.held, h)
					fx.deliveries = append(fx.deliveries, ds.Delivered...)
					fx.txs = draft.Txs()
					rest = rest[1:]
				}
				return nil
			})
		})
	}()
	switch {
	case err == nil:
	case full(err):
		c.logger.Info("[eez] Sync block full, the rest waits for the next slot", "event", "eez.composer.compose.full",
			"composed", len(fx.held), "waiting", len(rest), "reason", err)
	default:
		c.logger.Warn("[eez] composition stopped, the rest waits for the next slot", "event", "eez.composer.compose.stopped",
			"composed", len(fx.held), "waiting", len(rest), "err", err)
	}
	// Evict took the chains of the evicted out of the pool.
	rest = slices.DeleteFunc(append(deferred, rest...), func(h *Held) bool {
		return slices.ContainsFunc(evicted, func(e *Held) bool { return e.chainKey() == h.chainKey() && h.Nonce > e.Nonce })
	})
	c.x.Pool.PushFront(rest)
	if len(fx.held) == 0 {
		return nil
	}
	return fx
}

// full reports a Sync block or a posting without room for one more held
// transaction.
func full(err error) bool { return errors.Is(err, ErrFull) || errors.Is(err, errOverBudget) }

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
	c.ledger.Report(e.sync, common.Hash{}, watch, Failed, !owed(err))
	if unproven := (*missingWitness)(nil); errors.As(err, &unproven) {
		// Rebuilt, the block's witness is captured again.
		err := c.headTo(ctx, unproven.parent)
		c.logger.Error("[eez] block has no witness, head moved back to rebuild it", "event", "eez.composer.witness.missing",
			"number", unproven.number, "err", err)
	}
}

// nodeFailure is a failure to post a composition that lies with the node,
// which the same composition may not meet again.
type nodeFailure struct{ err error }

func (e *nodeFailure) Error() string { return e.err.Error() }
func (e *nodeFailure) Unwrap() error { return e.err }

func nodeFault(err error) error { return &nodeFailure{err} }

// owed reports a failure to post a composition that lies with what it
// composed, any but the node's: the same composition meets it again.
func owed(err error) bool {
	_, ok := errors.AsType[*nodeFailure](err)
	return !ok
}

// refused reports a prover's refusal of a window, which the same window
// meets again.
func refused(err error) bool {
	switch status.Code(err) {
	case codes.OK, codes.Unknown, codes.Canceled:
		return false
	}
	return !prove.Retryable(err)
}

func (c *Composer) settleEffects(ctx context.Context, e emission, sync *types.Header) error {
	block, err := c.block(ctx, e.sync)
	if err != nil {
		return nodeFault(err)
	}
	if block.Hash() != sync.Hash() || !sameTxs(block, e.fx.txs) {
		return nodeFault(fmt.Errorf("committed block %d is not the composed one", e.sync))
	}
	_, w, err := c.windowBlock(ctx, e.sync)
	if err != nil {
		return nodeFault(err)
	}
	candidates, err := Candidates(c.x.L2Config, c.x.Engine, block, &stateless.Witness{State: w.State, Codes: w.Codes, Headers: w.Headers}, PairEnds(block.Transactions()))
	if err != nil {
		return err
	}
	blocks, err := c.blocks(ctx, e.cursor, e.sync)
	if err != nil {
		return nodeFault(err)
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
		if !refused(err) {
			err = nodeFault(err)
		}
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
		return nodeFault(err)
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
