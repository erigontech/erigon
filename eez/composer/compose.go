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
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/prove"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
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
	// Escrow returns the rollup's ether held by the manager at an L1 block,
	// what its outbound calls can carry.
	Escrow func(ctx context.Context, block common.Hash) (*uint256.Int, error)
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
	outbound   []protocol.ExecutionEntry
	deliveries []*Delivery
	// owners are the held transactions the deliveries are made for, one
	// per delivery.
	owners []*Held
	txs    []types.Transaction
	// anchor is the L1 block the transactions were simulated on, the parent
	// of the one the bundle aims at.
	anchor common.Hash
	// projected is the posting's gas as the drain projected it; targetGas
	// is the gas of the outbound calls the posting makes on L1.
	projected, targetGas uint64
}

// compose simulates held transactions on L1 state at block target-1, in the
// context of block target at ts, and composes their effects into a draft of
// the Sync block on parent, outbound before inbound, while the block and the
// gas of the posting settling the window from cursor hold them. A poison
// transaction, or one an empty block or a posting of the Sync block alone
// cannot hold, is evicted with the rest of its nonce chain; one whose nonce
// is spent is dropped; one whose value the manager cannot pay waits with the
// rest of its chain until its attempts run out. A transient failure or a
// full block or posting returns the rest to the pool. Up to maxDeferrals
// compositions, a transaction whose nonce is ahead of its sender's waits
// with the rest of its chain, out of the drain's count, and so does one only
// a lighter window has room for, in the count, while others behind it
// compose. An outbound transaction popped after a trigger ran waits for the
// next slot, in the count. Nil when none composes.
func (c *Composer) compose(ctx context.Context, cursor uint64, parent *types.Header, target, ts uint64) *effects {
	k := &composition{c: c, ctx: ctx, fx: &effects{}}
	queued := c.x.Pool.Len()
	k.pop(c.x.MaxTxs)
	c.logger.Info("[eez] held transactions drained", "event", "eez.composer.sync_slot.drain",
		"cursor", cursor, "parent", parent.Number.Uint64(), "pool", queued, "drained", len(k.rest), "max", c.x.MaxTxs)
	if len(k.rest) == 0 {
		return nil
	}
	fx := k.fx
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
			k.src = NewSource(c.x.L1Config, child(c.x.L1Config, anchor, ts), reader, blockHash, c.x.Manager, protocol.MainnetRollupID)
			tip := c.poster.Tip()
			if err := k.src.PostedBy(c.poster.From(), &tip, c.cfg.MaxGas); err != nil {
				return err
			}
			return c.x.L2(ctx, parent, func(pre PreState) error {
				header, err := c.x.Header(parent, pre.Headers)
				if err != nil {
					return err
				}
				if k.draft, err = NewDraft(c.x.L2Config, c.x.Engine, header, pre); err != nil {
					return err
				}
				k.draft.ctx = ctx
				if err := c.x.Pool.Prune(k.draft.Nonce, c.finalized()); err != nil {
					return err
				}
				// The posting carries the window's blocks before any effect; the
				// bundle's block may have 1/1024 less gas than the anchor.
				block := anchor.GasLimit - anchor.GasLimit/1024
				// The posting is signed with the ceiling as its gas limit.
				if c.cfg.MaxGas > block {
					return fmt.Errorf("posting gas limit %d over the L1 block's %d", c.cfg.MaxGas, block)
				}
				sync := types.NewBlockWithHeader(header, nil)
				if k.gas, err = windowBudget(c.cfg.Batcher, c.cfg.MaxGas, block, append(window, sync)); err != nil {
					return err
				}
				// The lightest window a posting of effects settles.
				if k.alone, err = windowBudget(c.cfg.Batcher, c.cfg.MaxGas, block, []*types.Block{sync}); err != nil {
					return err
				}
				return k.drain()
			})
		})
	}()
	waiting := len(k.deferred) + len(k.rest)
	switch {
	case err == nil:
	case full(err):
		event := "eez.composer.sync_slot.gas_budget_cut"
		if errors.Is(err, ErrFull) {
			event = "eez.composer.sync_slot.block_gas_cut"
		}
		c.logger.Info("[eez] Sync block full, the rest waits for the next slot", "event", event,
			"composed", len(fx.held), "waiting", waiting, "reason", err)
	default:
		c.logger.Warn("[eez] composition stopped, the rest waits for the next slot", "event", "eez.composer.phase2.transient",
			"composed", len(fx.held), "waiting", waiting, "err", err)
	}
	deferred := k.deferred
	if len(fx.held) > 0 && ctx.Err() != nil {
		// Committed past its deadline, the Sync block could not be proven in
		// time; each chain keeps its nonce order with the composed first.
		c.logger.Warn("[eez] composition past its deadline dropped, its transactions wait", "event", "eez.composer.compose.late",
			"composed", len(fx.held))
		deferred, fx.held = append(fx.held, deferred...), nil
	}
	c.x.Pool.PushFront(append(deferred, k.rest...))
	if len(fx.held) == 0 {
		return nil
	}
	fx.projected, fx.targetGas = k.gas.projected, k.gas.target
	c.logger.Info("[eez] Sync block composed", "event", "eez.composer.sync_slot.built", "held", len(fx.held), "outbound", len(fx.outbound),
		"inbound", len(fx.deliveries), "txs", len(fx.txs), "waiting", waiting, "postingGas", fx.projected)
	return fx
}

// child returns the context of the block on parent at time as far as it is
// known before the block is built: its number, time, base fee and excess
// blob gas; the rest is parent's.
func child(config *chain.Config, parent *types.Header, time uint64) *types.Header {
	h := types.CopyHeader(parent)
	h.ParentHash = parent.Hash()
	h.Number.AddUint64(&parent.Number, 1)
	h.Time, h.GasUsed = time, 0
	if config.IsLondon(h.Number.Uint64()) {
		h.BaseFee = misc.CalcBaseFee(config, parent)
	}
	if config.IsCancun(time) {
		excess := misc.CalcExcessBlobGas(config, parent, time)
		h.ExcessBlobGas, h.BlobGasUsed = &excess, new(uint64)
	}
	return h
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
	var culprit *Held
	if r, ok := errors.AsType[*refusal](err); ok {
		culprit = r.culprit
	}
	watch := c.ledger.Begin(Posted[*Held]{Height: e.sync, Terminal: sync, Txs: e.fx.held, Culprit: culprit})
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

// refusal is the provers' refusal of a composition for culprit, one of its
// held transactions.
type refusal struct {
	culprit *Held
	err     error
}

func (e *refusal) Error() string { return e.err.Error() }
func (e *refusal) Unwrap() error { return e.err }

// errUnresolved reports a refused effect no held transaction made.
var errUnresolved = errors.New("no held transaction made the effect")

// culprit returns the held transaction r names in the request proving
// batch, whose terminal block is terminal: an outbound one by its index and
// hash in terminal, an inbound one by the index and hash of its entry in
// batch, anchor first, then the outbound entries.
func (fx *effects) culprit(r prove.Refused, terminal *types.Block, batch *protocol.ProofSystemBatch) (*Held, error) {
	if r.Outbound {
		txs := terminal.Transactions()
		if int(r.Index) >= len(txs) {
			return nil, fmt.Errorf("transaction %d of %d", r.Index, len(txs))
		}
		if h := txs[r.Index].Hash(); h != r.Hash {
			return nil, fmt.Errorf("transaction %d is %x", r.Index, h)
		}
		for _, h := range fx.held {
			if h.Direction == Outbound && h.Hash == r.Hash {
				return h, nil
			}
		}
		return nil, errUnresolved
	}
	if int(r.Index) >= len(batch.Entries) {
		return nil, fmt.Errorf("entry %d of %d", r.Index, len(batch.Entries))
	}
	h, err := protocol.ExecutionEntryHash(&batch.Entries[r.Index])
	switch {
	case err != nil:
		return nil, err
	case h != r.Hash:
		return nil, fmt.Errorf("entry %d is %x", r.Index, h)
	}
	if i := int(r.Index) - 1 - len(fx.outbound); i >= 0 && i < len(fx.owners) {
		return fx.owners[i], nil
	}
	return nil, errUnresolved
}

// refused returns err, the provers' refusal of e's window naming r, as a
// refusal for the held transaction r names, or as it is when r does not
// match the request.
func (c *Composer) refused(err error, r prove.Refused, e emission, terminal *types.Block, batch *protocol.ProofSystemBatch) error {
	culprit, cerr := e.fx.culprit(r, terminal, batch)
	switch {
	case errors.Is(cerr, errUnresolved):
		c.logger.Error("[eez] refused effect matches no held transaction, all are charged", "event", "eez.composer.prover.actionable_unresolved",
			"sync", e.sync, "refused", r)
		return err
	case cerr != nil:
		c.logger.Error("[eez] refused effect does not match the request, all are charged", "event", "eez.composer.prover.actionable_invalid",
			"sync", e.sync, "refused", r, "err", cerr)
		return err
	}
	return &refusal{culprit: culprit, err: err}
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
	if errors.Is(err, stateless.ErrWitnessHeaders) || errors.Is(err, stateless.ErrIncompleteWitness) {
		// The witness the node captured for the block falls short.
		err = nodeFault(err)
	}
	if err != nil {
		return err
	}
	blocks, err := c.blocks(ctx, e.cursor, e.sync)
	if err != nil {
		return nodeFault(err)
	}
	if last := blocks[len(blocks)-1]; last.Hash() != sync.Hash() {
		return nodeFault(fmt.Errorf("block %d is %x, no longer the committed %x", e.sync, last.Hash(), sync.Hash()))
	}
	fx := Effects{Outbound: e.fx.outbound}
	for _, d := range e.fx.deliveries {
		fx.Inbound = append(fx.Inbound, *d.Sidecar)
	}
	batch, err := c.cfg.Batcher.Batch(blocks[0].ParentHash(), blocks, fx, candidates, true)
	if err != nil {
		return err
	}
	gas, err := postingGas(batch, e.fx.targetGas)
	if err != nil {
		return err
	}
	if gas > c.cfg.MaxGas {
		return fmt.Errorf("%w: %d over %d", errOverBudget, gas, c.cfg.MaxGas)
	}
	anchor, err := c.anchor(ctx, blocks[0])
	if err != nil {
		return nodeFault(err)
	}
	start := time.Unix(int64(e.target.Time), 0).Add(-c.cfg.Slack - c.cfg.Proof)
	w2 := &prove.Window{
		RollupID: c.cfg.Batcher.RollupID, Batch: batch, Anchor: anchor, From: e.cursor + 1, To: e.sync, Block: c.windowBlock,
		L1Parent: e.fx.anchor, Time: sync.Time,
	}
	atts, err := c.prover.ProveBy(ctx, w2, start, c.cfg.Proof)
	if err != nil {
		if !prove.Charged(err) {
			err = nodeFault(err)
		} else if r, ok := prove.Refusal(err); ok {
			err = c.refused(err, r, e, block, batch)
		}
		return fmt.Errorf("prove %d..%d: %w", e.cursor+1, e.sync, err)
	}
	attach(batch, atts)
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
	for _, h := range bundled(e.fx.held) {
		trigger, err := types.DecodeTransaction(h.Raw)
		if err != nil {
			return err
		}
		bundle = append(bundle, trigger)
	}
	c.send(emitEffects, e.cursor, sync, batch, bundle, e.target, e.fx.held)
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
