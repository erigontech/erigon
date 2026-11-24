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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1/post"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/prove"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
)

// errOverBudget reports a batch whose posting exceeds the gas limit.
var errOverBudget = errors.New("posting over the gas limit")

// DefaultL1Read is the default of Config.L1Read.
const DefaultL1Read = 2 * time.Second

// Chain is the local chain a composer settles; a height without a canonical
// block reads as nil.
type Chain interface {
	BlockByNumber(ctx context.Context, number uint64) (*types.Block, error)
	HeaderByNumber(ctx context.Context, number uint64) (*types.Header, error)
}

// Witnesses holds the witnesses of the sequencer's blocks.
type Witnesses interface {
	Get(ctx context.Context, number uint64, hash common.Hash) (json.RawMessage, error)
}

// Prover attests settlement windows.
type Prover interface {
	ProveBy(ctx context.Context, w *prove.Window, start time.Time, per time.Duration) ([]byte, error)
}

// Poster signs postings and sends them to L1.
type Poster interface {
	Sign(ctx context.Context, input []byte, gas uint64) (types.Transaction, error)
	Send(ctx context.Context, txs []types.Transaction, target post.Target) (post.Sent, error)
}

// Config sets a composer.
type Config struct {
	Batcher *Batcher
	// K is the number of blocks between Sync blocks.
	K uint64
	// MaxBlocks bounds the blocks of a batch, a multiple of K.
	MaxBlocks uint64
	// MaxGas is the gas limit of a posting.
	MaxGas uint64
	// Proof is the time a proof takes; Slack the time a bundle needs to
	// reach the builder before its block.
	Proof, Slack time.Duration
	// L1Read bounds an L1 read on the sequencer's goroutine, so that a node
	// that stops answering does not hold block production; zero is
	// DefaultL1Read.
	L1Read time.Duration
}

// Composer settles the sequencer's blocks on L1 with anchor-only batches:
// one batch in flight at a time, from derivation's cursor to an empty Sync
// block, or to an empty grid block when the backlog exceeds MaxBlocks.
type Composer struct {
	cfg       Config
	chain     Chain
	witnesses Witnesses
	prover    Prover
	poster    Poster
	observer  *Observer
	// cursor is derivation's last settled block; finalized its last final one.
	cursor, finalized func() uint64
	// headTo moves the head back to a block.
	headTo func(ctx context.Context, hash common.Hash) error
	// pool holds the cross-chain transactions; substitute replaces a failed
	// Sync block with an empty one.
	pool       *HeldPool
	substitute func(ctx context.Context, failed *types.Header) (*types.Header, error)
	// x, when set, composes cross-chain effects in steady slots.
	x      *CrossChain
	logger log.Logger

	ledger *Ledger[*Held]
	// pending is the posting ComposeSyncSlot decided for SyncCommitted.
	pending emission
	// last is the cursor RecoverFailed saw last.
	last uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var _ sequencer.SyncSlotComposer = (*Composer)(nil)

// New returns a composer of chain, whose sends and observers run until
// Close. headTo rewinds the chain below a block with no witness, which no
// batch can prove.
func New(cfg Config, chain Chain, witnesses Witnesses, prover Prover, poster Poster, observer *Observer, cursor, finalized func() uint64, headTo func(context.Context, common.Hash) error, logger log.Logger) *Composer {
	if cfg.L1Read == 0 {
		cfg.L1Read = DefaultL1Read
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Composer{
		cfg: cfg, chain: chain, witnesses: witnesses, prover: prover, poster: poster, observer: observer,
		cursor: cursor, finalized: finalized, headTo: headTo, logger: logger,
		ledger: NewLedger[*Held](), ctx: ctx, cancel: cancel,
	}
}

// Recover sets what a failed batch carrying held transactions is recovered
// with: the pool that holds them, and the sequencer's substitution of its
// Sync block.
func (c *Composer) Recover(pool *HeldPool, substitute func(ctx context.Context, failed *types.Header) (*types.Header, error)) {
	c.pool, c.substitute = pool, substitute
}

// Close stops the observers.
func (c *Composer) Close() {
	c.cancel()
	c.wg.Wait()
}

type emitKind uint8

const (
	emitNone emitKind = iota
	// emitAnchor settles cursor+1..sync.
	emitAnchor
	// emitChunk settles cursor+1 to an empty grid block below sync.
	emitChunk
	// emitEffects settles cursor+1..sync, a Sync block with effects.
	emitEffects
)

// emission is what a Sync slot posts once its block commits.
type emission struct {
	kind         emitKind
	cursor, sync uint64
	// target is the L1 block of the bundle; time pins a steady slot's.
	target post.Target
	// fx, with emitEffects, is the Sync block's composition.
	fx *effects
}

// decide returns the posting of the Sync slot at sync on cursor: none while
// a batch is in flight; a chunk when the backlog exceeds MaxBlocks, which an
// empty slot posts too; else an anchor, unpinned in a catch-up slot. target
// is the L1 block the sequencer aims the slot at, zero for any.
func (c *Composer) decide(cursor, sync, ts, target uint64, mode sequencer.Mode, inFlight bool) emission {
	switch {
	case inFlight:
		return emission{}
	case sync-cursor > c.cfg.MaxBlocks:
		return emission{kind: emitChunk, cursor: cursor, sync: sync, target: post.Target{Block: target}}
	case mode == sequencer.ModeEmpty:
		return emission{}
	case mode == sequencer.ModeCatchup:
		return emission{kind: emitAnchor, cursor: cursor, sync: sync}
	}
	return emission{kind: emitAnchor, cursor: cursor, sync: sync, target: post.Target{Block: target, Time: ts}}
}

// ComposeSyncSlot answers the Sync block's transactions in a steady slot
// that composes held transactions, else an empty Sync block: an
// anchor-only batch carries no actions, from which derivation would rebuild
// its transactions.
func (c *Composer) ComposeSyncSlot(ctx context.Context, parent *types.Header, ts, target uint64, mode sequencer.Mode) sequencer.Composed {
	c.abandon()
	cursor := c.settle(ctx)
	sync := parent.Number.Uint64() + 1
	at, inFlight := c.ledger.Blocking(cursor)
	if inFlight {
		lvl := log.LvlInfo
		if sync-cursor > 2*c.cfg.MaxBlocks {
			lvl = log.LvlWarn
		}
		c.logger.Log(lvl, "[eez] batch in flight, Sync slot posts nothing", "event", "eez.composer.settlement.backlog",
			"sync", sync, "cursor", cursor, "inFlight", at)
	}
	c.pending = c.decide(cursor, sync, ts, target, mode, inFlight)
	if e := &c.pending; c.x != nil && e.kind == emitAnchor && e.target.Time != 0 {
		// Its L1 reads end when the slot's proof must start.
		cctx, cancel := context.WithDeadline(ctx, time.Unix(int64(ts), 0).Add(-c.cfg.Slack-c.cfg.Proof))
		fx := c.compose(cctx, parent, target)
		cancel()
		if fx != nil {
			e.kind, e.fx = emitEffects, fx
			return sequencer.Composed{Txs: fx.txs}
		}
	}
	return sequencer.Composed{Empty: true}
}

// SyncCommitted posts what ComposeSyncSlot decided for sync.
func (c *Composer) SyncCommitted(ctx context.Context, sync *types.Header) {
	e := c.pending
	c.pending = emission{}
	if e.kind == emitNone {
		return
	}
	if n := sync.Number.Uint64(); n != e.sync {
		c.logger.Warn("[eez] committed Sync block is not the composed one", "event", "eez.composer.sync.mismatch", "number", n, "composed", e.sync)
		c.pending = e
		c.abandon()
		return
	}
	if e.kind == emitEffects {
		c.postEffects(ctx, e, sync)
		return
	}
	var unproven *missingWitness
	if e.kind == emitAnchor {
		start, per := time.Now().Add(c.cfg.Proof), time.Duration(0)
		if e.target.Time != 0 {
			start, per = time.Unix(int64(e.target.Time), 0).Add(-c.cfg.Slack-c.cfg.Proof), c.cfg.Proof
		}
		err := c.post(ctx, e.cursor, sync, e.target, start, per)
		if err == nil {
			return
		}
		errors.As(err, &unproven)
		c.logger.Warn("[eez] anchor batch not posted, posting a chunk", "event", "eez.composer.anchor.failed",
			"cursor", e.cursor, "sync", e.sync, "err", err)
		e.cursor, e.target = c.cursor(), post.Target{}
	}
	if err := c.chunk(ctx, e.cursor, e.sync, e.target); err != nil {
		if unproven == nil {
			errors.As(err, &unproven)
		}
		c.logger.Warn("[eez] historical chunk not posted", "event", "eez.composer.chunk.failed",
			"cursor", e.cursor, "sync", e.sync, "err", err)
	}
	if unproven != nil {
		// Rebuilt, the block's witness is captured again.
		err := c.headTo(ctx, unproven.parent)
		c.logger.Error("[eez] block has no witness, head moved back to rebuild it", "event", "eez.composer.witness.missing",
			"number", unproven.number, "err", err)
	}
}

// abandon returns the transactions of a composition whose Sync block never
// committed to the pool.
func (c *Composer) abandon() {
	e := c.pending
	c.pending = emission{}
	if e.fx == nil {
		return
	}
	c.logger.Warn("[eez] composed Sync block not committed, its transactions wait", "event", "eez.composer.sync.uncommitted",
		"sync", e.sync, "held", len(e.fx.held))
	c.x.Pool.PushFront(e.fx.held)
}

// missingWitness reports a block whose witness the store lacks.
type missingWitness struct {
	number uint64
	parent common.Hash
}

func (e *missingWitness) Error() string { return fmt.Sprintf("no witness of block %d", e.number) }

// chunk posts the largest batch from cursor to an empty grid block below
// sync within MaxBlocks and the gas limit.
func (c *Composer) chunk(ctx context.Context, cursor, sync uint64, target post.Target) error {
	boundary, err := ChunkBoundary(cursor, sync, c.cfg.K, c.cfg.MaxBlocks)
	if err != nil {
		return err
	}
	blocks, err := c.blocks(ctx, cursor, boundary)
	if err != nil {
		return err
	}
	down := make([]*types.Header, len(blocks))
	for i, b := range blocks {
		down[len(blocks)-1-i] = b.Header()
	}
	ends := EmptyBoundaries(down, c.cfg.K)
	// A chunk's gas grows with its terminal: the highest within the limit is
	// found without pricing every one above it.
	lo, hi := 0, len(ends)
	for lo < hi {
		m := (lo + hi) / 2
		fits, err := c.fits(blocks[:ends[m]-cursor])
		if err != nil {
			return err
		}
		if fits {
			hi = m
		} else {
			lo = m + 1
		}
	}
	if lo == len(ends) {
		return fmt.Errorf("no empty grid block above cursor %d within the gas limit", cursor)
	}
	return c.postBlocks(ctx, cursor, blocks[:ends[lo]-cursor], target, time.Now().Add(c.cfg.Proof), 0)
}

// fits reports whether the anchor-only batch of blocks posts within the gas
// limit.
func (c *Composer) fits(blocks []*types.Block) (bool, error) {
	batch, err := c.cfg.Batcher.AnchorBatch(blocks[0].ParentHash(), blocks)
	if err != nil {
		return false, err
	}
	gas, err := PostBatchGas(batch)
	return gas <= c.cfg.MaxGas, err
}

// blocks returns the blocks cursor+1..to.
func (c *Composer) blocks(ctx context.Context, cursor, to uint64) ([]*types.Block, error) {
	blocks := make([]*types.Block, 0, to-cursor)
	for n := cursor + 1; n <= to; n++ {
		b, err := c.block(ctx, n)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, nil
}

// post proves, signs and sends the anchor-only batch of cursor+1..terminal,
// proving with start and per as ProveBy takes them, and watches it.
func (c *Composer) post(ctx context.Context, cursor uint64, terminal *types.Header, target post.Target, start time.Time, per time.Duration) error {
	blocks, err := c.blocks(ctx, cursor, terminal.Number.Uint64())
	if err != nil {
		return err
	}
	if last := blocks[len(blocks)-1].Hash(); last != terminal.Hash() {
		return fmt.Errorf("block %d is %x, not the terminal %x", terminal.Number.Uint64(), last, terminal.Hash())
	}
	return c.postBlocks(ctx, cursor, blocks, target, start, per)
}

// postBlocks proves, signs and sends the anchor-only batch of blocks, the
// chain from the cursor's child, and watches it.
func (c *Composer) postBlocks(ctx context.Context, cursor uint64, blocks []*types.Block, target post.Target, start time.Time, per time.Duration) error {
	terminal := blocks[len(blocks)-1].Header()
	to := terminal.Number.Uint64()
	// The window's blocks are canonical, so the first one's parent is the cursor.
	batch, err := c.cfg.Batcher.AnchorBatch(blocks[0].ParentHash(), blocks)
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
	w := &prove.Window{RollupID: c.cfg.Batcher.RollupID, Batch: batch, Anchor: anchor, From: cursor + 1, To: to, Block: c.windowBlock}
	sig, err := c.prover.ProveBy(ctx, w, start, per)
	if err != nil {
		return fmt.Errorf("prove %d..%d: %w", cursor+1, to, err)
	}
	batch.Proofs = [][]byte{sig}
	input, err := protocol.EncodePostAndVerifyBatch(batch)
	if err != nil {
		return err
	}
	if floor := FloorGas(input); floor > c.cfg.MaxGas {
		return fmt.Errorf("%w: calldata floor %d over %d", errOverBudget, floor, c.cfg.MaxGas)
	}
	sctx, cancel := context.WithTimeout(ctx, c.cfg.L1Read)
	tx, err := c.poster.Sign(sctx, input, c.cfg.MaxGas)
	cancel()
	if err != nil {
		return err
	}
	c.send(cursor, terminal, batch, []types.Transaction{tx}, target, nil)
	return nil
}

// send records the posting bundle[0] of batch, cursor+1..terminal, carrying
// held, then sends bundle for target and watches it, off the sequencer's
// goroutine: a builder that hangs holds the verdict, not the chain.
func (c *Composer) send(cursor uint64, terminal *types.Header, batch *protocol.ProofSystemBatch, bundle []types.Transaction, target post.Target, held []*Held) {
	to, hash := terminal.Number.Uint64(), bundle[0].Hash()
	judged := *batch
	judged.CallData, judged.Proofs = nil, nil
	watch := c.ledger.Begin(Posted[*Held]{Height: to, Post: hash, Terminal: terminal, Batch: &judged, Txs: held})
	c.wg.Go(func() {
		sent, err := c.poster.Send(c.ctx, bundle, target)
		switch {
		case errors.Is(err, post.ErrUnsure):
			// Failing it would have the next posting take the following nonce
			// from the same cursor while this one may still land.
			c.logger.Warn("[eez] batch may have been sent, watching it", "event", "eez.composer.batch.unsure", "to", to, "post", hash,
				"l1Block", sent.Block, "err", err)
		case err != nil:
			// A bundle the builder refused may be refused for what it holds.
			c.ledger.Report(to, hash, watch, Failed, !errors.Is(err, post.ErrBundleRefused))
			c.logger.Warn("[eez] batch not sent", "event", "eez.composer.batch.unsent", "to", to, "post", hash, "err", err)
			return
		default:
			c.logger.Info("[eez] batch posted", "event", "eez.composer.batch.posted", "from", cursor+1, "to", to, "post", hash,
				"l1Block", sent.Block, "pinned", sent.Time != 0, "triggers", len(held))
		}
		c.observe(to, hash, watch, batch, terminal, sent)
	})
}

// anchor returns the header of first's parent, the block a window
// starting at first settles from.
func (c *Composer) anchor(ctx context.Context, first *types.Block) (*types.Header, error) {
	n := first.NumberU64() - 1
	h, err := c.header(ctx, n)
	if err != nil {
		return nil, err
	}
	if h.Hash() != first.ParentHash() {
		return nil, fmt.Errorf("block %d is not the parent %x of block %d", n, first.ParentHash(), n+1)
	}
	return h, nil
}

// observe records the verdict of watch, a watch of hash, the posting of
// batch, sent as sent, whose terminal block is terminal.
func (c *Composer) observe(to uint64, hash common.Hash, watch uint64, batch *protocol.ProofSystemBatch, terminal *types.Header, sent post.Sent) {
	verdict, err := c.observer.Watch(c.ctx, hash, batch, terminal.Hash(), sent)
	if err != nil {
		return
	}
	c.ledger.Report(to, hash, watch, verdict, verdict == Failed && c.missed(sent))
	c.logger.Info("[eez] batch verdict", "event", "eez.composer.batch.verdict", "to", to, "post", hash, "verdict", verdict)
}

// block returns block n, failing when it is not canonical: the follower may
// have rewound the chain since the slot was decided.
func (c *Composer) block(ctx context.Context, n uint64) (*types.Block, error) {
	b, err := c.chain.BlockByNumber(ctx, n)
	if err == nil && b == nil {
		err = fmt.Errorf("no canonical block %d", n)
	}
	return b, err
}

// header returns the header of block n, failing as block does.
func (c *Composer) header(ctx context.Context, n uint64) (*types.Header, error) {
	h, err := c.chain.HeaderByNumber(ctx, n)
	if err == nil && h == nil {
		err = fmt.Errorf("no canonical block %d", n)
	}
	return h, err
}

// missed reports a posting pinned to an L1 slot no block took: the block
// of its number has another time, or reads none.
func (c *Composer) missed(sent post.Sent) bool {
	if sent.Time == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.L1Read)
	defer cancel()
	h, err := c.observer.Node.HeaderByNumber(ctx, sent.Block)
	return err != nil || h == nil || h.Time != sent.Time
}

// windowBlock returns block n with its witness from the store.
func (c *Composer) windowBlock(ctx context.Context, n uint64) (*types.Block, *prove.Witness, error) {
	b, err := c.block(ctx, n)
	if err != nil {
		return nil, nil, err
	}
	raw, err := c.witnesses.Get(ctx, n, b.Hash())
	if err != nil {
		return nil, nil, err
	}
	if raw == nil {
		return nil, nil, &missingWitness{number: n, parent: b.ParentHash()}
	}
	var w struct {
		State, Codes, Keys, Headers []hexutil.Bytes
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, nil, fmt.Errorf("witness of block %d: %w", n, err)
	}
	return b, &prove.Witness{State: plain(w.State), Codes: plain(w.Codes), Keys: plain(w.Keys), Headers: plain(w.Headers)}, nil
}

func plain(in []hexutil.Bytes) [][]byte {
	out := make([][]byte, len(in))
	for i, b := range in {
		out[i] = b
	}
	return out
}

// RecoverFailed applies derivation's cursor and finality to the ledger. An
// anchor-only batch that failed is dropped: its blocks stay, and the next
// slot posts them again.
func (c *Composer) RecoverFailed(ctx context.Context) {
	c.settle(ctx)
	if p, ok := c.ledger.TakeFailed(c.last); ok {
		c.logger.Warn("[eez] batch failed on L1, the next Sync slot posts again", "event", "eez.composer.batch.failed",
			"to", p.Height, "post", p.Post, "txs", len(p.Txs))
		if len(p.Txs) > 0 {
			c.recoverHeld(ctx, p)
		}
	}
	for _, p := range c.ledger.TakeFinalized(c.finalized()) {
		// Derivation passing a posting that never landed confirms the height,
		// not the posting.
		if p.Landed {
			c.wg.Go(func() { c.audit(p) })
		}
	}
}

// maxAttempts is how many failed bundles a held transaction is carried in.
const maxAttempts = 3

// recoverHeld undoes p, a failed batch carrying held transactions: its Sync
// block is replaced by an empty one, the triggers that landed on L1 are let
// go, the rest go back to the front of the pool.
func (c *Composer) recoverHeld(ctx context.Context, p Posted[*Held]) {
	landed := map[common.Hash]bool{}
	rctx, cancel := context.WithTimeout(ctx, c.cfg.L1Read)
	defer cancel()
	for _, tx := range p.Txs {
		r, err := c.observer.Node.Receipt(rctx, tx.Hash)
		if err != nil {
			c.ledger.ReinsertFailed(p)
			return
		}
		landed[tx.Hash] = r != nil
	}
	_, err := c.substitute(ctx, p.Terminal)
	switch {
	case errors.Is(err, follower.ErrBelowSafe):
		// L1 settled the height after all: the verdict was stale.
		c.pool.Release(p.Txs)
		return
	case err != nil && !errors.Is(err, follower.ErrNotCanonical) && !errors.Is(err, follower.ErrStaleParent):
		c.ledger.ReinsertFailed(p)
		return
	}
	var keep, spent []*Held
	for _, tx := range p.Txs {
		if landed[tx.Hash] {
			spent = append(spent, tx)
			continue
		}
		keep = append(keep, tx)
	}
	c.pool.Release(spent)
	if !p.SlotSkipped {
		keep = c.charge(keep)
	}
	c.pool.PushFront(keep)
}

// charge counts a failed bundle against each of charged, by default all of
// txs, and returns those of txs left: one at maxAttempts is evicted with the
// rest of its nonce chain.
func (c *Composer) charge(txs []*Held, charged ...*Held) []*Held {
	type root struct {
		sender    common.Address
		direction Direction
		nonce     uint64
	}
	if len(charged) == 0 {
		charged = txs
	}
	var roots []root
	for _, tx := range charged {
		tx.Attempts++
		if tx.Attempts >= maxAttempts {
			roots = append(roots, root{tx.Sender, tx.Direction, tx.Nonce})
		}
	}
	keep := txs[:0]
	for _, tx := range txs {
		if !slices.ContainsFunc(roots, func(r root) bool {
			return r.sender == tx.Sender && r.direction == tx.Direction && tx.Nonce >= r.nonce
		}) {
			keep = append(keep, tx)
		}
	}
	for _, r := range roots {
		evicted := c.pool.Evict(r.sender, r.direction, r.nonce)
		c.logger.Error("[eez] held transaction evicted after failed bundles", "event", "eez.composer.recovery.evicted",
			"sender", r.sender, "direction", r.direction, "nonce", r.nonce, "queued", len(evicted))
	}
	return keep
}

// settle applies derivation's cursor to the ledger and returns it. A
// settlement whose posting left L1, rolled out by an L1 reorg that moved the
// cursor back or dropped before derivation saw it, is watched again: back
// in the node's pool, the posting may land again on the nonce the next
// posting would take. One derivation has yet to see fails once L1 includes
// its posting again without the settlement.
func (c *Composer) settle(ctx context.Context) uint64 {
	cursor := c.cursor()
	if cursor < c.last {
		for _, p := range c.ledger.RolledOut(cursor) {
			c.rewatch(p)
		}
	}
	c.last = cursor
	rctx, cancel := context.WithTimeout(ctx, c.cfg.L1Read)
	defer cancel()
	for _, p := range c.ledger.SettledAbove(cursor) {
		r, err := c.observer.Node.Receipt(rctx, p.Post)
		switch {
		case err != nil:
		case r == nil:
			c.rewatch(p)
		default:
			if v, ok := c.observer.verdict(rctx, r, p.Post, p.Batch, p.Terminal.Hash()); ok && v == Failed && c.ledger.Unsettle(p.Height, p.Post) {
				c.logger.Warn("[eez] settled batch's posting is on L1 again without the settlement", "event", "eez.composer.batch.unsettled",
					"to", p.Height, "post", p.Post)
			}
		}
	}
	c.release(c.ledger.ResolveBelowCursor(cursor))
	for {
		p, ok := c.ledger.PartialToDispose(cursor)
		if !ok {
			break
		}
		if len(p.Txs) == 0 {
			c.ledger.ClaimPartial(p.Height, p.Post, nil)
			continue
		}
		if !c.disposePartial(ctx, p) {
			break
		}
	}
	return cursor
}

// rewatch has p, a settlement whose posting left L1, pending again under a
// new watch, as a posting the node may hold in its pool from L1's head on.
// The watch waits for that head: its grace counts from it.
func (c *Composer) rewatch(p Posted[*Held]) {
	watch, ok := c.ledger.Repend(p.Height, p.Post)
	if !ok {
		return
	}
	c.logger.Warn("[eez] settled batch's posting left L1, watching it again", "event", "eez.composer.batch.rolled_out", "to", p.Height, "post", p.Post)
	c.wg.Go(func() {
		if from, ok := c.head(); ok {
			c.observe(p.Height, p.Post, watch, p.Batch, p.Terminal, post.Sent{Block: from, Pooled: true})
		}
	})
}

// head reads L1's head, each read within L1Read, until one answers or the
// composer closes.
func (c *Composer) head() (uint64, bool) {
	for {
		ctx, cancel := context.WithTimeout(c.ctx, c.cfg.L1Read)
		h, err := c.observer.Node.Head(ctx)
		cancel()
		if err == nil {
			return h.Number, true
		}
		select {
		case <-c.ctx.Done():
			return 0, false
		case <-time.After(c.observer.Every):
		}
	}
}

// release ends the reservations of txs, which a batch settled.
func (c *Composer) release(txs []*Held) {
	if c.pool != nil && len(txs) > 0 {
		c.pool.Release(txs)
	}
}

// audit checks that the posting of a finalized settlement is on L1: one
// missing means a reorg the follower did not see.
func (c *Composer) audit(p Posted[*Held]) {
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.L1Read)
	defer cancel()
	r, err := c.observer.Node.Receipt(ctx, p.Post)
	switch {
	case err != nil:
		c.logger.Error("[eez] finality audit failed", "event", "eez.composer.finality_audit.check_failed", "to", p.Height, "post", p.Post, "err", err)
	case r == nil:
		c.logger.Error("[eez] finalized settlement's posting is not on L1", "event", "eez.composer.finality_audit.rolled_out", "to", p.Height, "post", p.Post)
	}
}

// partialCulprit returns the transaction where L1 stopped a partial batch of
// txs, given those spent: its first one when unspent, as one behind a spent
// trigger may only be stuck behind it. Nil names none.
func partialCulprit(txs []*Held, spent map[common.Hash]bool) *Held {
	if len(txs) == 0 || spent[txs[0].Hash] {
		return nil
	}
	return txs[0]
}

// disposePartial disposes of p, a batch L1 kept part of, by what ran: the
// triggers L1 has are spent and let go, kept with p for a later reorg; the
// rest return to the pool, the one where L1 stopped charged. False leaves p
// for the next call, a read having failed.
func (c *Composer) disposePartial(ctx context.Context, p Posted[*Held]) bool {
	rctx, cancel := context.WithTimeout(ctx, c.cfg.L1Read)
	defer cancel()
	spent := map[common.Hash]bool{}
	for _, tx := range p.Txs {
		r, err := c.observer.Node.Receipt(rctx, tx.Hash)
		if err != nil {
			return false
		}
		spent[tx.Hash] = r != nil
	}
	var ran, rest []*Held
	for _, tx := range p.Txs {
		if spent[tx.Hash] {
			ran = append(ran, tx)
		} else {
			rest = append(rest, tx)
		}
	}
	// A reorg during the reads may have taken the batch and requeued it.
	if !c.ledger.ClaimPartial(p.Height, p.Post, ran) {
		return true
	}
	culprit, charged := partialCulprit(p.Txs, spent), common.Hash{}
	if culprit != nil {
		charged = culprit.Hash
	}
	c.logger.Info("[eez] partial settlement disposed of", "event", "eez.composer.recovery.settled_partial",
		"to", p.Height, "post", p.Post, "txs", len(p.Txs), "spent", len(ran), "charged", charged)
	c.release(ran)
	if culprit != nil {
		rest = c.charge(rest, culprit)
	}
	if len(rest) > 0 {
		c.pool.PushFront(rest)
	}
	return true
}
