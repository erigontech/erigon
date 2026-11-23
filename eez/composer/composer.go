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
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1/post"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/prove"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/types"
)

// errOverBudget reports a batch whose posting exceeds the gas limit.
var errOverBudget = errors.New("posting over the gas limit")

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
	logger log.Logger

	ledger *Ledger[types.Transaction]
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
	ctx, cancel := context.WithCancel(context.Background())
	return &Composer{
		cfg: cfg, chain: chain, witnesses: witnesses, prover: prover, poster: poster, observer: observer,
		cursor: cursor, finalized: finalized, headTo: headTo, logger: logger,
		ledger: NewLedger[types.Transaction](), ctx: ctx, cancel: cancel,
	}
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
)

// emission is what a Sync slot posts once its block commits.
type emission struct {
	kind         emitKind
	cursor, sync uint64
	// target is the L1 block of the bundle; time pins a steady slot's.
	target post.Target
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
		return emission{emitChunk, cursor, sync, post.Target{Block: target}}
	case mode == sequencer.ModeEmpty:
		return emission{}
	case mode == sequencer.ModeCatchup:
		return emission{emitAnchor, cursor, sync, post.Target{}}
	}
	return emission{emitAnchor, cursor, sync, post.Target{Block: target, Time: ts}}
}

// ComposeSyncSlot always answers an empty Sync block: an anchor-only batch
// carries no actions, from which derivation would rebuild its transactions.
func (c *Composer) ComposeSyncSlot(ctx context.Context, parent *types.Header, ts, target uint64, mode sequencer.Mode) sequencer.Composed {
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
	down := make([]*types.Header, 0, boundary-cursor)
	for n := boundary; n > cursor; n-- {
		h, err := c.header(ctx, n)
		if err != nil {
			return err
		}
		down = append(down, h)
	}
	for _, b := range EmptyBoundaries(down, c.cfg.K) {
		err := c.post(ctx, cursor, down[boundary-b], target, time.Now().Add(c.cfg.Proof), 0)
		if !errors.Is(err, errOverBudget) {
			return err
		}
	}
	return fmt.Errorf("no empty grid block above cursor %d within the gas limit", cursor)
}

// post proves, signs and sends the anchor-only batch of cursor+1..terminal,
// proving with start and per as ProveBy takes them, and watches it.
func (c *Composer) post(ctx context.Context, cursor uint64, terminal *types.Header, target post.Target, start time.Time, per time.Duration) error {
	to := terminal.Number.Uint64()
	blocks := make([]*types.Block, 0, to-cursor)
	for n := cursor + 1; n <= to; n++ {
		b, err := c.block(ctx, n)
		if err != nil {
			return err
		}
		blocks = append(blocks, b)
	}
	if last := blocks[len(blocks)-1].Hash(); last != terminal.Hash() {
		return fmt.Errorf("block %d is %x, not the terminal %x", to, last, terminal.Hash())
	}
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
	anchor, err := c.header(ctx, cursor)
	if err != nil {
		return err
	}
	if anchor.Hash() != blocks[0].ParentHash() {
		return fmt.Errorf("block %d is not the parent %x of block %d", cursor, blocks[0].ParentHash(), cursor+1)
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
	tx, err := c.poster.Sign(ctx, input, c.cfg.MaxGas)
	if err != nil {
		return err
	}
	hash := tx.Hash()
	c.ledger.Begin(Posted[types.Transaction]{Height: to, Post: hash, Terminal: terminal})
	// A builder that hangs holds the bundle's verdict, not the chain.
	c.wg.Go(func() {
		sent, err := c.poster.Send(c.ctx, []types.Transaction{tx}, target)
		if err != nil {
			c.ledger.MarkFailed(to, hash, true)
			c.logger.Warn("[eez] batch not sent", "event", "eez.composer.batch.unsent", "to", to, "post", hash, "err", err)
			return
		}
		c.logger.Info("[eez] batch posted", "event", "eez.composer.batch.posted", "from", cursor+1, "to", to, "post", hash,
			"l1Block", sent.Block, "pinned", sent.Time != 0)
		verdict, err := c.observer.Watch(c.ctx, hash, batch, terminal.Hash(), sent.Block)
		if err != nil {
			return
		}
		switch verdict {
		case Settled:
			c.ledger.MarkSettled(to, hash)
		case SettledPartial:
			c.ledger.MarkSettledPartial(to, hash)
		default:
			c.ledger.MarkFailed(to, hash, false)
		}
		c.logger.Info("[eez] batch verdict", "event", "eez.composer.batch.verdict", "to", to, "post", hash, "verdict", verdict)
	})
	return nil
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
			"to", p.Height, "post", p.Post)
	}
	for _, p := range c.ledger.TakeFinalized(c.finalized()) {
		c.wg.Go(func() { c.audit(p) })
	}
}

// settle applies derivation's cursor to the ledger and returns it: an L1
// reorg that moved it back rolls out the settlements above it. A
// settlement derivation has yet to see fails once its posting leaves L1,
// whatever the cursor did meanwhile.
func (c *Composer) settle(ctx context.Context) uint64 {
	cursor := c.cursor()
	if cursor < c.last {
		c.ledger.TakeRolledOut(cursor)
	}
	c.last = cursor
	for _, p := range c.ledger.SettledAbove(cursor) {
		if r, err := c.observer.Node.Receipt(ctx, p.Post); err == nil && r == nil && c.ledger.Unsettle(p.Height, p.Post) {
			c.logger.Warn("[eez] settled batch's posting left L1", "event", "eez.composer.batch.rolled_out", "to", p.Height, "post", p.Post)
		}
	}
	c.ledger.ResolveBelowCursor(cursor)
	for {
		p, ok := c.ledger.PartialToDispose(cursor)
		if !ok {
			break
		}
		// An anchor-only batch holds no transactions to dispose of.
		c.ledger.ClaimPartial(p.Height, p.Post, nil)
	}
	return cursor
}

// audit checks that the posting of a finalized settlement is on L1: one
// missing means a reorg the follower did not see.
func (c *Composer) audit(p Posted[types.Transaction]) {
	r, err := c.observer.Node.Receipt(c.ctx, p.Post)
	switch {
	case err != nil:
		c.logger.Error("[eez] finality audit failed", "event", "eez.composer.finality_audit.check_failed", "to", p.Height, "post", p.Post, "err", err)
	case r == nil:
		c.logger.Error("[eez] finalized settlement's posting is not on L1", "event", "eez.composer.finality_audit.rolled_out", "to", p.Height, "post", p.Post)
	}
}
