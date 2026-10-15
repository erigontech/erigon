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

package deriver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// ErrFinalizedReorged reports an L1 reorg that drops settlements the
// follower finalized.
var ErrFinalizedReorged = errors.New("L1 reorg drops a finalized settlement")

// Deriver rebuilds the L2 chain from the batches L1 settled.
type Deriver struct {
	chain  Chain
	config *chain.Config
	l2     *config.L2Config
	sync   protocol.SyncTxContext
	index  Index
	dir    string
	logger log.Logger
	// safe is the latest settled block, finalized the latest one settled
	// in a finalized L1 block; finalized never exceeds safe.
	safe, finalized uint64
	// seed is the settlement derivation started after; the batches of its
	// L1 block up to it are behind the cursor.
	seed *Record
}

// New returns a deriver extending chain, an EEZ chain configured by cfg,
// that keeps its checkpoint in dir unless dir is empty.
func New(chain Chain, cfg *chain.Config, dir string, logger log.Logger) (*Deriver, error) {
	l2, ok := cfg.L2.(*config.L2Config)
	if !ok {
		return nil, fmt.Errorf("chain %s is not an EEZ chain", cfg.ChainName)
	}
	return &Deriver{
		chain:  chain,
		config: cfg,
		l2:     l2,
		sync:   protocol.SyncTxContext{ChainID: cfg.ChainID.Uint64(), RollupID: l2.RollupID},
		dir:    dir,
		logger: logger,
	}, nil
}

// Seed starts derivation after settlement r, whose block the local chain
// holds, before any batch is reconciled.
func (d *Deriver) Seed(r Record) {
	d.index.Append(r)
	d.safe, d.seed = r.LastL2Block, &r
}

// Last returns the latest derived settlement.
func (d *Deriver) Last() (Record, bool) { return d.index.Last() }

// Cursor is the L2 block the latest derived settlement ended at.
func (d *Deriver) Cursor() uint64 { return d.index.Cursor() }

// Safe is the safe L2 block.
func (d *Deriver) Safe() uint64 { return d.safe }

// Finalized is the finalized L2 block.
func (d *Deriver) Finalized() uint64 { return d.finalized }

// Reconcile derives the blocks of scanned batches in L1 order, skipping
// those it derived already, and makes the block the last one settled safe.
// The batches derived before an error stay derived. Two batches of one L1
// block can rewrite the same block, so batches come in whole L1 blocks.
func (d *Deriver) Reconcile(ctx context.Context, batches []l1.ScannedBatch) (err error) {
	var appended bool
	defer func() {
		if appended {
			err = errors.Join(err, d.saveCheckpoint(ctx))
		}
	}()
	for i := range batches {
		b := &batches[i]
		if d.seed != nil && b.L1Block >= d.seed.L1Block {
			// Batches come in L1 order, the seed's own last of those skipped.
			if b.L1Block == d.seed.L1Block {
				if b.TxHash == d.seed.TxHash {
					d.seed = nil
				}
				continue
			}
			d.seed = nil
		}
		if len(b.Settlement.Applied) == 0 || d.index.Contains(b.TxHash) {
			continue
		}
		p, err := DecodePayload(b.Batch, d.l2.RollupID)
		if err != nil {
			return d.diverged(b, d.index.Cursor()+1, "DA payload: %w", err)
		}
		if p == nil {
			continue
		}
		r, err := d.reconcileBatch(ctx, b, p)
		if err != nil {
			return err
		}
		d.index.Append(r)
		appended = true
	}
	if cursor := d.index.Cursor(); cursor > d.safe {
		d.safe = cursor
		return d.forkChoice(ctx)
	}
	return nil
}

// OnFinalized finalizes the block settled by the L1 blocks up to
// l1Finalized, bounded by the safe block.
func (d *Deriver) OnFinalized(ctx context.Context, l1Finalized uint64) error {
	n, ok := d.index.SettledAt(l1Finalized)
	if n = min(n, d.safe); !ok || n <= d.finalized {
		return nil
	}
	d.finalized = n
	d.logger.Debug("[eez] finalized", "l1Finalized", l1Finalized, "l2Finalized", n)
	return d.forkChoice(ctx)
}

// OnReorg drops the settlements of L1 blocks above ancestor and moves the
// head and safe block back to the block the remaining ones settled.
func (d *Deriver) OnReorg(ctx context.Context, ancestor uint64) error {
	if d.index.RetreatOnReorg(ancestor) == 0 {
		return nil
	}
	cursor := d.index.Cursor()
	if cursor < d.finalized {
		return fmt.Errorf("%w: L1 ancestor %d leaves L2 block %d settled, %d is finalized", ErrFinalizedReorged, ancestor, cursor, d.finalized)
	}
	head, err := d.canonicalHash(ctx, cursor)
	if err != nil {
		return err
	}
	d.safe = min(d.safe, cursor)
	d.logger.Info("[eez] L1 reorg", "ancestor", ancestor, "cursor", cursor)
	if err := d.forkChoiceTo(ctx, head); err != nil {
		return err
	}
	return d.saveCheckpoint(ctx)
}

// forkChoice applies the safe and finalized blocks.
func (d *Deriver) forkChoice(ctx context.Context) error {
	head, err := d.chain.Head(ctx)
	if err != nil {
		return err
	}
	return d.forkChoiceTo(ctx, head.Hash())
}

func (d *Deriver) forkChoiceTo(ctx context.Context, head common.Hash) error {
	safe, finalized, err := d.anchors(ctx)
	if err != nil {
		return err
	}
	return d.chain.ForkChoice(ctx, head, safe, finalized)
}

// anchors returns the hashes of the safe and finalized blocks.
func (d *Deriver) anchors(ctx context.Context) (safe, finalized common.Hash, err error) {
	if safe, err = d.canonicalHash(ctx, d.safe); err != nil {
		return
	}
	finalized, err = d.canonicalHash(ctx, d.finalized)
	return
}

func (d *Deriver) reconcileBatch(ctx context.Context, b *l1.ScannedBatch, p *Payload) (Record, error) {
	s := &b.Settlement
	r, err := SelectRange(ctx, d.chain, d.index.Cursor(), p.Blocks(), s)
	if err != nil {
		return Record{}, err
	}
	anchor, err := d.canonicalHash(ctx, r.Anchor)
	if err != nil {
		return Record{}, err
	}
	if !CursorAccepted(anchor, s) {
		return Record{}, d.diverged(b, r.From, "local block %d is %x, neither the batch's entry state %x nor its settled state %x",
			r.Anchor, anchor, s.EntryState, s.FinalState)
	}
	replayed, err := d.reconcileBlocks(ctx, b, p, r)
	if err != nil {
		return Record{}, err
	}
	end, err := d.settledEnd(ctx, b, r, replayed)
	if err != nil {
		return Record{}, err
	}
	d.logger.Debug("[eez] batch derived", "l1Block", b.L1Block, "tx", b.TxHash, "from", r.From, "to", r.To, "settled", end, "resumed", r.Resumed)
	return Record{L1Block: b.L1Block, L1BlockHash: b.L1BlockHash, TxHash: b.TxHash, LastL2Block: end}, nil
}

// reconcileBlocks rebuilds the range r from the payload and returns the
// hash of the Sync block it built, zero when it built none.
func (d *Deriver) reconcileBlocks(ctx context.Context, b *l1.ScannedBatch, p *Payload, r Range) (common.Hash, error) {
	// User transactions cannot come from the system address, so its nonce
	// before the range is its nonce before the Sync block.
	nonce, err := d.chain.Nonce(ctx, r.From-1, protocol.SystemAddress)
	if err != nil {
		return common.Hash{}, err
	}
	sb, err := BuildSyncBlock(p, &b.Settlement, d.sync, nonce)
	if errors.Is(err, ErrSyncBlock) {
		return common.Hash{}, d.diverged(b, r.To, "%w", err)
	}
	if err != nil {
		return common.Hash{}, err
	}
	parent, err := d.header(ctx, r.From-1)
	if err != nil {
		return common.Hash{}, err
	}
	last := int(p.Blocks()) - 1
	var syncBlock *types.BlockWithReceipts
	if r.Resumed {
		existing, err := d.chain.BlockByNumber(ctx, r.From)
		if err != nil {
			return common.Hash{}, err
		}
		if existing == nil || existing.ParentHash() != parent.Hash() {
			return common.Hash{}, d.diverged(b, r.From, "resumed batch's Sync block is missing or off the local chain")
		}
		existingTxs, err := encodeTxs(existing.Transactions())
		if err != nil {
			return common.Hash{}, err
		}
		txs, replay := ResumeTxs(existing.Hash(), b.Settlement.FinalState, existingTxs, sb.Txs)
		if replay {
			if syncBlock, err = d.extend(ctx, b, parent, DeriveInputs(d.l2, parent, p.Span, last), txs); err != nil {
				return common.Hash{}, err
			}
		}
	} else {
		derived, err := d.rangeDerived(ctx, r, parent, b.Settlement.FinalState)
		if err != nil {
			return common.Hash{}, err
		}
		// Once a block is rebuilt, every block after it is rebuilt on it.
		var replaying bool
		for i := 0; i <= last && !derived; i++ {
			txs := p.BlockTxs(i)
			if i == last {
				txs = sb.Txs
			}
			in := DeriveInputs(d.l2, parent, p.Span, i)
			if !replaying {
				local, err := d.localBlock(ctx, in, txs)
				if err != nil {
					return common.Hash{}, err
				}
				if local != nil {
					parent = local
					continue
				}
				replaying = true
			}
			built, err := d.extend(ctx, b, parent, in, txs)
			if err != nil {
				return common.Hash{}, err
			}
			parent = built.Block.Header()
			if i == last {
				syncBlock = built
			}
		}
	}
	if err := d.checkOutbound(ctx, b, r, sb.Outbound, syncBlock); err != nil {
		return common.Hash{}, err
	}
	if syncBlock == nil {
		return common.Hash{}, nil
	}
	return syncBlock.Block.Hash(), nil
}

// rangeDerived reports whether the local chain holds the range r settled at
// finalState. The terminal block's hash commits to every block below it, so
// its match proves the range; the range's first block must descend from
// parent for the terminal to belong to it.
func (d *Deriver) rangeDerived(ctx context.Context, r Range, parent *types.Header, finalState common.Hash) (bool, error) {
	first, err := d.chain.HeaderByNumber(ctx, r.From)
	if err != nil || first == nil || first.ParentHash != parent.Hash() {
		return false, err
	}
	terminal, ok, err := d.chain.CanonicalHash(ctx, r.To)
	return ok && terminal == finalState, err
}

// localBlock returns the header of the local block at in.Number when it is
// the block of inputs in and transactions txs.
func (d *Deriver) localBlock(ctx context.Context, in HeaderInputs, txs [][]byte) (*types.Header, error) {
	b, err := d.chain.BlockByNumber(ctx, in.Number)
	if err != nil || b == nil || !in.Matches(b.Header()) || len(b.Transactions()) != len(txs) {
		return nil, err
	}
	local, err := encodeTxs(b.Transactions())
	if err != nil {
		return nil, err
	}
	if !slices.EqualFunc(local, txs, bytes.Equal) {
		return nil, nil
	}
	return b.Header(), nil
}

// extend builds the block of txs on parent; a block the local chain refuses
// is a divergence. Rewriting the safe block or one below it moves safe
// to parent, and the finalized block is never rewritten.
func (d *Deriver) extend(ctx context.Context, b *l1.ScannedBatch, parent *types.Header, in HeaderInputs, encoded [][]byte) (*types.BlockWithReceipts, error) {
	txs, err := DecodeTxs(encoded)
	if err != nil {
		return nil, d.diverged(b, in.Number, "%w", err)
	}
	if in.Number <= d.safe {
		if in.Number <= d.finalized {
			return nil, d.diverged(b, in.Number, "rewrites a block at or below finalized block %d", d.finalized)
		}
		d.safe = in.Number - 1
	}
	safe, finalized, err := d.anchors(ctx)
	if err != nil {
		return nil, err
	}
	built, err := d.chain.Extend(ctx, parent, in, txs, safe, finalized)
	if errors.Is(err, ErrRejected) {
		return nil, d.diverged(b, in.Number, "%w", err)
	}
	return built, err
}

// checkOutbound verifies the outbound entries L1 paid against the calls the
// Sync block made.
func (d *Deriver) checkOutbound(ctx context.Context, b *l1.ScannedBatch, r Range, outbound []protocol.ExecutionEntry, built *types.BlockWithReceipts) error {
	if len(outbound) == 0 {
		return nil
	}
	var receipts types.Receipts
	if built != nil {
		receipts = built.Receipts
	} else {
		block, err := d.chain.BlockByNumber(ctx, r.To)
		if err != nil {
			return err
		}
		if block == nil {
			return d.diverged(b, r.To, "Sync block missing")
		}
		if receipts, err = d.chain.Receipts(ctx, block); err != nil {
			return err
		}
	}
	var logs []*types.Log
	for _, rc := range receipts {
		logs = append(logs, rc.Logs...)
	}
	if err := VerifyOutbound(outbound, logs, d.l2.RollupID); err != nil {
		return d.diverged(b, r.To, "%w", err)
	}
	return nil
}

// settledEnd checks the batch's state commitments against the local chain
// and returns the block L1's settlement ended at: the claimed range can be
// settled only in part. replayed is the Sync block just built, if any.
func (d *Deriver) settledEnd(ctx context.Context, b *l1.ScannedBatch, r Range, replayed common.Hash) (uint64, error) {
	s := &b.Settlement
	// A resumed batch starts at the anchor itself, which the cursor guard
	// checked.
	if !r.Resumed {
		pre, err := d.canonicalHash(ctx, r.From-1)
		if err != nil {
			return 0, err
		}
		if pre != s.EntryState {
			return 0, d.diverged(b, r.From-1, "local block is %x, the batch's entry state is %x", pre, s.EntryState)
		}
	}
	n, ok, err := d.chain.CanonicalNumber(ctx, s.FinalState)
	if err != nil {
		return 0, err
	}
	if ok && n >= r.Anchor && n <= r.To {
		return n, nil
	}
	local := replayed
	if local == (common.Hash{}) {
		h, ok, err := d.chain.CanonicalHash(ctx, r.To)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, d.diverged(b, r.To, "the batch's range ends above the local chain")
		}
		local = h
	}
	if local != s.FinalState {
		return 0, d.diverged(b, r.To, "local block is %x, L1 settled %x", local, s.FinalState)
	}
	return r.To, nil
}

// saveCheckpoint records the latest settlement, removing the checkpoint when
// there is none.
func (d *Deriver) saveCheckpoint(ctx context.Context) error {
	if d.dir == "" {
		return nil
	}
	last, ok := d.index.Last()
	if !ok {
		return RemoveCheckpoint(d.dir)
	}
	hash, err := d.canonicalHash(ctx, last.LastL2Block)
	if err != nil {
		return err
	}
	c := Checkpoint{L1Block: last.L1Block, L1BlockHash: last.L1BlockHash, TxHash: last.TxHash, L2Cursor: last.LastL2Block, L2BlockHash: hash}
	return c.Save(d.dir)
}

func (d *Deriver) header(ctx context.Context, number uint64) (*types.Header, error) {
	h, err := d.chain.HeaderByNumber(ctx, number)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, fmt.Errorf("local L2 block %d missing", number)
	}
	return h, nil
}

func (d *Deriver) canonicalHash(ctx context.Context, number uint64) (common.Hash, error) {
	h, ok, err := d.chain.CanonicalHash(ctx, number)
	if err != nil {
		return common.Hash{}, err
	}
	if !ok {
		return common.Hash{}, fmt.Errorf("local L2 block %d missing", number)
	}
	return h, nil
}

func (d *Deriver) diverged(b *l1.ScannedBatch, l2Block uint64, format string, args ...any) error {
	cause := fmt.Errorf(format, args...)
	err := &DivergenceError{L1Block: b.L1Block, L1BlockHash: b.L1BlockHash, TxHash: b.TxHash, L2Block: l2Block, Reason: cause.Error(), cause: cause}
	d.logger.Error("[eez] local chain diverged from L1", "err", err)
	return err
}

func encodeTxs(txs types.Transactions) ([][]byte, error) {
	out := make([][]byte, len(txs))
	for i, tx := range txs {
		var buf bytes.Buffer
		if err := tx.MarshalBinary(&buf); err != nil {
			return nil, err
		}
		out[i] = buf.Bytes()
	}
	return out, nil
}
