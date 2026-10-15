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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// Deriver rebuilds the L2 chain from the batches L1 settled.
type Deriver struct {
	chain  Chain
	config *chain.Config
	l2     *config.L2Config
	sync   protocol.SyncTxContext
	index  Index
	dir    string
	logger log.Logger
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

// Cursor is the L2 block the latest derived settlement ended at.
func (d *Deriver) Cursor() uint64 { return d.index.Cursor() }

// Reconcile derives the blocks of scanned batches in L1 order, skipping
// those it derived already. The batches derived before an error stay
// derived.
func (d *Deriver) Reconcile(ctx context.Context, batches []l1.ScannedBatch) (err error) {
	var appended bool
	defer func() {
		if appended {
			err = errors.Join(err, d.saveCheckpoint(ctx))
		}
	}()
	for i := range batches {
		b := &batches[i]
		if len(b.Settlement.Applied) == 0 || d.index.Contains(b.TxHash) {
			continue
		}
		p, err := DecodePayload(b.Batch, d.l2.RollupID)
		if err != nil {
			return fmt.Errorf("batch %x in L1 block %d: %w", b.TxHash, b.L1Block, err)
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
	return nil
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
		return common.Hash{}, d.diverged(b, r.To, "%v", err)
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
		for i := 0; i <= last; i++ {
			txs := p.BlockTxs(i)
			if i == last {
				txs = sb.Txs
			}
			built, err := d.extend(ctx, b, parent, DeriveInputs(d.l2, parent, p.Span, i), txs)
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

// extend builds the block of txs on parent; a block the local chain refuses
// is a divergence.
func (d *Deriver) extend(ctx context.Context, b *l1.ScannedBatch, parent *types.Header, in HeaderInputs, encoded [][]byte) (*types.BlockWithReceipts, error) {
	txs, err := DecodeTxs(encoded)
	if err != nil {
		return nil, d.diverged(b, in.Number, "%v", err)
	}
	built, err := d.chain.Extend(ctx, parent, in, txs, common.Hash{}, common.Hash{})
	if errors.Is(err, ErrRejected) {
		return nil, d.diverged(b, in.Number, "%v", err)
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
		return d.diverged(b, r.To, "%v", err)
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

func (d *Deriver) saveCheckpoint(ctx context.Context) error {
	if d.dir == "" {
		return nil
	}
	last, _ := d.index.Last()
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
	err := &DivergenceError{L1Block: b.L1Block, L1BlockHash: b.L1BlockHash, TxHash: b.TxHash, L2Block: l2Block, Reason: fmt.Sprintf(format, args...)}
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
