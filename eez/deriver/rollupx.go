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
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/types"
)

// rollupXPayload is the DA payload of a rollupX batch, its cross-chain
// transactions bound to its settlement.
type rollupXPayload struct {
	Payload
	effects *protocol.RollupXEffects
	// actions reports cross-chain transactions, which a catch-up candidate
	// cannot carry.
	actions bool
}

// decodeRollupX reads a rollupX batch's payload from its blobs, which carry
// all of it, and binds its cross-chain transactions to the batch's entries.
// A blob the source does not serve is retried.
func (d *Deriver) decodeRollupX(ctx context.Context, b *l1.ScannedBatch) (*rollupXPayload, error) {
	at := d.index.Cursor() + 1
	n := len(b.BlobHashes)
	if len(b.Batch.CallData) != 0 || n == 0 || len(b.Batch.BlobIndices) != n {
		return nil, d.diverged(b, at, "DA payload: %d blobs selected of %d, %d callData bytes; rollupX takes all blobs and no callData", len(b.Batch.BlobIndices), n, len(b.Batch.CallData))
	}
	for i, idx := range b.Batch.BlobIndices {
		if !idx.IsUint64() || idx.Uint64() != uint64(i) {
			return nil, d.diverged(b, at, "DA payload: blob index %d selects blob %v", i, idx)
		}
	}
	if d.blobs == nil {
		return nil, fmt.Errorf("%w: batch %x carries its payload in blobs and no beacon node is set", l1.ErrBlobUnavailable, b.TxHash)
	}
	blobs, err := d.blobs.Blobs(ctx, &l1.Header{Number: b.L1Block, Hash: b.L1BlockHash, Time: b.L1Time}, b.BlobHashes)
	if err != nil {
		return nil, fmt.Errorf("batch %x: %w", b.TxHash, err)
	}
	native, err := da.DecodeNative(blobs, d.l2.RollupID)
	if err != nil {
		return nil, d.diverged(b, at, "DA payload: %w", err)
	}
	span, err := da.DecodeRollupXSpan(native.Operations)
	if err != nil {
		return nil, d.diverged(b, at, "DA payload: %w", err)
	}
	for i, tx := range span.Transactions {
		if err := checkPureTx(tx); err != nil {
			return nil, d.diverged(b, at, "DA payload: transaction %d: %w", i, err)
		}
	}
	effects, err := protocol.NewRollupXEffects(native.Txs, d.l2.RollupID, d.config.ChainID.Uint64())
	if errors.Is(err, protocol.ErrUnsupportedSemantics) {
		return nil, fmt.Errorf("batch %x: %w", b.TxHash, err)
	}
	if err != nil {
		return nil, d.diverged(b, at, "%w", err)
	}
	if err := effects.Bind(b.Batch); err != nil {
		return nil, d.diverged(b, at, "%w", err)
	}
	return &rollupXPayload{Payload: Payload{Span: span}, effects: effects, actions: len(native.Txs) > 0}, nil
}

// checkPureTx checks a pure L2 transaction of a rollupX span: a canonical
// EIP-2718 encoding of a legacy, access-list, dynamic-fee or set-code
// transaction.
func checkPureTx(enc []byte) error {
	tx, err := types.UnmarshalTransactionFromBinary(enc, false)
	if err != nil {
		return err
	}
	switch tx.Type() {
	case types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType, types.SetCodeTxType:
	default:
		return fmt.Errorf("type %d", tx.Type())
	}
	var buf bytes.Buffer
	if err := tx.MarshalBinary(&buf); err != nil {
		return err
	}
	if !bytes.Equal(buf.Bytes(), enc) {
		return errors.New("non-canonical encoding")
	}
	return nil
}

// schedule returns the chain's rollupX schedule.
func (d *Deriver) schedule(ctx context.Context) (*Schedule, error) {
	if d.sched != nil {
		return d.sched, nil
	}
	genesis, err := d.header(ctx, 0)
	if err != nil {
		return nil, err
	}
	if d.sched, err = NewSchedule(d.l2.RollupX, genesis); err != nil {
		return nil, err
	}
	return d.sched, nil
}

// seedFor returns the prevRandao of block n. At or below the latest
// settlement's endpoint it is the local block's. Above it, a live anchor
// seeds the mixHash of its L1 block; a catch-up anchor keeps the seed of its
// endpoint; before any settlement the genesis's L1 reference seeds it.
func (d *Deriver) seedFor(ctx context.Context, n uint64) (common.Hash, error) {
	r, ok := d.index.Last()
	if !ok {
		return d.l2.RollupX.L1Reference.PrevRandao, nil
	}
	if n <= r.LastL2Block {
		h, err := d.header(ctx, n)
		if err != nil {
			return common.Hash{}, err
		}
		return h.MixDigest, nil
	}
	s, err := d.schedule(ctx)
	if err != nil {
		return common.Hash{}, err
	}
	t, err := s.Timestamp(r.LastL2Block)
	if err != nil {
		return common.Hash{}, err
	}
	kind, err := s.AnchorKind(r.LastL2Block, t, r.L1Time)
	if err != nil {
		return common.Hash{}, fmt.Errorf("settlement of block %d in L1 block %d: %w", r.LastL2Block, r.L1Block, err)
	}
	if kind == LiveAnchor {
		return r.L1MixDigest, nil
	}
	return d.seedFor(ctx, r.LastL2Block)
}

// producing returns the effects s applied as (skip, take) over the batch's
// effects, the nOut outbound ones first.
func producing(s *protocol.Settlement, nOut int) (skip, take int) {
	for _, a := range s.Applied {
		if a.Role == protocol.RoleAnchor {
			continue
		}
		pos := a.Ordinal
		if a.Role == protocol.RoleInbound {
			pos += nOut
		}
		if take == 0 {
			skip = pos
		}
		take++
	}
	return skip, take
}

// reconcileRollupX derives a rollupX batch: its range, which must be a
// candidate of whole Sync intervals from a Sync block, and its Sync block of
// pure transactions, then the outbound pairs and deliveries L1 applied.
func (d *Deriver) reconcileRollupX(ctx context.Context, b *l1.ScannedBatch, p *rollupXPayload) (Record, error) {
	s := &b.Settlement
	k := p.Blocks()
	r, err := SelectRange(ctx, d.chain, d.index.Cursor(), k, s)
	if err != nil {
		return Record{}, err
	}
	anchor, err := d.canonicalHash(ctx, r.Anchor)
	if err != nil {
		return Record{}, err
	}
	if !CursorAccepted(anchor, s) {
		return Record{}, d.divergedAt("eez.deriver.cursor.misaligned", b, r.From, "local block %d is %x, neither the batch's entry state %x nor its settled state %x",
			r.Anchor, anchor, s.EntryState, s.FinalState)
	}
	sched, err := d.schedule(ctx)
	if err != nil {
		return Record{}, err
	}
	if r.To < k {
		return Record{}, d.diverged(b, r.To, "%d blocks end at block %d", k, r.To)
	}
	origin, err := d.header(ctx, r.To-k)
	if err != nil {
		return Record{}, err
	}
	end, _, err := sched.Candidate(origin, k, b.L1Time, p.actions)
	if err != nil {
		return Record{}, d.diverged(b, r.To, "%w", err)
	}
	if end != r.To {
		return Record{}, d.diverged(b, r.To, "candidate ends at block %d", end)
	}
	replayed, err := d.reconcileRollupXBlocks(ctx, b, p, r, sched)
	if err != nil {
		return Record{}, err
	}
	settled, err := d.settledEnd(ctx, b, r, replayed)
	if err != nil {
		return Record{}, err
	}
	d.logger.Debug("[eez] batch derived", "l1Block", b.L1Block, "tx", b.TxHash, "from", r.From, "to", r.To, "settled", settled, "resumed", r.Resumed)
	return Record{L1Block: b.L1Block, L1BlockHash: b.L1BlockHash, L1Time: b.L1Time, L1MixDigest: b.L1MixDigest, TxHash: b.TxHash, LastL2Block: settled}, nil
}

// reconcileRollupXBlocks rebuilds the range r and returns the hash of the
// Sync block it built, zero when it built none.
func (d *Deriver) reconcileRollupXBlocks(ctx context.Context, b *l1.ScannedBatch, p *rollupXPayload, r Range, sched *Schedule) (common.Hash, error) {
	nOut, nIn := len(p.effects.Outbound), len(p.effects.Inbound)
	skip, take := producing(&b.Settlement, nOut)
	if skip+take > nOut+nIn {
		return common.Hash{}, d.diverged(b, r.To, "L1 applied effects %d to %d of %d", skip, skip+take, nOut+nIn)
	}
	outSkip := min(skip, nOut)
	outTake := min(take, nOut-outSkip)
	inSkip := min(skip-outSkip, nIn)
	inTake := min(take-outTake, nIn-inSkip)
	// User transactions cannot come from the system address, so its nonce
	// before the range is its nonce before the Sync block. Each skipped
	// effect took one system transaction.
	nonce, err := d.chain.Nonce(ctx, r.From-1, protocol.SystemAddress)
	if err != nil {
		return common.Hash{}, err
	}
	suffix, err := p.effects.SyncSuffix(d.sync, span(outSkip, outTake), span(inSkip, inTake), nonce+uint64(outSkip+inSkip))
	if err != nil {
		return common.Hash{}, d.diverged(b, r.To, "%w", err)
	}
	parent, err := d.header(ctx, r.From-1)
	if err != nil {
		return common.Hash{}, err
	}
	inputs := func(parent *types.Header, i int) (HeaderInputs, error) {
		in := DeriveInputs(d.l2, parent, p.Span, i)
		t, err := sched.Timestamp(in.Number)
		if err != nil {
			return in, err
		}
		in.Time = t
		in.MixDigest, err = d.seedFor(ctx, in.Number)
		return in, err
	}
	last := int(p.Blocks()) - 1
	terminal := p.BlockTxs(last)
	var syncBlock *types.BlockWithReceipts
	gateFrom := 0
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
		if len(existingTxs) < len(terminal) || !slices.EqualFunc(existingTxs[:len(terminal)], terminal, bytes.Equal) {
			return common.Hash{}, d.diverged(b, r.From, "resumed batch's pure transactions differ from the Sync block's")
		}
		txs, replay := ResumeTxs(existing.Hash(), b.Settlement.FinalState, existingTxs, suffix)
		gateFrom = len(txs) - len(suffix)
		d.logger.Info("[eez] resumed batch appends to a Sync block", "event", "eez.deriver.resumed.placement", "l2Block", r.From, "tx", b.TxHash, "replay", replay)
		if replay {
			in, err := inputs(parent, last)
			if err != nil {
				return common.Hash{}, err
			}
			if syncBlock, err = d.extend(ctx, b, parent, in, txs); err != nil {
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
				txs = append(slices.Clone(terminal), suffix...)
			}
			in, err := inputs(parent, i)
			if err != nil {
				return common.Hash{}, err
			}
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
	if outTake+inTake > 0 {
		var expected []common.Hash
		for _, i := range span(outSkip, outTake) {
			for j := range p.effects.Outbound[i].Sources {
				expected = append(expected, p.effects.Outbound[i].Sources[j].ProxyEntryHash)
			}
		}
		if err := d.gateRollupX(ctx, b, r, expected, syncBlock, gateFrom); err != nil {
			return common.Hash{}, err
		}
	}
	if syncBlock == nil {
		return common.Hash{}, nil
	}
	return syncBlock.Block.Hash(), nil
}

func span(from, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = from + i
	}
	return out
}

// gateRollupX checks that the Sync block, from its transaction from on, made
// exactly the L2-to-L1 calls expected: EEZL2 logs CrossChainCallExecuted
// once per call, with zero call gas, and for nothing else.
func (d *Deriver) gateRollupX(ctx context.Context, b *l1.ScannedBatch, r Range, expected []common.Hash, built *types.BlockWithReceipts, from int) error {
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
	observed := map[common.Hash]int{}
	eezl2 := d.l2.EEZL2()
	for _, rc := range receipts[min(from, len(receipts)):] {
		for _, l := range rc.Logs {
			if l.Address != eezl2 {
				continue
			}
			e, err := protocol.ParseL2CrossChainCallExecuted(l)
			if err != nil {
				continue
			}
			if e.CallGas != 0 {
				return d.diverged(b, r.To, "%w: call %x with call gas %d", ErrOutboundUnauthorized, e.CrossChainCallHash, e.CallGas)
			}
			observed[e.CrossChainCallHash]++
		}
	}
	for _, h := range expected {
		if observed[h] == 0 {
			return d.diverged(b, r.To, "%w: call %x", ErrOutboundUnauthorized, h)
		}
		observed[h]--
	}
	for h, n := range observed {
		if n > 0 {
			return d.diverged(b, r.To, "%w: L2 made call %x that L1 did not settle", ErrOutboundUnauthorized, h)
		}
	}
	return nil
}
