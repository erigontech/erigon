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
	"math"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/types"
)

// rollupXPayload is the DA payload of a rollupX batch, its cross-chain
// transactions bound to its settlement.
type rollupXPayload struct {
	Payload
	rec *protocol.RollupXReconstruction
	// actions reports cross-chain transactions, which a catch-up candidate
	// cannot carry.
	actions bool
	// history holds each block's L1 history update under v0.1, nil for a
	// block without one.
	history []*protocol.HistoryTx
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
	v01 := d.l2.RuleSet() == config.RollupXV01
	var history []*protocol.HistoryTx
	if v01 {
		history = make([]*protocol.HistoryTx, len(span.TxCounts))
	}
	next := 0
	for block, count := range span.TxCounts {
		for j := range int(count) {
			update, err := checkPureTx(span.Transactions[next], v01 && j == 0, d.config.ChainID.Uint64())
			if err != nil {
				return nil, d.diverged(b, at, "DA payload: transaction %d: %w", next, err)
			}
			if update != nil {
				history[block] = update
			}
			next++
		}
	}
	rec, err := protocol.NewRollupXReconstruction(native.Txs, d.l2.RollupID, d.config.ChainID.Uint64())
	if err != nil {
		return nil, d.diverged(b, at, "%w", err)
	}
	if err := rec.BindSettlement(b.Batch); err != nil {
		return nil, d.diverged(b, at, "%w", err)
	}
	actions := len(native.Txs) > 0
	if v01 {
		// Under v0.1 lookups and static rows count as actions too.
		e := b.Batch.Entries
		actions = actions || len(e) != 1 || len(b.Batch.StaticEntries) > 0 || len(e[0].L2ToL1Calls) > 0 || len(e[0].ExpectedL1ToL2Calls) > 0
	}
	return &rollupXPayload{Payload: Payload{Span: span}, rec: rec, actions: actions, history: history}, nil
}

// checkPureTx checks a pure L2 transaction of a rollupX span: a canonical
// EIP-2718 encoding of a legacy, access-list, dynamic-fee or set-code
// transaction, or, when history is allowed, an L1 history update of chain
// chainID, which it returns.
func checkPureTx(enc []byte, history bool, chainID uint64) (*protocol.HistoryTx, error) {
	tx, err := types.UnmarshalTransactionFromBinary(enc, false)
	if err != nil {
		return nil, err
	}
	var update *protocol.HistoryTx
	switch tx.Type() {
	case types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType, types.SetCodeTxType:
	case protocol.HistoryTxType:
		h, ok := tx.(*systemtx.HistoryTransaction)
		if !history || !ok {
			return nil, fmt.Errorf("history update not first in its block")
		}
		body := h.Body()
		if body.ChainID != chainID {
			return nil, fmt.Errorf("history update of chain %d", body.ChainID)
		}
		update = &body
	default:
		return nil, fmt.Errorf("type %d", tx.Type())
	}
	var buf bytes.Buffer
	if err := tx.MarshalBinary(&buf); err != nil {
		return nil, err
	}
	if !bytes.Equal(buf.Bytes(), enc) {
		return nil, errors.New("non-canonical encoding")
	}
	return update, nil
}

// anchorV01 returns the L1 block a rollupX v0.1 batch is anchored to and
// whether it is a historical anchor. The batch's blockNumber selects it:
// the maximum anchors to the parent of the batch's L1 block, any other
// value to that recent block, which a candidate with actions cannot.
func (d *Deriver) anchorV01(ctx context.Context, b *l1.ScannedBatch, actions bool) (*l1.Header, bool, error) {
	inclusion, err := d.l1.HeaderByNumber(ctx, b.L1Block)
	if err != nil {
		return nil, false, err
	}
	if inclusion.Hash != b.L1BlockHash {
		return nil, false, fmt.Errorf("%w: L1 block %d is %x, the batch's %x", l1.ErrInconsistent, b.L1Block, inclusion.Hash, b.L1BlockHash)
	}
	number := b.Batch.BlockNumber
	if number == math.MaxUint64 {
		if b.L1Block == 0 {
			return nil, false, d.diverged(b, d.index.Cursor()+1, "batch in L1 genesis")
		}
		parent, err := d.l1.HeaderByNumber(ctx, b.L1Block-1)
		if err != nil {
			return nil, false, err
		}
		if parent.Hash != inclusion.ParentHash || parent.Time >= inclusion.Time {
			return nil, false, fmt.Errorf("%w: L1 block %d does not follow %d", l1.ErrInconsistent, b.L1Block, b.L1Block-1)
		}
		return parent, false, nil
	}
	switch age := b.L1Block - number; {
	case number == 0 || number >= b.L1Block || age > 256:
		return nil, false, d.diverged(b, d.index.Cursor()+1, "historical anchor %d in L1 block %d", number, b.L1Block)
	case actions:
		return nil, false, d.diverged(b, d.index.Cursor()+1, "historical anchor %d with cross-chain actions", number)
	}
	h, err := d.l1.HeaderByNumber(ctx, number)
	return h, true, err
}

// originsV01 checks the history updates of p's blocks after Sync parent
// origin against the canonical L1 chain up to anchor, and returns each
// block's L1 origin.
func (d *Deriver) originsV01(ctx context.Context, b *l1.ScannedBatch, p *rollupXPayload, origin *types.Header, anchor *l1.Header, sched *Schedule) ([]*l1.Header, error) {
	state, err := d.historyAt(ctx, origin.Number.Uint64())
	if err != nil {
		return nil, err
	}
	br, err := d.readBridge(ctx, state.endpoint, anchor.Number)
	if errors.Is(err, ErrHistory) {
		return nil, d.diverged(b, origin.Number.Uint64()+1, "%w", err)
	}
	if err != nil {
		return nil, err
	}
	genesis, err := d.header(ctx, 0)
	if err != nil {
		return nil, err
	}
	hs := newHistorySchedule(d.l2.RollupX, genesis.Time)
	origins := make([]*l1.Header, len(p.history))
	for i, update := range p.history {
		n := origin.Number.Uint64() + uint64(i) + 1
		t, err := sched.Timestamp(n)
		if err != nil {
			return nil, err
		}
		if origins[i], err = hs.transition(t, state, br, update); err != nil {
			return nil, d.diverged(b, n, "%w", err)
		}
		state = state.apply(origins[i], update)
	}
	return origins, nil
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

// settlementEffects returns the settlement entries of b that produce L2
// effects: the outbound hosts before the immediate count, and the inbound
// entries after it but the lookups.
func settlementEffects(b *protocol.ProofSystemBatch, rollupID uint64) (hosts, inbound []protocol.ExecutionEntry) {
	immediate := int(b.ImmediateEntryCount.Uint64())
	for i := immediate; i < len(b.Entries); i++ {
		if !protocol.IsLookupOnly(&b.Entries[i], rollupID) {
			inbound = append(inbound, b.Entries[i])
		}
	}
	return b.Entries[1:immediate], inbound
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
	target := b.L1Time
	var v01 *v01Context
	if d.l2.RuleSet() == config.RollupXV01 {
		if enc, err := protocol.EncodePostAndVerifyBatch(b.Batch); err != nil || !bytes.Equal(enc, b.Input) {
			return Record{}, d.diverged(b, r.From, "settlement calldata is not canonical")
		}
		anchor, historical, err := d.anchorV01(ctx, b, p.actions)
		if err != nil {
			return Record{}, err
		}
		if historical {
			target = anchor.Time
		}
		origins, err := d.originsV01(ctx, b, p, origin, anchor, sched)
		if err != nil {
			return Record{}, err
		}
		v01 = &v01Context{anchor: anchor, origins: origins}
	}
	end, _, err := sched.Candidate(origin, k, target, p.actions)
	if err != nil {
		return Record{}, d.diverged(b, r.To, "%w", err)
	}
	if end != r.To {
		return Record{}, d.diverged(b, r.To, "candidate ends at block %d", end)
	}
	replayed, err := d.reconcileRollupXBlocks(ctx, b, p, r, sched, v01)
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

// v01Context is what a rollupX v0.1 batch adds to its derivation: the L1
// block it is anchored to, and each of its blocks' L1 origin.
type v01Context struct {
	anchor  *l1.Header
	origins []*l1.Header
}

// reconcileRollupXBlocks rebuilds the range r and returns the hash of the
// Sync block it built, zero when it built none.
func (d *Deriver) reconcileRollupXBlocks(ctx context.Context, b *l1.ScannedBatch, p *rollupXPayload, r Range, sched *Schedule, v01 *v01Context) (common.Hash, error) {
	hosts, inbound := settlementEffects(b.Batch, d.l2.RollupID)
	hostRoots, err := protocol.OutboundRootCounts(hosts)
	if err != nil {
		return common.Hash{}, d.diverged(b, r.To, "%w", err)
	}
	skip, take := producing(&b.Settlement, len(hosts))
	out, in, err := protocol.SelectGroups(skip, take, hostRoots, p.rec.Sources(), len(p.rec.Inbound))
	if err != nil {
		return common.Hash{}, d.diverged(b, r.To, "%w", err)
	}
	if len(inbound) != len(p.rec.Inbound) {
		return common.Hash{}, d.diverged(b, r.To, "%d inbound effects for %d deliveries", len(inbound), len(p.rec.Inbound))
	}
	// The gate expects every committed call the applied transactions make
	// from the rollup to L1, as the settlement and L2 tables recompute it.
	var expected []common.Hash
	for i := out[0]; i < out[1]; i++ {
		o := &p.rec.Outbound[i]
		calls, err := protocol.VerifyOutbound(&hosts[i], o.Mutable, o.Static, d.l2.RollupID)
		if err != nil {
			return common.Hash{}, d.diverged(b, r.To, "outbound transaction %d: %w", i, err)
		}
		expected = append(expected, calls...)
	}
	for i := in[0]; i < in[1]; i++ {
		calls, err := protocol.VerifyInbound(&inbound[i], &p.rec.Inbound[i].Entry, d.l2.RollupID)
		if err != nil {
			return common.Hash{}, d.diverged(b, r.To, "delivery %d: %w", i, err)
		}
		expected = append(expected, calls...)
	}
	outGroups, inGroups := span(out[0], out[1]-out[0]), span(in[0], in[1]-in[0])
	var suffix [][]byte
	if v01 != nil {
		if suffix, err = p.rec.NativeSuffix(d.sync, outGroups, inGroups, v01.anchor.Hash); err != nil {
			return common.Hash{}, d.diverged(b, r.To, "%w", err)
		}
	} else {
		// User transactions cannot come from the system address, so its nonce
		// before the range is its nonce before the Sync block. Each skipped
		// transaction and delivery took one system transaction.
		nonce, err := d.chain.Nonce(ctx, r.From-1, protocol.SystemAddress)
		if err != nil {
			return common.Hash{}, err
		}
		if suffix, err = p.rec.SyncSuffix(d.sync, outGroups, inGroups, nonce+uint64(out[0]+in[0])); err != nil {
			return common.Hash{}, d.diverged(b, r.To, "%w", err)
		}
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
		if v01 != nil {
			in.MixDigest = v01.origins[i].MixDigest
			return in, nil
		}
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
		var txs [][]byte
		var replay bool
		if v01 != nil {
			if txs, replay, err = ResumeNativeTxs(existing.Hash(), &b.Settlement, existingTxs, suffix); err != nil {
				return common.Hash{}, d.diverged(b, r.From, "resumed batch: %w", err)
			}
		} else {
			txs, replay = ResumeTxs(existing.Hash(), b.Settlement.FinalState, existingTxs, suffix)
		}
		// The gate reads the suffix this batch settled, at the end of the
		// block it builds or of the local one.
		gateFrom = max(0, len(existingTxs)-len(suffix))
		if replay {
			gateFrom = len(txs) - len(suffix)
		}
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
	if len(outGroups)+len(inGroups) > 0 {
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
