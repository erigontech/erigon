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

package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/types"
)

// This file ports blob_semantics/reconstruction.rs, entries/inbound.rs and
// the Sync-group construction of system_tx.rs of eez-rollupX d0130ab9: the
// L2 transactions a rollupX batch's cross-chain transactions settle into,
// and the binding of their L1 tables to the batch's settlement.

var (
	// ErrSemantics reports cross-chain transactions rollupX rejects.
	ErrSemantics = errors.New("invalid cross-chain transactions")
	// ErrBinding reports settlement entries that differ from the ones the
	// cross-chain transactions describe.
	ErrBinding = errors.New("settlement does not match the DA payload")
)

// RollupXOutbound is a signed L2 transaction of a Sync block and the source
// entries it consumes: its mutable entries, L1-shaped, then its static
// reads.
type RollupXOutbound struct {
	Mutable []ExecutionEntry
	Static  []L2StaticExecutionEntry
	UserTx  []byte
}

// RollupXDelivery is an L1 call a Sync block applies to the rollup: its
// L1-shaped delivery entry and the value the delivery mints.
type RollupXDelivery struct {
	Entry ExecutionEntry
	Value uint256.Int
}

// RollupXReconstruction is what the cross-chain transactions of a rollupX
// batch do on its rollup, and the L1 tables settlement must carry for them.
type RollupXReconstruction struct {
	rollupID uint64
	// Outbound holds the transactions starting on the rollup, in order.
	Outbound []RollupXOutbound
	// Inbound holds the deliveries of the L1 calls that apply, in order;
	// failed, reverted and static roots are lookups and deliver nothing.
	Inbound []RollupXDelivery
	tables  []l1Tables
}

// l1Tables are the L1 tables of one transaction, before settlement supplies
// their root updates.
type l1Tables struct {
	outbound bool
	entries  []ExecutionEntry
	traces   []entryTrace
	statics  []StaticExecutionEntry
}

// NewRollupXReconstruction reconstructs txs on rollupID, an L2 of chain id
// chainID: transactions starting on the rollup first, each with its signed
// transaction, then those starting on L1.
func NewRollupXReconstruction(txs []da.SemanticTx, rollupID, chainID uint64) (*RollupXReconstruction, error) {
	if rollupID == MainnetRollupID {
		return nil, fmt.Errorf("%w: rollup id 0", ErrSemantics)
	}
	r := &RollupXReconstruction{rollupID: rollupID}
	inbound := false
	for i := range txs {
		if err := r.add(&txs[i], &inbound, chainID); err != nil {
			return nil, fmt.Errorf("%w: transaction %d: %w", ErrSemantics, i, err)
		}
	}
	return r, nil
}

func (r *RollupXReconstruction) add(tx *da.SemanticTx, inbound *bool, chainID uint64) error {
	if len(tx.Actions) == 0 {
		return fmt.Errorf("no calls")
	}
	var outbound bool
	switch tx.Origin {
	case r.rollupID:
		if *inbound {
			return fmt.Errorf("starts on the rollup after one from L1")
		}
		if err := CheckOriginTx(tx.TxData, chainID); err != nil {
			return err
		}
		outbound = true
	case MainnetRollupID:
		*inbound = true
	default:
		return fmt.Errorf("starts on rollup %d", tx.Origin)
	}
	for i := range tx.Actions {
		for _, id := range []uint64{tx.Actions[i].SourceRollupID, tx.Actions[i].TargetRollupID} {
			if id != MainnetRollupID && id != r.rollupID {
				return fmt.Errorf("call on rollup %d", id)
			}
		}
	}
	batches, err := materializeBatches(tx.Actions, tx.Origin)
	if err != nil {
		return err
	}
	source := batches[tx.Origin]
	if source == nil {
		return failMaterialize("no source table")
	}
	if len(source.entries) != source.sourceEntryCount || len(source.entries) != len(source.traces) {
		return failMaterialize("source entries and traces differ")
	}
	var deliveries []RollupXDelivery
	if t := batches[r.rollupID]; t != nil && tx.Origin != r.rollupID {
		if len(t.staticEntries) > 0 {
			return failMaterialize("static target entries")
		}
		for i := range t.entries {
			d, err := r.delivery(&t.entries[i])
			if err != nil {
				return err
			}
			deliveries = append(deliveries, d)
		}
	}
	host := batches[MainnetRollupID]
	if outbound {
		if host == nil || len(source.staticEntries) > 0 || len(deliveries) > 0 {
			return failMaterialize("outbound transaction with standalone callbacks")
		}
		if len(host.entries) != 1 || len(host.staticEntries) > 0 {
			return failMaterialize("outbound transaction with %d settlement hosts", len(host.entries))
		}
		o := RollupXOutbound{Static: source.l2StaticEntries, UserTx: tx.TxData}
		for i := range source.entries {
			e := &source.entries[i]
			if e.DestinationRollupID != MainnetRollupID || len(e.RollupUpdates) > 0 {
				return failMaterialize("outbound source routing")
			}
			o.Mutable = append(o.Mutable, *e)
		}
		if len(o.Mutable)+len(o.Static) == 0 {
			return failMaterialize("outbound transaction has no source table")
		}
		r.Outbound = append(r.Outbound, o)
		r.tables = append(r.tables, l1Tables{outbound: true, entries: host.entries, traces: host.traces})
		return nil
	}
	if len(source.l2StaticEntries) > 0 {
		return failMaterialize("L1 source with L2 static entries")
	}
	applied, err := l1AppliedRootMask(tx.Actions)
	if err != nil {
		return err
	}
	var mutable, applying []int
	for i := range tx.Actions {
		if a := &tx.Actions[i]; a.Parent < 0 && !a.Static {
			mutable = append(mutable, i)
			if applied[i] {
				applying = append(applying, i)
			}
		}
	}
	if len(mutable) != len(source.entries) {
		return failMaterialize("%d mutable roots for %d settlement entries", len(mutable), len(source.entries))
	}
	if len(applying) != len(deliveries) {
		return failMaterialize("%d applying roots for %d deliveries", len(applying), len(deliveries))
	}
	for k, i := range applying {
		h, err := destinationCallHash(&tx.Actions[i])
		if err != nil {
			return err
		}
		if h != deliveries[k].Entry.ProxyEntryHash {
			return failMaterialize("root %d and its delivery differ", i)
		}
	}
	r.Inbound = append(r.Inbound, deliveries...)
	r.tables = append(r.tables, l1Tables{entries: source.entries, traces: source.traces, statics: source.staticEntries})
	return nil
}

// delivery checks a materialized L2 delivery entry: one mutable root from
// L1 to the rollup without call gas, a supported shape, and, for a flat
// call, the rolling hash of its outcome.
func (r *RollupXReconstruction) delivery(e *ExecutionEntry) (RollupXDelivery, error) {
	switch {
	case e.DestinationRollupID != r.rollupID:
		return RollupXDelivery{}, failMaterialize("delivery to rollup %d", e.DestinationRollupID)
	case len(e.RollupUpdates) > 0:
		return RollupXDelivery{}, failMaterialize("delivery with root updates")
	case !e.Success:
		return RollupXDelivery{}, failMaterialize("failed delivery")
	case len(e.L2ToL1Calls) != 1:
		return RollupXDelivery{}, failMaterialize("delivery with %d root calls", len(e.L2ToL1Calls))
	}
	c := &e.L2ToL1Calls[0]
	if c.SourceRollupID != MainnetRollupID || c.IsStatic || c.Gas != 0 {
		return RollupXDelivery{}, failMaterialize("delivery root is not a mutable call from L1")
	}
	if err := checkEntryShape(e); err != nil {
		return RollupXDelivery{}, err
	}
	h, err := CrossChainCallHash(&CallHashInput{SourceAddress: c.SourceAddress, TargetAddress: c.TargetAddress, TargetRollupID: e.DestinationRollupID, Value: c.Value, Data: c.Data})
	if err != nil {
		return RollupXDelivery{}, err
	}
	if h == (common.Hash{}) || h != e.ProxyEntryHash {
		return RollupXDelivery{}, failMaterialize("delivery root differs from its entry")
	}
	if len(e.ExpectedL1ToL2Calls) == 0 && c.RevertNextNCalls == 0 {
		want := L2EntrySeed(h)
		want.CallBegin(h)
		want.CallEnd(true, e.ReturnData)
		if want.Sum() != e.RollingHash {
			return RollupXDelivery{}, failMaterialize("delivery rolling hash differs from its outcome")
		}
	}
	l2 := lowerEntry(e)
	value, err := committedValue([]L2ExecutionEntry{l2})
	if err != nil {
		return RollupXDelivery{}, err
	}
	return RollupXDelivery{Entry: *e, Value: value}, nil
}

// BindSettlement checks that b carries exactly the L1 tables of the
// reconstruction: a root-only anchor, the outbound transactions' hosts
// before the immediate count, the inbound transactions' entries after it
// and their static reads in the static pool. Each entry takes its one update
// of the rollup from b; its rolling hash then follows and it must encode
// identically.
func (r *RollupXReconstruction) BindSettlement(b *ProofSystemBatch) error {
	if b.ImmediateStaticEntryCount != nil && b.ImmediateStaticEntryCount.Sign() != 0 {
		return fmt.Errorf("%w: immediate static entries", ErrBinding)
	}
	if !b.ImmediateEntryCount.IsUint64() || b.ImmediateEntryCount.Uint64() == 0 || b.ImmediateEntryCount.Uint64() > uint64(len(b.Entries)) {
		return fmt.Errorf("%w: immediate entry count %v of %d entries", ErrBinding, b.ImmediateEntryCount, len(b.Entries))
	}
	if !IsRootOnly(&b.Entries[0], r.rollupID) {
		return fmt.Errorf("%w: entry 0 is no anchor", ErrBinding)
	}
	immediate := int(b.ImmediateEntryCount.Uint64())
	out, in, statics := b.Entries[1:immediate], b.Entries[immediate:], b.StaticEntries
	for i := range r.tables {
		t := &r.tables[i]
		queue := &in
		if t.outbound {
			queue = &out
		}
		if err := r.bindEntries(t, queue); err != nil {
			return fmt.Errorf("%w: transaction %d: %w", ErrBinding, i, err)
		}
		for j := range t.statics {
			if len(statics) == 0 {
				return fmt.Errorf("%w: transaction %d: static entry %d missing", ErrBinding, i, j)
			}
			supplied := &statics[0]
			statics = statics[1:]
			if len(supplied.ExpectedRoots) != 1 || supplied.ExpectedRoots[0].RollupID != r.rollupID {
				return fmt.Errorf("%w: transaction %d: static entry %d does not pin rollup %d alone", ErrBinding, i, j, r.rollupID)
			}
			want := t.statics[j]
			want.ExpectedRoots = supplied.ExpectedRoots
			if !equalStatic(&want, supplied) {
				return fmt.Errorf("%w: transaction %d: static entry %d differs", ErrBinding, i, j)
			}
		}
	}
	if len(out)+len(in)+len(statics) > 0 {
		return fmt.Errorf("%w: %d outbound, %d inbound and %d static entries left over", ErrBinding, len(out), len(in), len(statics))
	}
	return nil
}

func (r *RollupXReconstruction) bindEntries(t *l1Tables, queue *[]ExecutionEntry) error {
	if len(*queue) < len(t.entries) {
		return fmt.Errorf("%d entries for %d", len(*queue), len(t.entries))
	}
	supplied := (*queue)[:len(t.entries)]
	*queue = (*queue)[len(t.entries):]
	entries := make([]ExecutionEntry, len(t.entries))
	for i := range t.entries {
		if len(supplied[i].RollupUpdates) != 1 || supplied[i].RollupUpdates[0].RollupID != r.rollupID {
			return fmt.Errorf("entry %d does not update rollup %d alone", i, r.rollupID)
		}
		entries[i] = cloneEntry(&t.entries[i])
		entries[i].RollupUpdates = supplied[i].RollupUpdates
	}
	if err := finalizeL1EntryTraces(entries, t.traces); err != nil {
		return err
	}
	for i := range entries {
		want, err := EncodeExecutionEntry(&entries[i])
		if err != nil {
			return err
		}
		got, err := EncodeExecutionEntry(&supplied[i])
		if err != nil {
			return err
		}
		if !bytes.Equal(want, got) {
			return fmt.Errorf("entry %d differs", i)
		}
	}
	return nil
}

// cloneEntry copies e deeply enough for its rows' keys to be set.
func cloneEntry(e *ExecutionEntry) ExecutionEntry {
	c := *e
	c.ExpectedL1ToL2Calls = slices.Clone(e.ExpectedL1ToL2Calls)
	return c
}

func equalStatic(a, b *StaticExecutionEntry) bool {
	ea, err := EncodeStaticExecutionEntry(a)
	if err != nil {
		return false
	}
	eb, err := EncodeStaticExecutionEntry(b)
	return err == nil && bytes.Equal(ea, eb)
}

// SyncSuffix returns the transactions a rollupX v0.0.1 Sync block carries
// after its pure transactions: for each outbound transaction of out a
// loadExecutionTable of its sources followed by the transaction, then for
// each delivery of in an executeIncomingCrossChainCall minting its value.
// System nonces count up from nonce.
func (r *RollupXReconstruction) SyncSuffix(ctx SyncTxContext, out, in []int, nonce uint64) ([][]byte, error) {
	return r.suffix(out, in, func(_ int, value uint256.Int, input []byte) ([]byte, error) {
		if nonce == math.MaxUint64 {
			return nil, errNonceOverflow
		}
		tx := SystemTx{ChainID: ctx.ChainID, Nonce: nonce, To: ctx.EEZL2, Value: value, Input: input}
		nonce++
		return tx.MarshalBinary()
	})
}

// NativeSuffix is SyncSuffix under rollupX v0.1: type-0x45 transactions,
// each identified by its group's index among the batch's outbound
// transactions then deliveries, under the L1 parent parentHash of the
// batch. Their gas limits are the cap; building the block lowers them to
// what their positions leave.
func (r *RollupXReconstruction) NativeSuffix(ctx SyncTxContext, out, in []int, parentHash common.Hash) ([][]byte, error) {
	return r.suffix(out, in, func(group int, value uint256.Int, input []byte) ([]byte, error) {
		tx := NativeTx{
			ChainID:    ctx.ChainID,
			SourceHash: SourceHash(ctx.ChainID, parentHash, uint64(group)),
			GasLimit:   NativeTxGasCap,
			To:         ctx.EEZL2,
			Value:      value,
			Input:      input,
		}
		return tx.MarshalBinary()
	})
}

func (r *RollupXReconstruction) suffix(out, in []int, system func(group int, value uint256.Int, input []byte) ([]byte, error)) ([][]byte, error) {
	var txs [][]byte
	for _, i := range out {
		o := &r.Outbound[i]
		lowered := make([]L2ExecutionEntry, len(o.Mutable))
		for j := range o.Mutable {
			if err := checkEntryShape(&o.Mutable[j]); err != nil {
				return nil, err
			}
			lowered[j] = lowerEntry(&o.Mutable[j])
		}
		for j := range o.Static {
			if err := checkL2StaticEntryShape(&o.Static[j]); err != nil {
				return nil, err
			}
		}
		input, err := EncodeLoadExecutionTable(&L2ExecutionTable{Entries: lowered, StaticEntries: nonNil(o.Static)})
		if err != nil {
			return nil, err
		}
		value, err := committedValue(lowered)
		if err != nil {
			return nil, err
		}
		tx, err := system(i, value, input)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx, o.UserTx)
	}
	for _, i := range in {
		d := &r.Inbound[i]
		input, err := EncodeExecuteIncomingCrossChainCall(&L2ExecutionTable{Entries: []L2ExecutionEntry{lowerEntry(&d.Entry)}, StaticEntries: []L2StaticExecutionEntry{}})
		if err != nil {
			return nil, err
		}
		tx, err := system(len(r.Outbound)+i, d.Value, input)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

// Sources returns the source counts of the reconstruction's outbound
// transactions.
func (r *RollupXReconstruction) Sources() []int {
	counts := make([]int, len(r.Outbound))
	for i := range r.Outbound {
		counts[i] = len(r.Outbound[i].Mutable) + len(r.Outbound[i].Static)
	}
	return counts
}

// SelectGroups maps the producing run L1 applied, skip and take over the
// outbound hosts then the deliveries, to the ranges of outbound
// transactions and deliveries it covers. A host carries hostRoots source
// roots and a transaction groupRoots; a run must not split a transaction.
func SelectGroups(skip, take int, hostRoots, groupRoots []int, inbound int) (out, in [2]int, err error) {
	sum := func(counts []int) int {
		s := 0
		for _, c := range counts {
			s += c
		}
		return s
	}
	if slices.Contains(hostRoots, 0) || slices.Contains(groupRoots, 0) || sum(hostRoots) != sum(groupRoots) {
		return out, in, fmt.Errorf("%w: outbound hosts and transactions cover different roots", ErrBinding)
	}
	if skip < 0 || take < 0 || skip+take > len(hostRoots)+inbound {
		return out, in, fmt.Errorf("%w: L1 applied effects %d to %d of %d", ErrBinding, skip, skip+take, len(hostRoots)+inbound)
	}
	outSkip := min(skip, len(hostRoots))
	outTake := min(take, len(hostRoots)-outSkip)
	inSkip := min(skip-outSkip, inbound)
	inTake := min(take-outTake, inbound-inSkip)
	boundary := func(root int) int {
		position := 0
		if root == 0 {
			return 0
		}
		for i, size := range groupRoots {
			position += size
			if position == root {
				return i + 1
			}
			if position > root {
				return -1
			}
		}
		return -1
	}
	from, to := boundary(sum(hostRoots[:outSkip])), boundary(sum(hostRoots[:outSkip+outTake]))
	if from < 0 || to < 0 {
		return out, in, fmt.Errorf("%w: L1 applied part of an outbound transaction", ErrBinding)
	}
	return [2]int{from, to}, [2]int{inSkip, inSkip + inTake}, nil
}

// committedValue is the value a table's incoming calls carry, its own and
// those of its expected outgoing calls.
func committedValue(entries []L2ExecutionEntry) (uint256.Int, error) {
	var sum uint256.Int
	add := func(calls []CrossChainCall) error {
		for i := range calls {
			v, err := toUint256(calls[i].Value)
			if err != nil {
				return err
			}
			if _, overflow := sum.AddOverflow(&sum, &v); overflow {
				return fmt.Errorf("%w: table value overflows", ErrEntryShape)
			}
		}
		return nil
	}
	for i := range entries {
		if err := add(entries[i].IncomingCalls); err != nil {
			return sum, err
		}
		for j := range entries[i].ExpectedOutgoingCalls {
			if err := add(entries[i].ExpectedOutgoingCalls[j].Calls); err != nil {
				return sum, err
			}
		}
	}
	return sum, nil
}

// CheckOriginTx checks the signed transaction a cross-chain transaction
// starts with on an L2 of chain id chainID: one canonical EIP-2718
// envelope, no blobs, the chain's id when it names one, and a recoverable
// sender.
func CheckOriginTx(enc []byte, chainID uint64) error {
	tx, err := types.UnmarshalTransactionFromBinary(enc, false)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := tx.MarshalBinary(&buf); err != nil {
		return err
	}
	switch {
	case !bytes.Equal(buf.Bytes(), enc):
		return errors.New("non-canonical transaction")
	case tx.Type() == types.BlobTxType || tx.Type() == SystemTxType || tx.Type() == NativeTxType || tx.Type() == HistoryTxType:
		return fmt.Errorf("transaction of type %d", tx.Type())
	}
	id := uint256.NewInt(chainID)
	if c := tx.GetChainID(); c != nil && !c.IsZero() && !c.Eq(id) {
		return fmt.Errorf("transaction of chain %v", c)
	}
	if _, err := tx.Sender(*types.LatestSignerForChainID(id)); err != nil {
		return err
	}
	return nil
}
