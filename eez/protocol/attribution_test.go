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
	"math/big"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/execution/types"
)

const (
	testRollup = 1
	batchLog   = 10
	laterLog   = 20
)

func repeatHash(b byte) common.Hash { return common.Hash(bytes.Repeat([]byte{b}, 32)) }

// batchChain builds a batch whose entry i moves testRollup from the previous
// root to roots[i]. Entry 0 is the anchor; entries below immediate run inline
// with a zero proxy entry hash, the others are queued deliveries.
func batchChain(pre common.Hash, roots []common.Hash, immediate int) *ProofSystemBatch {
	entries := make([]ExecutionEntry, len(roots))
	for i := range roots {
		current := pre
		if i > 0 {
			current = roots[i-1]
		}
		entries[i] = ExecutionEntry{
			RollupUpdates:       []RollupUpdate{{RollupID: testRollup, EtherDelta: new(big.Int), CurrentRoot: current, NewRoot: roots[i]}},
			DestinationRollupID: testRollup,
			Success:             true,
		}
		if i >= immediate {
			entries[i].ProxyEntryHash = repeatHash(byte(0x80 + i))
		}
		if i > 0 {
			entries[i].L2ToL1Calls = []CrossChainCall{{SourceRollupID: testRollup, Value: new(big.Int)}}
		}
	}
	return &ProofSystemBatch{
		Entries:                   entries,
		ImmediateEntryCount:       big.NewInt(int64(immediate)),
		RollupIDsWithProofSystems: []RollupIDWithProofSystems{{RollupID: testRollup, ProofSystemIndexes: []uint64{0}}},
	}
}

// evidenceFor is what L1 emits when exactly the applied entries of b run.
func evidenceFor(b *ProofSystemBatch, applied ...int) *SettlementEvidence {
	immediate := int(b.ImmediateEntryCount.Int64())
	var ev SettlementEvidence
	var queue []int
	for i := immediate; i < len(b.Entries); i++ {
		queue = append(queue, i)
	}
	for i := range immediate {
		if !slices.Contains(applied, i) {
			ev.Skipped = append(ev.Skipped, uint64(i))
		}
	}
	for _, i := range applied {
		ev.Roots = append(ev.Roots, b.Entries[i].RollupUpdates[0].NewRoot)
		if i >= immediate {
			slot := slices.Index(queue, i)
			ev.Consumed = append(ev.Consumed, ConsumedEntry{Slot: uint64(slot), CallHash: b.Entries[i].ProxyEntryHash, LogIndex: laterLog})
		}
	}
	return &ev
}

func appliedIndexes(s *Settlement) []int {
	out := []int{}
	for _, a := range s.Applied {
		out = append(out, a.Index)
	}
	return out
}

func roots(from, to byte) []common.Hash {
	var out []common.Hash
	for b := from; b <= to; b++ {
		out = append(out, repeatHash(b))
	}
	return out
}

func TestAttributionAnchorOnly(t *testing.T) {
	pre := repeatHash(0x01)
	for _, root := range []common.Hash{repeatHash(0xaa), repeatHash(0xbb)} {
		b := batchChain(pre, []common.Hash{root}, 1)
		s, err := AttributeSettlement(b, testRollup, batchLog, evidenceFor(b, 0))
		require.NoError(t, err)
		require.Equal(t, []AppliedEntry{{Index: 0, Role: RoleAnchor}}, s.Applied)
		require.Equal(t, root, s.FinalState)
		require.Equal(t, pre, s.EntryState)
	}
}

func TestAttributionEmptyWindow(t *testing.T) {
	b := batchChain(repeatHash(0x01), []common.Hash{repeatHash(0xaa)}, 1)
	s, err := AttributeSettlement(b, testRollup, batchLog, evidenceFor(b))
	require.NoError(t, err)
	require.Equal(t, &Settlement{}, s)
}

func TestAttributionPartialAndSkipped(t *testing.T) {
	rs := roots(0x11, 0x12)
	b := batchChain(repeatHash(0x01), rs, 2)
	ev := evidenceFor(b, 0)
	require.Equal(t, []uint64{1}, ev.Skipped, "L2TxSkipped removes immediate entry 1")
	s, err := AttributeSettlement(b, testRollup, batchLog, ev)
	require.NoError(t, err)
	require.Equal(t, []int{0}, appliedIndexes(s))
	require.Equal(t, rs[0], s.FinalState)
}

func TestAttributionRoles(t *testing.T) {
	b := batchChain(repeatHash(0x01), roots(0x11, 0x13), 3)
	require.Equal(t, []AppliedEntry{{Role: RoleAnchor}, {Role: RoleOutbound}, {Role: RoleOutbound, Ordinal: 1}}, classifyEntries(b))

	// A delivery carries no calls; its proxy entry hash makes it inbound.
	b = batchChain(repeatHash(0x01), roots(0x11, 0x13), 1)
	b.Entries[1].L2ToL1Calls = nil
	require.Equal(t, []AppliedEntry{{Role: RoleAnchor}, {Role: RoleInbound}, {Role: RoleInbound, Ordinal: 1}}, classifyEntries(b))
	s, err := AttributeSettlement(b, testRollup, batchLog, evidenceFor(b, 0, 1, 2))
	require.NoError(t, err)
	require.Equal(t, []AppliedEntry{{Index: 0, Role: RoleAnchor}, {Index: 1, Role: RoleInbound}, {Index: 2, Role: RoleInbound, Ordinal: 1}}, s.Applied)
}

func TestAttributionResumed(t *testing.T) {
	rs := roots(0x11, 0x13)
	b := batchChain(repeatHash(0x01), rs, 1)
	for _, applied := range [][]int{{0, 1, 2}, {0}} {
		s, err := AttributeSettlement(b, testRollup, batchLog, evidenceFor(b, applied...))
		require.NoError(t, err)
		require.False(t, s.Resumed())
	}
	s, err := AttributeSettlement(b, testRollup, batchLog, evidenceFor(b, 1, 2))
	require.NoError(t, err)
	require.True(t, s.Resumed())
	require.Equal(t, rs[0], s.EntryState, "the run resumes at a rival's endpoint")
	require.False(t, (&Settlement{}).Resumed())
}

// TestAttributionTransientTable covers a consumption logged before the
// batch's BatchPosted: it reads the transient table loaded from the immediate
// entries after the leading run, which the queue cannot address.
func TestAttributionTransientTable(t *testing.T) {
	rs := roots(0x11, 0x12)
	b := batchChain(repeatHash(0x01), rs, 1)
	b.ImmediateEntryCount = big.NewInt(2)
	ev := &SettlementEvidence{Roots: rs, Consumed: []ConsumedEntry{{Slot: 0, CallHash: b.Entries[1].ProxyEntryHash, LogIndex: 1}}}
	s, err := AttributeSettlement(b, testRollup, batchLog, ev)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, appliedIndexes(s))
	require.Equal(t, rs[1], s.FinalState)

	ev.Consumed[0].CallHash = repeatHash(0xee)
	_, err = AttributeSettlement(b, testRollup, batchLog, ev)
	require.ErrorIs(t, err, ErrAttribution)
}

// TestAttributionCollidingCallHashes tells identical calls in the transient
// table and the queue apart by where the consumption was logged.
func TestAttributionCollidingCallHashes(t *testing.T) {
	rs := roots(0x11, 0x13)
	b := batchChain(repeatHash(0x01), rs, 1)
	b.ImmediateEntryCount = big.NewInt(2)
	shared := repeatHash(0x77)
	b.Entries[1].ProxyEntryHash, b.Entries[2].ProxyEntryHash = shared, shared

	s, err := AttributeSettlement(b, testRollup, batchLog, &SettlementEvidence{
		Roots: []common.Hash{rs[0], rs[1]}, Consumed: []ConsumedEntry{{Slot: 0, CallHash: shared, LogIndex: 1}},
	})
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, appliedIndexes(s))

	s, err = AttributeSettlement(b, testRollup, batchLog, &SettlementEvidence{
		Roots: []common.Hash{rs[0], rs[2]}, Consumed: []ConsumedEntry{{Slot: 0, CallHash: shared, LogIndex: laterLog}},
	})
	require.NoError(t, err)
	require.Equal(t, []int{0, 2}, appliedIndexes(s))
}

func TestAttributionRejects(t *testing.T) {
	b := batchChain(repeatHash(0x01), roots(0x11, 0x12), 1)
	for name, mutate := range map[string]func(ev *SettlementEvidence){
		"slot beyond the batch": func(ev *SettlementEvidence) { ev.Consumed[0].Slot = 99 },
		"call hash mismatch":    func(ev *SettlementEvidence) { ev.Consumed[0].CallHash = repeatHash(0xee) },
		"extra root":            func(ev *SettlementEvidence) { ev.Roots = append(ev.Roots, repeatHash(0x99)) },
		"missing root":          func(ev *SettlementEvidence) { ev.Roots = ev.Roots[:1] },
		"applied twice":         func(ev *SettlementEvidence) { ev.Consumed = append(ev.Consumed, ev.Consumed[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			ev := evidenceFor(b, 0, 1)
			mutate(ev)
			_, err := AttributeSettlement(b, testRollup, batchLog, ev)
			require.ErrorIs(t, err, ErrAttribution)
		})
	}
}

// blockLog places an event log at a transaction and block log index.
func blockLog(t *testing.T, tx, index uint, l *types.Log) *types.Log {
	t.Helper()
	l.TxIndex, l.Index = hexutil.Uint(tx), hexutil.Uint(index)
	return l
}

func rootLog(t *testing.T, tx, index uint, rollup uint64, root common.Hash) *types.Log {
	return blockLog(t, tx, index, makeLog(t, &l2ExecutionPerformedEvent, []common.Hash{uint64Topic(rollup)}, root, new(big.Int)))
}

func consumedLog(t *testing.T, tx, index uint, callHash common.Hash, slot uint64) *types.Log {
	return blockLog(t, tx, index, makeLog(t, &executionConsumedEvent, []common.Hash{callHash, uint64Topic(testRollup), uint64Topic(slot)}))
}

func postedLog(t *testing.T, tx, index uint) *types.Log {
	return blockLog(t, tx, index, makeLog(t, &batchPostedEvent, nil, common.Hash{}, []uint64{testRollup}))
}

// TestAttributeBlockWindows splits one block's logs among its batches: a
// later batch for the rollup ends the earlier one's window at its tx index,
// and a batch for another rollup neither settles ours nor ends a window.
func TestAttributeBlockWindows(t *testing.T) {
	a := batchChain(repeatHash(0x01), roots(0xa1, 0xa2), 1)
	other := batchChain(repeatHash(0x02), roots(0xc1, 0xc1), 1)
	other.RollupIDsWithProofSystems[0].RollupID = 2
	b := batchChain(repeatHash(0xa2), roots(0xb1, 0xb2), 1)
	batches := []PostedBatch{
		{Batch: a, TxIndex: 0, LogIndex: 1},
		{Batch: other, TxIndex: 1, LogIndex: 4},
		{Batch: b, TxIndex: 3, LogIndex: 7},
	}
	logs := []*types.Log{
		rootLog(t, 0, 0, testRollup, repeatHash(0xa1)), postedLog(t, 0, 1),
		rootLog(t, 1, 2, 2, repeatHash(0xc1)), rootLog(t, 1, 3, testRollup, repeatHash(0xee)), postedLog(t, 1, 4),
		consumedLog(t, 2, 5, a.Entries[1].ProxyEntryHash, 0), rootLog(t, 2, 6, testRollup, repeatHash(0xa2)),
		rootLog(t, 3, 7, testRollup, repeatHash(0xb1)), postedLog(t, 3, 8),
		consumedLog(t, 4, 9, b.Entries[1].ProxyEntryHash, 0), rootLog(t, 4, 10, testRollup, repeatHash(0xb2)),
	}
	// The root at tx 1 lands in a's window, which reaches tx 3.
	_, err := AttributeBlock(batches, logs, testRollup)
	require.ErrorIs(t, err, ErrAttribution)

	logs = slices.Delete(logs, 3, 4)
	got, err := AttributeBlock(batches, logs, testRollup)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, []int{0, 1}, appliedIndexes(&got[0]))
	require.Equal(t, repeatHash(0xa2), got[0].FinalState)
	require.Empty(t, got[1].Applied)
	require.Equal(t, []int{0, 1}, appliedIndexes(&got[2]))
	require.Equal(t, repeatHash(0xa2), got[2].EntryState)
	require.Equal(t, repeatHash(0xb2), got[2].FinalState)
}

func TestAttributeBlockSkippedBelongsToItsTx(t *testing.T) {
	b := batchChain(repeatHash(0x01), roots(0x11, 0x12), 2)
	skipped := blockLog(t, 0, 1, makeLog(t, &l2TxSkippedEvent, []common.Hash{common.BigToHash(big.NewInt(1))}, []byte{0xde}))
	got, err := AttributeBlock([]PostedBatch{{Batch: b, TxIndex: 0, LogIndex: 2}}, []*types.Log{
		rootLog(t, 0, 0, testRollup, repeatHash(0x11)), skipped, postedLog(t, 0, 2),
	}, testRollup)
	require.NoError(t, err)
	require.Equal(t, []int{0}, appliedIndexes(&got[0]))
}

// TestAttributeChiadoBlock attributes the live settlement's logs: its anchor
// entry applied, from the entry's current root to the registry's root.
func TestAttributeChiadoBlock(t *testing.T) {
	s, err := testfixtures.LoadChiadoSettlement()
	require.NoError(t, err)
	b, err := DecodePostAndVerifyBatch(s.Transaction.Input)
	require.NoError(t, err)
	var logs []*types.Log
	posted := PostedBatch{Batch: b, TxIndex: uint64(s.Transaction.TransactionIndex)}
	for i := range s.Logs {
		l := chiadoLog(&s.Logs[i])
		l.TxIndex, l.Index = hexutil.Uint(s.Transaction.TransactionIndex), hexutil.Uint(s.Logs[i].LogIndex)
		if l.Topics[0] == BatchPostedTopic {
			posted.LogIndex = uint64(s.Logs[i].LogIndex)
		}
		logs = append(logs, l)
	}
	got, err := AttributeBlock([]PostedBatch{posted}, logs, 1)
	require.NoError(t, err)
	require.Equal(t, []AppliedEntry{{Index: 0, Role: RoleAnchor}}, got[0].Applied)
	require.Equal(t, s.StateAtBlock.RollupRoot, got[0].FinalState)
	require.Equal(t, b.Entries[0].RollupUpdates[0].CurrentRoot, got[0].EntryState)
}

// TestAttributeWindow84 attributes the devnet batch with the
// L2ExecutionPerformed and BatchPosted logs its anchor entry emits.
func TestAttributeWindow84(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	b, err := DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	pre, err := w.OracleHash("window_pre_block_hash")
	require.NoError(t, err)
	post, err := w.OracleHash("window_post_block_hash")
	require.NoError(t, err)
	got, err := AttributeBlock([]PostedBatch{{Batch: b, TxIndex: 0, LogIndex: 1}},
		[]*types.Log{rootLog(t, 0, 0, testRollup, post), postedLog(t, 0, 1)}, testRollup)
	require.NoError(t, err)
	require.Equal(t, []AppliedEntry{{Index: 0, Role: RoleAnchor}}, got[0].Applied)
	require.Equal(t, pre, got[0].EntryState)
	require.Equal(t, post, got[0].FinalState)
}
