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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol/da"
)

const matRollup uint64 = 1

// act is call i of a test forest, made from chain src to chain dst; its
// fields are distinct per index.
func act(i, parent int, src, dst uint64) da.SemanticAction {
	return da.SemanticAction{
		Parent:         parent,
		SourceRollupID: src,
		SourceAddress:  common.Address{19: byte(0x10 + i)},
		TargetRollupID: dst,
		TargetAddress:  common.Address{19: byte(0x20 + i)},
		Value:          *uint256.NewInt(uint64(i)),
		Data:           []byte{byte(0x30 + i)},
		Success:        true,
		ReturnData:     []byte{byte(0x40 + i)},
	}
}

func static(a da.SemanticAction) da.SemanticAction {
	a.Static, a.Value = true, uint256.Int{}
	return a
}

func failed(a da.SemanticAction) da.SemanticAction {
	a.Success, a.ReturnData = false, []byte("reverted")
	return a
}

func spanning(a da.SemanticAction, span int) da.SemanticAction {
	a.RevertSpan = span
	return a
}

// settle gives the L1 entries of tables root updates of matRollup and
// finalizes their traces.
func settle(t *testing.T, b *materializedBatch) {
	t.Helper()
	for i := range b.entries {
		root := common.Hash{byte(i + 1)}
		next := common.Hash{byte(i + 2)}
		b.entries[i].RollupUpdates = []RollupUpdate{{RollupID: matRollup, EtherDelta: new(big.Int), CurrentRoot: root, NewRoot: next}}
	}
	require.NoError(t, finalizeL1EntryTraces(b.entries, b.traces))
}

// roundTrip materializes calls starting on origin, settles the L1 tables,
// and verifies every L1 entry against the L2 tables realizing it. It
// returns the committed calls from the rollup to L1 the verifier finds.
func roundTrip(t *testing.T, calls []da.SemanticAction, origin uint64) []common.Hash {
	t.Helper()
	batches, err := materializeBatches(calls, origin)
	require.NoError(t, err)
	l1, l2 := batches[MainnetRollupID], batches[matRollup]
	require.NotNil(t, l1)
	settle(t, l1)
	var outbound []common.Hash
	if origin == matRollup {
		require.Len(t, l1.entries, 1, "one host per L2 transaction")
		hashes, err := VerifyOutbound(&l1.entries[0], l2.entries, l2.l2StaticEntries, matRollup)
		require.NoError(t, err)
		require.Equal(t, l2.sourceOutboundHashes, hashes)
		return hashes
	}
	applied, err := l1AppliedRootMask(calls)
	require.NoError(t, err)
	delivery := 0
	root := 0
	for i := range calls {
		if calls[i].Parent >= 0 || calls[i].Static {
			continue
		}
		if applied[i] {
			require.NotNil(t, l2)
			hashes, err := VerifyInbound(&l1.entries[root], &l2.entries[delivery], matRollup)
			require.NoError(t, err, "root %d", i)
			outbound = append(outbound, hashes...)
			delivery++
		} else {
			require.True(t, IsLookupOnly(&l1.entries[root], matRollup) || !l1.entries[root].Success || len(l1.entries[root].L2ToL1Calls) == 0, "root %d", i)
		}
		root++
	}
	if l2 != nil {
		require.Len(t, l2.entries, delivery)
	}
	return outbound
}

const l1 = MainnetRollupID

func TestMaterializeRoundTrip(t *testing.T) {
	r := matRollup
	for name, tc := range map[string]struct {
		origin uint64
		calls  []da.SemanticAction
	}{
		"inbound flat":              {l1, []da.SemanticAction{act(0, -1, l1, r)}},
		"inbound with callback":     {l1, []da.SemanticAction{act(0, -1, l1, r), act(1, 0, r, l1)}},
		"inbound three deep":        {l1, []da.SemanticAction{act(0, -1, l1, r), act(1, 0, r, l1), act(2, 1, l1, r)}},
		"inbound three roots":       {l1, []da.SemanticAction{act(0, -1, l1, r), act(1, -1, l1, r), act(2, 1, r, l1), act(3, -1, l1, r)}},
		"inbound two callbacks":     {l1, []da.SemanticAction{act(0, -1, l1, r), act(1, 0, r, l1), act(2, 0, r, l1)}},
		"inbound failed root":       {l1, []da.SemanticAction{failed(act(0, -1, l1, r))}},
		"inbound failed callback":   {l1, []da.SemanticAction{act(0, -1, l1, r), failed(act(1, 0, r, l1))}},
		"inbound reverted root":     {l1, []da.SemanticAction{spanning(act(0, -1, l1, r), 2), act(1, 0, r, l1), act(2, -1, l1, r)}},
		"inbound reverted callback": {l1, retriedCallback()},
		"inbound static root":       {l1, []da.SemanticAction{static(act(0, -1, l1, r)), act(1, -1, l1, r)}},
		"inbound static lookup":     {l1, []da.SemanticAction{act(0, -1, l1, r), static(act(1, 0, r, l1))}},
		"inbound static callbacks":  {l1, []da.SemanticAction{static(act(0, -1, l1, r)), static(act(1, 0, r, l1)), act(2, -1, l1, r)}},
		"outbound flat":             {r, []da.SemanticAction{act(0, -1, r, l1)}},
		"outbound with callback":    {r, []da.SemanticAction{act(0, -1, r, l1), act(1, 0, l1, r)}},
		"outbound ping-pong":        {r, []da.SemanticAction{act(0, -1, r, l1), act(1, 0, l1, r), act(2, 1, r, l1), act(3, 2, l1, r)}},
		"outbound two roots":        {r, []da.SemanticAction{act(0, -1, r, l1), act(1, 1-2, r, l1)}},
		"outbound failed root":      {r, []da.SemanticAction{failed(act(0, -1, r, l1))}},
		"outbound failed callback":  {r, []da.SemanticAction{act(0, -1, r, l1), failed(act(1, 0, l1, r))}},
		"outbound static read":      {r, []da.SemanticAction{static(act(0, -1, r, l1))}},
		"outbound static callback":  {r, []da.SemanticAction{static(act(0, -1, r, l1)), static(act(1, 0, l1, r))}},
		"outbound read and write":   {r, []da.SemanticAction{static(act(0, -1, r, l1)), act(1, -1, r, l1)}},
	} {
		t.Run(name, func(t *testing.T) {
			roundTrip(t, tc.calls, tc.origin)
		})
	}
}

// retriedCallback is an L1 call whose callback to L1 reverts in its frame
// and is made again: the retry reuses the reverted row.
func retriedCallback() []da.SemanticAction {
	callback := spanning(act(1, 0, matRollup, l1), 1)
	retry := callback
	retry.RevertSpan = 0
	return []da.SemanticAction{act(0, -1, l1, matRollup), callback, retry}
}

// TestRevertedRetryReusesItsSlot follows an L2 transaction whose first call
// to L1 reverts in its frame and is retried: both attempts take the same
// source cursor slot, and only the retry commits.
func TestRevertedRetryReusesItsSlot(t *testing.T) {
	first := spanning(act(0, -1, matRollup, l1), 1)
	retry := first
	retry.RevertSpan = 0
	hashes := roundTrip(t, []da.SemanticAction{first, retry}, matRollup)
	h, err := sourceCallHash(&retry)
	require.NoError(t, err)
	require.Equal(t, []common.Hash{h}, hashes)

	batches, err := materializeBatches([]da.SemanticAction{first, retry}, matRollup)
	require.NoError(t, err)
	require.Len(t, batches[matRollup].entries, 1)
	require.Equal(t, uint16(1), batches[l1].entries[0].L2ToL1Calls[0].RevertNextNCalls)
}

// TestOutboundCalls checks which calls the gate expects from a transaction:
// every committed call from the rollup to L1, nested ones included, none
// inside a failed or reverted frame.
func TestOutboundCalls(t *testing.T) {
	r := matRollup
	calls := []da.SemanticAction{act(0, -1, l1, r), act(1, 0, r, l1), failed(act(2, 0, r, l1))}
	hashes := roundTrip(t, calls, l1)
	h, err := sourceCallHash(&calls[1])
	require.NoError(t, err)
	require.Equal(t, []common.Hash{h}, hashes)

	calls = []da.SemanticAction{act(0, -1, r, l1), act(1, 0, l1, r), act(2, 1, r, l1)}
	hashes = roundTrip(t, calls, r)
	h0, _ := sourceCallHash(&calls[0])
	h2, _ := sourceCallHash(&calls[2])
	require.Equal(t, []common.Hash{h0, h2}, hashes)
}

func TestMaterializeRejects(t *testing.T) {
	r := matRollup
	valued := static(act(0, -1, l1, r))
	valued.Value = *uint256.NewInt(1)
	for name, tc := range map[string]struct {
		origin uint64
		calls  []da.SemanticAction
	}{
		"call to its own chain":          {l1, []da.SemanticAction{act(0, -1, l1, l1)}},
		"failed call with descendants":   {l1, []da.SemanticAction{failed(act(0, -1, l1, r)), act(1, 0, r, l1)}},
		"static call with value":         {l1, []da.SemanticAction{valued}},
		"nested static with descendants": {l1, []da.SemanticAction{act(0, -1, l1, r), static(act(1, 0, r, l1)), static(act(2, 1, l1, r))}},
		"mutable callback under a read":  {l1, []da.SemanticAction{static(act(0, -1, l1, r)), act(1, 0, r, l1)}},
		"root from another chain":        {l1, []da.SemanticAction{act(0, -1, r, l1)}},
		"child of the wrong chain":       {l1, []da.SemanticAction{act(0, -1, l1, r), act(1, 0, l1, r)}},
		"overlapping regions":            {l1, []da.SemanticAction{spanning(act(0, -1, l1, r), 2), spanning(act(1, 0, r, l1), 1)}},
		"region cutting a subtree":       {l1, []da.SemanticAction{act(0, -1, l1, r), act(1, 0, r, l1), spanning(act(2, -1, l1, r), 1), act(3, 2, r, l1)}},
		"parents out of preorder":        {l1, []da.SemanticAction{act(0, -1, l1, r), act(1, -1, l1, r), act(2, 0, r, l1)}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := materializeBatches(tc.calls, tc.origin)
			require.ErrorIs(t, err, ErrMaterialize)
		})
	}
}

// TestVerifyRejectsTampering changes each part of a settled nested pair in
// turn: the verifier recomputes both tables and rejects every change.
func TestVerifyRejectsTampering(t *testing.T) {
	r := matRollup
	calls := []da.SemanticAction{act(0, -1, l1, r), act(1, 0, r, l1), act(2, 1, l1, r)}
	batches, err := materializeBatches(calls, l1)
	require.NoError(t, err)
	settle(t, batches[l1])
	host, delivery := batches[l1].entries[0], batches[r].entries[0]
	_, err = VerifyInbound(&host, &delivery, r)
	require.NoError(t, err)

	for name, tamper := range map[string]func(h, d *ExecutionEntry){
		"row key":          func(h, d *ExecutionEntry) { d.ExpectedL1ToL2Calls[0].Hash[0] ^= 1 },
		"row result":       func(h, d *ExecutionEntry) { d.ExpectedL1ToL2Calls[0].ReturnData = []byte{9} },
		"nested call data": func(h, d *ExecutionEntry) { d.ExpectedL1ToL2Calls[0].Calls[0].Data = []byte{9} },
		"root return data": func(h, d *ExecutionEntry) { h.ReturnData = []byte{9} },
		"delivery hash":    func(h, d *ExecutionEntry) { d.RollingHash[0] ^= 1 },
		"host hash":        func(h, d *ExecutionEntry) { h.RollingHash[0] ^= 1 },
		"call value":       func(h, d *ExecutionEntry) { d.L2ToL1Calls[0].Value = big.NewInt(7) },
		"extra row": func(h, d *ExecutionEntry) {
			h.ExpectedL1ToL2Calls = append(h.ExpectedL1ToL2Calls, h.ExpectedL1ToL2Calls[0])
		},
		"delivery succeeded": func(h, d *ExecutionEntry) { d.ReturnData = append(bytes.Clone(d.ReturnData), 1) },
	} {
		t.Run(name, func(t *testing.T) {
			h, d := deepEntry(&host), deepEntry(&delivery)
			tamper(&h, &d)
			_, err := VerifyInbound(&h, &d, r)
			require.Error(t, err)
		})
	}
}

// deepEntry copies e and every slice under it.
func deepEntry(e *ExecutionEntry) ExecutionEntry {
	c := *e
	c.ReturnData = bytes.Clone(e.ReturnData)
	c.L2ToL1Calls = deepCalls(e.L2ToL1Calls)
	c.ExpectedL1ToL2Calls = make([]ExpectedCall, len(e.ExpectedL1ToL2Calls))
	for i, row := range e.ExpectedL1ToL2Calls {
		row.Calls = deepCalls(row.Calls)
		row.ReturnData = bytes.Clone(row.ReturnData)
		c.ExpectedL1ToL2Calls[i] = row
	}
	return c
}

func deepCalls(calls []CrossChainCall) []CrossChainCall {
	out := make([]CrossChainCall, len(calls))
	for i, c := range calls {
		c.Data = bytes.Clone(c.Data)
		c.Value = new(big.Int).Set(c.Value)
		out[i] = c
	}
	return out
}
