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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol/da"
)

const localRollup = 1

func testAction(source, target uint64, value uint64, data []byte, success bool) *da.Action {
	return &da.Action{
		SourceRollupID: source,
		TargetRollupID: target,
		SourceAddress:  common.HexToAddress("0xcc"),
		TargetAddress:  common.HexToAddress("0xbb"),
		Value:          *uint256.NewInt(value),
		Data:           data,
		Success:        success,
		ReturnData:     []byte{0xab, 0xcd},
	}
}

// TestActionEntryRoundTrip requires lowering to be lossless both ways for the
// inbound and outbound shapes: derivation rebuilds entries from actions.
func TestActionEntryRoundTrip(t *testing.T) {
	for name, a := range map[string]*da.Action{
		"inbound":           testAction(MainnetRollupID, localRollup, 0, []byte{0x11, 0x22}, true),
		"inbound deposit":   testAction(MainnetRollupID, localRollup, 42, []byte{}, true),
		"inbound from peer": testAction(7, localRollup, 0, []byte{1}, true),
		"inbound failure":   testAction(MainnetRollupID, localRollup, 0, []byte{1}, false),
		"outbound":          testAction(localRollup, MainnetRollupID, 7, []byte{0x33}, true),
	} {
		t.Run(name, func(t *testing.T) {
			e, err := EntryFromAction(a, localRollup)
			require.NoError(t, err)
			back, err := ActionFromEntry(e, localRollup)
			require.NoError(t, err)
			require.Equal(t, a, back)
			again, err := EntryFromAction(back, localRollup)
			require.NoError(t, err)
			enc, err := EncodeExecutionEntry(e)
			require.NoError(t, err)
			reenc, err := EncodeExecutionEntry(again)
			require.NoError(t, err)
			require.Equal(t, enc, reenc)
		})
	}
}

func TestOutboundEntryLeavesHashesToSettlement(t *testing.T) {
	e, err := EntryFromAction(testAction(localRollup, MainnetRollupID, 7, nil, true), localRollup)
	require.NoError(t, err)
	require.Equal(t, common.Hash{}, e.ProxyEntryHash)
	require.Equal(t, common.Hash{}, e.RollingHash)
	require.Equal(t, uint64(localRollup), e.DestinationRollupID)
}

// TestInboundActionMatchesGoldenEntry lowers the action behind the reference
// client's golden inbound transaction and requires the hashes of the entry
// alloy encoded there.
func TestInboundActionMatchesGoldenEntry(t *testing.T) {
	table, err := DecodeExecuteIncomingCrossChainCall(goldenInboundCalldata(t))
	require.NoError(t, err)
	golden := &table.Entries[0]
	c := &golden.IncomingCalls[0]
	a := &da.Action{
		SourceRollupID: c.SourceRollupID,
		TargetRollupID: localRollup,
		SourceAddress:  c.SourceAddress,
		TargetAddress:  c.TargetAddress,
		Data:           c.Data,
		Success:        golden.Success,
		ReturnData:     golden.ReturnData,
	}
	e, err := EntryFromAction(a, localRollup)
	require.NoError(t, err)
	require.Equal(t, golden.ProxyEntryHash, e.ProxyEntryHash)
	require.Equal(t, golden.RollingHash, e.RollingHash)
	require.Equal(t, []CrossChainCall{*c}, e.L2ToL1Calls)
}

func TestLoweringRejects(t *testing.T) {
	_, err := EntryFromAction(testAction(2, 3, 0, nil, true), localRollup)
	require.Error(t, err)
	_, err = EntryFromAction(testAction(localRollup, 5, 0, nil, true), localRollup)
	require.Error(t, err)

	call := CrossChainCall{SourceRollupID: MainnetRollupID, Value: new(big.Int)}
	for name, e := range map[string]ExecutionEntry{
		"no call":        {DestinationRollupID: localRollup},
		"two calls":      {L2ToL1Calls: []CrossChainCall{call, call}, DestinationRollupID: localRollup},
		"expected calls": {L2ToL1Calls: []CrossChainCall{call}, ExpectedL1ToL2Calls: []ExpectedCall{{}}, DestinationRollupID: localRollup},
		"static":         {L2ToL1Calls: []CrossChainCall{{IsStatic: true, Value: new(big.Int)}}, DestinationRollupID: localRollup},
		"revert span":    {L2ToL1Calls: []CrossChainCall{{RevertNextNCalls: 1, Value: new(big.Int)}}, DestinationRollupID: localRollup},
		"foreign":        {L2ToL1Calls: []CrossChainCall{{SourceRollupID: 2, Value: new(big.Int)}}, DestinationRollupID: 3},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ActionFromEntry(&e, localRollup)
			require.ErrorIs(t, err, ErrEntryShape)
		})
	}
}

func TestCheckEntryShape(t *testing.T) {
	flat := CrossChainCall{Value: new(big.Int)}
	require.NoError(t, CheckEntryShape(&ExecutionEntry{L2ToL1Calls: []CrossChainCall{flat}, Success: true}))
	require.NoError(t, CheckEntryShape(&ExecutionEntry{Success: true}))
	for name, e := range map[string]ExecutionEntry{
		"multi-call":  {L2ToL1Calls: []CrossChainCall{flat, flat}, Success: true},
		"nested":      {L2ToL1Calls: []CrossChainCall{flat}, ExpectedL1ToL2Calls: []ExpectedCall{{}}, Success: true},
		"failed":      {L2ToL1Calls: []CrossChainCall{flat}},
		"static":      {L2ToL1Calls: []CrossChainCall{{IsStatic: true}}, Success: true},
		"revert span": {L2ToL1Calls: []CrossChainCall{{RevertNextNCalls: 2}}, Success: true},
		"gas":         {L2ToL1Calls: []CrossChainCall{{Gas: 21000}}, Success: true},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, CheckEntryShape(&e), ErrEntryShape)
		})
	}
}
