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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// goldenIncomingCall is the delivery behind goldenInboundSystemTx.
func goldenIncomingCall() *IncomingCall {
	return &IncomingCall{
		SourceAddress:  common.HexToAddress("0xcc"),
		SourceRollupID: MainnetRollupID,
		TargetAddress:  common.HexToAddress("0xbb"),
		RollupID:       1,
		Value:          new(big.Int),
		Data:           common.FromHex("0x1234"),
		ReturnData:     common.FromHex("0xabcd"),
	}
}

// TestIncomingEntryGolden rebuilds the calldata of the reference client's
// golden inbound system transaction from the delivered call.
func TestIncomingEntryGolden(t *testing.T) {
	e, err := BuildIncomingEntry(goldenIncomingCall())
	require.NoError(t, err)
	enc, err := EncodeExecuteIncomingCrossChainCall(&L2ExecutionTable{Entries: []L2ExecutionEntry{*e}})
	require.NoError(t, err)
	require.Equal(t, goldenInboundCalldata(t), enc)
}

func TestIncomingEntryHashes(t *testing.T) {
	c := goldenIncomingCall()
	c.Value = big.NewInt(42)
	e, err := BuildIncomingEntry(c)
	require.NoError(t, err)
	callHash, err := CrossChainCallHash(&CallHashInput{
		SourceAddress:  c.SourceAddress,
		SourceRollupID: c.SourceRollupID,
		TargetAddress:  c.TargetAddress,
		TargetRollupID: c.RollupID,
		Value:          c.Value,
		Data:           c.Data,
	})
	require.NoError(t, err)
	r := L2EntrySeed(callHash)
	r.CallBegin(callHash)
	r.CallEnd(true, c.ReturnData)
	require.Equal(t, callHash, e.ProxyEntryHash)
	require.Equal(t, r.Sum(), e.RollingHash)
	require.Len(t, e.IncomingCalls, 1)
	require.Zero(t, e.IncomingCalls[0].Gas)
	require.True(t, e.Success)
}

func TestOutgoingEntryHashes(t *testing.T) {
	c := &OutgoingCall{
		SourceAddress: common.HexToAddress("0xee"),
		RollupID:      1,
		TargetAddress: common.HexToAddress("0xdd"),
		Value:         big.NewInt(7),
		Data:          common.FromHex("0x5524107707"),
		ReturnData:    common.FromHex("0x01"),
	}
	e, err := BuildOutgoingEntry(c)
	require.NoError(t, err)
	callHash, err := CrossChainCallHash(&CallHashInput{
		SourceAddress:  c.SourceAddress,
		SourceRollupID: c.RollupID,
		TargetAddress:  c.TargetAddress,
		TargetRollupID: MainnetRollupID,
		Value:          c.Value,
		Data:           c.Data,
	})
	require.NoError(t, err)
	seed := L2EntrySeed(callHash)
	require.Equal(t, callHash, e.ProxyEntryHash)
	require.Equal(t, seed.Sum(), e.RollingHash)
	require.Empty(t, e.IncomingCalls)
	require.Equal(t, c.ReturnData, e.ReturnData)
}

// TestInboundSidecar requires the sidecar to round-trip, to equal the entry
// lowered from the same inbound action, and to reject any other encoding.
func TestInboundSidecar(t *testing.T) {
	c := goldenIncomingCall()
	s, err := NewInboundSidecar(c)
	require.NoError(t, err)
	back, err := IncomingCallFromSidecar(s)
	require.NoError(t, err)
	require.Equal(t, c, back)

	a, err := ActionFromEntry(s, c.RollupID)
	require.NoError(t, err)
	lowered, err := EntryFromAction(a, c.RollupID)
	require.NoError(t, err)
	want, err := EncodeExecutionEntry(s)
	require.NoError(t, err)
	got, err := EncodeExecutionEntry(lowered)
	require.NoError(t, err)
	require.Equal(t, want, got)

	for name, mutate := range map[string]func(e *ExecutionEntry){
		"failed":          func(e *ExecutionEntry) { e.Success = false },
		"gas":             func(e *ExecutionEntry) { e.L2ToL1Calls[0].Gas = 1 },
		"static":          func(e *ExecutionEntry) { e.L2ToL1Calls[0].IsStatic = true },
		"rolling hash":    func(e *ExecutionEntry) { e.RollingHash[0] ^= 1 },
		"proxy hash":      func(e *ExecutionEntry) { e.ProxyEntryHash[0] ^= 1 },
		"rollup update":   func(e *ExecutionEntry) { e.RollupUpdates = []RollupUpdate{{EtherDelta: new(big.Int)}} },
		"return data":     func(e *ExecutionEntry) { e.ReturnData = []byte{0xff} },
		"two calls":       func(e *ExecutionEntry) { e.L2ToL1Calls = append(e.L2ToL1Calls, e.L2ToL1Calls[0]) },
		"to L1":           func(e *ExecutionEntry) { e.DestinationRollupID = MainnetRollupID },
		"from own rollup": func(e *ExecutionEntry) { e.L2ToL1Calls[0].SourceRollupID = e.DestinationRollupID },
	} {
		t.Run(name, func(t *testing.T) {
			e, err := NewInboundSidecar(goldenIncomingCall())
			require.NoError(t, err)
			mutate(e)
			_, err = IncomingCallFromSidecar(e)
			require.ErrorIs(t, err, ErrEntryShape)
		})
	}
}
