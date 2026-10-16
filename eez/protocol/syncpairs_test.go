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
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol/da"
)

var testSyncCtx = SyncTxContext{ChainID: 1, RollupID: 1}

// TestSyncPairsGolden rebuilds the reference client's golden inbound system
// transaction, built there at system nonce 7.
func TestSyncPairsGolden(t *testing.T) {
	s, err := NewInboundSidecar(goldenIncomingCall())
	require.NoError(t, err)
	pairs, err := BuildSyncPairs(nil, []ExecutionEntry{*s}, testSyncCtx, 7)
	require.NoError(t, err)
	require.Equal(t, []SyncPair{{SystemTx: common.FromHex(goldenInboundSystemTx)}}, pairs)
}

func outboundEntry(t *testing.T, value uint64, data []byte) *ExecutionEntry {
	t.Helper()
	e, err := EntryFromAction(&da.Action{
		SourceRollupID: testSyncCtx.RollupID,
		TargetRollupID: MainnetRollupID,
		SourceAddress:  common.HexToAddress("0xee"),
		TargetAddress:  common.HexToAddress("0xdd"),
		Value:          *uint256.NewInt(value),
		Data:           data,
		Success:        true,
		ReturnData:     []byte{0x01},
	}, testSyncCtx.RollupID)
	require.NoError(t, err)
	return e
}

func inboundSidecar(t *testing.T, rollup, value uint64) ExecutionEntry {
	t.Helper()
	c := goldenIncomingCall()
	c.RollupID, c.Value = rollup, new(big.Int).SetUint64(value)
	s, err := NewInboundSidecar(c)
	require.NoError(t, err)
	return *s
}

// TestSyncPairsOrder checks the Sync-block layout [load_i, user_i]…,
// deliver_j… and the system nonce sequence on synthetic entries.
func TestSyncPairsOrder(t *testing.T) {
	outbound := []OutboundSettlement{
		{Entry: outboundEntry(t, 0, []byte{0xa0}), UserTx: []byte{0xf0}},
		{Entry: outboundEntry(t, 5, []byte{0xa1}), UserTx: []byte{0xf1}},
	}
	inbound := []ExecutionEntry{inboundSidecar(t, 1, 0), inboundSidecar(t, 2, 3), inboundSidecar(t, 1, 9)}
	pairs, err := BuildSyncPairs(outbound, inbound, testSyncCtx, 10)
	require.NoError(t, err)
	require.Len(t, pairs, 4)

	for i, o := range outbound {
		tx, err := DecodeSystemTx(pairs[i].SystemTx)
		require.NoError(t, err)
		require.Equal(t, uint64(10+i), tx.Nonce)
		require.True(t, tx.Value.IsZero())
		require.Equal(t, o.UserTx, pairs[i].UserTx)
		c := &o.Entry.L2ToL1Calls[0]
		want, err := BuildOutgoingEntry(&OutgoingCall{c.SourceAddress, testSyncCtx.RollupID, c.TargetAddress, c.Value, c.Data, o.Entry.ReturnData})
		require.NoError(t, err)
		table, err := DecodeLoadExecutionTable(tx.Input)
		require.NoError(t, err)
		require.Len(t, table.Entries, 1)
		require.Equal(t, want.ProxyEntryHash, table.Entries[0].ProxyEntryHash)
		require.Equal(t, want.RollingHash, table.Entries[0].RollingHash)
	}
	for j, want := range []uint64{0, 9} {
		p := pairs[2+j]
		require.Nil(t, p.UserTx)
		tx, err := DecodeSystemTx(p.SystemTx)
		require.NoError(t, err)
		require.Equal(t, uint64(12+j), tx.Nonce)
		require.Equal(t, want, tx.Value.Uint64())
		table, err := DecodeExecuteIncomingCrossChainCall(tx.Input)
		require.NoError(t, err)
		require.Equal(t, 0, table.Entries[0].IncomingCalls[0].Value.Cmp(new(big.Int).SetUint64(want)))
	}

	require.Equal(t, [][]byte{
		pairs[0].SystemTx, {0xf0}, pairs[1].SystemTx, {0xf1}, pairs[2].SystemTx, pairs[3].SystemTx,
	}, SyncBlockTxs(pairs))
}

func TestSyncPairsRejects(t *testing.T) {
	user := []byte{0xf0}
	failed := outboundEntry(t, 0, nil)
	failed.Success = false
	withGas := outboundEntry(t, 0, nil)
	withGas.L2ToL1Calls[0].Gas = 1
	noCall := &ExecutionEntry{Success: true}
	for name, o := range map[string]OutboundSettlement{
		"failed outbound":  {Entry: failed, UserTx: user},
		"outbound gas":     {Entry: withGas, UserTx: user},
		"outbound no call": {Entry: noCall, UserTx: user},
		"no user tx":       {Entry: outboundEntry(t, 0, nil)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := BuildSyncPairs([]OutboundSettlement{o}, nil, testSyncCtx, 0)
			require.ErrorIs(t, err, ErrEntryShape)
		})
	}

	tampered := inboundSidecar(t, 2, 0)
	tampered.RollingHash[0] ^= 1
	_, err := BuildSyncPairs(nil, []ExecutionEntry{tampered}, testSyncCtx, 0)
	require.ErrorIs(t, err, ErrEntryShape, "foreign sidecars are validated too")

	_, err = BuildSyncPairs(nil, []ExecutionEntry{inboundSidecar(t, 1, 0)}, testSyncCtx, math.MaxUint64)
	require.ErrorIs(t, err, errNonceOverflow)
}
