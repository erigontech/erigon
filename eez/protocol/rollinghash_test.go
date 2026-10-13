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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
)

// TestRollingHashVectors is T-VEC-1: the folds RollingHashVectors.t.sol
// computes with the 855fe06 contracts, in the order its harness applies them.
func TestRollingHashVectors(t *testing.T) {
	v, err := testfixtures.LoadRollingHashVectors()
	require.NoError(t, err)
	// newRoot and etherDelta are not part of the seed; the oracle sets them
	// to show it.
	updates := []RollupUpdate{
		{RollupID: 1, EtherDelta: big.NewInt(1), CurrentRoot: common.HexToHash("0x11"), NewRoot: common.HexToHash("0xaa")},
		{RollupID: math.MaxUint64, EtherDelta: big.NewInt(-1), CurrentRoot: common.HexToHash("0x22"), NewRoot: common.HexToHash("0xbb")},
	}
	proxyEntryHash, callHash := common.HexToHash("0x33"), common.HexToHash("0x44")
	returnData := common.FromHex("0xaabbcc")

	r := L1EntrySeed(updates, proxyEntryHash)
	require.Equal(t, v.L1Seed, r.Sum())
	r.CallBegin(callHash)
	require.Equal(t, v.AfterCallBegin, r.Sum())
	r.CallEnd(true, returnData)
	require.Equal(t, v.AfterCallEnd, r.Sum())
	r.NestedBegin(callHash)
	require.Equal(t, v.AfterNestedBegin, r.Sum())
	r.NestedEnd()
	require.Equal(t, v.AfterNestedEnd, r.Sum())
	r.CallNotFound(callHash)
	require.Equal(t, v.AfterCallNotFound, r.Sum())

	l2 := L2EntrySeed(proxyEntryHash)
	require.Equal(t, v.L2Seed, l2.Sum())

	first := StaticResult(common.Hash{}, true, returnData)
	require.Equal(t, v.AfterStaticSuccess, first)
	require.Equal(t, v.AfterStaticFailure, StaticResult(first, false, common.FromHex("0xdeadbeef")))
}

// TestCallInsufficientGas covers the one tag without a Solidity vector.
func TestCallInsufficientGas(t *testing.T) {
	r := L2EntrySeed(common.HexToHash("0x33"))
	prev := r.Sum()
	r.CallInsufficientGas()
	require.Equal(t, common.BytesToHash(crypto.Keccak256(prev[:], []byte{6})), r.Sum())
}

// TestRecordedEntryRollingHashes recomputes the rolling hash of entries
// produced by the reference client: the anchor entries of the devnet and
// Chiado batches, which run no calls, and the alloy-encoded inbound L2 entry,
// whose proxy entry hash is the hash of its only call executed on rollup 1.
func TestRecordedEntryRollingHashes(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	s, err := testfixtures.LoadChiadoSettlement()
	require.NoError(t, err)
	for _, input := range [][]byte{w.PostBatch, s.Transaction.Input} {
		b, err := DecodePostAndVerifyBatch(input)
		require.NoError(t, err)
		e := &b.Entries[0]
		require.Empty(t, e.L2ToL1Calls)
		r := L1EntrySeed(e.RollupUpdates, e.ProxyEntryHash)
		require.Equal(t, e.RollingHash, r.Sum())
	}

	table, err := DecodeExecuteIncomingCrossChainCall(goldenInboundCalldata(t))
	require.NoError(t, err)
	e := &table.Entries[0]
	c := &e.IncomingCalls[0]
	callHash, err := CrossChainCallHash(&CallHashInput{
		IsStatic:       c.IsStatic,
		SourceAddress:  c.SourceAddress,
		SourceRollupID: c.SourceRollupID,
		TargetAddress:  c.TargetAddress,
		TargetRollupID: 1,
		Value:          c.Value,
		CallGas:        c.Gas,
		Data:           c.Data,
	})
	require.NoError(t, err)
	require.Equal(t, e.ProxyEntryHash, callHash)
	r := L2EntrySeed(e.ProxyEntryHash)
	r.CallBegin(callHash)
	r.CallEnd(e.Success, e.ReturnData)
	require.Equal(t, e.RollingHash, r.Sum())
}
