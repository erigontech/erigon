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
package da

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
)

// replayBlobs returns the blobs of batch i of the eez.dev replay: of its
// slot's blobs, those the batch's transaction names, found by trying each.
func replayBlobs(t *testing.T, i int) [][]byte {
	t.Helper()
	r, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	b := r.Batches[i]
	require.Len(t, b.Transaction.BlobVersionedHashes, 1)
	for _, blob := range b.Blobs() {
		if p, err := DecodeNative([][]byte{blob}, r.RollupID); err == nil && p != nil {
			msgs, err := Decode([][]byte{blob}, nil)
			require.NoError(t, err)
			if s, err := DecodeRollupXSpan(msgs[0].Data); err == nil && uint64(len(s.TxCounts)) == b.L2Range[1]-b.L2Range[0]+1 {
				return [][]byte{blob}
			}
		}
	}
	t.Fatalf("batch %d: no blob decodes to its range", i)
	return nil
}

func TestDecodeNativeReplay(t *testing.T) {
	// Batch 11 settles L2 blocks 1901 to 1950 and a deposit.
	p, err := DecodeNative(replayBlobs(t, 11), 1)
	require.NoError(t, err)
	require.Len(t, p.Operations, 26)
	require.Len(t, p.Txs, 1)
	tx := p.Txs[0]
	require.Zero(t, tx.Origin)
	require.Empty(t, tx.TxData)
	require.Equal(t, []SemanticAction{{
		Parent:         -1,
		SourceRollupID: 0,
		SourceAddress:  common.HexToAddress("0x79acb6eab88c13b4696a9f290190dbf50edf563f"),
		TargetRollupID: 1,
		TargetAddress:  common.HexToAddress("0xd47258b613950f125983bb34354a5b310f7fadd3"),
		Value:          *uint256.NewInt(10_000_000_000_000_000),
		Data:           []byte{},
		Success:        true,
		ReturnData:     []byte{},
	}}, tx.Actions)

	// Batch 12 carries two pure L2 transactions and no cross-chain one.
	p, err = DecodeNative(replayBlobs(t, 12), 1)
	require.NoError(t, err)
	require.Empty(t, p.Txs)
	s, err := DecodeRollupXSpan(p.Operations)
	require.NoError(t, err)
	require.Len(t, s.Transactions, 2)

	_, err = DecodeNative(replayBlobs(t, 12), 2)
	require.ErrorIs(t, err, ErrProfile)
}

func TestDecodeNativeRejects(t *testing.T) {
	blob := replayBlobs(t, 11)[0]
	// The stream takes 141 bytes; field element 10 holds padding.
	padded := append([]byte(nil), blob...)
	padded[10*32+5] = 1
	_, err := DecodeNative([][]byte{padded}, 1)
	require.ErrorIs(t, err, ErrPadding)
	_, err = DecodeNative(nil, 1)
	require.ErrorIs(t, err, ErrProfile)

	call := func(to uint64, gas uint64) Message {
		return Message{Type: Call, RollupID: to, Gas: gas}
	}
	ret := Message{Type: ReturnSuccess}
	initiate := Message{Type: InitiateCrossRollupTransaction}
	finish := Message{Type: FinishCrossRollupTransaction}
	for name, msgs := range map[string][]Message{
		"call gas":        {initiate, call(1, 5), ret, finish},
		"empty region":    {initiate, {Type: Snapshot}, {Type: Revert}, call(1, 0), ret, finish},
		"revert in call":  {initiate, {Type: Snapshot}, call(1, 0), {Type: Revert}, ret, finish},
		"open at finish":  {initiate, call(1, 0), finish},
		"outside":         {call(1, 0), ret},
		"unfinished":      {initiate, call(1, 0), ret},
		"overlap regions": {initiate, {Type: Snapshot}, {Type: Snapshot}, call(1, 0), ret, {Type: Revert}, {Type: Revert}, finish},
	} {
		_, err := SemanticTxs(msgs)
		require.ErrorIs(t, err, ErrSemantic, name)
	}
	deep := []Message{initiate}
	for range MaxDepth + 1 {
		deep = append(deep, call(1, 0))
	}
	_, err = SemanticTxs(deep)
	require.ErrorIs(t, err, ErrSemantic)

	txs, err := SemanticTxs([]Message{initiate, {Type: Snapshot}, call(1, 0), call(0, 0), {Type: ReturnFail}, ret, {Type: Revert}, finish})
	require.NoError(t, err)
	require.Equal(t, []int{-1, 0}, []int{txs[0].Actions[0].Parent, txs[0].Actions[1].Parent})
	require.Equal(t, 2, txs[0].Actions[0].RevertSpan)
	require.False(t, txs[0].Actions[1].Success)
}
