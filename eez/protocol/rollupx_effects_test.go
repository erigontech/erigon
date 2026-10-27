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
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol/da"
)

// replayBatch returns batch i of the eez.dev replay and its native payload.
func replayBatch(t *testing.T, i int) (*ProofSystemBatch, *da.NativePayload) {
	t.Helper()
	r, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	b := r.Batches[i]
	require.Len(t, b.SlotBlobs, 1)
	p, err := da.DecodeNative(b.Blobs(), r.RollupID)
	require.NoError(t, err)
	batch, err := DecodePostAndVerifyBatch(b.Transaction.Input)
	require.NoError(t, err)
	return batch, p
}

// TestRollupXDeposit rebuilds the Sync block of eez.dev L2 block 1950: the
// delivery of a 0.01 ETH deposit, whose system transaction the reference
// client mined as 0x4fb4890f…2678.
func TestRollupXDeposit(t *testing.T) {
	batch, p := replayBatch(t, 11)
	e, err := NewRollupXEffects(p.Txs, 1, 699069)
	require.NoError(t, err)
	require.Empty(t, e.Outbound)
	require.Len(t, e.Inbound, 1)
	require.NoError(t, e.Bind(batch))

	ctx := SyncTxContext{ChainID: 699069, RollupID: 1, EEZL2: RollupXEEZL2Address}
	txs, err := e.SyncSuffix(ctx, nil, []int{0}, 0)
	require.NoError(t, err)
	require.Len(t, txs, 1)
	require.Equal(t, common.HexToHash("0x4fb4890fd715062e52da029a1c3398fda4c97f47b9b0a8d95178f853cfa02678"), crypto.Keccak256Hash(txs[0]))
	tx, err := DecodeSystemTx(txs[0])
	require.NoError(t, err)
	require.Equal(t, uint64(10_000_000_000_000_000), tx.Value.Uint64())
	require.Len(t, tx.Input, 772)

	// Every settlement field binds.
	tampered := *batch
	tampered.Entries = append([]ExecutionEntry(nil), batch.Entries...)
	tampered.Entries[1].ReturnData = []byte{1}
	require.ErrorIs(t, e.Bind(&tampered), ErrBinding)
	tampered.Entries = batch.Entries[:1]
	require.ErrorIs(t, e.Bind(&tampered), ErrBinding)
	tampered = *batch
	tampered.ImmediateEntryCount = big.NewInt(2)
	require.ErrorIs(t, e.Bind(&tampered), ErrBinding)
}

func TestRollupXAnchorOnly(t *testing.T) {
	batch, p := replayBatch(t, 12)
	e, err := NewRollupXEffects(p.Txs, 1, 699069)
	require.NoError(t, err)
	require.NoError(t, e.Bind(batch))
	txs, err := e.SyncSuffix(SyncTxContext{ChainID: 699069, RollupID: 1, EEZL2: RollupXEEZL2Address}, nil, nil, 0)
	require.NoError(t, err)
	require.Empty(t, txs)
}

func TestRollupXSemanticsRejects(t *testing.T) {
	call := func(src, dst uint64) da.SemanticAction {
		return da.SemanticAction{Parent: -1, SourceRollupID: src, TargetRollupID: dst, Success: true}
	}
	for name, txs := range map[string][]da.SemanticTx{
		"no call":          {{Origin: 0}},
		"foreign origin":   {{Origin: 2, Actions: []da.SemanticAction{call(2, 1)}}},
		"foreign target":   {{Origin: 0, Actions: []da.SemanticAction{call(0, 2)}}},
		"unsigned origin":  {{Origin: 1, Actions: []da.SemanticAction{call(1, 0)}}},
		"outbound inbound": {{Origin: 0, Actions: []da.SemanticAction{call(0, 1)}}, {Origin: 1, Actions: []da.SemanticAction{call(1, 0)}}},
	} {
		_, err := NewRollupXEffects(txs, 1, 699069)
		require.ErrorIs(t, err, ErrSemantics, name)
	}
	nested := []da.SemanticTx{{Origin: 0, Actions: []da.SemanticAction{call(0, 1), {Parent: 0, SourceRollupID: 1}}}}
	_, err := NewRollupXEffects(nested, 1, 699069)
	require.ErrorIs(t, err, ErrUnsupportedSemantics)
}
