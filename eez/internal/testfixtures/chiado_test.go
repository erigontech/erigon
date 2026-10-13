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

package testfixtures

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// TestChiadoSettlement rebuilds the recorded transaction from its fields and
// requires the mined hash and the recorded sender, so a corrupted field fails.
func TestChiadoSettlement(t *testing.T) {
	s, err := LoadChiadoSettlement()
	require.NoError(t, err)
	tx := &s.Transaction

	u256 := func(name string, b *hexutil.Big) uint256.Int {
		v, overflow := b.ToUint256()
		require.False(t, overflow, name)
		return *v
	}
	chainID := u256("chainId", &tx.ChainID)
	require.Equal(t, s.L1ChainID, chainID.Uint64())
	require.True(t, chainID.IsUint64())
	require.Equal(t, uint64(types.BlobTxType), uint64(tx.Type))
	require.Equal(t, s.Registry, tx.To)
	require.Empty(t, tx.AccessList)
	require.Len(t, tx.BlobVersionedHashes, 1)
	require.Equal(t, uint64(1), s.ReceiptStatus)

	to := tx.To
	btx := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    uint64(tx.Nonce),
				GasLimit: uint64(tx.Gas),
				To:       &to,
				Value:    u256("value", &tx.Value),
				Data:     tx.Input,
				V:        *uint256.NewInt(uint64(tx.YParity)),
				R:        u256("r", &tx.R),
				S:        u256("s", &tx.S),
			},
			ChainID: chainID,
			TipCap:  u256("maxPriorityFeePerGas", &tx.MaxPriorityFeePerGas),
			FeeCap:  u256("maxFeePerGas", &tx.MaxFeePerGas),
		},
		MaxFeePerBlobGas:    u256("maxFeePerBlobGas", &tx.MaxFeePerBlobGas),
		BlobVersionedHashes: tx.BlobVersionedHashes,
	}
	require.Equal(t, tx.Hash, btx.Hash())
	sender, err := types.LatestSignerForChainID(&chainID).Sender(btx)
	require.NoError(t, err)
	require.Equal(t, tx.From, sender.Value())

	require.NotEmpty(t, s.Logs)
	for i, l := range s.Logs {
		require.Equal(t, s.Registry, l.Address, "log %d", i)
		require.NotEmpty(t, l.Topics, "log %d", i)
		if i > 0 {
			require.Greater(t, l.LogIndex, s.Logs[i-1].LogIndex, "log %d", i)
		}
	}

	// The ECDSA proof system's verification key is the left-padded signer.
	v := &s.StateAtBlock
	require.Equal(t, common.BytesToHash(v.ProofSystemSigner[:]), v.ManagerVerificationKey)
	require.Equal(t, s.RollupManager, v.RollupContract)
	require.NotZero(t, v.ManagerThreshold)
}

// TestChiadoRollupCreated checks the registration record against the
// settlement recorded later from the same registry.
func TestChiadoRollupCreated(t *testing.T) {
	r, err := LoadChiadoRollupCreated()
	require.NoError(t, err)
	s, err := LoadChiadoSettlement()
	require.NoError(t, err)
	require.Equal(t, s.L1ChainID, r.L1ChainID)
	require.Less(t, r.L1BlockNumber, s.L1BlockNumber)
	require.Less(t, r.L1BlockTimestamp, s.L1BlockTimestamp)
	require.Equal(t, s.Registry, r.Log.Address)
	require.Len(t, r.Log.Topics, 3)
	require.Equal(t, s.RollupManager, common.BytesToAddress(r.Log.Topics[2][12:]))
	require.Len(t, r.Log.Data, 32)
}
