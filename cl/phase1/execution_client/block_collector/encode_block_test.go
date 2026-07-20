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

package block_collector

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
)

func signedTestTx(t testing.TB, nonce uint64, data ...byte) types.Transaction {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	to := common.HexToAddress("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")
	txn, err := types.SignTx(types.NewTransaction(nonce, to, uint256.NewInt(1000), 21000, uint256.NewInt(1), data), *types.LatestSignerForChainID(uint256.NewInt(1)), key)
	require.NoError(t, err)
	return txn
}

// TestEncodeDecodeBlockRoundTrip pins the encodeBlock/decodeBlock contract:
// consecutive encodes on one collector reuse its scratch buffers, and each
// result must decode back to the original execution block.
func TestEncodeDecodeBlockRoundTrip(t *testing.T) {
	c := &PersistentBlockCollector{beaconChainCfg: &clparams.MainnetBeaconConfig}
	c.mu.Lock()
	defer c.mu.Unlock()

	parent := common.HexToHash("0xaa")
	tx0, tx1, tx2 := signedTestTx(t, 0), signedTestTx(t, 1), signedTestTx(t, 2)
	b1 := makeBeaconBlock(t, 1, 0x01, parent, tx0)
	b2 := makeBeaconBlock(t, 2, 0x02, blockHash(b1), tx1, tx2)

	for _, tc := range []struct {
		bb  *cltypes.BeaconBlock
		txs []types.Transaction
	}{
		{b1, []types.Transaction{tx0}},
		{b2, []types.Transaction{tx1, tx2}},
	} {
		payload := tc.bb.Body.ExecutionPayload
		encoded, err := c.encodeBlock(payload, tc.bb.ParentRoot, tc.bb.Body.GetExecutionRequestsList())
		require.NoError(t, err)

		decoded, bal, err := c.decodeBlock(encoded)
		require.NoError(t, err)
		require.Empty(t, bal)
		require.Equal(t, payload.BlockHash, decoded.Hash())
		require.Equal(t, payload.BlockNumber, decoded.NumberU64())
		require.Equal(t, payload.ParentHash, decoded.ParentHash())
		// decodeBlock leaves transactions undecoded, so check the raw body bytes.
		decodedTxs, err := types.DecodeTransactions(decoded.RawBody().Transactions)
		require.NoError(t, err)
		require.Len(t, decodedTxs, len(tc.txs))
		for i, txn := range tc.txs {
			require.Equal(t, txn.Hash(), decodedTxs[i].Hash())
		}
	}
}
