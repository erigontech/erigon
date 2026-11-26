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

package freezeblocks

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/types"
)

// BenchmarkTxnLookupInFiles finds the block of a transaction that is only in the block files.
func BenchmarkTxnLookupInFiles(b *testing.B) {
	dirs := datadir.New(b.TempDir())
	db := temporaltest.NewTestDB(b, dirs)
	logger := log.New()
	ver := version.V1_0

	key, err := crypto.GenerateKey()
	require.NoError(b, err)
	signer := types.LatestSignerForChainID(uint256.NewInt(1))
	to := common.Address{1}
	dynFee := func(nonce uint64) types.DynamicFeeTransaction {
		return types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce, To: &to, GasLimit: 1_000_000, Data: make([]byte, 100)},
			ChainID:  *uint256.NewInt(1),
			TipCap:   *uint256.NewInt(1),
			FeeCap:   *uint256.NewInt(2),
		}
	}
	cases := []struct {
		name string
		txn  types.Transaction
	}{
		{"legacy", types.NewTransaction(0, to, uint256.NewInt(1), 1_000_000, uint256.NewInt(1), make([]byte, 100))},
		{"dynamicfee", &types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: 1, To: &to, GasLimit: 1_000_000, Data: make([]byte, 100)}, ChainID: *uint256.NewInt(1), TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(2)}},
		{"setcode", &types.SetCodeTransaction{DynamicFeeTransaction: dynFee(2), Authorizations: make([]types.Authorization, 8)}},
	}
	txns := make([]types.Transaction, len(cases))
	for i, c := range cases {
		txns[i], err = types.SignTx(c.txn, *signer, key)
		require.NoError(b, err)
	}
	createTestSegmentFile(b, 1, 1000, snaptype2.Enums.Headers, dirs.Snap, ver, logger)
	createTestSegmentFile(b, 1, 1000, snaptype2.Enums.Bodies, dirs.Snap, ver, logger)
	createTestTxnSegment(b, 1, 1000, dirs.Snap, ver, txns, logger)

	snapshots := db.(HasBlockFiles).DebugBlockFiles()
	require.NoError(b, snapshots.OpenFolder())
	blockReader := NewBlockReader(snapshots)
	tx, err := db.BeginTemporalRo(b.Context())
	require.NoError(b, err)
	defer tx.Rollback()

	for i, c := range cases {
		hash := txns[i].Hash()
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, ok, err := blockReader.TxnLookup(b.Context(), tx, hash); err != nil || !ok {
					b.Fatal(ok, err)
				}
			}
		})
	}
}
