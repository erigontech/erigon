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

package txpool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type countingStateDB struct {
	kv.TemporalRoDB
	reads int
}

func (db *countingStateDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	db.reads++
	return db.TemporalRoDB.BeginTemporalRo(ctx)
}

func BenchmarkRemoteAdmission(b *testing.B) {
	for _, name := range []string{"dynamic_fee", "set_code"} {
		b.Run(name, func(b *testing.B) {
			ctx, pool, _, coreDB, sender := newTestPoolWithFundedSender(b, accounts.EmptyCodeHash)
			pool.started.Store(true)
			stateDB := &countingStateDB{TemporalRoDB: coreDB}
			pool._chainDB = stateDB
			key, err := crypto.GenerateKey()
			require.NoError(b, err)
			var batch TxnSlots
			for i := range 32 {
				txn := newTestTxnSlot(uint64(i), 0, 1, 2, 100_000)
				if name == "set_code" {
					auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, uint64(i))
					require.NoError(b, err)
					txn = newTestSetCodeTxnSlot(uint64(i), 0, 1, 2, 100_000)
					txn.Txn.(*types.SetCodeTransaction).Authorizations = []types.Authorization{auth}
				}
				txn.IDHash[0] = byte(i + 1)
				batch.Append(txn, sender[:], false)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j, txn := range batch.Txns {
					pool.AddRemoteTxns(ctx, TxnSlots{Txns: []*TxnSlot{txn}, Senders: batch.Senders.At(j), IsLocal: []bool{false}}, nil, nil)
				}
				require.NoError(b, pool.processRemoteTxns(ctx))
				b.StopTimer()
				require.Len(b, pool.byHash, len(batch.Txns))
				for _, mt := range pool.byHash {
					pool.removeFromSubPool(mt, "benchmark")
					pool.discardLocked(mt, txpoolcfg.Mined)
				}
				pool.discardReasonsLRU.Purge()
				pool.deletedTxns = nil
				for _, txn := range batch.Txns {
					txn.AuthAndNonces = nil // Each iteration represents fresh incoming transactions.
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(stateDB.reads)/float64(b.N), "state-reads/op")
		})
	}
}
