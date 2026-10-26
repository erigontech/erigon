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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestP2PPinsPreserveKnownHashPolicy(t *testing.T) {
	for _, reason := range []txpoolcfg.DiscardReason{txpoolcfg.NonceTooLow, txpoolcfg.NonceTooDistant} {
		t.Run(reason.String(), func(t *testing.T) {
			ctx, pool, db, _, _ := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			hash := common.Hash{1}
			staleRow := reason == txpoolcfg.NonceTooDistant
			if staleRow {
				require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
					return tx.Put(kv.PoolTransaction, hash[:], []byte{1})
				}))
			}
			pool.lock.Lock()
			pool.setDiscard(string(hash[:]), discardRecord{reason: reason})
			pool.lock.Unlock()
			release := pool.PinP2PHashes(hash[:])
			t.Cleanup(release)
			pool.discardReasonsLRU.Purge()
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				known, err := pool.IdHashKnown(tx, hash[:])
				require.NoError(t, err)
				require.False(t, known)
				return nil
			}))
			release()
			require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
				known, err := pool.IdHashKnown(tx, hash[:])
				require.NoError(t, err)
				require.Equal(t, staleRow, known)
				return nil
			}))
		})
	}
}
