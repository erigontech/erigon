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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestRemoteSpammerKeepsLocalTransactions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		localFrom int
		localTo   int
		want      []uint64
	}{
		{"local", 0, 17, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{"local suffix", 9, 17, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{"local prefix", 0, 9, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8}},
		{"local middle", 12, 13, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
		{"remote", 17, 17, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, poolDB, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
			require.NoError(t, pool.start(ctx))
			pool.cfg.AccountSlots = 16
			pool.cfg.MaxNonceGap = 64

			for i := range 17 {
				txn := newTestTxnSlot(uint64(i), 0, 1, 2, 21_000)
				txn.IDHash[0] = byte(i + 1)
				var slots TxnSlots
				isLocal := i >= tc.localFrom && i < tc.localTo
				slots.Append(txn, sender[:], isLocal)
				if isLocal {
					reasons, err := pool.AddLocalTxns(ctx, slots)
					require.NoError(t, err)
					require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
				} else {
					pool.AddRemoteTxns(ctx, slots, nil, nil)
					require.NoError(t, pool.processRemoteTxns(ctx))
				}
			}
			pending, baseFee, queued := pool.CountContent()
			require.Equal(t, 17, pending)
			require.Zero(t, baseFee)
			require.Zero(t, queued)

			replay := newTestTxnSlot(82, 0, 1, 2, 21_000)
			replay.IDHash[0] = 99
			var slots TxnSlots
			slots.Append(replay, sender[:], true)
			reasons, err := pool.AddLocalTxns(ctx, slots)
			require.NoError(t, err)
			require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.NonceTooDistant}, reasons)
			require.NoError(t, poolDB.View(ctx, func(tx kv.Tx) error {
				known, err := pool.IdHashKnown(tx, replay.IDHash[:])
				require.NoError(t, err)
				require.False(t, known)
				return nil
			}))

			pool.AddRemoteTxns(ctx, slots, nil, nil)
			require.NoError(t, pool.processRemoteTxns(ctx))
			require.NotContains(t, pool.byHash, string(replay.IDHash[:]))
			var nonces []uint64
			pool.all.ascend(replay.SenderID, func(mt *metaTxn) bool {
				nonces = append(nonces, mt.TxnSlot.Nonce)
				return true
			})
			require.Equal(t, tc.want, nonces)
			pending, baseFee, queued = pool.CountContent()
			require.Equal(t, len(tc.want), pending)
			require.Zero(t, baseFee)
			require.Zero(t, queued)
		})
	}
}
