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

package state

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Flush must not skip a delete because the pre-batch committedStorage
// snapshot is stale: the final write for a key must always propagate.
func TestBlockStateCacheFlushClearsAcrossBlocks(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	addr := accounts.InternAddress([20]byte{0x00, 0x00, 0x09, 0x61, 0xef, 0x48})
	slot := accounts.InternKey([32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01})
	slotVal := slot.Value()
	addrVal := addr.Value()
	composite := append(append([]byte(nil), addrVal[:]...), slotVal[:]...)

	cache := NewBlockStateCache()

	const block1TxNum uint64 = 100
	domains.SetTxNum(block1TxNum)
	cache.PutCommittedStorage(addr, slot, nil)
	cache.WriteStorage(addr, slot, []byte{0x01}, block1TxNum)
	require.NoError(t, cache.Flush(domains, tx))

	enc1, _, err := domains.GetLatest(kv.StorageDomain, tx, composite)
	require.NoError(t, err)
	require.True(t, bytes.Equal(enc1, []byte{0x01}),
		"after block 1 flush, domain should hold value 0x01, got %x", enc1)

	const block2TxNum uint64 = 200
	domains.SetTxNum(block2TxNum)
	cache.WriteStorage(addr, slot, nil, block2TxNum)
	require.NoError(t, cache.Flush(domains, tx))

	enc2, _, err := domains.GetLatest(kv.StorageDomain, tx, composite)
	require.NoError(t, err)
	require.Empty(t, enc2,
		"after block 2 flush, domain should be cleared (value=empty); "+
			"got %x — this is the 24839762 trie-root race: Flush skipped "+
			"the delete because committedStorage was never refreshed",
	)
}

func ctxFor(t testing.TB) context.Context { //nolint:unused
	t.Helper()
	return context.Background()
}

// Flush must stamp each write at its own tx's txNum, not the block's
// finalize txNum, so intra-block GetAsOf reads see per-tx history.
func TestBlockStateCacheFlushPreservesPerTxHistory(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	addr := accounts.InternAddress([20]byte{0xc0, 0x1b, 0xa5, 0xeb, 0xeb, 0xeb})
	addrVal := addr.Value()

	preAcc := accounts.NewAccount()
	preAcc.Balance.SetUint64(1000)
	preEnc := accounts.SerialiseV3(&preAcc)
	const preTxNum uint64 = 1
	domains.SetTxNum(preTxNum)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrVal[:], preEnc, preTxNum, nil))

	cache := NewBlockStateCache()
	cache.PutCommittedAccount(addr, &preAcc)

	tx3Acc := accounts.NewAccount()
	tx3Acc.Balance.SetUint64(1100)
	tx3Enc := accounts.SerialiseV3(&tx3Acc)
	cache.WriteAccount(addr, tx3Enc, 3)

	tx5Acc := accounts.NewAccount()
	tx5Acc.Balance.SetUint64(1300)
	tx5Enc := accounts.SerialiseV3(&tx5Acc)
	cache.WriteAccount(addr, tx5Enc, 5)

	domains.SetTxNum(5)
	require.NoError(t, cache.Flush(domains, tx))

	latest, _, err := domains.GetLatest(kv.AccountsDomain, tx, addrVal[:])
	require.NoError(t, err)
	require.Equal(t, tx5Enc, latest, "latest should be the tx-5 value")

	asOfTx3, ok, err := domains.GetAsOf(kv.AccountsDomain, addrVal[:], 4)
	require.NoError(t, err)
	require.True(t, ok, "GetAsOf at txNum=4 should find the tx-3 history entry")
	assert.Equal(t, tx3Enc, asOfTx3,
		"intra-block history at txNum=4 must show tx-3's post-state; "+
			"if this fails, Flush is collapsing per-tx writes onto a "+
			"single txNum and breaking history-reading consumers")

	asOfTx5, ok, err := domains.GetAsOf(kv.AccountsDomain, addrVal[:], 6)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, tx5Enc, asOfTx5, "history at txNum=6 should show tx-5's post-state")
}
