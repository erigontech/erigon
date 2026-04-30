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

// TestBlockStateCacheFlushClearsAcrossBlocks reproduces the trie-root race
// at block 24839762 (EIP-7002 predeploy slots 0x01/0x03).
//
// The parallel executor reuses one BlockStateCache across all blocks in a
// batch. When an early block's syscall writes value 0x01 to a storage slot
// and a later block's syscall writes 0x00 (clearing it), Flush's dedup
// compared the final value against `committedStorage[key]` — a lazily
// populated pre-batch snapshot that was never refreshed. If the pre-batch
// value happened to match the clearing value (both nil/zero), Flush
// **skipped** the write, leaving sd.mem stuck at the earlier block's
// stale 0x01. Subsequent commitment computation then saw value 0x01 for
// that slot and produced a wrong trie root.
//
// The fix removes the stale-committed dedup skip in Flush.
//
// The test seeds a slot at pre-batch value 0x00 in the domain, then
// simulates two block-level cache usages:
//
//	Block 1: SSTORE slot = 0x01, Flush → domain must hold 0x01
//	Block 2: SSTORE slot = 0x00 (clear), Flush → domain must hold 0x00
//
// Pre-fix: block 2's Flush sees current==committed==nil and skips the
// delete, so the domain still reads 0x01 afterwards. The test asserts
// the domain is cleared — post-fix it passes.
func TestBlockStateCacheFlushClearsAcrossBlocks(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	addr := accounts.InternAddress([20]byte{0x00, 0x00, 0x09, 0x61, 0xef, 0x48})
	slot := accounts.InternKey([32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01})
	slotVal := slot.Value()
	addrVal := addr.Value()
	composite := append(append([]byte(nil), addrVal[:]...), slotVal[:]...)

	// Share one cache across both blocks — this is the production pattern
	// in exec3_parallel.go (one BlockStateCache per blockExecutor/batch).
	cache := NewBlockStateCache()

	// Block 1: syscall reads slot first (populating committedStorage with
	// the pre-batch value, empty here), then writes 0x01, then Flush.
	// The read is what CachedReaderV3.ReadAccountStorage does on first
	// access — that's the production path that seeds committedStorage.
	const block1TxNum uint64 = 100
	domains.SetTxNum(block1TxNum)
	// Simulate the first read — value is empty (pre-batch slot is zero).
	// CachedReaderV3 caches this as committed[slot] = nil/empty.
	cache.PutCommittedStorage(addr, slot, nil)
	cache.WriteStorage(addr, slot, []byte{0x01}, block1TxNum)
	require.NoError(t, cache.Flush(domains, tx))

	enc1, _, err := domains.GetLatest(kv.StorageDomain, tx, composite)
	require.NoError(t, err)
	require.True(t, bytes.Equal(enc1, []byte{0x01}),
		"after block 1 flush, domain should hold value 0x01, got %x", enc1)

	// Block 2: syscall clears the slot back to 0x00. Prior to the fix the
	// Flush dedup compared the (nil) cleared value against the stale
	// `committedStorage[slot]` (still nil from a never-populated entry)
	// and skipped the delete. After the fix the delete propagates and the
	// domain no longer returns 0x01.
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

// ctxFor satisfies the expectation that NewTestRwTx-backed code paths use a
// valid context; kept as a small helper so the intent of the test above is
// crisp even if the helper signature evolves.
func ctxFor(t testing.TB) context.Context { //nolint:unused
	t.Helper()
	return context.Background()
}

// TestBlockStateCacheFlushPreservesPerTxHistory pins down the per-tx
// history-granularity invariant: when multiple txs in one block write
// to the same key (e.g. the coinbase, which receives a tip from every
// tx), Flush must replay each write at its own per-tx txNum so the
// AccountsDomain / StorageDomain history matches what the serial
// executor (and main's parallel executor) produce by calling DomainPut
// directly at per-tx txNum.
//
// Regression guard: an earlier version of Flush stamped every dirty
// entry with the block's finalize txNum, collapsing per-tx history
// entries into one. That broke intra-block GetAsOf reads —
// silently — because end-of-block trie roots were unaffected and
// only history-reading consumers (sd.GetAsOf at intra-block txNum,
// the commitment domain's calculator, eth_getBalance at historic
// blocks served via state replay) saw wrong values.
//
// The test seeds a coinbase-like account, pushes two writes at txNums
// 3 and 5, Flushes, then asserts:
//
//	GetAsOf(addr, txNum=3)  → tx-3 post-state (V1)
//	GetAsOf(addr, txNum=4)  → tx-3 post-state (V1, no write at txNum=4)
//	GetAsOf(addr, txNum=5)  → tx-5 post-state (V2)
//
// With last-writer-wins stamping, GetAsOf(txNum=3) would return the
// pre-block value (committed) because no history entry exists at
// txNum=3 — the test catches that.
func TestBlockStateCacheFlushPreservesPerTxHistory(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	addr := accounts.InternAddress([20]byte{0xc0, 0x1b, 0xa5, 0xeb, 0xeb, 0xeb})
	addrVal := addr.Value()

	// Pre-block: account exists at the test's first txNum boundary with
	// some baseline balance.
	preAcc := accounts.NewAccount()
	preAcc.Balance.SetUint64(1000)
	preEnc := accounts.SerialiseV3(&preAcc)
	const preTxNum uint64 = 1
	domains.SetTxNum(preTxNum)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addrVal[:], preEnc, preTxNum, nil))

	cache := NewBlockStateCache()
	cache.PutCommittedAccount(addr, &preAcc)

	// Tx 3 increments balance to 1100.
	tx3Acc := accounts.NewAccount()
	tx3Acc.Balance.SetUint64(1100)
	tx3Enc := accounts.SerialiseV3(&tx3Acc)
	cache.WriteAccount(addr, tx3Enc, 3)

	// Tx 5 increments balance to 1300.
	tx5Acc := accounts.NewAccount()
	tx5Acc.Balance.SetUint64(1300)
	tx5Enc := accounts.SerialiseV3(&tx5Acc)
	cache.WriteAccount(addr, tx5Enc, 5)

	// Block-end Flush.
	domains.SetTxNum(5)
	require.NoError(t, cache.Flush(domains, tx))

	// GetLatest must reflect the final write.
	latest, _, err := domains.GetLatest(kv.AccountsDomain, tx, addrVal[:])
	require.NoError(t, err)
	require.Equal(t, tx5Enc, latest, "latest should be the tx-5 value")

	// GetAsOf at the per-tx txNums must reflect each tx's post-state.
	// This requires the per-tx history entries to be present — which
	// only happens if Flush emitted DomainPut at each per-tx txNum.
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
