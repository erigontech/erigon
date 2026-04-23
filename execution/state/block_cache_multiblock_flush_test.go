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
	cache.WriteStorage(addr, slot, []byte{0x01})
	require.NoError(t, cache.Flush(domains, tx, block1TxNum))

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
	cache.WriteStorage(addr, slot, nil)
	require.NoError(t, cache.Flush(domains, tx, block2TxNum))

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
