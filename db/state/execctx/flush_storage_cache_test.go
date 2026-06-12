// Copyright 2024 The Erigon Authors
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

package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
)

// TestCommit_UpdatesStorageStateCache is the deterministic regression for the
// stateCache stale-storage bug: storage values live in a dedicated ordered
// btree (sd.storage), not sd.domains[StorageDomain], so the flush-callback loop
// — which iterated sd.domains[domain] only — never fired the StorageDomain
// callback. The storage cache was therefore only ever read-populated and never
// refreshed on commit: once a slot was cached, a later write to it was invisible
// and the cache served the stale value on hit. (Surfaced under parallel exec as
// a swap reading a stale reserve → revert → gas mismatch.)
//
// The caches are commit-gated: they are populated only after tx.Commit succeeds
// (Commit stashes the flush-callback tuples and applies them post-commit), never
// by a bare Flush. The test commits a slot, then commits an overwrite in a second
// tx, and asserts the module-scope cache reflects the second write, not the first.
func TestCommit_UpdatesStorageStateCache(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	const stepSize = uint64(16)
	ctx := t.Context()
	db := newTestDb(t, stepSize)

	// composite storage key: 20-byte addr || 32-byte slot
	key := make([]byte, 52)
	key[0] = 0xab
	key[20] = 0x01

	val1 := []byte{0x01, 0x11}
	val2 := []byte{0x02, 0x22}

	// Module-scope cache, shared across the two commit cycles (the real cache is
	// aggregator-lifetime, outliving any single SharedDomains/tx).
	sc := cache.NewDefaultStateCache()

	commit := func(txNum uint64, val, prevVal []byte) {
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback() // safety net; Rollback after a successful Commit is a no-op

		sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer sd.Close()

		sd.SetStateCache(sc)
		if !sd.HasStateCache() {
			t.Skip("state cache disabled (USE_STATE_CACHE=false); coherence test needs it on")
		}

		sd.SetTxNum(txNum)
		require.NoError(t, sd.DomainPut(kv.StorageDomain, rwTx, key, val, txNum, prevVal))
		// Commit commits rwTx and only then applies the flush-callback tuples to
		// the cache. The StorageDomain callback must fire (iterate sd.storage).
		require.NoError(t, sd.Commit(ctx, rwTx))
	}

	// First commit: the storage callback must fire and populate the cache.
	commit(1, val1, nil)
	got, ok := sc.Get(kv.StorageDomain, key)
	require.True(t, ok, "storage cache must be populated by the commit callback")
	require.Equal(t, val1, got)

	// Overwrite in a second tx: the callback must fire again and refresh the
	// entry — not leave the stale val1 behind.
	commit(stepSize+1, val2, val1)
	got, ok = sc.Get(kv.StorageDomain, key)
	require.True(t, ok)
	require.Equal(t, val2, got, "commit must refresh the storage cache; stale value served on hit was the bug")
}
