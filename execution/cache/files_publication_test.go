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

package cache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func TestStateCacheFilesPublication(t *testing.T) {
	stateCache, publisher := readyStateCache(t, 1)
	key := makeAddr(1)
	value := makeValue(1)

	publication := publisher.Begin()
	publication.Publish(testStateGeneration(2), 101, []Update{{
		Domain: kv.AccountsDomain,
		Key:    key,
		Value:  value,
		Step:   1,
	}}, false)
	view := stateCache.View(testStateGeneration(2))
	got, ok := view.Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, value, got)

	var filesEnd [kv.DomainLen]uint64
	filesEnd[kv.AccountsDomain] = 101
	change := stateCache.BeginFilesPublication(filesEnd)
	require.NotNil(t, change)
	_, ok = view.Get(kv.AccountsDomain, key)
	require.False(t, ok, "a files change must revoke views pinned to the old files")
	change.Finish()
	_, ok = stateCache.View(testStateGeneration(2)).Get(kv.AccountsDomain, key)
	require.False(t, ok, "a transaction first bound after publication must present the new files identity")
	covered := stateCache.View(StateGeneration(2, 101, 0, 0))
	_, ok = covered.Get(kv.AccountsDomain, key)
	require.True(t, ok, "files covered by committed updates must retain cache entries")

	filesEnd[kv.AccountsDomain] = 150
	change = stateCache.BeginFilesPublication(filesEnd)
	require.NotNil(t, change)
	_, ok = covered.Get(kv.AccountsDomain, key)
	require.False(t, ok, "foreign files must revoke the published generation")
	change.Finish()

	publication = publisher.Begin()
	publication.Publish(StateGeneration(3, 150, 0, 0), 150, nil, false)
	current := stateCache.View(StateGeneration(3, 150, 0, 0))
	_, ok = current.Get(kv.AccountsDomain, key)
	require.False(t, ok, "the next commit must not reactivate entries from the old backing view")

	current.Fill(kv.AccountsDomain, key, value, 1)
	require.Nil(t, stateCache.BeginFilesPublication(filesEnd))
	_, ok = current.Get(kv.AccountsDomain, key)
	require.True(t, ok, "an already absorbed files view must not clear again")
}

func TestStateCacheFilesPublicationRetainsSparseDomainsCoveredByCommit(t *testing.T) {
	stateCache, publisher := readyStateCache(t, 1)
	accountKey := makeAddr(1)
	accountValue := makeValue(1)
	storageKey := append(makeAddr(2), make([]byte, 32)...)
	storageValue := makeValue(2)
	stateCache.View(testStateGeneration(1)).Fill(kv.StorageDomain, storageKey, storageValue, 0)

	publication := publisher.Begin()
	publication.Publish(testStateGeneration(2), 101, []Update{{
		Domain: kv.AccountsDomain,
		Key:    accountKey,
		Value:  accountValue,
		Step:   1,
	}}, false)

	var filesEnd [kv.DomainLen]uint64
	filesEnd[kv.AccountsDomain] = 101
	filesEnd[kv.StorageDomain] = 101
	filesEnd[kv.CodeDomain] = 101
	change := stateCache.BeginFilesPublication(filesEnd)
	require.NotNil(t, change)
	change.Finish()

	view := stateCache.View(StateGeneration(2, 101, 101, 101))
	got, ok := view.Get(kv.AccountsDomain, accountKey)
	require.True(t, ok)
	require.Equal(t, accountValue, got)
	got, ok = view.Get(kv.StorageDomain, storageKey)
	require.True(t, ok)
	require.Equal(t, storageValue, got)
}

func TestStateCacheCanonicalClearResetsFileProvenance(t *testing.T) {
	stateCache, publisher, key := stateCacheWithPublishedCoverage(t)
	publication := publisher.Begin()
	publication.Publish(testStateGeneration(3), 0, nil, true)
	requireStateCacheForeignFilesClear(t, stateCache, key, 3)
}

func TestStateCacheInitializeMismatchResetsFileProvenance(t *testing.T) {
	stateCache, publisher, key := stateCacheWithPublishedCoverage(t)
	publisher.Initialize(testStateGeneration(3))
	requireStateCacheForeignFilesClear(t, stateCache, key, 3)
}

func stateCacheWithPublishedCoverage(t *testing.T) (*StateCache, Publisher, []byte) {
	t.Helper()
	stateCache, publisher := readyStateCache(t, 1)
	key := makeAddr(1)
	publication := publisher.Begin()
	publication.Publish(testStateGeneration(2), 101, []Update{{
		Domain: kv.AccountsDomain,
		Key:    key,
		Value:  makeValue(1),
		Step:   1,
	}}, false)
	return stateCache, publisher, key
}

func requireStateCacheForeignFilesClear(t *testing.T, stateCache *StateCache, key []byte, stateVersion uint64) {
	t.Helper()
	current := stateCache.View(testStateGeneration(stateVersion))
	current.Fill(kv.AccountsDomain, key, makeValue(2), 2)

	var filesEnd [kv.DomainLen]uint64
	filesEnd[kv.AccountsDomain] = 50
	change := stateCache.BeginFilesPublication(filesEnd)
	require.NotNil(t, change)
	change.Finish()

	_, ok := stateCache.View(StateGeneration(stateVersion, 50, 0, 0)).Get(kv.AccountsDomain, key)
	require.False(t, ok, "files not covered by the current lineage must clear cache entries")
}

func TestFilesPublicationBlocksCachePublicationUntilVisible(t *testing.T) {
	stateCache, publisher := readyStateCache(t, 1)
	var filesEnd [kv.DomainLen]uint64
	filesEnd[kv.AccountsDomain] = 1

	change := stateCache.BeginFilesPublication(filesEnd)
	require.NotNil(t, change)
	require.False(t, stateCache.generation.publicationMu.TryLock(),
		"cache publication must stay blocked while the backing-file view changes")

	change.Finish()
	locked := stateCache.generation.publicationMu.TryLock()
	require.True(t, locked)
	if locked {
		stateCache.generation.publicationMu.Unlock()
	}

	publication := publisher.Begin()
	publication.Abort()
}
