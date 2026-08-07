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
	publication.Publish(2, []Update{{
		Domain: kv.AccountsDomain,
		Key:    key,
		Value:  value,
		Step:   1,
		TxNum:  100,
	}}, false)
	view := stateCache.View(2)
	got, ok := view.Get(kv.AccountsDomain, key)
	require.True(t, ok)
	require.Equal(t, value, got)

	var filesEnd [kv.DomainLen]uint64
	filesEnd[kv.AccountsDomain] = 101
	require.Nil(t, stateCache.BeginFilesPublication(filesEnd))
	_, ok = view.Get(kv.AccountsDomain, key)
	require.True(t, ok, "files covered by committed updates must not clear the cache")

	filesEnd[kv.AccountsDomain] = 150
	change := stateCache.BeginFilesPublication(filesEnd)
	require.NotNil(t, change)
	_, ok = view.Get(kv.AccountsDomain, key)
	require.False(t, ok, "foreign files must revoke the published generation")
	change.Finish()

	publication = publisher.Begin()
	publication.Publish(3, nil, false)
	current := stateCache.View(3)
	_, ok = current.Get(kv.AccountsDomain, key)
	require.False(t, ok, "the next commit must not reactivate entries from the old backing view")

	current.Fill(kv.AccountsDomain, key, value, 1)
	require.Nil(t, stateCache.BeginFilesPublication(filesEnd))
	_, ok = current.Get(kv.AccountsDomain, key)
	require.True(t, ok, "an already absorbed files view must not clear again")
}

func TestFilesPublicationBlocksCachePublicationUntilVisible(t *testing.T) {
	stateCache, publisher := readyStateCache(t, 1)
	var filesEnd [kv.DomainLen]uint64
	filesEnd[kv.AccountsDomain] = 1

	change := stateCache.BeginFilesPublication(filesEnd)
	require.NotNil(t, change)
	require.False(t, stateCache.version.publicationMu.TryLock(),
		"cache publication must stay blocked while the backing-file view changes")

	change.Finish()
	locked := stateCache.version.publicationMu.TryLock()
	require.True(t, locked)
	if locked {
		stateCache.version.publicationMu.Unlock()
	}

	publication := publisher.Begin()
	publication.Abort()
}
