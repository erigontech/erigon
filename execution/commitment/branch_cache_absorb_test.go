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

package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBranchCacheFilesPublication(t *testing.T) {
	branchCache := NewBranchCache(64)
	t.Cleanup(branchCache.Close)
	publisher := branchCache.Publisher()
	publisher.Initialize(1)
	key := []byte{0x01}
	value := []byte{0xbb}

	publication := publisher.Begin()
	publication.Publish(2, []BranchUpdate{{
		Key:   key,
		Value: value,
		Step:  1,
		TxNum: 100,
	}}, false, nil)
	view := branchCache.View(2)
	got, _, ok := view.Get(key)
	require.True(t, ok)
	require.Equal(t, value, got)

	require.Nil(t, branchCache.BeginFilesPublication(101))
	_, _, ok = view.Get(key)
	require.True(t, ok, "files covered by committed updates must not clear the cache")

	change := branchCache.BeginFilesPublication(150)
	require.NotNil(t, change)
	_, _, ok = view.Get(key)
	require.False(t, ok, "foreign files must revoke the published generation")
	change.Finish()

	publication = publisher.Begin()
	publication.Publish(3, nil, false, nil)
	current := branchCache.View(3)
	_, _, ok = current.Get(key)
	require.False(t, ok, "the next commit must not reactivate entries from the old backing view")

	current.Fill(key, value, 1)
	require.Nil(t, branchCache.BeginFilesPublication(150))
	_, _, ok = current.Get(key)
	require.True(t, ok, "an already absorbed files view must not clear again")
}
