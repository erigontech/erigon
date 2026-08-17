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

func newAbsorbFilesTestCache(t *testing.T) *StateCache {
	t.Helper()
	c := NewStateCache(1<<20, 1<<20, 1<<20, 1<<20)
	t.Cleanup(c.Close)
	return c
}

// State files can expose newer state without Commit publishing cache updates.
// Absorbing that extension must clear cached entries, advance the domain
// frontiers, and revoke older views so they cannot refill the cleared values.
func TestAbsorbFilesExtension(t *testing.T) {
	t.Parallel()

	c := newAbsorbFilesTestCache(t)
	key := make([]byte, 20)
	key[0] = 1
	preView := c.View(FrontierFunc(func(kv.Domain) (uint64, bool) { return 10, true }))
	preView.Fill(kv.AccountsDomain, key, []byte{1}, 5)
	_, ok := c.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "pre-publication fill lands on a cold cache")

	c.Applier().AbsorbFilesExtension(FrontierFunc(func(kv.Domain) (uint64, bool) { return 50, true }))

	_, ok = c.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "absorbing the extension must drop pre-publication entries")

	preView.Fill(kv.AccountsDomain, key, []byte{1}, 5)
	_, ok = c.View(nil).Get(kv.AccountsDomain, key)
	require.False(t, ok, "a pre-publication view must not refill past the absorbed extension")

	postView := c.View(FrontierFunc(func(kv.Domain) (uint64, bool) { return 50, true }))
	postView.Fill(kv.AccountsDomain, key, []byte{2}, 45)
	got, ok := c.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "a post-publication view fills normally")
	require.Equal(t, []byte{2}, got)
}

// If cache publication already covers the file frontiers, absorption must not
// clear valid entries or otherwise churn the cache.
func TestAbsorbFilesExtensionNoOpWhenCovered(t *testing.T) {
	t.Parallel()

	c := newAbsorbFilesTestCache(t)
	key := make([]byte, 20)
	key[0] = 1
	c.Applier().Initialize(0)
	c.Applier().Publish(0, 1, []StateUpdate{{Domain: kv.AccountsDomain, Key: key, Value: []byte{1}, TxNum: 100}})

	c.Applier().AbsorbFilesExtension(FrontierFunc(func(d kv.Domain) (uint64, bool) {
		if d == kv.AccountsDomain {
			return 50, true
		}
		return 0, false
	}))

	got, ok := c.View(nil).Get(kv.AccountsDomain, key)
	require.True(t, ok, "an already-covered extension must not drop applied entries")
	require.Equal(t, []byte{1}, got)
}
