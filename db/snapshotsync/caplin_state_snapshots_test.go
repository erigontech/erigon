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

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func newTestCaplinStateSnapshots(t *testing.T) *CaplinStateSnapshots {
	t.Helper()
	snapTypes := SnapshotTypes{
		KeyValueGetters: map[string]KeyValueGetter{"t": func(uint64) ([]byte, []byte, error) { return nil, nil, nil }},
		Compression:     map[string]bool{"t": false},
	}
	return NewCaplinStateSnapshots(
		ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		nil, datadir.New(t.TempDir()), snapTypes, log.New(),
	)
}

// TestCaplinStateSnapshots_BundleRefcount pins the reclamation invariants: a View
// pins the whole visible bundle with a single refcount and keeps reading its
// pinned generation across a concurrent republish (snapshot isolation); the
// generation chain collapses once the reader drains.
func TestCaplinStateSnapshots_BundleRefcount(t *testing.T) {
	require := require.New(t)
	c := newTestCaplinStateSnapshots(t)
	defer c.Close()

	v := c.View()
	require.Equal(int32(1), c.visible.Load().refcnt.Load())
	pinned := v.visible

	// A republish publishes a new generation; the live View must keep the one it
	// pinned, and the retired generation stays until the reader drains.
	c.recalcVisibleFiles(nil)
	require.NotSame(pinned, c.visible.Load(), "recalc must publish a new generation")
	require.Same(pinned, v.visible, "view keeps its pinned generation")
	require.NotSame(c.visible.Load(), c.oldestVisible, "retired generation still pinned by reader")

	v.Close()
	require.Zero(c.visible.Load().refcnt.Load())
	require.Same(c.visible.Load(), c.oldestVisible, "chain collapses once drained")
}
