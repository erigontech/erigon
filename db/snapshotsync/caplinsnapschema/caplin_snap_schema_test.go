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

package caplinsnapschema

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

func TestNewCaplinSchemaUsesRegisteredStateTypes(t *testing.T) {
	dirs := datadir.New(t.TempDir())

	for _, typ := range snaptype.CaplinStateSnapshotTypes {
		t.Run(typ.Name(), func(t *testing.T) {
			schemas := NewCaplinSchema(dirs, 1000, snapshotsync.SnapshotTypes{
				KeyValueGetters: map[string]snapshotsync.KeyValueGetter{typ.Name(): nil},
			})
			schema := schemas.GetState(typ.Name())

			dataFile, err := schema.DataFile(statecfg.Version{}, 0, 50_000)
			require.NoError(t, err)
			require.Equal(t, filepath.Join(dirs.SnapCaplin, typ.FileName(version.ZeroVersion, 0, 50_000)), dataFile)

			indexFile, err := schema.AccessorIdxFile(statecfg.Version{}, 0, 50_000, 0)
			require.NoError(t, err)
			require.Equal(t, filepath.Join(dirs.SnapCaplin, typ.IdxFileName(typ.Indexes()[0].Version.Current, 0, 50_000)), indexFile)
		})
	}
}
