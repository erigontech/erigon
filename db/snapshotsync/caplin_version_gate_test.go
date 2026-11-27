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

package snapshotsync

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func recoverPanic(f func()) (p any) {
	defer func() { p = recover() }()
	f()
	return nil
}

func TestCaplinStateSnapshotOpenRejectsUnsupportedVersions(t *testing.T) {
	for _, ver := range []version.Version{{Major: 0, Minor: 1}, version.V2_0} {
		t.Run(ver.String(), func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			typ := snaptype.PendingDepositsDump
			segPath, _ := writeCaplinStateFixtureVersion(t, dirs.SnapCaplin, typ.Name(), 0, 2, ver, log.New())
			s := NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs, SnapshotTypes{
				KeyValueGetters: map[string]KeyValueGetter{typ.Name(): nil},
				Compression:     map[string]bool{},
			}, log.New())
			t.Cleanup(s.Close)

			panicValue := recoverPanic(func() { _ = s.OpenFolder() })
			require.Contains(t, fmt.Sprint(panicValue), filepath.Base(segPath))
			require.Contains(t, fmt.Sprint(panicValue), "Snapshot file")
		})
	}
}

func TestCaplinStateSnapshotOpenAcceptsSupportedVersions(t *testing.T) {
	for _, ver := range []version.Version{version.V1_0, version.V1_1, version.V1_2, {Major: 1, Minor: 9}} {
		t.Run(ver.String(), func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			typ := snaptype.PendingDepositsDump
			segPath, _ := writeCaplinStateFixtureVersion(t, dirs.SnapCaplin, typ.Name(), 0, 2, ver, log.New())
			s := NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs, SnapshotTypes{
				KeyValueGetters: map[string]KeyValueGetter{typ.Name(): nil},
				Compression:     map[string]bool{},
			}, log.New())
			t.Cleanup(s.Close)
			require.NoError(t, s.OpenFolder())
			require.Contains(t, s.Files(), filepath.Base(segPath))
		})
	}
}
