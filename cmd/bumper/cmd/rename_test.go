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

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	datadir2 "github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestRenameFilesDomainInvertedIndex(t *testing.T) {
	dirs := datadir2.New(t.TempDir())
	for _, f := range []string{
		filepath.Join(dirs.SnapIdx, "v1.0-code.0-128.ef"),
		filepath.Join(dirs.SnapAccessors, "v1.0-code.0-128.efi"),
	} {
		require.NoError(t, os.WriteFile(f, nil, 0o644))
	}

	_, err := renameFiles([]string{"code"}, []string{".ef", ".efi"}, dirs)
	require.NoError(t, err)

	ii := statecfg.Schema.GetDomainCfg(kv.CodeDomain).GetVersions().II
	require.FileExists(t, filepath.Join(dirs.SnapIdx, ii.DataEF.Current.String()+"-code.0-128.ef"))
	require.FileExists(t, filepath.Join(dirs.SnapAccessors, ii.AccessorEFI.Current.String()+"-code.0-128.efi"))
}
