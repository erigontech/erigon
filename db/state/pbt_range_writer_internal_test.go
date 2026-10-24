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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/version"
)

func TestPBinAccountFilesExcludeHistoryAndIndex(t *testing.T) {
	files := kv.VisibleFiles{
		pbinRangeWriterVisibleFile{path: "v2.0-accounts.0-1.kv", start: 0, end: 8},
		pbinRangeWriterVisibleFile{path: "v2.1-accounts.0-2.v", start: 0, end: 16},
		pbinRangeWriterVisibleFile{path: "v3.1-accounts.0-1.ef", start: 0, end: 8},
		pbinRangeWriterVisibleFile{path: "v2.0-accounts.1-2.kv", start: 8, end: 16},
	}
	got := pbinAccountFiles(files)
	require.Len(t, got, 2)
	require.Equal(t, "v2.0-accounts.0-1.kv", got[0].Fullpath())
	require.Equal(t, "v2.0-accounts.1-2.kv", got[1].Fullpath())
}

type pbinRangeWriterVisibleFile struct {
	path  string
	start uint64
	end   uint64
}

func (f pbinRangeWriterVisibleFile) Fullpath() string         { return f.path }
func (f pbinRangeWriterVisibleFile) StartRootNum() uint64     { return f.start }
func (f pbinRangeWriterVisibleFile) EndRootNum() uint64       { return f.end }
func (f pbinRangeWriterVisibleFile) Version() version.Version { return version.V2_0 }
