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

package tiering

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/version"
)

func TestColdBoundary(t *testing.T) {
	require.EqualValues(t, 25_000_000-216_000, ColdBoundary(25_000_000, 216_000))
	require.Zero(t, ColdBoundary(100_000, 216_000))
	require.Zero(t, ColdBoundary(216_000, 216_000))
}

type file struct{ start, end uint64 }

func (f file) Fullpath() string         { return "" }
func (f file) StartRootNum() uint64     { return f.start }
func (f file) EndRootNum() uint64       { return f.end }
func (f file) Version() version.Version { return version.Version{} }

func TestColdFiles(t *testing.T) {
	f0, f1, f2 := file{0, 100}, file{100, 150}, file{150, 160}
	files := kv.VisibleFiles{f0, f1, f2}
	require.Empty(t, ColdFiles(files, 99))
	require.Equal(t, kv.VisibleFiles{f0}, ColdFiles(files, 100))
	require.Equal(t, kv.VisibleFiles{f0, f1}, ColdFiles(files, 159))
	require.Equal(t, files, ColdFiles(files, 160))
}
