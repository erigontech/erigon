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

type file struct {
	start, end uint64
	path       string
}

func (f file) Fullpath() string         { return f.path }
func (f file) StartRootNum() uint64     { return f.start }
func (f file) EndRootNum() uint64       { return f.end }
func (f file) Version() version.Version { return version.Version{} }

func TestColdFiles(t *testing.T) {
	f0, f1, f2 := file{start: 0, end: 100}, file{start: 100, end: 150}, file{start: 150, end: 160}
	files := kv.VisibleFiles{f0, f1, f2}
	require.Empty(t, ColdFiles(files, 99))
	require.Equal(t, kv.VisibleFiles{f0}, ColdFiles(files, 100))
	require.Equal(t, kv.VisibleFiles{f0, f1}, ColdFiles(files, 159))
	require.Equal(t, files, ColdFiles(files, 160))
}

func TestLag(t *testing.T) {
	files := []File{
		{Start: 0, End: 100, Size: 1000, Path: "/hdd/domain/a.kv"},
		{Start: 100, End: 150, Size: 500, Path: "/ssd/domain/b.kv"},
		{Start: 150, End: 160, Size: 100, Path: "/ssd/domain/c.kv"},
		{Start: 160, End: 170, Size: 10, Path: "/ssd/domain/d.kv"},
	}
	require.Equal(t, Lag{
		ColdResident: Usage{Files: 1, Bytes: 1000},
		CopyPending:  Usage{Files: 1, Bytes: 500},
		Straddling:   Usage{Files: 1, Bytes: 100},
	}, ComputeLag(files, 155, "/hdd/domain"))

	require.Equal(t, Lag{CopyPending: Usage{Files: 2, Bytes: 1500}}, ComputeLag(files, 150, ""), "no cold dir; end == boundary is fully cold")
	require.Equal(t, Lag{CopyPending: Usage{Files: 1, Bytes: 1000}}, ComputeLag(files, 100, "/hdd/dom"), "prefix of a dir name is not the cold dir")
}

func TestKVFiles(t *testing.T) {
	kvFile, history, ii := file{path: "/d/domain/v2.0-accounts.0-8192.kv"}, file{path: "/d/history/v2.0-accounts.0-8192.v"}, file{path: "/d/idx/v2.0-accounts.0-8192.ef"}
	require.Equal(t, kv.VisibleFiles{kvFile}, KVFiles(kv.VisibleFiles{kvFile, history, ii}))
}
