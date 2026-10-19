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
	"os"
	"path/filepath"
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
		CopyPending:  Usage{Files: 1, Bytes: 500, Txs: 50},
		Straddling:   Usage{Files: 1, Bytes: 100, Txs: 5},
		Hot:          Usage{Files: 3, Bytes: 610},
		ColdEnd:      100,
		HotStart:     100,
	}, ComputeLag(files, 155, "/hdd/domain"))

	require.Equal(t, Lag{
		CopyPending: Usage{Files: 2, Bytes: 1500, Txs: 150},
		Hot:         Usage{Files: 4, Bytes: 1610},
	}, ComputeLag(files, 150, ""), "no cold dir; end == boundary is fully cold")
	require.Equal(t, Lag{
		CopyPending: Usage{Files: 1, Bytes: 1000, Txs: 100},
		Hot:         Usage{Files: 4, Bytes: 1610},
	}, ComputeLag(files, 100, "/hdd/dom"), "prefix of a dir name is not the cold dir")
	cold := []File{{Start: 0, End: 100, Size: 1000, Path: "/hdd/domain/a.kv"}, {Start: 100, End: 150, Size: 500, Path: "/hdd/domain/b.kv"}}
	require.Equal(t, Lag{
		ColdResident: Usage{Files: 2, Bytes: 1500},
		ColdEnd:      150,
		HotStart:     150,
	}, ComputeLag(cold, 200, "/hdd/domain"), "all files cold: the hot tier starts where the cold tier ends")
}

func TestKVFiles(t *testing.T) {
	kvFile, history, ii := file{path: "/d/domain/v2.0-accounts.0-8192.kv"}, file{path: "/d/history/v2.0-accounts.0-8192.v"}, file{path: "/d/idx/v2.0-accounts.0-8192.ef"}
	require.Equal(t, kv.VisibleFiles{kvFile}, KVFiles(kv.VisibleFiles{kvFile, history, ii}))
}

func writeFile(t *testing.T, path string, size int) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, make([]byte, size), 0o644))
}

func TestAccessorBytes(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "v2.1-commitment.0-8.kvi"), 30)
	writeFile(t, filepath.Join(dir, "v2.1-commitment.0-8.kvei"), 4)
	writeFile(t, filepath.Join(dir, "v2.1-commitment.8-9.bt"), 5)
	writeFile(t, filepath.Join(dir, "v2.1-commitment.9-10.kvi"), 700)
	writeFile(t, filepath.Join(dir, "v2.1-commitment.0-8.kvi.torrent"), 1000)
	writeFile(t, filepath.Join(dir, "v2.1-accounts.0-8.kvi"), 1000)
	files := []File{{Path: "/hdd/v2.2-commitment.0-8.kv"}, {Path: filepath.Join(dir, "v2.2-commitment.8-9.kv")}}
	n, err := AccessorBytes(dir, files)
	require.NoError(t, err)
	require.EqualValues(t, 39, n, "only accessors of the given files count, whatever their version or tier")
}

func TestDuplicateBytes(t *testing.T) {
	cold := t.TempDir()
	writeFile(t, filepath.Join(cold, "v2.2-commitment.8-9.kv"), 50)
	writeFile(t, filepath.Join(cold, "v2.2-commitment.0-8.kv"), 800)
	hot := []File{{Path: "/ssd/domain/v2.2-commitment.8-9.kv"}, {Path: "/ssd/domain/v2.2-commitment.9-10.kv"}}
	n, err := DuplicateBytes(cold, hot)
	require.NoError(t, err)
	require.EqualValues(t, 50, n)

	n, err = DuplicateBytes("", hot)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestValidateDemote(t *testing.T) {
	snapDomain, coldDir := t.TempDir(), t.TempDir()
	require.ErrorContains(t, ValidateDemote(0, coldDir, snapDomain, true), "lookback-blocks")
	require.ErrorContains(t, ValidateDemote(21_600, "", snapDomain, true), "cold-dir")
	require.ErrorContains(t, ValidateDemote(21_600, filepath.Join(coldDir, "missing"), snapDomain, true), "missing")
	require.ErrorContains(t, ValidateDemote(21_600, coldDir, snapDomain, false), "same filesystem")
	require.NoError(t, ValidateDemote(21_600, coldDir, snapDomain, true))
}
