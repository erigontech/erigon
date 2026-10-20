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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
)

func writeEmptySeg(t *testing.T, dir string, typ snaptype.Type, from, to uint64) string {
	t.Helper()
	name := typ.FileName(version.ZeroVersion, from, to)
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte{0}, 0o644))
	return name
}

// snaptype.Segments sorts by range before type, so files of one type are not adjacent
// once several types share a range — the shape every erigon datadir has.
func TestFindOverlapsAcrossInterleavedTypes(t *testing.T) {
	dir := t.TempDir()

	writeEmptySeg(t, dir, snaptype2.Headers, 0, 500_000)
	writeEmptySeg(t, dir, snaptype2.Bodies, 0, 500_000)
	writeEmptySeg(t, dir, snaptype2.Transactions, 0, 500_000)
	subset := writeEmptySeg(t, dir, snaptype2.Headers, 100_000, 200_000)

	list, err := snaptype.Segments(dir)
	require.NoError(t, err)
	require.Len(t, list, 4)

	keep, overlapped := findOverlaps(list)

	names := make([]string, 0, len(overlapped))
	for _, f := range overlapped {
		names = append(names, f.Name())
	}
	require.Equal(t, []string{subset}, names, "a covered subset must be found even when another type sorts between it and its superset")
	require.Len(t, keep, 3)
}
