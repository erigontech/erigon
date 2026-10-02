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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
)

func TestPBTImportMarkerRoundTripAndInvalidStartupRefusal(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	variant := TrieVariantHexBin
	hash := "blake3"
	marker := &PBTImportMarker{SnapshotPath: "/tmp/snapshot", SnapshotHash: "digest", Files: []string{"domain/v3.0-commitment-bin.0-1.kv"}, Settings: &ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash}}
	require.NoError(t, WritePBTImportMarker(dirs, marker))
	got, err := ReadPBTImportMarker(dirs)
	require.NoError(t, err)
	require.Equal(t, marker, got)
	require.ErrorContains(t, RefusePBTImportMarker(dirs), "integration commitment import-pbt --datadir="+dirs.DataDir+" --snapshot=/tmp/snapshot")
	require.NoError(t, RemovePBTImportMarker(dirs))
	require.NoError(t, os.WriteFile(PBTImportMarkerPath(dirs), []byte("{"), 0o644))
	err = RefusePBTImportMarker(dirs)
	require.ErrorContains(t, err, "integration commitment import-pbt --datadir="+dirs.DataDir+" --snapshot=<snapshot>")
	require.NotContains(t, err.Error(), "marker is invalid")
	require.NotContains(t, err.Error(), "restore the previous")
}

func TestPBTImportMarkerRecoveryNamesCleanupRemedy(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	variant := TrieVariantHexBin
	previousVariant := TrieVariantHex
	hash := "blake3"
	marker := &PBTImportMarker{
		SnapshotPath:     "/tmp/snapshot",
		SnapshotHash:     "digest",
		Files:            []string{"domain/v3.0-commitment-bin.0-1.kv"},
		Settings:         &ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash},
		PreviousSettings: &ErigonDBSettings{TrieVariant: &previousVariant},
	}
	require.NoError(t, WritePBTImportMarker(dirs, marker))
	require.ErrorContains(t, RefusePBTImportMarker(dirs), "integration commitment import-pbt --datadir="+dirs.DataDir+" --snapshot=/tmp/snapshot")
	require.NotContains(t, RefusePBTImportMarker(dirs).Error(), "incomplete for")
}

func TestPBTImportMarkerAtomicReplacementIgnoresTargetMode(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	variant := TrieVariantHexBin
	hash := "blake3"
	old := &PBTImportMarker{SnapshotPath: "/tmp/old", SnapshotHash: "old", Files: []string{"old"}, Settings: &ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash}}
	newMarker := &PBTImportMarker{SnapshotPath: "/tmp/new", SnapshotHash: "new", Files: []string{"new"}, Settings: &ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash}}
	require.NoError(t, WritePBTImportMarker(dirs, old))
	require.NoError(t, os.Chmod(PBTImportMarkerPath(dirs), 0o444))
	require.NoError(t, WritePBTImportMarker(dirs, newMarker))
	got, err := ReadPBTImportMarker(dirs)
	require.NoError(t, err)
	require.Equal(t, newMarker, got)
}
