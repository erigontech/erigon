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
	"os/exec"
	"syscall"
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
	require.ErrorContains(t, RefusePBTImportMarker(dirs, "mainnet"), "integration commitment import-pbt --datadir="+dirs.DataDir+" --chain=mainnet --snapshot=/tmp/snapshot")
	require.NoError(t, RemovePBTImportMarker(dirs))
	require.NoError(t, os.WriteFile(PBTImportMarkerPath(dirs), []byte("{"), 0o644))
	err = RefusePBTImportMarker(dirs, "mainnet")
	require.ErrorContains(t, err, "integration commitment import-pbt --datadir="+dirs.DataDir+" --chain=mainnet --snapshot=<snapshot>")
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
	require.ErrorContains(t, RefusePBTImportMarker(dirs, "mainnet"), "integration commitment import-pbt --datadir="+dirs.DataDir+" --chain=mainnet --snapshot=/tmp/snapshot")
	require.NotContains(t, RefusePBTImportMarker(dirs, "mainnet").Error(), "incomplete for")
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

func TestPBTMarkerWriteFailureKeepsOldMarker(t *testing.T) {
	if os.Getenv("GO_WANT_PBT_MARKER_LIMIT_HELPER") == "1" {
		limit := &syscall.Rlimit{Cur: 4096, Max: 4096}
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, limit); err != nil {
			os.Exit(2)
		}
		dirs := datadir.New(os.Getenv("PBT_MARKER_DATADIR"))
		marker := &PBTImportMarker{SnapshotPath: string(make([]byte, 8192)), SnapshotHash: "hash", Files: []string{"file"}, Settings: &ErigonDBSettings{}}
		if err := WritePBTImportMarker(dirs, marker); err == nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	dirs := datadir.New(t.TempDir())
	path := PBTImportMarkerPath(dirs)
	old := []byte(`{"snapshot_path":"old"}`)
	require.NoError(t, os.WriteFile(path, old, 0o644))
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPBTMarkerWriteFailureKeepsOldMarker$", "-test.v")
	command.Env = append(os.Environ(),
		"GO_WANT_PBT_MARKER_LIMIT_HELPER=1",
		"PBT_MARKER_DATADIR="+dirs.DataDir,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, old, got)
}
