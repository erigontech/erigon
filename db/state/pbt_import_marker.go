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
	"errors"
	"fmt"
	"path/filepath"

	"github.com/erigontech/erigon/db/datadir"
)

const PBTImportMarkerFileName = "import-pbt.in-progress.json"

type PBTImportMarker struct {
	SnapshotPath     string            `json:"snapshot_path"`
	SnapshotHash     string            `json:"snapshot_hash"`
	Files            []string          `json:"files"`
	Settings         *ErigonDBSettings `json:"settings"`
	PreviousSettings *ErigonDBSettings `json:"previous_settings,omitempty"`
}

func PBTImportMarkerPath(dirs datadir.Dirs) string {
	return filepath.Join(dirs.Snap, PBTImportMarkerFileName)
}

func ReadPBTImportMarker(dirs datadir.Dirs) (*PBTImportMarker, error) {
	var marker PBTImportMarker
	found, err := readPBTMarker(PBTImportMarkerPath(dirs), &marker)
	if !found && err == nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", PBTImportMarkerFileName, err)
	}
	if marker.SnapshotPath == "" || marker.SnapshotHash == "" || len(marker.Files) == 0 || marker.Settings == nil {
		return nil, fmt.Errorf("decode %s: incomplete marker", PBTImportMarkerFileName)
	}
	return &marker, nil
}

func WritePBTImportMarker(dirs datadir.Dirs, marker *PBTImportMarker) error {
	if marker == nil || marker.SnapshotPath == "" || marker.SnapshotHash == "" || marker.Settings == nil {
		return errors.New("import-pbt marker is incomplete")
	}
	return writePBTMarker(PBTImportMarkerPath(dirs), marker)
}

func RemovePBTImportMarker(dirs datadir.Dirs) error {
	return removePBTMarker(PBTImportMarkerPath(dirs))
}

func RefusePBTImportMarker(dirs datadir.Dirs) error {
	marker, err := ReadPBTImportMarker(dirs)
	if err != nil {
		return fmt.Errorf("commitment import-pbt marker is invalid; remove %s and all commitment-bin files, then rerun import-pbt: %w", PBTImportMarkerPath(dirs), err)
	}
	if marker == nil {
		return nil
	}
	if marker.PreviousSettings != nil {
		return fmt.Errorf("commitment import-pbt is incomplete for %s; remove the marker and commitment-bin files, then rerun import-pbt --snapshot %s", marker.SnapshotPath, marker.SnapshotPath)
	}
	return fmt.Errorf("commitment import-pbt is incomplete for %s; rerun import-pbt --snapshot %s", marker.SnapshotPath, marker.SnapshotPath)
}
