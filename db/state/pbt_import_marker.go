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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
)

const PBTImportMarkerFileName = "import-pbt.in-progress.json"

type PBTImportMarker struct {
	SnapshotPath string            `json:"snapshot_path"`
	SnapshotHash string            `json:"snapshot_hash"`
	Settings     *ErigonDBSettings `json:"settings"`
}

func PBTImportMarkerPath(dirs datadir.Dirs) string {
	return filepath.Join(dirs.Snap, PBTImportMarkerFileName)
}

func ReadPBTImportMarker(dirs datadir.Dirs) (*PBTImportMarker, error) {
	data, err := os.ReadFile(PBTImportMarkerPath(dirs))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var marker PBTImportMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return nil, fmt.Errorf("decode %s: %w", PBTImportMarkerFileName, err)
	}
	if marker.SnapshotPath == "" || marker.SnapshotHash == "" || marker.Settings == nil {
		return nil, fmt.Errorf("decode %s: incomplete marker", PBTImportMarkerFileName)
	}
	return &marker, nil
}

func WritePBTImportMarker(dirs datadir.Dirs, marker *PBTImportMarker) error {
	if marker == nil || marker.SnapshotPath == "" || marker.SnapshotHash == "" || marker.Settings == nil {
		return errors.New("import-pbt marker is incomplete")
	}
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	if err := dir.WriteFileWithFsync(PBTImportMarkerPath(dirs), data, 0o644); err != nil {
		return err
	}
	return dir.FsyncDir(dirs.Snap)
}

func RemovePBTImportMarker(dirs datadir.Dirs) error {
	if err := dir.RemoveFile(PBTImportMarkerPath(dirs)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return dir.FsyncDir(dirs.Snap)
}
