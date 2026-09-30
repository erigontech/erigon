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

const PBTAttachMarkerFileName = "attach-pbt.in-progress.json"

type PBTAttachMarker struct {
	PublishedPath string            `json:"published_path"`
	Settings      *ErigonDBSettings `json:"settings"`
}

func PBTAttachMarkerPath(dirs datadir.Dirs) string {
	return filepath.Join(dirs.Snap, PBTAttachMarkerFileName)
}

func ReadPBTAttachMarker(dirs datadir.Dirs) (*PBTAttachMarker, error) {
	data, err := os.ReadFile(PBTAttachMarkerPath(dirs))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var marker PBTAttachMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return nil, fmt.Errorf("decode %s: %w", PBTAttachMarkerFileName, err)
	}
	if marker.PublishedPath == "" || marker.Settings == nil {
		return nil, fmt.Errorf("decode %s: incomplete marker", PBTAttachMarkerFileName)
	}
	return &marker, nil
}

func WritePBTAttachMarker(dirs datadir.Dirs, marker *PBTAttachMarker) error {
	if marker == nil || marker.PublishedPath == "" || marker.Settings == nil {
		return errors.New("attach-pbt marker is incomplete")
	}
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	return dir.WriteFileWithFsync(PBTAttachMarkerPath(dirs), data, 0o644)
}

func RemovePBTAttachMarker(dirs datadir.Dirs) error {
	if err := dir.RemoveFile(PBTAttachMarkerPath(dirs)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func RefusePBTAttachMarker(dirs datadir.Dirs) error {
	marker, err := ReadPBTAttachMarker(dirs)
	if err != nil {
		return err
	}
	if marker == nil {
		return nil
	}
	return fmt.Errorf("commitment attach-pbt is incomplete for %s; rerun attach-pbt --from %s", marker.PublishedPath, marker.PublishedPath)
}
