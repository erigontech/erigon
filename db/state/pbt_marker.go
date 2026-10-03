// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package state

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/dir"
)

func readPBTMarker(path string, value any) (bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, value); err != nil {
		return true, err
	}
	return true, nil
}

func writePBTMarker(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writePBTMarkerData(path, data)
}

func writePBTMarkerData(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = dir.RemoveFile(tmpPath)
	}()
	if err := tmp.Chmod(0o644); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return dir.FsyncDir(filepath.Dir(path))
}

func removePBTMarker(path string) error {
	if err := dir.RemoveFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return dir.FsyncDir(filepath.Dir(path))
}
