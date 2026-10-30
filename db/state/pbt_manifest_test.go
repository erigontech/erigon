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

package state_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/state"
)

func TestPBTManifestRoundTripAndTamperChecks(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first.kv")
	second := filepath.Join(root, "second.kvi")
	require.NoError(t, os.WriteFile(first, []byte("first"), 0o644))
	require.NoError(t, os.WriteFile(second, []byte("second"), 0o644))
	sources := []state.PBTManifestFileSource{{Name: "first.kv", Path: first}, {Name: "second.kvi", Path: second}}
	manifest, err := state.BuildPBTManifest(context.Background(), state.PBTManifestPoint{Block: 7, TxNum: 11}, "pbt", "hex", "blake3", true, sources, 2, nil)
	require.NoError(t, err)
	path := filepath.Join(root, "pbt-manifest.toml")
	require.NoError(t, state.WritePBTManifest(path, manifest))
	got, err := state.ReadPBTManifest(path)
	require.NoError(t, err)
	require.Equal(t, manifest, got)
	validate := func() error {
		read, err := state.ReadPBTManifest(path)
		if err != nil {
			return err
		}
		return state.ValidatePBTManifestFiles(t.Context(), read, sources, 2, nil)
	}
	require.NoError(t, validate())

	t.Run("changed size", func(t *testing.T) {
		require.NoError(t, os.WriteFile(first, []byte("changed-size"), 0o644))
		err := validate()
		require.ErrorContains(t, err, "size mismatch")
		require.NoError(t, os.WriteFile(first, []byte("first"), 0o644))
	})
	t.Run("changed digest", func(t *testing.T) {
		require.NoError(t, os.WriteFile(first, []byte("other"), 0o644))
		err := validate()
		require.ErrorContains(t, err, "digest mismatch")
		require.NoError(t, os.WriteFile(first, []byte("first"), 0o644))
	})
	t.Run("missing file", func(t *testing.T) {
		require.NoError(t, dir.RemoveFile(first))
		err := validate()
		require.ErrorContains(t, err, "missing file")
		require.NoError(t, os.WriteFile(first, []byte("first"), 0o644))
	})
	t.Run("extra file", func(t *testing.T) {
		modified := manifest
		modified.Files = append(modified.Files, state.PBTManifestFile{Name: "extra.kv", Size: 0, Blake3: ""})
		require.NoError(t, state.WritePBTManifest(path, modified))
		err := validate()
		require.ErrorContains(t, err, "extra file")
		require.NoError(t, state.WritePBTManifest(path, manifest))
	})
	t.Run("duplicate file", func(t *testing.T) {
		modified := manifest
		modified.Files = append(modified.Files, manifest.Files[0])
		require.NoError(t, state.WritePBTManifest(path, modified))
		read, err := state.ReadPBTManifest(path)
		require.NoError(t, err)
		require.ErrorContains(t, state.ValidatePBTManifestFileSet(read, sources), "duplicate file name")
		require.NoError(t, state.WritePBTManifest(path, manifest))
	})
	t.Run("unknown format", func(t *testing.T) {
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(data), "format = 1", "format = 2", 1)), 0o644))
		_, err := state.ReadPBTManifest(path)
		require.ErrorContains(t, err, "unknown format")
	})
	t.Run("missing format", func(t *testing.T) {
		require.NoError(t, state.WritePBTManifest(path, manifest))
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		data = []byte(strings.Replace(string(data), "format = 1\n", "", 1))
		require.NoError(t, os.WriteFile(path, data, 0o644))
		_, err := state.ReadPBTManifest(path)
		require.ErrorContains(t, err, "unknown format 0")
	})
}
