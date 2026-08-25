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
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

type e2ePBinFile struct {
	keys   [][]byte
	values [][]byte
}

func TestConvertPBinRecordFilesEndToEnd(t *testing.T) {
	setPBinTestFlags(t)
	db, source, sourceDirs := rebuildVariantDatadir(t)

	_, report, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), false,
		state.RebuildTarget{
			Variant:       commitment.VariantBinPatriciaTrie,
			HashName:      commitment.PBinHashBlake3,
			MaxShardSteps: 2,
		})
	require.NoError(t, err)
	require.NotEmpty(t, report.Ranges)

	view := source.BeginFilesRo()
	files := view.Files(kv.CommitmentDomain)
	view.Close()
	require.Len(t, files, 2)

	settings, err := state.ReadErigonDBSettings(sourceDirs)
	require.NoError(t, err)
	variant, hash := state.TrieVariantBin, commitment.PBinHashBlake3
	settings.TrieVariant = &variant
	settings.TrieHash = &hash
	require.NoError(t, state.WriteErigonDBSettings(sourceDirs, settings))

	for _, file := range files {
		rewritePBinFileAsLegacy(t, source, file)
	}

	sourceFiles := make(map[string]e2ePBinFile, len(files))
	for _, file := range files {
		keys, values := readKVFileWithCompression(t, file.Fullpath(), source.Cfg(kv.CommitmentDomain).Compression)
		sourceFiles[filepath.Base(file.Fullpath())] = e2ePBinFile{keys: keys, values: values}
	}

	require.NoError(t, dir.RemoveAll(sourceDirs.Migrations))
	sourceChecksum := checksumDataDir(t, sourceDirs.DataDir)
	tempBefore := regularFileSet(t, sourceDirs.Tmp)

	outputDirs := datadir.New(t.TempDir())
	linkSnapshotTree(t, sourceDirs.Snap, outputDirs.Snap)
	outputSettings, err := state.ReadErigonDBSettings(outputDirs)
	require.NoError(t, err)
	output := state.NewTest(outputDirs).
		StepSize(source.StepSize()).
		WithErigonDBSettings(outputSettings).
		Logger(log.New()).
		MustOpen(t.Context(), db)
	t.Cleanup(output.Close)
	require.NoError(t, output.OpenFolder())

	at := output.BeginFilesRo()
	err = state.ConvertPBinRecordFiles(t.Context(), at, log.New(), 2)
	at.Close()
	require.NoError(t, err)

	assertE2EStagedNonCommitmentHardlinks(t, sourceDirs.Snap, outputDirs.Snap)
	assertE2EConvertedPBinFiles(t, sourceFiles, output, sourceDirs.Snap, outputDirs.Snap)

	require.Equal(t, sourceChecksum, checksumDataDir(t, sourceDirs.DataDir))
	require.Equal(t, tempBefore, regularFileSet(t, sourceDirs.Tmp))
	_, err = os.Stat(sourceDirs.Migrations)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func assertE2EStagedNonCommitmentHardlinks(t *testing.T, sourceRoot, outputRoot string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(sourceRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || strings.Contains(entry.Name(), kv.CommitmentDomain.String()) {
			return nil
		}
		rel, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		sourceInfo, err := os.Stat(path)
		if err != nil {
			return err
		}
		outputInfo, err := os.Stat(filepath.Join(outputRoot, rel))
		if err != nil {
			return err
		}
		if !os.SameFile(sourceInfo, outputInfo) {
			return fmt.Errorf("%s is not staged as a hardlink", rel)
		}
		return nil
	}))
}

func assertE2EConvertedPBinFiles(t *testing.T, sourceFiles map[string]e2ePBinFile, output *state.Aggregator, sourceRoot, outputRoot string) {
	t.Helper()
	converter := commitment.NewPBinRecordConverter()
	sampledCells := 0
	stateRoots := 0
	for name, sourceFile := range sourceFiles {
		outputPath := filepath.Join(output.Dirs().SnapDomain, name)
		keys, values := readKVFileWithCompression(t, outputPath, output.Cfg(kv.CommitmentDomain).Compression)
		require.Len(t, keys, len(sourceFile.keys), name)
		require.Len(t, values, len(sourceFile.values), name)
		for i, key := range sourceFile.keys {
			require.Equal(t, key, keys[i], "%s pair %d key", name, i)
			sourceValue, outputValue := sourceFile.values[i], values[i]
			switch {
			case bytes.Equal(key, commitmentdb.KeyCommitmentState):
				require.NoError(t, state.VerifyPBinStateConversionForTest(sourceValue, outputValue), name)
				stateRoots++
			case isPBinRootKey(key):
				require.Equal(t, sourceValue, outputValue, "%s pair %d root", name, i)
			case len(sourceValue) > 0:
				if i%2 == 0 {
					require.NoError(t, converter.CompareLegacy(key, sourceValue, outputValue), "%s pair %d", name, i)
					sampledCells++
				}
				require.NotEqual(t, byte(0), outputValue[0], "%s pair %d remains legacy", name, i)
			}
		}

		sourceInfo, err := os.Stat(filepath.Join(sourceRoot, "domain", name))
		require.NoError(t, err)
		outputInfo, err := os.Stat(outputPath)
		require.NoError(t, err)
		require.False(t, os.SameFile(sourceInfo, outputInfo), "%s must be replaced in the output", name)
	}
	require.Positive(t, sampledCells)
	require.Positive(t, stateRoots)
}

func checksumDataDir(t *testing.T, root string) [sha256.Size]byte {
	t.Helper()
	h := sha256.New()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("%s is not a regular file", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		_, _ = h.Write([]byte(rel))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(data)
		_, _ = h.Write([]byte{0})
		return nil
	})
	require.NoError(t, err)
	var checksum [sha256.Size]byte
	copy(checksum[:], h.Sum(nil))
	return checksum
}

func regularFileSet(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("%s is not a regular file", path)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, rel)
		return nil
	})
	require.NoError(t, err)
	sort.Strings(files)
	return files
}
