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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

type pbinOutputFixture struct {
	db          kv.TemporalRwDB
	source      *state.Aggregator
	output      *state.Aggregator
	sourcePath  string
	outputPath  string
	sourceBytes []byte
}

func newPBinOutputFixture(t *testing.T, legacy bool, smallOnly bool) pbinOutputFixture {
	t.Helper()
	setPBinTestFlags(t)

	db, source, _ := rebuildVariantDatadir(t)
	_, _, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), false,
		state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: commitment.PBinHashBlake3})
	require.NoError(t, err)

	sourceView := source.BeginFilesRo()
	files := sourceView.Files(kv.CommitmentDomain)
	sourceView.Close()
	require.NotEmpty(t, files)
	var selected kv.VisibleFile
	for _, file := range files {
		span := file.EndRootNum() - file.StartRootNum()
		if (smallOnly && span < state.DomainMinStepsToCompress) ||
			(!smallOnly && (selected == nil || span > selected.EndRootNum()-selected.StartRootNum())) {
			selected = file
		}
	}
	require.NotNil(t, selected)

	if legacy {
		rewritePBinFileAsLegacy(t, source, selected)
	}

	sourceBytes, err := os.ReadFile(selected.Fullpath())
	require.NoError(t, err)

	outputDirs := datadir.New(t.TempDir())
	linkSnapshotTree(t, source.Dirs().Snap, outputDirs.Snap)
	keepOnlyCommitmentRange(t, outputDirs.SnapDomain, filepath.Base(selected.Fullpath()))
	settings, err := state.ReadErigonDBSettings(source.Dirs())
	require.NoError(t, err)
	output := state.NewTest(outputDirs).
		StepSize(source.StepSize()).
		WithErigonDBSettings(settings).
		Logger(log.New()).
		MustOpen(t.Context(), db)
	require.NoError(t, output.OpenFolder())
	if legacy {
		keys, values := readKVFile(t, output, filepath.Join(outputDirs.SnapDomain, filepath.Base(selected.Fullpath())))
		legacyCount := 0
		for i, key := range keys {
			if !bytes.Equal(key, commitmentdb.KeyCommitmentState) && isPBinRootKey(key) {
				continue
			}
			if len(values[i]) > 0 && values[i][0] == 0 {
				legacyCount++
			}
		}
		require.Positive(t, legacyCount)
	}

	return pbinOutputFixture{
		db:          db,
		source:      source,
		output:      output,
		sourcePath:  selected.Fullpath(),
		outputPath:  filepath.Join(outputDirs.SnapDomain, filepath.Base(selected.Fullpath())),
		sourceBytes: sourceBytes,
	}
}

func keepOnlyCommitmentRange(t *testing.T, dirPath, selectedName string) {
	t.Helper()
	_, suffix, ok := strings.Cut(selectedName, "-commitment.")
	require.True(t, ok)
	rangeName := strings.TrimSuffix(suffix, filepath.Ext(suffix))
	entries, err := os.ReadDir(dirPath)
	require.NoError(t, err)
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "-commitment.") && !strings.Contains(entry.Name(), "."+rangeName+".") {
			require.NoError(t, dir.RemoveFile(filepath.Join(dirPath, entry.Name())))
		}
	}
}

func setPBinTestFlags(t *testing.T) {
	t.Helper()
	oldBin := statecfg.ExperimentalBinCommitment
	oldHash := statecfg.BinCommitmentHash
	oldSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = oldBin
		statecfg.BinCommitmentHash = oldHash
		require.NoError(t, commitment.SetPBinHashSuite(oldSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func rewritePBinFileAsLegacy(t *testing.T, agg *state.Aggregator, file kv.VisibleFile) {
	t.Helper()
	path := file.Fullpath()
	cfg := agg.Cfg(kv.CommitmentDomain)
	compression := cfg.Compression
	keys, values := readKVFileWithCompression(t, path, compression)
	require.NoError(t, dir.RemoveFile(path))

	comp, err := seg.NewCompressor(t.Context(), "pbin legacy fixture", path, agg.Dirs().Tmp, cfg.CompressCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	w := seg.NewWriter(comp, cfg.Compression)
	for i := range keys {
		value := values[i]
		switch {
		case bytes.Equal(keys[i], commitmentdb.KeyCommitmentState):
			value, err = legacyPBinStateValue(value)
		case isPBinRootKey(keys[i]):
			value = append([]byte(nil), value...)
		case len(value) > 0:
			value, err = commitment.PBinEncodeLegacyRecord(keys[i], value)
		}
		require.NoError(t, err)
		_, err = w.Write(keys[i])
		require.NoError(t, err)
		_, err = w.Write(value)
		require.NoError(t, err)
	}
	require.NoError(t, comp.Compress())
	comp.Close()
}

func legacyPBinStateValue(value []byte) ([]byte, error) {
	if commitment.IsPBinState(value) {
		return commitment.PBinEncodeLegacyState(value)
	}
	if len(value) < 18 || !commitment.IsPBinState(value[18:]) {
		return nil, fmt.Errorf("unexpected pbin state value %x", value)
	}
	legacy, err := commitment.PBinEncodeLegacyState(value[18:])
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), value[:18]...)
	binary.BigEndian.PutUint16(out[16:18], uint16(len(legacy)))
	return append(out, legacy...), nil
}

func isPBinRootKey(key []byte) bool {
	return len(key) == 1 && key[0] == 0x08
}

func readKVFileWithCompression(t *testing.T, path string, compression seg.FileCompression) ([][]byte, [][]byte) {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	r := seg.NewReader(d.MakeGetter(), compression)
	r.Reset(0)
	var keys, values [][]byte
	for r.HasNext() {
		key, _ := r.Next(nil)
		require.True(t, r.HasNext(), "value missing for key in %s", path)
		value, _ := r.Next(nil)
		keys = append(keys, append([]byte(nil), key...))
		values = append(values, append([]byte(nil), value...))
	}
	return keys, values
}

func linkSnapshotTree(t *testing.T, source, output string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		dst := filepath.Join(output, rel)
		if entry.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		return os.Link(path, dst)
	}))
}

func convertPBinOutputFixture(t *testing.T, fixture pbinOutputFixture) error {
	t.Helper()
	at := fixture.output.BeginFilesRo()
	defer at.Close()
	return state.ConvertPBinRecordFiles(t.Context(), at, log.New())
}

func TestConvertPBinRecordFilesKeepsCurrentHardlink(t *testing.T) {
	fixture := newPBinOutputFixture(t, false, false)
	require.NoError(t, convertPBinOutputFixture(t, fixture))

	sourceInfo, err := os.Stat(fixture.sourcePath)
	require.NoError(t, err)
	outputInfo, err := os.Stat(fixture.outputPath)
	require.NoError(t, err)
	require.True(t, os.SameFile(sourceInfo, outputInfo))
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
}

func TestConvertPBinRecordFilesReplacesLegacyHardlink(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	sourceInfoBefore, err := os.Stat(fixture.sourcePath)
	require.NoError(t, err)
	require.NoError(t, convertPBinOutputFixture(t, fixture))

	sourceInfoAfter, err := os.Stat(fixture.sourcePath)
	require.NoError(t, err)
	outputInfo, err := os.Stat(fixture.outputPath)
	require.NoError(t, err)
	require.False(t, os.SameFile(sourceInfoAfter, outputInfo))
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	require.True(t, sourceInfoBefore.ModTime().Equal(sourceInfoAfter.ModTime()))

	keys, values := readKVFile(t, fixture.output, fixture.outputPath)
	require.NotEmpty(t, keys)
	for i, key := range keys {
		if bytes.Equal(key, commitmentdb.KeyCommitmentState) {
			require.NoError(t, validatePBinStateValue(values[i]))
			continue
		}
		require.NotEmpty(t, values[i])
		require.NotEqual(t, byte(0), values[i][0])
	}
}

func validatePBinStateValue(value []byte) error {
	if commitment.ValidatePBinStateFormat(value) == nil {
		return nil
	}
	if len(value) < 18 {
		return fmt.Errorf("short pbin state value")
	}
	return commitment.ValidatePBinStateFormat(value[18:])
}

func TestConvertPBinRecordFilesRejectsOutputBasenameChange(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	fixture.output.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)

	err := convertPBinOutputFixture(t, fixture)
	require.Error(t, err)
	require.Contains(t, err.Error(), "basename")
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
}

func TestConvertPBinRecordFilesUsesDomainCodecForSmallShard(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, true)
	require.NoError(t, convertPBinOutputFixture(t, fixture))

	keys, values := readKVFile(t, fixture.output, fixture.outputPath)
	require.NotEmpty(t, keys)
	require.Len(t, values, len(keys))
}
