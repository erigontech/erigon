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
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	selectedPath := selected.Fullpath()

	if legacy {
		rewritePBinFileAsLegacy(t, source, selectedPath)
	}

	sourceBytes, err := os.ReadFile(selectedPath)
	require.NoError(t, err)

	outputDirs := datadir.New(t.TempDir())
	linkSnapshotTree(t, source.Dirs().Snap, outputDirs.Snap)
	keepOnlyCommitmentRange(t, outputDirs.SnapDomain, filepath.Base(selectedPath))
	settings, err := state.ReadErigonDBSettings(source.Dirs())
	require.NoError(t, err)
	output := state.NewTest(outputDirs).
		StepSize(source.StepSize()).
		WithErigonDBSettings(settings).
		Logger(log.New()).
		MustOpen(t.Context(), db)
	// Windows refuses to unlink a mapped file, so the mmaps must go before
	// t.TempDir's own cleanup runs.
	t.Cleanup(output.Close)
	t.Cleanup(source.Close)
	require.NoError(t, output.OpenFolder())
	if legacy {
		keys, values := readKVFile(t, output, filepath.Join(outputDirs.SnapDomain, filepath.Base(selectedPath)))
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
		sourcePath:  selectedPath,
		outputPath:  filepath.Join(outputDirs.SnapDomain, filepath.Base(selectedPath)),
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

// Takes a path, not a kv.VisibleFile: dropping the mmaps invalidates every handle
// the caller is still holding.
func rewritePBinFileAsLegacy(t *testing.T, agg *state.Aggregator, path string) {
	t.Helper()
	cfg := agg.Cfg(kv.CommitmentDomain)
	compression := cfg.Compression
	keys, values := readKVFileWithCompression(t, path, compression)
	agg.CloseMappedFilesForTest()
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
			value, err = commitment.PBinEncodeLegacyRootRecord(value)
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
	require.NoError(t, agg.ReloadFiles())
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
	return state.ConvertPBinRecordFiles(t.Context(), at, log.New(), 0)
}

func convertPBinOutputFixtureWithSample(t *testing.T, fixture pbinOutputFixture, sample uint64) error {
	t.Helper()
	at := fixture.output.BeginFilesRo()
	defer at.Close()
	return state.ConvertPBinRecordFiles(t.Context(), at, log.New(), sample)
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

func TestConvertPBinRecordFilesRejectsDroppedRecord(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	state.SetPBinConvertDropPairHookForTest(func(pair uint64) bool { return pair == 1 })
	t.Cleanup(func() {
		state.SetPBinConvertDropPairHookForTest(nil)
	})

	err := convertPBinOutputFixture(t, fixture)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pair count")
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	assertPBinOutputRemoved(t, fixture)
}

func TestConvertPBinRecordFilesRejectsMangledStateRoot(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	keys, values := readKVFile(t, fixture.output, fixture.outputPath)
	var legacy []byte
	for i, key := range keys {
		if bytes.Equal(key, commitmentdb.KeyCommitmentState) {
			legacy = values[i]
			break
		}
	}
	require.NotEmpty(t, legacy)

	converter := commitment.NewPBinRecordConverter()
	var current []byte
	if commitment.IsPBinState(legacy) {
		var err error
		current, err = converter.ConvertState(legacy)
		require.NoError(t, err)
	} else {
		require.GreaterOrEqual(t, len(legacy), 18)
		converted, err := converter.ConvertState(legacy[18:])
		require.NoError(t, err)
		current = append([]byte(nil), legacy[:18]...)
		binary.BigEndian.PutUint16(current[16:18], uint16(len(converted)))
		current = append(current, converted...)
	}
	require.Greater(t, len(current), 5)
	mangled := append([]byte(nil), current...)
	mangled[len(mangled)-1] ^= 1

	err := state.VerifyPBinStateConversionForTest(legacy, mangled)
	require.Error(t, err)
	require.Contains(t, err.Error(), "state root")
}

func TestConvertPBinRecordFilesRemovesOutputAfterStateFailure(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	keys, values := readKVFile(t, fixture.output, fixture.outputPath)
	for i, key := range keys {
		if bytes.Equal(key, commitmentdb.KeyCommitmentState) {
			values[i] = []byte{0}
			break
		}
	}
	rewritePBinFile(t, fixture, keys, values)
	require.NoError(t, fixture.output.ReloadFiles())

	err := convertPBinOutputFixture(t, fixture)
	require.Error(t, err)
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	assertPBinOutputRemoved(t, fixture)
	restagePBinOutputFromSource(t, fixture)
	require.NoError(t, fixture.output.ReloadFiles())
	require.NoError(t, convertPBinOutputFixture(t, fixture))
	assertPBinOutputComplete(t, fixture)
}

func TestConvertPBinRecordFilesPanicsOnSingleCellWithoutChangingSource(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	keys, values := readKVFile(t, fixture.output, fixture.outputPath)
	replaced := false
	for i, key := range keys {
		if bytes.Equal(key, commitmentdb.KeyCommitmentState) || isPBinRootKey(key) || len(values[i]) == 0 {
			continue
		}
		values[i] = []byte{0, 1, 0, 1, 2, 0}
		replaced = true
		break
	}
	require.True(t, replaced, "fixture has no branch record")
	rewritePBinFile(t, fixture, keys, values)
	require.NoError(t, fixture.output.ReloadFiles())

	require.Panics(t, func() { _ = convertPBinOutputFixture(t, fixture) })
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	assertPBinOutputRemoved(t, fixture)
}

func TestConvertPBinRecordFilesCancellationLeavesRunResumable(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	ctx, cancel := context.WithCancel(t.Context())
	var pairs atomic.Int32
	state.SetPBinConvertPairHookForTest(func() {
		if pairs.Add(1) == 2 {
			cancel()
		}
	})
	t.Cleanup(func() {
		state.SetPBinConvertPairHookForTest(nil)
		cancel()
	})

	at := fixture.output.BeginFilesRo()
	err := state.ConvertPBinRecordFiles(ctx, at, log.New(), 0)
	at.Close()
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	assertPBinOutputRemoved(t, fixture)

	state.SetPBinConvertPairHookForTest(nil)
	require.NoError(t, fixture.output.ReloadFiles())
	require.NoError(t, convertPBinOutputFixture(t, fixture))
	require.NoError(t, fixture.output.ReloadFiles())
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	assertPBinOutputComplete(t, fixture)
}

func TestConvertPBinRecordFilesResumeRebuildsIncompleteShard(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	require.NoError(t, convertPBinOutputFixture(t, fixture))
	require.NoError(t, fixture.output.ReloadFiles())

	convertedInfo, err := os.Stat(fixture.outputPath)
	require.NoError(t, err)
	require.NoError(t, convertPBinOutputFixture(t, fixture))
	skippedInfo, err := os.Stat(fixture.outputPath)
	require.NoError(t, err)
	require.True(t, os.SameFile(convertedInfo, skippedInfo))

	removePBinOutputAccessors(t, fixture)
	require.NoError(t, fixture.output.ReloadFiles())
	at := fixture.output.BeginFilesRo()
	require.Empty(t, at.Files(kv.CommitmentDomain), "an incomplete shard must not be visible")
	at.Close()

	require.NoError(t, convertPBinOutputFixture(t, fixture))
	require.NoError(t, fixture.output.ReloadFiles())
	rebuiltInfo, err := os.Stat(fixture.outputPath)
	require.NoError(t, err)
	require.False(t, os.SameFile(convertedInfo, rebuiltInfo))
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	assertPBinOutputComplete(t, fixture)
}

func TestConvertPBinRecordFilesSampleRejectsWrongKey(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	state.SetPBinConvertAfterBuildHookForTest(func(path string) {
		rewritePBinFileWithWrongBranchKey(t, fixture, path)
	})
	t.Cleanup(func() {
		state.SetPBinConvertAfterBuildHookForTest(nil)
	})

	err := convertPBinOutputFixtureWithSample(t, fixture, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "sample")
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	assertPBinOutputRemoved(t, fixture)
}

func TestConvertPBinRecordFilesSampleZeroDisablesReadBack(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	state.SetPBinConvertAfterBuildHookForTest(func(path string) {
		rewritePBinFileWithWrongBranchKey(t, fixture, path)
	})
	t.Cleanup(func() {
		state.SetPBinConvertAfterBuildHookForTest(nil)
	})

	require.NoError(t, convertPBinOutputFixtureWithSample(t, fixture, 0))
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	assertPBinOutputComplete(t, fixture)
}

func TestConvertPBinRecordFilesSamplesOnlyLegacyBranches(t *testing.T) {
	fixture := newPBinOutputFixture(t, true, false)
	state.SetPBinConvertAfterBuildHookForTest(func(path string) {
		rewritePBinFileWithWrongRootKey(t, fixture, path)
	})
	t.Cleanup(func() {
		state.SetPBinConvertAfterBuildHookForTest(nil)
	})

	require.NoError(t, convertPBinOutputFixtureWithSample(t, fixture, 1))
	require.Equal(t, fixture.sourceBytes, readFileBytes(t, fixture.sourcePath))
	assertPBinOutputComplete(t, fixture)
}

func rewritePBinFile(t *testing.T, fixture pbinOutputFixture, keys, values [][]byte) {
	t.Helper()
	config := fixture.output.Cfg(kv.CommitmentDomain)
	require.NoError(t, dir.RemoveFile(fixture.outputPath))
	comp, err := seg.NewCompressor(t.Context(), "pbin test rewrite", fixture.outputPath, fixture.output.Dirs().Tmp, config.CompressCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	writer := seg.NewWriter(comp, config.Compression)
	for i := range keys {
		_, err = writer.Write(keys[i])
		require.NoError(t, err)
		_, err = writer.Write(values[i])
		require.NoError(t, err)
	}
	require.NoError(t, comp.Compress())
	comp.Close()
}

func rewritePBinFileWithWrongBranchKey(t *testing.T, fixture pbinOutputFixture, path string) {
	t.Helper()
	keys, values := readKVFile(t, fixture.output, path)
	for i, key := range keys {
		if bytes.Equal(key, commitmentdb.KeyCommitmentState) || isPBinRootKey(key) || len(values[i]) == 0 {
			continue
		}
		keys[i] = append(append([]byte(nil), key...), 0)
		rewritePBinFileAt(t, fixture, path, keys, values)
		return
	}
	require.Fail(t, "fixture has no branch record")
}

func rewritePBinFileWithWrongRootKey(t *testing.T, fixture pbinOutputFixture, path string) {
	t.Helper()
	keys, values := readKVFile(t, fixture.output, path)
	for i, key := range keys {
		if !isPBinRootKey(key) {
			continue
		}
		keys[i] = append(append([]byte(nil), key...), 0)
		rewritePBinFileAt(t, fixture, path, keys, values)
		return
	}
	require.Fail(t, "fixture has no root record")
}

func rewritePBinFileAt(t *testing.T, fixture pbinOutputFixture, path string, keys, values [][]byte) {
	t.Helper()
	config := fixture.output.Cfg(kv.CommitmentDomain)
	require.NoError(t, dir.RemoveFile(path))
	comp, err := seg.NewCompressor(t.Context(), "pbin test post-build rewrite", path, fixture.output.Dirs().Tmp, config.CompressCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	writer := seg.NewWriter(comp, config.Compression)
	for i := range keys {
		_, err = writer.Write(keys[i])
		require.NoError(t, err)
		_, err = writer.Write(values[i])
		require.NoError(t, err)
	}
	require.NoError(t, comp.Compress())
	comp.Close()
}

func removePBinOutputAccessors(t *testing.T, fixture pbinOutputFixture) {
	t.Helper()
	fixture.output.CloseMappedFilesForTest()
	entries, err := os.ReadDir(filepath.Dir(fixture.outputPath))
	require.NoError(t, err)
	removed := 0
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "-commitment.") && filepath.Ext(entry.Name()) != ".kv" {
			require.NoError(t, dir.RemoveFile(filepath.Join(filepath.Dir(fixture.outputPath), entry.Name())))
			removed++
		}
	}
	require.Positive(t, removed)
}

// A failed conversion is staged, never half-swapped: the shard the output started
// from is still in place and nothing the run built survives.
func assertPBinOutputRemoved(t *testing.T, fixture pbinOutputFixture) {
	t.Helper()
	require.FileExists(t, fixture.outputPath, "a failed conversion must leave the shard it started from")
	assertPBinStageDirEmpty(t, fixture)
}

func assertPBinStageDirEmpty(t *testing.T, fixture pbinOutputFixture) {
	t.Helper()
	stageDir := filepath.Join(fixture.output.Dirs().Tmp, "pbin_convert")
	entries, err := os.ReadDir(stageDir)
	if os.IsNotExist(err) {
		return
	}
	require.NoError(t, err)
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "-commitment.") {
			require.Failf(t, "staged pbin output remains", "found %s", entry.Name())
		}
	}
}

func assertPBinOutputComplete(t *testing.T, fixture pbinOutputFixture) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(fixture.outputPath))
	require.NoError(t, err)
	require.FileExists(t, fixture.outputPath)
	accessors := 0
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "-commitment.") && filepath.Ext(entry.Name()) != ".kv" {
			accessors++
		}
	}
	require.Positive(t, accessors)
	assertPBinStageDirEmpty(t, fixture)
}

// restagePBinOutputFromSource is what an operator does after a failed run: discard
// the output's commitment shard and link it in again from the untouched source.
func restagePBinOutputFromSource(t *testing.T, fixture pbinOutputFixture) {
	t.Helper()
	fixture.output.CloseMappedFilesForTest()
	outputDir := filepath.Dir(fixture.outputPath)
	entries, err := os.ReadDir(outputDir)
	require.NoError(t, err)
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "-commitment.") {
			require.NoError(t, dir.RemoveFile(filepath.Join(outputDir, entry.Name())))
		}
	}
	entries, err = os.ReadDir(filepath.Dir(fixture.sourcePath))
	require.NoError(t, err)
	for _, entry := range entries {
		if !strings.Contains(entry.Name(), "-commitment.") {
			continue
		}
		source := filepath.Join(filepath.Dir(fixture.sourcePath), entry.Name())
		require.NoError(t, os.Link(source, filepath.Join(outputDir, entry.Name())))
	}
}
