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

package commands

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	commitmenttemporal "github.com/erigontech/erigon/internal/commitmenttest/temporal"
	"github.com/stretchr/testify/require"
)

func TestPBTLegacyAttachMarkerCanResume(t *testing.T) {
	selectPBTCommandSuite(t)
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, step: func(step string) error {
		if step == "marker" {
			return errors.New("interrupted attach")
		}
		return nil
	}}
	require.ErrorContains(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks), "interrupted attach")
	marker, err := dbstate.ReadPBTAttachMarker(source.Dirs)
	require.NoError(t, err)
	require.NotNil(t, marker)
	marker.NodeHexRoot, marker.HeaderRoot = "", ""
	require.NoError(t, dbstate.WritePBTAttachMarker(source.Dirs, marker))
	err = attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
	require.NoError(t, err, "same-manifest retry with a legacy marker must finish while the original node state is intact")
}

func TestPBTResumeRejectsOutputMDBXRows(t *testing.T) {
	source, output := pbtResumeFixture(t, 1, false, false)
	dirs := datadir.Open(output)
	db := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		value := binary.BigEndian.AppendUint64(nil, ^uint64(0))
		return tx.Put(kv.TblCommitmentBinVals, []byte("stray"), value)
	}))
	db.Close()
	before := snapshotTreeDigest(t, output)
	err := convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "MDBX rows", "resume must refuse output commitment MDBX rows before changing files; got %v", err)
	after := snapshotTreeDigest(t, output)
	require.Equal(t, before, after, "MDBX-base refusal must leave output file list and bytes unchanged")
}

func TestPBTResumeLateFailurePreservesPublishedOutput(t *testing.T) {
	testPBTResumeFailurePreservesPublishedOutput(t, func(hooks *pbtConvertHooks) {
		hooks.standalone = func(datadir.Dirs) error { return errors.New("late resume failure") }
	})
}

func TestPBTResumeEarlyFailurePreservesPublishedOutput(t *testing.T) {
	testPBTResumeFailurePreservesPublishedOutput(t, func(hooks *pbtConvertHooks) {
		hooks.afterOutput = func() error { return errors.New("early resume failure") }
	})
}

func testPBTResumeFailurePreservesPublishedOutput(t *testing.T, setHook func(*pbtConvertHooks)) {
	t.Helper()
	source, output := pbtResumeFixture(t, 1, false, false)
	wantOutput := filepath.Join(t.TempDir(), "expected-output")
	copyPBTResumeSnapshot(t, output, wantOutput)
	before := snapshotTreeDigest(t, datadir.Open(output).Snap)
	hooks := pbtConvertHooks{resume: true, workers: 1}
	setHook(&hooks)
	err := convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), hooks)
	require.Error(t, err)
	require.Equal(t, before, snapshotTreeDigest(t, datadir.Open(output).Snap))
	manifestPath := filepath.Join(datadir.Open(output).Snap, "pbt-manifest.toml")
	manifest, err := dbstate.ReadPBTManifest(manifestPath)
	require.NoError(t, err)
	require.NoError(t, validatePBTAttachManifestFiles(t.Context(), manifest, datadir.Open(output), 1, false, log.New(), "resume failure"))
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1}))
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, wantOutput, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1}))
	wantManifest, err := dbstate.ReadPBTManifest(filepath.Join(datadir.Open(wantOutput).Snap, "pbt-manifest.toml"))
	require.NoError(t, err)
	gotManifest, err := dbstate.ReadPBTManifest(manifestPath)
	require.NoError(t, err)
	require.Equal(t, wantManifest, gotManifest)
}

func copyPBTResumeSnapshot(t *testing.T, source, destination string) {
	t.Helper()
	sourceRoot := datadir.Open(source).Snap
	destinationRoot := datadir.Open(destination).Snap
	require.NoError(t, filepath.WalkDir(sourceRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destinationRoot, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm())
	}))
}

func pbtResumeStopSourceBin(t *testing.T, source *execmoduletester.PBTAcceptanceChain, baseOutput string) *dbstate.ErigonDBSettings {
	t.Helper()
	settings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantHexBin, settings.TrieVariantName())
	require.NoError(t, removePBTFiles(source.Tester.Dirs, func(file pbtAttachFile) bool { return file.domain == kv.CommitmentBinDomain }))
	baseDirs := datadir.Open(baseOutput)
	baseFiles, err := pbtAttachFiles(baseDirs)
	require.NoError(t, err)
	for _, file := range baseFiles {
		if file.domain != kv.CommitmentBinDomain {
			continue
		}
		rel, err := filepath.Rel(baseDirs.Snap, file.path)
		require.NoError(t, err)
		destination := filepath.Join(source.Tester.Dirs.Snap, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o755))
		require.NoError(t, os.Link(file.path, destination))
	}
	return settings
}

func TestPBTResumeEmptyOutputFailureLeavesNoFiles(t *testing.T) {
	source, baseOutput := pbtResumeFixture(t, 1, false, false)
	output := filepath.Join(t.TempDir(), "empty-output")
	pbtResumeStopSourceBin(t, source, baseOutput)
	require.NoError(t, os.MkdirAll(output, 0o755))
	err := convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{
		resume:     true,
		workers:    1,
		standalone: func(datadir.Dirs) error { return errors.New("empty resume failure") },
	})
	require.Error(t, err)
	hasFiles, err := datadirHasFiles(output)
	require.NoError(t, err)
	require.False(t, hasFiles)
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1}))
}

func TestPBTResumeRecoversInterruptedSwap(t *testing.T) {
	source, output := pbtResumeFixture(t, 1, false, false)
	outputDirs := datadir.Open(output)
	backup := pbtResumePublishedPath(outputDirs)
	require.NoError(t, os.Rename(outputDirs.Snap, backup))
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1}))
	_, err := os.Stat(backup)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPBTEmptyOutputRefusalPreservesDirectory(t *testing.T) {
	selectPBTCommandSuite(t)
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, os.MkdirAll(output, 0o755))
	err := convertPBTWithOptions(t.Context(), source.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "empty resume output requires a source binary commitment domain")
	_, err = os.Stat(output)
	require.NoError(t, err, "resume refusal must preserve an existing empty output directory")
}

func TestPBTBytecodeRewriteRefusalPreservesOutput(t *testing.T) {
	source, output := pbtResumeFixture(t, 1, false, false)
	db := dbCfg(dbcfg.ChainDB, source.Tester.Dirs.Chaindata).MustOpen()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		hash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		config, err := rawdb.ReadChainConfig(tx, hash)
		if err != nil {
			return err
		}
		config.ChainName = "gnosis"
		return rawdb.WriteChainConfig(tx, hash, config)
	}))
	db.Close()
	before := domainFileNames(t, datadir.Open(output).SnapDomain)
	err := convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "bytecode rewrite")
	after := domainFileNames(t, datadir.Open(output).SnapDomain)
	require.Equal(t, before, after, "bytecode-rewrite refusal changed the output domain file list: before=%d after=%d", len(before), len(after))
}

func pbtResumeFixture(t *testing.T, frozen uint64, mergeBase, mergeSource bool) (*execmoduletester.PBTAcceptanceChain, string) {
	return pbtResumeFixtureWithSource(t, frozen, []int{1, 1, 1, 1}, 0, mergeBase, mergeSource)
}

func pbtResumeFixtureWithSource(t *testing.T, frozen uint64, sourceTxCounts []int, sourceEnd uint64, mergeBase, mergeSource bool) (*execmoduletester.PBTAcceptanceChain, string) {
	t.Helper()
	selectPBTCommandSuite(t)
	base, err := execmoduletester.NewPBTAcceptanceChainWithTxCounts(t, true, true, 1, frozen, []int{1, 1})
	require.NoError(t, err)
	require.NoError(t, base.Tester.InsertChain(base.Chain))
	base.Tester.Close()
	buildPBTResumeFiles(t, base, pbtAcceptanceLastTxNumRaw(t, base.Tester.Dirs.Chaindata), mergeBase)
	output := filepath.Join(t.TempDir(), "base-output")
	require.NoError(t, convertPBT(t.Context(), base.Tester.Dirs.DataDir, output, true, "", log.New()))
	source, err := execmoduletester.NewPBTAcceptanceChainWithTxCounts(t, true, true, 1, frozen, sourceTxCounts)
	require.NoError(t, err)
	source.Tester.Close()
	copyPBTStateSalt(t, base, source)
	source.Tester = execmoduletester.New(
		t,
		execmoduletester.WithExistingDataDir(source.Tester.Dirs),
		execmoduletester.WithGenesisSpec(source.Genesis),
		execmoduletester.WithKey(source.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
		execmoduletester.WithoutGenesisCommit(),
	)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	source.Tester.Close()
	if sourceEnd == 0 {
		sourceEnd = pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata)
	}
	buildPBTResumeFiles(t, source, sourceEnd, mergeSource)
	return source, output
}

func TestPBTResumeEqualEndPreservesOutput(t *testing.T) {
	selectPBTCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChainWithTxCounts(t, true, true, 1, 1, []int{1, 1})
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	source.Tester.Close()
	buildPBTAcceptanceFilesAt(t, source, pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata))
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New()))
	before := domainFileNames(t, datadir.Open(output).SnapDomain)
	err = convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "is at or past source files end")
	require.Equal(t, len(before), len(domainFileNames(t, datadir.Open(output).SnapDomain)), "X >= Y refusal must preserve the existing output file count")
}

func TestPBTResumeSourceFilesEndAtBlockStart(t *testing.T) {
	source, output := pbtResumeFixtureWithSource(t, 1, []int{1, 1, 1, 1, 0}, 14, false, false)
	settings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
	require.NoError(t, err)
	sourceEnd, err := pbtResumeSourceFilesEnd(source.Tester.Dirs, settings.StepSize)
	require.NoError(t, err)
	rawDB := dbCfg(dbcfg.ChainDB, source.Tester.Dirs.Chaindata).MustOpen()
	reader, view, closeReader, err := openPBTBlockReader(t.Context(), source.Tester.Dirs, rawDB, log.New())
	require.NoError(t, err)
	tx, err := rawDB.BeginRo(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	readerTx := pbtBlockFilesTx{Tx: tx, view: view}
	previousBlock, found, err := reader.TxnumReader().FindBlockNum(t.Context(), readerTx, sourceEnd-1)
	require.NoError(t, err)
	require.True(t, found)
	currentBlock, found, err := reader.TxnumReader().FindBlockNum(t.Context(), readerTx, sourceEnd)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEqual(t, previousBlock, currentBlock, "source files must end at the first transaction of a block")
	tx.Rollback()
	closeReader()
	rawDB.Close()
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1}))
}

func TestPBTResumeBaseLayouts(t *testing.T) {
	for _, test := range []struct {
		name                   string
		frozen                 uint64
		mergeBase, mergeSource bool
		message                string
	}{
		{name: "TestPBTResumeSourceMergedBeforeBase", frozen: 2, mergeSource: true, message: "resume must merge base ranges to match source ranges wholly before X"},
		{name: "TestPBTResumeUnchangedZeroStartBase", frozen: 8, mergeBase: true, mergeSource: true, message: "an unchanged zero-start base range contains no gap transactions"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, output := pbtResumeFixture(t, test.frozen, test.mergeBase, test.mergeSource)
			err := convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
			require.NoError(t, err, test.message)
		})
	}
}

func TestPBTResumeMDBXTombstonePastFiles(t *testing.T) {
	source, output := pbtResumeFixture(t, 1, false, false)
	dirs := source.Tester.Dirs
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	end, err := pbtResumeSourceFilesEnd(dirs, settings.StepSize)
	require.NoError(t, err)
	rawDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer rawDB.Close()
	agg, err := openPBTState(t.Context(), dirs, settings, rawDB, log.New())
	require.NoError(t, err)
	defer agg.Close()
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	defer db.Close()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer sd.Close()
	key := common.Address{6}
	previous, _, err := sd.GetLatest(kv.AccountsDomain, tx, key[:])
	require.NoError(t, err)
	require.NotEmpty(t, previous)
	require.NoError(t, sd.DomainDel(kv.AccountsDomain, tx, key[:], end+1, previous))
	require.NoError(t, sd.Flush(t.Context(), tx))
	value, step, found, err := tx.Debug().GetLatestFromDB(kv.AccountsDomain, key[:])
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, value)
	require.Greater(t, step.ToTxNum(settings.StepSize), end)
	require.NoError(t, tx.Commit())
	sd.Close()
	db.Close()
	agg.Close()
	rawDB.Close()
	err = convertPBTWithOptions(t.Context(), dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "past source files end", "MDBX tombstone at txNum %d is past files end %d", step.ToTxNum(settings.StepSize), end)
}

func TestPBTResumeStraddleRefusalPreservesOutput(t *testing.T) {
	source, output := pbtResumeFixture(t, 1, false, false)
	dirs := datadir.Open(output)
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	agg, err := openPBTState(t.Context(), dirs, settings, nil, log.New())
	require.NoError(t, err)
	require.NoError(t, agg.MergePBinRange(t.Context(), kv.CommitmentBinDomain, 1, 4))
	agg.Close()
	manifestPath := filepath.Join(dirs.Snap, "pbt-manifest.toml")
	manifest, err := dbstate.ReadPBTManifest(manifestPath)
	require.NoError(t, err)
	files, err := pbtAttachManifestSources(dirs)
	require.NoError(t, err)
	manifest, err = dbstate.BuildPBTManifest(t.Context(), manifest.Point, manifest.PBTRoot, manifest.HexRoot, manifest.TrieHash, manifest.SkipVerify, files, 1, nil)
	require.NoError(t, err)
	require.NoError(t, dbstate.WritePBTManifest(manifestPath, manifest))
	before := snapshotTreeDigest(t, output)
	err = convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "base bin file range 1-4 straddles source commitment ranges 1-2 and 2-3")
	after := snapshotTreeDigest(t, output)
	require.Equal(t, before, after, "straddle refusal must happen before any output change")
}

func TestPBTResumeEmptyOutputStraddleRefusedBeforeGapWalk(t *testing.T) {
	source, baseOutput := pbtResumeFixture(t, 1, false, false)
	settings := pbtResumeStopSourceBin(t, source, baseOutput)
	agg, err := openPBTState(t.Context(), source.Tester.Dirs, settings, nil, log.New())
	require.NoError(t, err)
	require.NoError(t, agg.MergePBinRange(t.Context(), kv.CommitmentBinDomain, 1, 4))
	agg.Close()
	output := filepath.Join(t.TempDir(), "empty-output")
	require.NoError(t, os.MkdirAll(output, 0o755))
	outputPrepared := false
	err = convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{
		resume:      true,
		workers:     1,
		afterOutput: func() error { outputPrepared = true; return nil },
	})
	require.ErrorContains(t, err, "base bin file range 1-4 straddles source commitment ranges 1-2 and 2-3")
	require.False(t, outputPrepared, "straddle refusal must happen before the output is prepared for the gap walk")
}

func TestVerifyPBTResumeStreamRootHonorsCancelledContext(t *testing.T) {
	source, _ := pbtResumeFixture(t, 1, false, false)
	settings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
	require.NoError(t, err)
	agg, err := openPBTState(t.Context(), source.Tester.Dirs, settings, nil, log.New())
	require.NoError(t, err)
	defer agg.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, verifyPBTResumeStreamRoot(ctx, agg, common.Hash{}, 2), context.Canceled)
}

func TestVerifyPBTResumeStreamRootDefaultsWorkers(t *testing.T) {
	source, _ := pbtResumeFixture(t, 1, false, false)
	settings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
	require.NoError(t, err)
	agg, err := openPBTState(t.Context(), source.Tester.Dirs, settings, nil, log.New())
	require.NoError(t, err)
	defer agg.Close()
	previousWorkers := dbg.PBTWorkers
	dbg.PBTWorkers = 2
	t.Cleanup(func() { dbg.PBTWorkers = previousWorkers })
	walkWorkers := commitmenttemporal.LeafWalkWorkers(t)
	require.ErrorContains(t, verifyPBTResumeStreamRoot(t.Context(), agg, common.Hash{}, 0), "differs from resumed root")
	require.GreaterOrEqual(t, walkWorkers(), 2, "--workers 0 must use the same default as the conversion")
}

func pbtManifestWithoutField(t *testing.T, data []byte, field string) []byte {
	t.Helper()
	pattern := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(field) + ` = .*\n`)
	if field == "point" {
		pattern = regexp.MustCompile(`(?m)^\[point\]\n(?:\w+ = .*\n)*`)
	}
	require.True(t, pattern.Match(data), "manifest has no field %s:\n%s", field, data)
	return pattern.ReplaceAll(data, nil)
}

func TestPBTManifestMissingFieldRefused(t *testing.T) {
	fields := []string{"point", "pbt_root", "hex_root", "trie_hash"}
	t.Run("resume", func(t *testing.T) {
		source, baseOutput := pbtResumeFixture(t, 1, false, false)
		for _, field := range fields {
			t.Run(field, func(t *testing.T) {
				output := filepath.Join(t.TempDir(), "output")
				copyPBTResumeSnapshot(t, baseOutput, output)
				manifestPath := filepath.Join(datadir.Open(output).Snap, "pbt-manifest.toml")
				data, err := os.ReadFile(manifestPath)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(manifestPath, pbtManifestWithoutField(t, data, field), 0o644))
				before := snapshotTreeDigest(t, output)
				err = convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
				require.ErrorContains(t, err, "missing field "+field)
				require.Equal(t, before, snapshotTreeDigest(t, output))
			})
		}
	})
	t.Run("attach", func(t *testing.T) {
		selectPBTCommandSuite(t)
		source, _ := newPBTConversionSource(t)
		published := filepath.Join(t.TempDir(), "published")
		require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
		setExecutionProgress(t, source.Chaindata, 1)
		manifestPath := filepath.Join(datadir.Open(published).Snap, "pbt-manifest.toml")
		original, err := os.ReadFile(manifestPath)
		require.NoError(t, err)
		for _, field := range fields {
			t.Run(field, func(t *testing.T) {
				require.NoError(t, os.WriteFile(manifestPath, pbtManifestWithoutField(t, original, field), 0o644))
				err := attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
				require.ErrorContains(t, err, "missing field "+field)
			})
		}
	})
}

func TestPBTResumeRefusesUnequalStateDomainEnds(t *testing.T) {
	source, output := pbtResumeFixture(t, 1, false, false)
	files, err := pbtAttachFiles(source.Tester.Dirs)
	require.NoError(t, err)
	var last uint64
	for _, file := range files {
		if file.domain == kv.StorageDomain && filepath.Dir(file.path) == source.Tester.Dirs.SnapDomain {
			last = max(last, file.to)
		}
	}
	removed := 0
	for _, file := range files {
		if file.domain == kv.StorageDomain && filepath.Dir(file.path) == source.Tester.Dirs.SnapDomain && file.to == last {
			require.NoError(t, dir.RemoveFile(file.path))
			removed++
		}
	}
	require.Positive(t, removed)
	before := snapshotTreeDigest(t, output)
	err = convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "state domain file ends differ")
	require.Equal(t, before, snapshotTreeDigest(t, output), "the refusal must leave the output intact")
}

func TestPBTResumeZeroStartSpanningRangeMatchesFresh(t *testing.T) {
	source, output := pbtResumeFixture(t, 1, false, false)
	sourceDirs := source.Tester.Dirs
	settings, err := dbstate.ReadErigonDBSettings(sourceDirs)
	require.NoError(t, err)
	baseSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	basePoint, err := readPBinSourcePoint(t.Context(), datadir.Open(output), baseSettings, false, log.New())
	require.NoError(t, err)
	t.Logf("X=%d", basePoint.TxNum)
	fresh := filepath.Join(t.TempDir(), "fresh")
	require.NoError(t, convertPBT(t.Context(), sourceDirs.DataDir, fresh, true, "", log.New()))
	agg, err := openPBTState(t.Context(), sourceDirs, settings, nil, log.New())
	require.NoError(t, err)
	require.NoError(t, agg.MergePBinRange(t.Context(), kv.CommitmentDomain, 0, basePoint.TxNum+3))
	agg.Close()
	err = convertPBTWithOptions(t.Context(), sourceDirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.NoError(t, err, "resume with nested base files must produce the spanning source range")
	require.Equal(t, readPBTBinRows(t, fresh, sourceDirs.Chaindata), readPBTBinRows(t, output, sourceDirs.Chaindata))
	require.Equal(t, readPBTBinRoot(t, fresh, sourceDirs.Chaindata), readPBTBinRoot(t, output, sourceDirs.Chaindata))
	require.Equal(t, pbtResumeRangeKeys(t, sourceDirs, kv.CommitmentDomain), pbtResumeRangeKeys(t, datadir.Open(output), kv.CommitmentBinDomain))
	setExecutionProgress(t, sourceDirs.Chaindata, 1)
	require.NoError(t, attachPBTWithHooks(t.Context(), sourceDirs.DataDir, output, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))
}

func TestPBTResumeEarlierOutputWithHexOnlySource(t *testing.T) {
	source, output := pbtResumeFixture(t, 1, false, false)
	settings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
	require.NoError(t, err)
	variant := dbstate.TrieVariantHex
	settings.TrieVariant = &variant
	settings.TrieHash = nil
	require.NoError(t, dbstate.WriteErigonDBSettings(datadir.Open(source.Tester.Dirs.DataDir), settings))
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1}))
}

func TestPBTResumeReadBaseHonorsCancelledContext(t *testing.T) {
	source, output := pbtResumeFixture(t, 1, false, false)
	settings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = readPBTResumeBase(ctx, source.Tester.Dirs, datadir.Open(output), true, settings, 1, log.New())
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "resume manifest")
}

func TestPBTResumeReadBaseRestoresSource(t *testing.T) {
	for _, variant := range []string{dbstate.TrieVariantHex, dbstate.TrieVariantHexBin} {
		t.Run(variant, func(t *testing.T) {
			source, output := pbtResumeFixture(t, 1, false, false)
			settings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
			require.NoError(t, err)
			settings.TrieVariant = &variant
			if variant == dbstate.TrieVariantHex {
				settings.TrieHash = nil
				statecfg.ConfigureCommitmentV3Records(false)
			}
			require.NoError(t, dbstate.WriteErigonDBSettings(source.Tester.Dirs, settings))
			configurePBTSourceVariant(settings)
			bin, dual, hash, v3 := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.BinCommitmentHash, statecfg.ExperimentalCommitmentV3
			_, err = readPBTResumeBase(t.Context(), source.Tester.Dirs, datadir.Open(output), true, settings, 1, log.New())
			require.NoError(t, err)
			require.Equal(t, bin, statecfg.ExperimentalBinCommitment)
			require.Equal(t, dual, statecfg.ExperimentalHexBinCommitment)
			require.Equal(t, hash, statecfg.BinCommitmentHash)
			require.Equal(t, v3, statecfg.ExperimentalCommitmentV3, "reading the resume output must restore the source record format")
		})
	}
}
