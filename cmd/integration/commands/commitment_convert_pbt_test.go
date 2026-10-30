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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snaptype"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	chainpkg "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
	commitmenttemporal "github.com/erigontech/erigon/internal/commitmenttest/temporal"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"lukechampine.com/blake3"
)

func convertPBTWithOutputTestHook(ctx context.Context, sourcePath, outputPath string, keepHex bool, chainName string, logger log.Logger, hook func() error) error {
	return convertPBTWithOptions(ctx, sourcePath, outputPath, keepHex, chainName, logger, pbtConvertHooks{afterOutput: hook})
}

func convertPBTWithStandaloneTestHook(ctx context.Context, sourcePath, outputPath string, keepHex bool, chainName string, logger log.Logger, hook func(datadir.Dirs) error) error {
	return convertPBTWithOptions(ctx, sourcePath, outputPath, keepHex, chainName, logger, pbtConvertHooks{standalone: hook})
}

func TestIsCommitmentFileNameAcceptsCommitmentBin(t *testing.T) {
	require.True(t, isCommitmentFileName("v1.0-commitment-bin.0-1024.kv"), "the converter must exclude commitment-bin files from its source links")
}

func TestConvertPBTResumeEarlierOutputMatchesFresh(t *testing.T) {
	source, baseOutput := pbtResumeFixture(t, 1, false, false)
	freshOutput := filepath.Join(t.TempDir(), "fresh-output")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, freshOutput, true, "", log.New()))
	require.NotEqual(t, readPBTBinRows(t, baseOutput, source.Tester.Dirs.Chaindata), readPBTBinRows(t, freshOutput, source.Tester.Dirs.Chaindata))
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, baseOutput, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1}))
	baseBinFiles := pbtCommitmentBinKVBytes(t, baseOutput)
	freshBinFiles := pbtCommitmentBinKVBytes(t, freshOutput)
	require.Equal(t, pbtSortedBinFileNames(freshBinFiles), pbtSortedBinFileNames(baseBinFiles))
	resumedSettings, resumedSettingsErr := dbstate.ReadErigonDBSettings(datadir.Open(baseOutput))
	require.NoError(t, resumedSettingsErr)
	resumedAgg, resumedAggErr := openPBTState(t.Context(), datadir.Open(baseOutput), resumedSettings, nil, log.New())
	require.NoError(t, resumedAggErr)
	resumedAt := resumedAgg.BeginFilesRo()
	resumedAt.Close()
	resumedAgg.Close()
	require.Equal(t, readPBTBinLeaves(t, freshOutput, source.Tester.Dirs.Chaindata), readPBTBinLeaves(t, baseOutput, source.Tester.Dirs.Chaindata))
	freshRows, resumedRows := readPBTBinRows(t, freshOutput, source.Tester.Dirs.Chaindata), readPBTBinRows(t, baseOutput, source.Tester.Dirs.Chaindata)
	require.Equal(t, freshRows, resumedRows)
	require.Equal(t, readPBTBinRoot(t, freshOutput, source.Tester.Dirs.Chaindata), readPBTBinRoot(t, baseOutput, source.Tester.Dirs.Chaindata))
	salt, saltErr := os.ReadFile(filepath.Join(datadir.Open(baseOutput).Snap, "salt-state.txt"))
	require.NoError(t, saltErr)
	require.NoError(t, os.WriteFile(filepath.Join(source.Tester.Dirs.Snap, "salt-state.txt"), salt, 0o644))
	setExecutionProgress(t, source.Tester.Dirs.Chaindata, 1)
	require.NoError(t, attachPBTWithHooks(t.Context(), source.Tester.Dirs.DataDir, baseOutput, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))
}

func TestConvertPBTResumeRejectsUntouchedStateDifference(t *testing.T) {
	for _, test := range []struct {
		name       string
		beforeFork bool
	}{
		{name: "after-binary-fork"},
		{name: "before-binary-fork", beforeFork: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			selectPBTCommandSuite(t)
			base, err := execmoduletester.NewPBTAcceptanceChainWithSharedCodeAndStepSize(t, true, true, []byte{0x60, 0x00, 0x60, 0x00, 0x52}, 1)
			require.NoError(t, err)
			require.NoError(t, base.Tester.InsertChain(base.Chain))
			base.Tester.Close()
			buildPBTResumeFiles(t, base, pbtAcceptanceLastTxNumRaw(t, base.Tester.Dirs.Chaindata), false)
			output := filepath.Join(t.TempDir(), "base-output")
			require.NoError(t, convertPBT(t.Context(), base.Tester.Dirs.DataDir, output, true, "", log.New()))

			source, err := execmoduletester.NewPBTAcceptanceChainWithStepSizeAndFrozenBlocks(t, true, true, 1, 1, 6)
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
			buildPBTResumeFiles(t, source, pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata), false)

			if test.beforeFork {
				rawDB := dbCfg(dbcfg.ChainDB, source.Tester.Dirs.Chaindata).MustOpen()
				updateErr := rawDB.Update(t.Context(), func(tx kv.RwTx) error {
					genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
					if err != nil {
						return err
					}
					config, err := rawdb.ReadChainConfig(tx, genesisHash)
					if err != nil {
						return err
					}
					forkTime := uint64(100)
					config.BinaryTrieTime = &forkTime
					return rawdb.WriteChainConfig(tx, genesisHash, config)
				})
				rawDB.Close()
				require.NoError(t, updateErr)
			}

			err = convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
			require.ErrorContains(t, err, "C-e stream root")
		})
	}
}

func TestConvertPBTResumeFromStoppedHexBinSource(t *testing.T) {
	runPBTResumeFromStoppedHexBinSource(t, 1, false, 2, 4, false)
}

func TestConvertPBTResumeFromMergedHexBinSource(t *testing.T) {
	runPBTResumeFromStoppedHexBinSource(t, 2, true, 2, 4, false)
}

func TestConvertPBTResumeAcrossMergedSourceFile(t *testing.T) {
	runPBTResumeFromStoppedHexBinSource(t, 2, true, 3, 4, true)
}

func TestConvertPBTResumeAcrossMergedSourceFileWithLaterFiles(t *testing.T) {
	runPBTResumeFromStoppedHexBinSource(t, 2, true, 3, 6, true)
}

func TestConvertPBTResumeFromSourceRangeAcrossBase(t *testing.T) {
	runPBTResumeFromStoppedHexBinSourceWithMerges(t, 1, false, true)
}

func TestPBTResumeRejectsBaseFileAcrossSourceRange(t *testing.T) {
	err := pbtResumeValidateBaseFileRanges(
		[]pbtAttachFile{{path: "v1.0-commitment-bin.0-12.kv", from: 0, to: 12}},
		[][2]uint64{{0, 8}, {8, 16}}, 1, 7,
	)
	require.ErrorContains(t, err, "base bin file range 0-12 straddles source commitment ranges 0-8 and 8-16")
}

func runPBTResumeFromStoppedHexBinSource(t *testing.T, stepsInFrozenFile uint64, merge bool, baseBlocks, sourceBlocks int, spansBase bool) {
	runPBTResumeFromStoppedHexBinSourceWithConfig(t, stepsInFrozenFile, merge, merge, baseBlocks, sourceBlocks, spansBase)
}

func runPBTResumeFromStoppedHexBinSourceWithMerges(t *testing.T, stepsInFrozenFile uint64, baseMerge, sourceMerge bool) {
	runPBTResumeFromStoppedHexBinSourceWithConfig(t, stepsInFrozenFile, baseMerge, sourceMerge, 2, 4, false)
}

func runPBTResumeFromStoppedHexBinSourceWithConfig(t *testing.T, stepsInFrozenFile uint64, baseMerge, sourceMerge bool, baseBlocks, sourceBlocks int, spansBase bool) {
	selectPBTCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChainWithStepSizeAndFrozenBlocks(t, true, true, 1, stepsInFrozenFile, sourceBlocks)
	require.NoError(t, err)
	source.Tester.Close()
	sourceSalt, err := os.ReadFile(filepath.Join(source.Tester.Dirs.Snap, "salt-state.txt"))
	require.NoError(t, err)
	base, err := execmoduletester.NewPBTAcceptanceChainWithStepSizeAndFrozenBlocks(t, true, true, 1, stepsInFrozenFile, baseBlocks)
	require.NoError(t, err)
	base.Tester.Close()
	require.NoError(t, os.WriteFile(filepath.Join(base.Tester.Dirs.Snap, "salt-state.txt"), sourceSalt, 0o644))
	base.Tester = execmoduletester.New(
		t,
		execmoduletester.WithExistingDataDir(base.Tester.Dirs),
		execmoduletester.WithGenesisSpec(base.Genesis),
		execmoduletester.WithKey(base.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
		execmoduletester.WithoutGenesisCommit(),
	)
	require.NoError(t, base.Tester.InsertChain(base.Chain))
	base.Tester.Close()
	baseEnd := pbtAcceptanceLastTxNumRaw(t, base.Tester.Dirs.Chaindata)
	buildPBTResumeFiles(t, base, baseEnd, baseMerge)
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
	sourceSettings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
	require.NoError(t, err)
	sourceEnd := pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata)
	buildPBTResumeFiles(t, source, sourceEnd, sourceMerge)
	freshOutput := filepath.Join(t.TempDir(), "fresh-output")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, freshOutput, true, "", log.New()))
	baseSettings, err := dbstate.ReadErigonDBSettings(base.Tester.Dirs)
	require.NoError(t, err)
	basePoint, err := readPBinSourcePoint(t.Context(), base.Tester.Dirs, baseSettings, false, log.New())
	require.NoError(t, err)
	require.NoError(t, removePBTFiles(source.Tester.Dirs, func(file pbtAttachFile) bool {
		return file.domain == kv.CommitmentBinDomain
	}))
	for _, roots := range [][2]string{
		{base.Tester.Dirs.Snap, source.Tester.Dirs.Snap},
		{base.Tester.Dirs.SnapDomain, source.Tester.Dirs.SnapDomain},
		{base.Tester.Dirs.SnapHistory, source.Tester.Dirs.SnapHistory},
		{base.Tester.Dirs.SnapIdx, source.Tester.Dirs.SnapIdx},
		{base.Tester.Dirs.SnapAccessors, source.Tester.Dirs.SnapAccessors},
	} {
		require.NoError(t, filepath.WalkDir(roots[0], func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() {
				return walkErr
			}
			parsed, _, ok := snaptype.ParseFileName(roots[0], entry.Name())
			if !ok || parsed.TypeString != kv.CommitmentBinDomain.String() {
				return nil
			}
			return os.Link(path, filepath.Join(roots[1], entry.Name()))
		}))
	}
	stoppedFiles, err := pbtAttachFiles(source.Tester.Dirs)
	require.NoError(t, err)
	var stoppedBinFiles int
	for _, file := range stoppedFiles {
		if file.domain == kv.CommitmentBinDomain {
			stoppedBinFiles++
		}
	}
	require.NotZero(t, stoppedBinFiles)
	stoppedPoint, err := readPBinSourcePoint(t.Context(), source.Tester.Dirs, sourceSettings, false, log.New())
	require.NoError(t, err)
	require.Equal(t, basePoint, stoppedPoint)
	for _, file := range stoppedFiles {
		if file.domain == kv.CommitmentBinDomain {
			require.LessOrEqual(t, file.from*sourceSettings.StepSize, stoppedPoint.TxNum, file.path)
		}
	}
	fileEnd, err := pbtResumeSourceFilesEnd(source.Tester.Dirs, sourceSettings.StepSize)
	require.NoError(t, err)
	require.Greater(t, fileEnd, stoppedPoint.TxNum)
	hexFiles, err := pbtResumeCommitmentFiles(source.Tester.Dirs, kv.CommitmentDomain)
	require.NoError(t, err)
	spanning := slices.ContainsFunc(hexFiles, func(file pbtAttachFile) bool {
		return file.from > 0 && file.from*sourceSettings.StepSize <= stoppedPoint.TxNum && file.to*sourceSettings.StepSize > stoppedPoint.TxNum+1
	})
	require.Equal(t, spansBase, spanning, "a source commitment file spans the base point %d", stoppedPoint.TxNum)
	output := filepath.Join(t.TempDir(), "resumed-output")
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1}))
	require.Equal(t, pbtSortedBinFileNames(pbtCommitmentBinKVBytes(t, freshOutput)), pbtSortedBinFileNames(pbtCommitmentBinKVBytes(t, output)))
	require.Equal(t, readPBTBinLeaves(t, freshOutput, source.Tester.Dirs.Chaindata), readPBTBinLeaves(t, output, source.Tester.Dirs.Chaindata))
	require.Equal(t, readPBTBinRows(t, freshOutput, source.Tester.Dirs.Chaindata), readPBTBinRows(t, output, source.Tester.Dirs.Chaindata))
	require.Equal(t, readPBTBinRoot(t, freshOutput, source.Tester.Dirs.Chaindata), readPBTBinRoot(t, output, source.Tester.Dirs.Chaindata))
	resumeSettings, err := dbstate.ReadErigonDBSettings(source.Tester.Dirs)
	require.NoError(t, err)
	sourceDomain, err := pbtResumeLayoutSourceDomain(resumeSettings)
	require.NoError(t, err)
	require.Equal(t, pbtResumeRangeKeys(t, source.Tester.Dirs, sourceDomain), pbtResumeRangeKeys(t, datadir.Open(output), kv.CommitmentBinDomain))
	setExecutionProgress(t, source.Tester.Dirs.Chaindata, 1)
	require.NoError(t, attachPBTWithHooks(t.Context(), source.Tester.Dirs.DataDir, output, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))
}

func buildPBTResumeFiles(t *testing.T, fixture *execmoduletester.PBTAcceptanceChain, endTxNum uint64, merge bool) {
	t.Helper()
	if merge {
		buildPBTAcceptanceFilesAtWithMerge(t, fixture, endTxNum, true)
		return
	}
	buildPBTAcceptanceFilesAt(t, fixture, endTxNum)
}

func TestPBTRealChainTxNumConvention(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	fixture, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	settings, settingsErr := dbstate.ReadErigonDBSettings(fixture.Tester.Dirs)
	require.NoError(t, settingsErr)
	require.NoError(t, fixture.Tester.InsertChain(fixture.Chain))
	tx, err := fixture.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	blockNum, lastTxNum, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	t.Logf("last block=%d tx=%d", blockNum, lastTxNum)
	tx.Rollback()
	fixture.Tester.Close()
	rawDB := dbCfg(dbcfg.ChainDB, fixture.Tester.Dirs.Chaindata).MustOpen()
	agg := dbstate.New(fixture.Tester.Dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			db.Close()
			agg.Close()
			rawDB.Close()
		}
	})
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, kv.Step(lastTxNum)+1, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), false))
	agg.WaitForFiles()
	require.ErrorContains(t, requirePBinSourceEndWithWorkers(context.Background(), agg, lastTxNum-1, 1, log.Root()), "pbin stamp walk")
	seekTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer seekTx.Rollback()
	seekDomains, err := execctx.NewSharedDomains(t.Context(), seekTx, log.New())
	require.NoError(t, err)
	gotTx, gotBlock, err := seekDomains.SeekCommitment(t.Context(), seekTx)
	require.NoError(t, err)
	t.Logf("seek=(%d,%d)", gotBlock, gotTx)
	seekDomains.Close()
	seekTx.Rollback()
	at := agg.BeginFilesRo()
	for _, domain := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		files := at.Files(domain)
		if len(files) > 0 {
			t.Logf("domain=%s files=%d first=[%d,%d) last=[%d,%d)", domain, len(files), files[0].StartRootNum(), files[0].EndRootNum(), files[len(files)-1].StartRootNum(), files[len(files)-1].EndRootNum())
		} else {
			t.Logf("domain=%s files=0", domain)
		}
		value, found, start, end, getErr := at.DebugGetLatestFromFiles(domain, commitment.KeyCommitmentV3State, ^uint64(0))
		if domain == kv.CommitmentBinDomain {
			value, found, start, end, getErr = at.DebugGetLatestFromFiles(domain, commitment.KeyCommitmentState, ^uint64(0))
		}
		require.NoError(t, getErr)
		require.True(t, found, "commitment state file")
		var stateBlock, stateTx uint64
		if domain == kv.CommitmentDomain {
			stateBlock, stateTx, _, err = commitment.DecodeCommitmentV3State(value)
		} else {
			stateTx, stateBlock = commitmentdb.DecodeTxBlockNums(value)
		}
		require.NoError(t, err)
		t.Logf("domain=%s blockMaxTx=%d state=(%d,%d) file=[%d,%d) seek=%d", domain, lastTxNum, stateBlock, stateTx, start, end, stateTx)
	}
	at.Close()
	require.Equal(t, uint64(4), blockNum)
	closed = true
	db.Close()
	agg.Close()
	rawDB.Close()
	output := filepath.Join(t.TempDir(), "converted")
	require.NoError(t, convertPBT(t.Context(), fixture.Tester.Dirs.DataDir, output, true, "", log.New()))
}

func TestConvertPBTHexSourceKeepHex(t *testing.T) {
	commitmentflags.Restore(t)
	previousDatadir := datadirCli
	previousChaindata := chaindata
	t.Cleanup(func() {
		datadirCli = previousDatadir
		chaindata = previousChaindata
	})
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	statecfg.InitSchemas()
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, wantRoot := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	datadirCli = source.DataDir
	chaindata = source.Chaindata
	statecfg.BinCommitmentHash = ""
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantHexBin, settings.TrieVariantName())
	require.Equal(t, commitment.PBinHashBlake3, settings.TrieHashName())
	blockNum, txNum, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), blockNum)
	require.Equal(t, uint64(7), txNum)
	_, err = os.Stat(filepath.Join(output, "chaindata"))
	require.ErrorIs(t, err, os.ErrNotExist)
	outputDirs := datadir.Open(output)
	var binFiles int
	outputNames := domainFileNames(t, outputDirs.SnapDomain)
	require.Equal(t, wantRoot, readPBTBinRoot(t, output, source.Chaindata))
	for _, name := range outputNames {
		parsed, _, ok := snaptype.ParseFileName(outputDirs.SnapDomain, name)
		if !ok || parsed.TypeString != kv.CommitmentBinDomain.String() {
			continue
		}
		binFiles++
	}
	require.NotZero(t, binFiles)

	secondOutput := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, secondOutput, true, "", log.New()))
	require.Equal(t, snapshotTree(t, output), snapshotTree(t, secondOutput))
}

func TestConvertPBTUsesRequestedKeccakSuite(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.InitSchemas()
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = commitment.PBinHashKeccak
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	require.Equal(t, commitment.PBinHashKeccak, settings.TrieHashName())
}

func TestConvertPBTKeepsCommitmentHistoryForAttach(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.InitSchemas()
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.EnableHistoricalCommitment()
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
	historyFiles, err := pbtAttachFiles(datadir.Open(output))
	require.NoError(t, err)
	var hasCommitmentHistory, hasCommitmentAccessor bool
	for _, file := range historyFiles {
		if file.domain == kv.CommitmentDomain && !file.data && (strings.HasSuffix(file.path, ".v") || strings.HasSuffix(file.path, ".ef")) {
			hasCommitmentHistory = true
		}
		if file.domain == kv.CommitmentDomain && !file.data && (strings.HasSuffix(file.path, ".vi") || strings.HasSuffix(file.path, ".efi")) {
			hasCommitmentAccessor = true
		}
	}
	require.True(t, hasCommitmentHistory)
	require.True(t, hasCommitmentAccessor, "commitment history and index accessors must be published")
	setExecutionProgress(t, source.Chaindata, 1)
	require.NoError(t, attachPBTWithHooks(t.Context(), source.DataDir, output, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis}))

	t.Run("published set without commitment history", func(t *testing.T) {
		source, _ := newPBTConversionSource(t)
		output := filepath.Join(t.TempDir(), "output")
		require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
		publishedDirs := datadir.Open(output)
		files, err := pbtAttachFiles(publishedDirs)
		require.NoError(t, err)
		for _, file := range files {
			if file.domain == kv.CommitmentDomain && (strings.HasSuffix(file.path, ".v") || strings.HasSuffix(file.path, ".ef") || strings.HasSuffix(file.path, ".vi") || strings.HasSuffix(file.path, ".efi")) {
				require.NoError(t, dir.RemoveFile(file.path))
			}
		}
		setExecutionProgress(t, source.Chaindata, 1)
		err = attachPBTWithHooks(t.Context(), source.DataDir, output, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
		require.ErrorContains(t, err, "pbt manifest")
	})
}

func TestConvertPBTSourceWithoutBinaryTables(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	previousTables := kv.ChaindataTablesCfg
	tables := maps.Clone(previousTables)
	for _, table := range []string{kv.TblCommitmentBinVals, kv.TblCommitmentBinHistoryKeys, kv.TblCommitmentBinHistoryVals, kv.TblCommitmentBinIdx} {
		delete(tables, table)
	}
	kv.ChaindataTablesCfg = tables
	t.Cleanup(func() { kv.ChaindataTablesCfg = previousTables })
	source, _ := newPBTConversionSource(t)
	kv.ChaindataTablesCfg = previousTables
	before := snapshotTree(t, source.DataDir)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
	require.Equal(t, before, snapshotTree(t, source.DataDir))
}

func TestConvertPBTPublishedRootMatchesDualShadowRoot(t *testing.T) {
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFiles(t, source)
	raw := dbCfg(dbcfg.ChainDB, source.Tester.Dirs.Chaindata).MustOpen()
	var shadow []byte
	require.NoError(t, raw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		shadow, err = rawdb.ReadShadowStateRoot(tx, source.Chain.Blocks[3].Hash(), 4)
		shadow = bytes.Clone(shadow)
		return err
	}))
	raw.Close()
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	blockNum, _, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(4), blockNum)
	require.NotEmpty(t, shadow)
	require.Equal(t, common.BytesToHash(shadow), readPBTBinRoot(t, output, source.Tester.Dirs.Chaindata))
}

func convertedPBTAcceptanceRows(t *testing.T, sharedCode []byte, maxOps int) (map[string][]byte, common.Hash, common.Hash, string, map[string][]byte, map[string][]byte, string) {
	t.Helper()
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChainWithSharedCode(t, false, false, sharedCode)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFiles(t, source)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	entries := pbtAcceptanceReferenceEntries(source)
	wantRoot := eip8297.StateRootWithHash(entries, eip8297.SelectedHash())
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{rangeWriterLimits: &dbstate.PBinRangeWriterLimits{MaxOps: maxOps, MaxBytes: 1 << 30}}))
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	binRoot := readPBTBinRoot(t, output, source.Tester.Dirs.Chaindata)
	referenceRows := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		referenceRows[string(entry.Key)] = bytes.Clone(entry.Value)
	}
	binLeaves := readPBTBinLeaves(t, output, source.Tester.Dirs.Chaindata)
	return binLeaves, binRoot, wantRoot, string(entries[len(entries)-1].Key), binLeaves, referenceRows, output
}

func TestConvertPBTWorkersMatchSequentialFiles(t *testing.T) {
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChainWithSharedCode(t, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFiles(t, source)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	for _, maxOps := range []int{100000, 7} {
		converted := make(map[bool]map[string][]byte, 2)
		for _, sequential := range []bool{true, false} {
			output := filepath.Join(t.TempDir(), "output")
			hooks := pbtConvertHooks{rangeWriterLimits: &dbstate.PBinRangeWriterLimits{MaxOps: maxOps, MaxBytes: 1 << 30}, sequentialCommitment: sequential, workers: 4}
			require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), hooks), "maxOps=%d sequential=%v", maxOps, sequential)
			converted[sequential] = pbtCommitmentBinFiles(t, output, ".kv")
			require.NotEmpty(t, converted[sequential])
		}
		require.Equal(t, converted[true], converted[false], "maxOps=%d", maxOps)
	}
}

func TestConvertPBTFileRangesMatchWorkers(t *testing.T) {
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChainWithSharedCode(t, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFiles(t, source)
	var wantKV, wantKVI map[string][]byte
	for _, workers := range []int{1, 16} {
		output := filepath.Join(t.TempDir(), fmt.Sprintf("output-workers-%d", workers))
		require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{
			workers:           workers,
			rangeWriterLimits: &dbstate.PBinRangeWriterLimits{MaxOps: 7, MaxBytes: 1 << 30},
		}), "workers=%d", workers)
		kvFiles, kviFiles := pbtCommitmentBinFileBytes(t, output)
		require.GreaterOrEqual(t, len(kvFiles), 3, "workers=%d: expected at least three ranges", workers)
		if wantKV == nil {
			wantKV, wantKVI = kvFiles, kviFiles
			continue
		}
		require.Equal(t, wantKV, kvFiles, "workers=%d .kv", workers)
		require.Equal(t, wantKVI, kviFiles, "workers=%d .kvi", workers)
	}
}

func pbtCommitmentBinFileBytes(t *testing.T, output string) (map[string][]byte, map[string][]byte) {
	return pbtCommitmentBinFiles(t, output, ".kv"), pbtCommitmentBinFiles(t, output, ".kvi")
}

func TestConvertPBTSourceWalkWorkersMatchSerial(t *testing.T) {
	selectPBTHexCommandSuite(t)
	source, _ := newPBTConversionSourceAt(t, 15)
	var wantLeaves map[string][]byte
	var wantRoot common.Hash
	walkWorkers := commitmenttemporal.LeafWalkWorkers(t)
	for _, workers := range []int{1, 4, 16} {
		output := filepath.Join(t.TempDir(), fmt.Sprintf("output-%d", workers))
		require.NoError(t, convertPBTWithOptions(t.Context(), source.DataDir, output, true, "", log.New(), pbtConvertHooks{workers: workers}), "workers=%d", workers)
		gotLeaves := readPBTBinLeaves(t, output, source.Chaindata)
		gotRoot := readPBTBinRoot(t, output, source.Chaindata)
		walked := walkWorkers()
		if workers == 1 {
			wantLeaves, wantRoot = gotLeaves, gotRoot
		} else {
			require.GreaterOrEqual(t, walked, 2, "workers=%d: the source walk did not run on several workers", workers)
			require.Equal(t, wantLeaves, gotLeaves, "workers=%d", workers)
			require.Equal(t, wantRoot, gotRoot, "workers=%d", workers)
		}
	}
}

func pbtAcceptanceReferenceEntries(source *execmoduletester.PBTAcceptanceChain) []eip8297.Entry {
	addressBytes := func(address common.Address) []byte { return append([]byte(nil), address[:]...) }
	hashBytes := func(hash common.Hash) []byte { return append([]byte(nil), hash[:]...) }
	states := make([]eip8297.State, 0, len(source.Genesis.Alloc))
	for address, account := range source.Genesis.Alloc {
		balance := new(big.Int)
		if account.Balance != nil {
			balance.Set(account.Balance)
		}
		slots := make(map[string][]byte, len(account.Storage))
		for slot, value := range account.Storage {
			slots[string(hashBytes(slot))] = hashBytes(value)
		}
		states = append(states, eip8297.State{Address: addressBytes(address), Nonce: account.Nonce, Balance: *uint256.MustFromBig(balance), Code: bytes.Clone(account.Code), Slots: slots})
	}
	for i := range states {
		switch string(states[i].Address) {
		case string(addressBytes(source.Sender)):
			states[i].Nonce = 4
		case string(addressBytes(source.Contract)):
			states[i].Slots[string(hashBytes(common.Hash{}))] = hashBytes(common.BigToHash(big.NewInt(4)))
		}
	}
	zeroAddress := common.Address{}
	zeroFunds := new(big.Int).Mul(big.NewInt(8), new(big.Int).SetUint64(common.Ether))
	states = append(states, eip8297.State{Address: addressBytes(zeroAddress), Balance: *uint256.MustFromBig(zeroFunds)})
	entries := eip8297.EmbedState([][]eip8297.State{states})
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	return entries
}

func TestConvertPBTCases(t *testing.T) {
	code := bytes.Repeat([]byte{1}, eip8297.StemSubtreeWidth*eip8297.ChunkDataLen+1)
	t.Run("code spanning groups", func(t *testing.T) {
		rows, binRoot, wantRoot, _, _, _, _ := convertedPBTAcceptanceRows(t, code, 2)
		codeHash := crypto.Keccak256Hash(code)
		for index, chunk := range eip8297.ChunkifyCode(code) {
			require.Equal(t, chunk[:], rows[string(eip8297.TreeKeyCodeChunk(codeHash, index))], "code chunk %d must be present in its stem", index)
		}
		require.Equal(t, wantRoot, binRoot, "code spanning groups root")
	})
	t.Run("shared code chunked once", func(t *testing.T) {
		rows, binRoot, wantRoot, _, _, referenceRows, _ := convertedPBTAcceptanceRows(t, code, 2)
		wantCode := make(map[string][]byte)
		gotCode := make(map[string][]byte)
		for key, value := range referenceRows {
			if len(key) > 0 && key[0] == eip8297.CodeZone {
				wantCode[key] = value
			}
		}
		for key, value := range rows {
			if len(key) > 0 && key[0] == eip8297.CodeZone {
				gotCode[key] = value
			}
		}
		require.Equal(t, wantCode, gotCode, "shared code chunks must match a single-holder run")
		require.Equal(t, wantRoot, binRoot, "shared code chunks root")
	})
	t.Run("zero chunks absent", func(t *testing.T) {
		rows, binRoot, wantRoot, _, _, _, _ := convertedPBTAcceptanceRows(t, code, 2)
		zeroCodeHash := crypto.Keccak256Hash(make([]byte, 31))
		for index := range eip8297.ChunkifyCode(make([]byte, 31)) {
			require.NotContains(t, rows, string(eip8297.TreeKeyCodeChunk(zeroCodeHash, index)), "zero code chunks must be absent")
		}
		require.Equal(t, wantRoot, binRoot, "zero chunks root")
	})
	t.Run("delegation without code leaves", func(t *testing.T) {
		rows, binRoot, wantRoot, _, _, _, _ := convertedPBTAcceptanceRows(t, code, 2)
		delegation := append(append([]byte(nil), eip8297.DelegationMarker[:]...), bytes.Repeat([]byte{7}, 20)...)
		delegationHash := crypto.Keccak256Hash(delegation)
		for index := range eip8297.ChunkifyCode(delegation) {
			require.NotContains(t, rows, string(eip8297.TreeKeyCodeChunk(delegationHash, index)), "delegation must not add code chunks")
		}
		require.Equal(t, wantRoot, binRoot, "delegation root")
	})
	t.Run("root and record parity", func(t *testing.T) {
		tinyRows, tinyRoot, tinyWantRoot, _, _, _, tinyOutput := convertedPBTAcceptanceRows(t, code, 2)
		defaultRows, defaultRoot, defaultWantRoot, _, _, _, defaultOutput := convertedPBTAcceptanceRows(t, code, 100_000)
		require.Equal(t, defaultRows, tinyRows, "multi-batch rows must match the default writer")
		require.Equal(t, defaultRoot, tinyRoot, "multi-batch root must match the default writer")
		require.Equal(t, defaultWantRoot, tinyWantRoot, "multi-batch reference root")
		require.Equal(t, pbtCommitmentBinKVBytes(t, defaultOutput), pbtCommitmentBinKVBytes(t, tinyOutput), "multi-batch files must match the default writer")
	})
	t.Run("right-edge reads", func(t *testing.T) {
		rows, binRoot, wantRoot, rightEdge, _, _, _ := convertedPBTAcceptanceRows(t, code, 2)
		require.NotEmpty(t, rows[rightEdge], "the right-edge row must be present in the output")
		require.Equal(t, wantRoot, binRoot, "right-edge root")
	})
}

func TestConvertPBTOutputPassesCommitmentIntegrity(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	rawDB := dbCfg(dbcfg.ChainDB, source.Chaindata).MustOpen()
	t.Cleanup(rawDB.Close)
	agg := dbstate.New(datadir.Open(output)).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	t.Cleanup(agg.Close)
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	hexRoot := readPBTHexRoot(t, output)
	reader := conversionIntegrityBlockReader{root: hexRoot}
	require.NoError(t, integrity.CheckCommitmentRoot(t.Context(), db, reader, true, log.New()))
}

func TestConvertPBTHexMultiRangeOutputPassesCommitmentIntegrity(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSourceAt(t, 15)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantHexBin, settings.TrieVariantName())
	rawDB := dbCfg(dbcfg.ChainDB, source.Chaindata).MustOpen()
	t.Cleanup(rawDB.Close)
	agg := dbstate.New(datadir.Open(output)).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	t.Cleanup(agg.Close)
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	reader := multiConversionIntegrityBlockReader{roots: map[uint64]common.Hash{1: readPBTFirstHexRoot(t, output, 8), 2: readPBTHexRoot(t, output)}}
	require.NoError(t, integrity.CheckCommitmentRoot(t.Context(), db, reader, true, log.New()))
}

func TestConvertPBTEmptyStateHasZeroRoot(t *testing.T) {
	commitmentflags.Restore(t)
	previousDatadir := datadirCli
	previousChaindata := chaindata
	t.Cleanup(func() {
		datadirCli = previousDatadir
		chaindata = previousChaindata
	})
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source := newPBTEmptyConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
	require.Equal(t, eip8297.EmptyTreeHash, readPBTBinRoot(t, output, source.Chaindata))
}

func TestConvertPBTPrunedSourceOpens(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(source.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		return dbstate.SavePruneValProgress(tx, kv.AccountsDomain.String(), &prune.Stat{TxTo: 8, KeyProgress: prune.Done, ValueProgress: prune.Done})
	}))
	rawDB.Close()
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
}

func TestConvertPBTHexBinSourceConfiguresVariantBeforeOpening(t *testing.T) {
	selectPBTCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFiles(t, source)
	wantRoot := readPBTFilesRoot(t, source.Tester.Dirs.DataDir)
	source.Tester.Close()
	selectPBTHexCommandSuite(t)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, output, false, "", log.New()))
	selectPBTBinaryCommandSuite(t)
	settings, err := dbstate.ResolveErigonDBSettings(datadir.Open(output), log.New(), true)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantBin, settings.TrieVariantName())
	legacyFiles, err := filepath.Glob(filepath.Join(datadir.Open(output).SnapDomain, "v3.0-commitment.*.kv"))
	require.NoError(t, err)
	require.Empty(t, legacyFiles, "bin-only output must not use the v3 hex schema")
	binFiles, err := filepath.Glob(filepath.Join(datadir.Open(output).SnapDomain, "v2.2-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, binFiles, "bin-only output must publish commitment files")
	statecfg.InitSchemas()
	statecfg.DisableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NotPanics(t, func() {
		agg := dbstate.New(datadir.Open(output)).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Logger(log.New()).MustOpen(t.Context())
		require.NoError(t, agg.OpenFolder(nil))
		at := agg.BeginFilesRo()
		require.NotEmpty(t, at.AllFiles())
		at.Close()
		agg.Close()
	})
	require.Equal(t, wantRoot, readPBTBinRoot(t, output, source.Tester.Dirs.Chaindata))
}

func TestConvertPBTHexBinSourceWithOrdinaryHexFlags(t *testing.T) {
	commitmentflags.Restore(t)
	previousDatadir := datadirCli
	previousChaindata := chaindata
	t.Cleanup(func() {
		datadirCli = previousDatadir
		chaindata = previousChaindata
	})
	statecfg.InitSchemas()
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	source, _ := newPBTConversionSource(t)
	statecfg.InitSchemas()
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	dualPath := filepath.Join(t.TempDir(), "dual")
	require.NotPanics(t, func() {
		require.NoError(t, convertPBT(t.Context(), source.DataDir, dualPath, true, "", log.New()))
	})
	dualRoot := readPBTBinRoot(t, dualPath, source.Chaindata)
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(source.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		forkTime := uint64(1)
		if err := rawdb.WriteChainConfig(tx, genesisHash, &chainpkg.Config{BinaryTrieTime: &forkTime}); err != nil {
			return err
		}
		header := &types.Header{Number: *uint256.NewInt(1), Time: 1, Root: dualRoot}
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return err
		}
		return rawdb.WriteCanonicalHash(tx, header.Hash(), 1)
	}))
	rawDB.Close()
	require.NoError(t, os.Symlink(source.Chaindata, filepath.Join(dualPath, "chaindata")))
	dual := pbtConversionSource{Dirs: datadir.Open(dualPath)}
	statecfg.InitSchemas()
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	statecfg.BinCommitmentHash = ""
	output := filepath.Join(t.TempDir(), "output")
	require.NotPanics(t, func() {
		require.NoError(t, convertPBT(t.Context(), dual.DataDir, output, false, "", log.New()))
	})
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantBin, settings.TrieVariantName())
}

func TestConvertPBTPostForkHeaderRootMismatch(t *testing.T) {
	selectPBTBinaryCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFiles(t, source)
	rawDB := dbCfg(dbcfg.ChainDB, source.Tester.Dirs.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		header := rawdb.ReadHeaderByNumber(tx, 4)
		if header == nil {
			return errors.New("header 4 is missing")
		}
		root := header.Root
		root[0]++
		header.Root = root
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return err
		}
		return rawdb.WriteCanonicalHash(tx, header.Hash(), 4)
	}))
	rawDB.Close()
	for _, keepHex := range []bool{true, false} {
		output := filepath.Join(t.TempDir(), "output")
		err = convertPBT(t.Context(), source.Tester.Dirs.DataDir, output, keepHex, "", log.New())
		require.ErrorContains(t, err, "differs from header root")
		_, statErr := os.Stat(output)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
}

func TestConvertPBTFailureRemovesOutput(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBTWithOutputTestHook(t.Context(), source.DataDir, output, true, "", log.New(), func() error {
		return errors.New("injected conversion failure")
	})
	require.ErrorContains(t, err, "injected conversion failure")
	_, statErr := os.Stat(output)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestConvertPBTVerifiesCorruptedWrittenRows(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	hook := func(dirs datadir.Dirs) error {
		return corruptPBTDomain(t.Context(), dirs, kv.CommitmentBinDomain)
	}
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBTWithStandaloneTestHook(t.Context(), source.DataDir, output, true, "", log.New(), hook)
	require.ErrorContains(t, err, "verify written rows")
}

func TestConvertPBTSkipVerifySkipsWrittenRowsCheck(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	previousHandler := log.Root().GetHandler()
	var logs pbtLogBuffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })
	output := filepath.Join(t.TempDir(), "output")
	hooks := pbtConvertHooks{skipVerify: true, standalone: func(dirs datadir.Dirs) error {
		return corruptPBTDomain(t.Context(), dirs, kv.CommitmentBinDomain)
	}}
	require.NoError(t, convertPBTWithOptions(t.Context(), source.DataDir, output, true, "", log.New(), hooks))
	for _, check := range []string{"C-a", "C-d", "C-e"} {
		require.Contains(t, logs.String(), "check="+check)
	}
	_, err := os.Stat(filepath.Join(datadir.Open(output).Snap, "pbt-manifest.toml"))
	require.NoError(t, err)
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantHexBin, settings.TrieVariantName())
}

func TestConvertPBTSkipVerifyRejectsStateFilesPastPoint(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	require.NoError(t, filepath.WalkDir(source.Snap, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.Contains(entry.Name(), "-accounts.0-1.") {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(strings.Replace(path, ".0-1.", ".1-2.", 1), data, 0o644)
	}))
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBTWithOptions(t.Context(), source.DataDir, output, true, "", log.New(), pbtConvertHooks{skipVerify: true})
	require.ErrorContains(t, err, "state files from step 1 hold writes past conversion txNum 7")
}

func TestConvertPBTStampWalkRejectsPastPoint(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	for _, addressByte := range []byte{0x66, 0xff} {
		source, _ := newPBTConversionSourceAt(t, 15)
		addPBTSourcePastPointFile(t, source, addressByte, 17)
		for _, workers := range []int{1, 16} {
			output := filepath.Join(t.TempDir(), fmt.Sprintf("output-%x-%d", addressByte, workers))
			err := convertPBTWithOptions(t.Context(), source.DataDir, output, true, "", log.New(), pbtConvertHooks{workers: workers})
			require.ErrorContains(t, err, "pbin stamp walk: key", "address=%x workers=%d", addressByte, workers)
			require.ErrorContains(t, err, "stamp", "address=%x workers=%d", addressByte, workers)
		}
	}
}

func TestConvertPBTCRejectsCopiedStateFile(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBTWithStandaloneTestHook(t.Context(), source.DataDir, output, true, "", log.New(), func(dirs datadir.Dirs) error {
		files, err := pbtAttachFiles(dirs)
		if err != nil {
			return err
		}
		for _, file := range files {
			if !pbtAttachStateDomain(file.domain) || filepath.Ext(file.path) != ".kv" {
				continue
			}
			data, readErr := os.ReadFile(file.path)
			if readErr != nil {
				return readErr
			}
			copyPath := file.path + ".copy"
			if writeErr := os.WriteFile(copyPath, data, 0o644); writeErr != nil {
				return writeErr
			}
			return os.Rename(copyPath, file.path)
		}
		return errors.New("no state file")
	})
	require.ErrorContains(t, err, "C-e output state file")
}

func TestConvertPBTCRejectsChangedStateRecord(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBTWithOptions(t.Context(), source.DataDir, output, true, "", log.New(), pbtConvertHooks{afterOutput: func() error {
		return corruptPBTDomain(t.Context(), source.Dirs, kv.CommitmentDomain)
	}})
	require.ErrorContains(t, err, "C-e commitment state")
}

func TestConvertPBTManifestCoversSelectedFiles(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
	manifest, err := dbstate.ReadPBTManifest(filepath.Join(datadir.Open(output).Snap, "pbt-manifest.toml"))
	require.NoError(t, err)
	files, err := pbtAttachFiles(datadir.Open(output))
	require.NoError(t, err)
	want := 0
	for _, file := range pbtAttachVisibleFiles(files) {
		if pbtAttachAdoptsFile(file) {
			want++
		}
	}
	require.Equal(t, want, len(manifest.Files))
}

func addPBTSourcePastPointFile(t *testing.T, source pbtConversionSource, addressByte byte, stamp uint64) {
	t.Helper()
	settings, err := dbstate.ReadErigonDBSettings(source.Dirs)
	require.NoError(t, err)
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(source.Chaindata).MustOpen()
	agg := dbstate.New(source.Dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	address := bytes.Repeat([]byte{addressByte}, length.Addr)
	account := accounts.Account{Nonce: 3, Balance: *uint256.NewInt(3), CodeHash: accounts.EmptyCodeHash}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), stamp, nil))
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
	domains.Close()
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), false))
	agg.WaitForFiles()
	db.Close()
	agg.Close()
	rawDB.Close()
}

type pbtDomainRow struct {
	key, value []byte
	stamp      uint64
}

func corruptPBTDomain(ctx context.Context, dirs datadir.Dirs, domain kv.Domain) error {
	return rewritePBTDomain(ctx, dirs, domain, func(rows *[]pbtDomainRow) error {
		if domain == kv.CommitmentDomain {
			state, err := commitment.EncodeCommitmentV3State(make([]byte, 32), 1, 6, nil)
			if err != nil {
				return err
			}
			changed := false
			for i := range *rows {
				if bytes.Equal((*rows)[i].key, commitment.KeyCommitmentV3State) {
					(*rows)[i].value = state
					changed = true
				}
			}
			if !changed {
				return errors.New("commitment state record is missing")
			}
			return nil
		}
		for i := range *rows {
			if !commitment.IsCommitmentStateKey((*rows)[i].key) {
				(*rows)[i].value[0] ^= 1
				return nil
			}
		}
		return errors.New("no binary row to corrupt")
	})
}

func removePBTGlobalRootDomain(ctx context.Context, dirs datadir.Dirs) error {
	return rewritePBTDomain(ctx, dirs, kv.CommitmentBinDomain, func(rows *[]pbtDomainRow) error {
		filtered := (*rows)[:0]
		for _, row := range *rows {
			if !bytes.Equal(row.key, pbt.GlobalRootKey()) {
				filtered = append(filtered, row)
			}
		}
		if len(filtered) == len(*rows) {
			return errors.New("binary root record is missing")
		}
		*rows = filtered
		return nil
	})
}

func rewritePBTDomain(ctx context.Context, dirs datadir.Dirs, domain kv.Domain, mutate func(*[]pbtDomainRow) error) error {
	refs := false
	variant := dbstate.TrieVariantHexBin
	hash := commitment.PBinHashBlake3
	settings := &dbstate.ErigonDBSettings{StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs, TrieVariant: &variant, TrieHash: &hash}
	if existing, readErr := dbstate.ReadErigonDBSettings(dirs); readErr == nil {
		settings = existing
		settings.TrieVariant = &variant
		settings.TrieHash = &hash
	}
	if err := dbstate.WriteErigonDBSettings(dirs, settings); err != nil {
		return err
	}
	configurePBTSourceVariant(settings)
	prefix := "convert-pbt-corrupt-chaindata-"
	if domain == kv.CommitmentDomain {
		prefix = "convert-pbt-corrupt-state-chaindata-"
	}
	rawPath, err := os.MkdirTemp("", prefix)
	if err != nil {
		return err
	}
	defer func() { _ = dir.RemoveAll(rawPath) }()
	rawDB, err := mdbx.New(dbcfg.ChainDB, log.New()).Path(rawPath).Open(ctx)
	if err != nil {
		return err
	}
	if err := rawDB.Update(ctx, func(tx kv.RwTx) error {
		if err := rawdbv3.TxNums.Append(tx, 0, 0); err != nil {
			return err
		}
		return rawdbv3.TxNums.Append(tx, 1, 7)
	}); err != nil {
		rawDB.Close()
		return err
	}
	agg, err := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).Open(ctx)
	if err != nil {
		rawDB.Close()
		return err
	}
	if err := agg.OpenFolder(rawDB); err != nil {
		agg.Close()
		rawDB.Close()
		return err
	}
	at := agg.BeginFilesRo()
	it, err := at.DebugRangeLatestFromFiles(domain, nil, nil, kv.Unlim)
	if err != nil {
		at.Close()
		agg.Close()
		rawDB.Close()
		return err
	}
	iter := it.(*dbstate.DomainLatestIterFile)
	rows := make([]pbtDomainRow, 0)
	for iter.HasNext() {
		key, value, nextErr := iter.Next()
		if nextErr != nil {
			iter.Close()
			at.Close()
			agg.Close()
			rawDB.Close()
			return nextErr
		}
		stamp := uint64(7)
		if domain == kv.CommitmentDomain {
			stamp = iter.Stamp()
		}
		rows = append(rows, pbtDomainRow{key: bytes.Clone(key), value: bytes.Clone(value), stamp: stamp})
	}
	iter.Close()
	at.Close()
	agg.Close()
	rawDB.Close()
	if err := mutate(&rows); err != nil {
		return err
	}
	if err := removePBTOutputDomainFilesForHook(dirs, domain.String()); err != nil {
		return err
	}
	rawDB, err = mdbx.New(dbcfg.ChainDB, log.New()).Path(rawPath).Open(ctx)
	if err != nil {
		return err
	}
	defer rawDB.Close()
	agg, err = dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).Open(ctx)
	if err != nil {
		return err
	}
	defer agg.Close()
	if err := agg.OpenFolder(rawDB); err != nil {
		return err
	}
	db, err := dbtemporal.New(rawDB, agg, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	if domain == kv.CommitmentDomain {
		cfg.Variant = commitment.VariantCommitmentV3
	}
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomainOnly(domain), execctx.WithoutCommitmentSeek())
	if err != nil {
		return err
	}
	defer domains.Close()
	for _, row := range rows {
		if err := domains.DomainPut(domain, tx, row.key, row.value, row.stamp, nil); err != nil {
			return err
		}
	}
	if err := domains.Flush(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return agg.BuildFiles2(ctx, db, 0, 1, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), false)
}

func TestRequirePBinSourceFilesEnd(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-1.kv"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v1.0-receipt.1-2.kv"), nil, 0o644))
	require.NoError(t, requirePBinSourceFilesEnd(dirs, 8, 7))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v1.0-storage.1-2.kv"), nil, 0o644))
	require.ErrorContains(t, requirePBinSourceFilesEnd(dirs, 8, 7), "state files from step 1 hold writes past conversion txNum 7")
}

func removePBTOutputDomainFilesForHook(dirs datadir.Dirs, domain string) error {
	for _, root := range []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrNotExist) {
					return nil
				}
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			parsed, _, ok := snaptype.ParseFileName(root, entry.Name())
			if ok && parsed.TypeString == domain {
				return dir.RemoveFile(path)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func TestConvertPBTRemovesFilesStartingAfterConversionPoint(t *testing.T) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 8)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFilesAt(t, source, 8)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	_, conversionTx, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	var past []string
	require.NoError(t, filepath.WalkDir(published, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		parsed, _, parsedOK := snaptype.ParseFileName(filepath.Dir(path), entry.Name())
		if parsedOK && parsed.From*settings.StepSize > conversionTx {
			past = append(past, path)
		}
		return nil
	}))
	require.Empty(t, past)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))
}

func TestConvertPBTStandaloneReopenRequiresBothDomains(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	for _, test := range []struct {
		name   string
		domain string
	}{
		{name: "hex", domain: kv.CommitmentDomain.String()},
		{name: "bin", domain: kv.CommitmentBinDomain.String()},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, _ := newPBTConversionSource(t)
			output := filepath.Join(t.TempDir(), "output")
			hook := func(dirs datadir.Dirs) error {
				removePBTOutputDomainFiles(t, dirs.SnapDomain, test.domain)
				return nil
			}
			err := convertPBTWithStandaloneTestHook(t.Context(), source.DataDir, output, true, "", log.New(), hook)
			require.ErrorContains(t, err, "C-e")
			require.ErrorContains(t, err, "state is not at conversion txNum")
			_, statErr := os.Stat(output)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestConvertPBTBinOnlyRefusesBeforeFork(t *testing.T) {
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBT(t.Context(), source.DataDir, output, false, "", log.New())
	require.ErrorContains(t, err, "after the binary trie fork")
	_, statErr := os.Stat(output)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestConvertPBTRefusesSourceFileEndAfterPoint(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSourceAt(t, 6)
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBT(t.Context(), source.DataDir, output, true, "", log.New())
	require.ErrorContains(t, err, "pbin stamp walk: key")
	require.ErrorContains(t, err, "stamp 7 after conversion txNum 6")
	_, statErr := os.Stat(output)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestConvertPBTUsesStateFilesWhenCommitmentFilesLag(t *testing.T) {
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	source.Tester.Close()
	lastTx := pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata)
	buildPBTAcceptanceFilesAtWithMerge(t, source, lastTx, false)
	for _, root := range []string{source.Tester.Dirs.SnapDomain, source.Tester.Dirs.SnapHistory, source.Tester.Dirs.SnapIdx, source.Tester.Dirs.SnapAccessors} {
		entries, err := os.ReadDir(root)
		require.NoError(t, err)
		for _, entry := range entries {
			parsed, _, ok := snaptype.ParseFileName(root, entry.Name())
			if ok && parsed.TypeString == kv.CommitmentDomain.String() && parsed.From >= 6 {
				require.NoError(t, dir.RemoveFile(filepath.Join(root, entry.Name())))
			}
		}
	}
	output := filepath.Join(t.TempDir(), "output")
	selectPBTCommandSuite(t)
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	_, txNum, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(5), txNum)
}

func TestConvertPBTRefusesCommitmentFileWithoutAccessor(t *testing.T) {
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	source.Tester.Close()
	lastTx := pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata)
	buildPBTAcceptanceFilesAtWithMerge(t, source, lastTx, true)
	for _, root := range []string{source.Tester.Dirs.SnapDomain, source.Tester.Dirs.SnapAccessors} {
		for _, pattern := range []string{"*-commitment.*.kvi", "*-commitment.*.kvei"} {
			matches, err := filepath.Glob(filepath.Join(root, pattern))
			require.NoError(t, err)
			for _, match := range matches {
				require.NoError(t, dir.RemoveFile(match))
			}
		}
	}
	output := filepath.Join(t.TempDir(), "output")
	conversionStarted := false
	err = convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{
		afterOutput: func() error {
			conversionStarted = true
			return nil
		},
	})
	require.ErrorContains(t, err, "no accessor")
	require.False(t, conversionStarted)
	_, statErr := os.Stat(output)
	require.ErrorIs(t, statErr, os.ErrNotExist)
	commands := printedPBTCommands(err.Error(), "ERIGON_COMMITMENT_V3=true erigon snapshots index")
	require.Len(t, commands, 1)
	erigonBinary := buildPBTTestErigon(t)
	runPrintedPBTCommand(t, erigonBinary, commands[0])
	require.NoError(t, convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, filepath.Join(t.TempDir(), "converted"), true, "", log.New(), pbtConvertHooks{}))
}

func TestRemovePBTFilesPastPointKeepsOnlyPublishedRanges(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	files := []struct {
		root string
		name string
	}{
		{dirs.SnapDomain, "v1.0-accounts.0-1.kv"},
		{dirs.SnapDomain, "v1.0-accounts.1-2.kv"},
		{dirs.SnapHistory, "v1.0-accounts.1-2.v"},
		{dirs.SnapIdx, "v1.0-accounts.1-2.ef"},
		{dirs.SnapAccessors, "v1.0-accounts.1-2.vi"},
	}
	for _, file := range files {
		require.NoError(t, os.MkdirAll(file.root, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(file.root, file.name), []byte(file.name), 0o644))
	}
	require.NoError(t, os.MkdirAll(dirs.Snap, 0o755))
	blockFile := filepath.Join(dirs.Snap, "v1.0-blocks.1-2.seg")
	require.NoError(t, os.WriteFile(blockFile, []byte("block"), 0o644))
	require.NoError(t, removePBTFilesPastPoint(dirs, 8, 7))
	require.FileExists(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-1.kv"))
	for _, file := range files[1:] {
		_, err := os.Stat(filepath.Join(file.root, file.name))
		require.ErrorIs(t, err, os.ErrNotExist, file.name)
	}
	require.FileExists(t, blockFile)
}

func TestConvertPBTIgnoresDatabaseRowsPastFiles(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSourceAtWithFutureLeaf(t, 7, 8)
	addPBTCommitmentStateAfterFiles(t, source, 8)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, output, true, "", log.New()))
}

func addPBTCommitmentStateAfterFiles(t *testing.T, source pbtConversionSource, txNum uint64) {
	t.Helper()
	settings, err := dbstate.ReadErigonDBSettings(source.Dirs)
	require.NoError(t, err)
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(source.Chaindata).MustOpen()
	defer rawDB.Close()
	agg := dbstate.New(source.Dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	defer agg.Close()
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	defer db.Close()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	state, _, err := domains.GetLatest(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State)
	require.NoError(t, err)
	block, _, root, err := commitment.DecodeCommitmentV3State(state)
	require.NoError(t, err)
	futureState, err := commitment.EncodeCommitmentV3State(root, block, txNum, nil)
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State, futureState, txNum, state))
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
}

func TestConvertPBTLegacyHexSourceIsRefusedAndRemoved(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = true
	removePBTConversionState(t, source)
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBT(t.Context(), source.DataDir, output, true, "", log.New())
	require.ErrorContains(t, err, "run commitment convert --v3 or collate first")
	_, statErr := os.Stat(output)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

type pbtConversionSource struct {
	datadir.Dirs
}

func newPBTConversionSource(t *testing.T) (pbtConversionSource, common.Hash) {
	return newPBTConversionSourceAt(t, 7)
}

func newPBTConversionSourceAt(t *testing.T, stateTx uint64) (pbtConversionSource, common.Hash) {
	return newPBTConversionSourceAtWithFutureLeaf(t, stateTx, 0)
}

func newPBTConversionSourceAtWithFutureLeaf(t *testing.T, stateTx, futureLeafStamp uint64) (pbtConversionSource, common.Hash) {
	return newPBTConversionSourceAtWithRows(t, stateTx, futureLeafStamp, 0, nil)
}

func newPBTConversionSourceAtWithRows(t *testing.T, stateTx, futureLeafStamp, storageSlots uint64, sharedCode []byte) (pbtConversionSource, common.Hash) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	refs := false
	variant := dbstate.TrieVariantHex
	require.NoError(t, dbstate.WriteErigonDBSettings(dirs, &dbstate.ErigonDBSettings{
		StepSize:                       8,
		StepsInFrozenFile:              1,
		ReferencesInCommitmentBranches: &refs,
		TrieVariant:                    &variant,
	}))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-state.txt"), []byte{1, 2, 3, 4}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-blocks.txt"), []byte("blocks"), 0o644))
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	agg := dbstate.NewTest(dirs).StepSize(8).StepsInFrozenFile(1).WithErigonDBSettings(&dbstate.ErigonDBSettings{
		StepSize:                       8,
		StepsInFrozenFile:              1,
		ReferencesInCommitmentBranches: &refs,
		TrieVariant:                    &variant,
	}).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			db.Close()
			agg.Close()
			rawDB.Close()
		}
	})
	probeTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer probeTx.Rollback()
	genesis := common.Hash{1}
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), probeTx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	address := bytes.Repeat([]byte{0x11}, length.Addr)
	account := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
	if len(sharedCode) != 0 {
		account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(sharedCode))
	}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, probeTx, address, accounts.SerialiseV3(&account), 1, nil))
	if len(sharedCode) != 0 {
		require.NoError(t, domains.DomainPut(kv.CodeDomain, probeTx, address, sharedCode, 1, nil))
	}
	slot := append(bytes.Clone(address), bytes.Repeat([]byte{0x22}, 32)...)
	require.NoError(t, domains.DomainPut(kv.StorageDomain, probeTx, slot, []byte{1}, 1, nil))
	updates := commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
	updates.TouchPlainKey(string(address), accounts.SerialiseV3(&account), updates.TouchAccount)
	if len(sharedCode) != 0 {
		updates.TouchPlainKey(string(address), sharedCode, updates.TouchCode)
	}
	updates.TouchPlainKey(string(slot), []byte{1}, updates.TouchStorage)
	require.NoError(t, putPBTConversionStorageRows(domains, probeTx, address, 1, storageSlots, updates))
	ctx := domains.GetCommitmentCtxForDomain(kv.CommitmentDomain)
	ctx.SetUpdates(updates)
	multiRange := stateTx == 15
	firstCommitmentTx := stateTx
	if multiRange {
		firstCommitmentTx = 7
	}
	hexRoot, err := ctx.ComputeCommitment(t.Context(), probeTx, true, 1, firstCommitmentTx, "conversion-source", nil)
	require.NoError(t, err)
	secondAddress := bytes.Repeat([]byte{0x33}, length.Addr)
	secondSlot := append(bytes.Clone(secondAddress), bytes.Repeat([]byte{0x44}, 32)...)
	secondAccount := accounts.Account{Nonce: 2, Balance: *uint256.NewInt(2), CodeHash: accounts.EmptyCodeHash}
	if len(sharedCode) != 0 {
		secondAccount.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(sharedCode))
	}
	finalRoot := hexRoot
	finalBlock := uint64(1)
	if multiRange {
		finalBlock = 2
	}
	if multiRange {
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, probeTx, secondAddress, accounts.SerialiseV3(&secondAccount), 9, nil))
		if len(sharedCode) != 0 {
			require.NoError(t, domains.DomainPut(kv.CodeDomain, probeTx, secondAddress, sharedCode, 9, nil))
		}
		require.NoError(t, domains.DomainPut(kv.StorageDomain, probeTx, secondSlot, []byte{2}, 9, nil))
		updates = commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
		updates.TouchPlainKey(string(secondAddress), accounts.SerialiseV3(&secondAccount), updates.TouchAccount)
		if len(sharedCode) != 0 {
			updates.TouchPlainKey(string(secondAddress), sharedCode, updates.TouchCode)
		}
		updates.TouchPlainKey(string(secondSlot), []byte{2}, updates.TouchStorage)
		ctx.SetUpdates(updates)
		finalRoot, err = ctx.ComputeCommitment(t.Context(), probeTx, false, finalBlock, 15, "conversion-source", nil)
		require.NoError(t, err)
	}
	domains.Close()
	probeTx.Rollback()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesis, 0))
	chainConfig := &chainpkg.Config{}
	if len(sharedCode) != 0 {
		binaryTrieTime := uint64(0)
		chainConfig.BinaryTrieTime = &binaryTrieTime
	}
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, chainConfig))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 0))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 7))
	if multiRange {
		require.NoError(t, rawdbv3.TxNums.Append(tx, 2, 15))
	}
	domains, err = execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	account.CodeHash = accounts.EmptyCodeHash
	if len(sharedCode) != 0 {
		account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(sharedCode))
	}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), 1, nil))
	if len(sharedCode) != 0 {
		require.NoError(t, domains.DomainPut(kv.CodeDomain, tx, address, sharedCode, 1, nil))
	}
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, slot, []byte{1}, 1, nil))
	hexState, err := commitment.EncodeCommitmentV3State(finalRoot, finalBlock, stateTx, nil)
	require.NoError(t, err)
	if multiRange {
		firstState, encodeErr := commitment.EncodeCommitmentV3State(hexRoot, 1, 7, nil)
		require.NoError(t, encodeErr)
		require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State, firstState, 7, nil))
	} else {
		require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State, hexState, stateTx, nil))
	}
	updates = commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
	updates.TouchPlainKey(string(address), accounts.SerialiseV3(&account), updates.TouchAccount)
	if len(sharedCode) != 0 {
		updates.TouchPlainKey(string(address), sharedCode, updates.TouchCode)
	}
	updates.TouchPlainKey(string(slot), []byte{1}, updates.TouchStorage)
	require.NoError(t, putPBTConversionStorageRows(domains, tx, address, 1, storageSlots, updates))
	ctx = domains.GetCommitmentCtxForDomain(kv.CommitmentDomain)
	ctx.SetUpdates(updates)
	computedRoot, err := ctx.ComputeCommitment(t.Context(), tx, true, 1, firstCommitmentTx, "conversion-source", nil)
	require.NoError(t, err)
	require.Equal(t, hexRoot, computedRoot)
	if multiRange {
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, secondAddress, accounts.SerialiseV3(&secondAccount), 9, nil))
		if len(sharedCode) != 0 {
			require.NoError(t, domains.DomainPut(kv.CodeDomain, tx, secondAddress, sharedCode, 9, nil))
		}
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, secondSlot, []byte{2}, 9, nil))
		updates = commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
		updates.TouchPlainKey(string(secondAddress), accounts.SerialiseV3(&secondAccount), updates.TouchAccount)
		if len(sharedCode) != 0 {
			updates.TouchPlainKey(string(secondAddress), sharedCode, updates.TouchCode)
		}
		updates.TouchPlainKey(string(secondSlot), []byte{2}, updates.TouchStorage)
		ctx.SetUpdates(updates)
		computedRoot, err = ctx.ComputeCommitment(t.Context(), tx, false, finalBlock, 15, "conversion-source", nil)
		require.NoError(t, err)
		require.Equal(t, finalRoot, computedRoot)
		previousState, _, getErr := domains.GetLatest(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State)
		require.NoError(t, getErr)
		require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State, hexState, stateTx, previousState))
	}
	if futureLeafStamp != 0 {
		futureAddress := bytes.Repeat([]byte{0x55}, length.Addr)
		futureAccount := accounts.Account{Nonce: 3, Balance: *uint256.NewInt(3), CodeHash: accounts.EmptyCodeHash}
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, futureAddress, accounts.SerialiseV3(&futureAccount), futureLeafStamp, nil))
	}
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
	domains.Close()
	toStep := kv.Step(1)
	if multiRange {
		toStep = 2
	}
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, toStep, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), false))
	agg.WaitForFiles()
	at := agg.BeginFilesRo()
	require.NotEmpty(t, at.Files(kv.AccountsDomain))
	hashFn := func(data []byte) common.Hash { return common.Hash(blake3.Sum256(data)) }
	if len(sharedCode) != 0 {
		hashFn = eip8297.HashBytes
	}
	builder, err := eip8297.NewStreamRootBuilder(hashFn)
	require.NoError(t, err)
	readTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer readTx.Rollback()
	require.NoError(t, dbstate.ForEachPBinLeaf(at, readTx, false, func(leaf dbstate.PBinLeaf) error {
		return builder.Add(leaf.Key, leaf.Value)
	}))
	pbtRoot, err := builder.RootHash()
	require.NoError(t, err)
	readTx.Rollback()
	if len(sharedCode) != 0 {
		require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
			header := &types.Header{Number: *uint256.NewInt(finalBlock), Time: 1, Root: pbtRoot}
			if err := rawdb.WriteHeader(tx, header); err != nil {
				return err
			}
			return rawdb.WriteCanonicalHash(tx, header.Hash(), finalBlock)
		}))
	}
	at.Close()
	db.Close()
	agg.Close()
	rawDB.Close()
	closed = true
	return pbtConversionSource{Dirs: dirs}, pbtRoot
}

func putPBTConversionStorageRows(domains *execctx.SharedDomains, tx kv.TemporalTx, address []byte, txNum, count uint64, updates *commitment.Updates) error {
	for slotIndex := uint64(eip8297.HeaderStorageSlots); slotIndex < eip8297.HeaderStorageSlots+count; slotIndex++ {
		slot := append(bytes.Clone(address), make([]byte, 32)...)
		binary.BigEndian.PutUint64(slot[length.Addr+24:], slotIndex)
		value := []byte{byte(slotIndex)}
		if err := domains.DomainPut(kv.StorageDomain, tx, slot, value, txNum, nil); err != nil {
			return err
		}
		updates.TouchPlainKey(string(slot), value, updates.TouchStorage)
	}
	return nil
}

func newPBTEmptyConversionSource(t *testing.T) pbtConversionSource {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	refs := false
	variant := dbstate.TrieVariantHex
	require.NoError(t, dbstate.WriteErigonDBSettings(dirs, &dbstate.ErigonDBSettings{
		StepSize:                       8,
		StepsInFrozenFile:              1,
		ReferencesInCommitmentBranches: &refs,
		TrieVariant:                    &variant,
	}))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-state.txt"), []byte{1, 2, 3, 4}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-blocks.txt"), []byte("blocks"), 0o644))
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	agg := dbstate.NewTest(dirs).StepSize(8).StepsInFrozenFile(1).WithErigonDBSettings(&dbstate.ErigonDBSettings{
		StepSize:                       8,
		StepsInFrozenFile:              1,
		ReferencesInCommitmentBranches: &refs,
		TrieVariant:                    &variant,
	}).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	genesis := common.Hash{1}
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesis, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, &chainpkg.Config{}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 0))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 7))
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	state, err := commitment.EncodeCommitmentV3State(make([]byte, 32), 1, 7, nil)
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State, state, 7, nil))
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
	domains.Close()
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), false))
	agg.WaitForFiles()
	db.Close()
	agg.Close()
	rawDB.Close()
	return pbtConversionSource{Dirs: dirs}
}

func removePBTConversionState(t *testing.T, source pbtConversionSource) {
	t.Helper()
	settings, err := dbstate.ReadErigonDBSettings(source.Dirs)
	require.NoError(t, err)
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(source.Chaindata).MustOpen()
	agg := dbstate.New(source.Dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	previous, _, err := domains.GetLatest(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State)
	require.NoError(t, err)
	require.NotEmpty(t, previous)
	require.NoError(t, domains.DomainDel(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State, 9, previous))
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
	domains.Close()
	db.Close()
	agg.Close()
	rawDB.Close()
	entries, err := os.ReadDir(source.SnapDomain)
	require.NoError(t, err)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "v3.") && strings.Contains(entry.Name(), "commitment") {
			require.NoError(t, dir.RemoveFile(filepath.Join(source.SnapDomain, entry.Name())))
		}
	}
}

func removePBTOutputDomainFiles(t *testing.T, snapDomain, domain string) {
	t.Helper()
	entries, err := os.ReadDir(snapDomain)
	require.NoError(t, err)
	removed := false
	for _, entry := range entries {
		parsed, _, ok := snaptype.ParseFileName(snapDomain, entry.Name())
		if !ok || parsed.TypeString != domain {
			continue
		}
		require.NoError(t, dir.RemoveFile(filepath.Join(snapDomain, entry.Name())))
		removed = true
	}
	require.True(t, removed)
}

func readPBTBinRoot(t *testing.T, output, rawPath string) common.Hash {
	t.Helper()
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	defer func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	}()
	dirs := datadir.Open(output)
	settings, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	domain := kv.CommitmentBinDomain
	if settings.TrieVariantName() == dbstate.TrieVariantBin {
		domain = kv.CommitmentDomain
	}
	rawDB := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	at := agg.BeginFilesRo()
	defer func() {
		at.Close()
		agg.Close()
		rawDB.Close()
	}()
	iter, err := at.DebugRangeLatestFromFiles(domain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	hasRows := false
	for iter.HasNext() {
		key, _, nextErr := iter.Next()
		require.NoError(t, nextErr)
		if commitment.IsCommitmentStateKey(key) {
			continue
		}
		hasRows = true
	}
	iter.Close()
	if !hasRows {
		return eip8297.EmptyTreeHash
	}
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithCommitmentDomainOnly(domain))
	require.NoError(t, err)
	root, err := domains.GetCommitmentCtxForDomain(domain).Trie().RootHash()
	require.NoError(t, err)
	domains.Close()
	tx.Rollback()
	db.Close()
	return common.BytesToHash(root)
}

func readPBTBinRows(t *testing.T, output, rawPath string) map[string][]byte {
	t.Helper()
	dirs := datadir.Open(output)
	settings, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	rawDB := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	defer rawDB.Close()
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	iter, err := at.DebugRangeLatestFromFiles(kv.CommitmentBinDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	rows := make(map[string][]byte)
	for iter.HasNext() {
		key, value, nextErr := iter.Next()
		require.NoError(t, nextErr)
		if commitment.IsCommitmentStateKey(key) {
			continue
		}
		rows[string(key)] = bytes.Clone(value)
	}
	iter.Close()
	return rows
}

func pbtCommitmentBinKVBytes(t *testing.T, output string) map[string][]byte {
	return pbtCommitmentBinFiles(t, output, ".kv")
}

func pbtCommitmentBinFiles(t *testing.T, output, suffix string) map[string][]byte {
	t.Helper()
	names, err := dir.ListFiles(datadir.Open(output).SnapDomain, suffix)
	require.NoError(t, err)
	files := make(map[string][]byte)
	for _, name := range names {
		if !strings.Contains(filepath.Base(name), kv.CommitmentBinDomain.String()) {
			continue
		}
		value, err := os.ReadFile(name)
		require.NoError(t, err)
		files[filepath.Base(name)] = value
	}
	return files
}

func pbtSortedBinFileNames(files map[string][]byte) []string {
	return slices.Sorted(maps.Keys(files))
}

func pbtResumeRangeKeys(t *testing.T, dirs datadir.Dirs, domain kv.Domain) []string {
	t.Helper()
	files, err := pbtResumeCommitmentFiles(dirs, domain)
	require.NoError(t, err)
	keys := make([]string, 0, len(files))
	seen := make(map[string]struct{}, len(files))
	for _, file := range files {
		if filepath.Ext(file.path) != ".kv" {
			continue
		}
		key := pbtResumeLayoutKey(file)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func readPBTBinLeaves(t *testing.T, output, rawPath string) map[string][]byte {
	t.Helper()
	leaves := make(map[string][]byte)
	for key, value := range readPBTBinRows(t, output, rawPath) {
		record, err := pbt.DecodeRecord([]byte(key), value)
		require.NoError(t, err)
		for i := range record.Cells {
			cell := &record.Cells[i]
			if cell.Kind == pbt.LeafCell {
				leaves[string(cell.Key)] = bytes.Clone(cell.Value[:])
			}
		}
	}
	return leaves
}

func readPBTHexRoot(t *testing.T, output string) common.Hash {
	return readPBTHexRootAt(t, output, ^uint64(0))
}

func readPBTFirstHexRoot(t *testing.T, output string, txNum uint64) common.Hash {
	return readPBTHexRootAt(t, output, txNum-1)
}

func readPBTHexRootAt(t *testing.T, output string, maxTxNum uint64) common.Hash {
	t.Helper()
	dirs := datadir.Open(output)
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(nil))
	at := agg.BeginFilesRo()
	defer func() {
		at.Close()
		agg.Close()
	}()
	value, found, _, _, err := at.DebugGetLatestFromFiles(kv.CommitmentDomain, commitment.KeyCommitmentV3State, maxTxNum)
	require.NoError(t, err)
	require.True(t, found)
	_, _, root, err := commitment.DecodeCommitmentV3State(value)
	require.NoError(t, err)
	return common.BytesToHash(root)
}

type conversionIntegrityBlockReader struct {
	dbservices.FullBlockReader
	root common.Hash
}

type multiConversionIntegrityBlockReader struct {
	dbservices.FullBlockReader
	roots map[uint64]common.Hash
}

func (r conversionIntegrityBlockReader) HeaderByNumber(context.Context, kv.Getter, uint64) (*types.Header, error) {
	return &types.Header{Root: r.root}, nil
}

func (r conversionIntegrityBlockReader) TxnumReader() rawdbv3.TxNumsReader {
	return rawdbv3.TxNums
}

func (r multiConversionIntegrityBlockReader) HeaderByNumber(_ context.Context, _ kv.Getter, blockNum uint64) (*types.Header, error) {
	return &types.Header{Root: r.roots[blockNum]}, nil
}

func (r multiConversionIntegrityBlockReader) TxnumReader() rawdbv3.TxNumsReader {
	return rawdbv3.TxNums
}

func TestConvertPBTRejectsMissingAccountsFiles(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBTWithStandaloneTestHook(t.Context(), source.DataDir, output, true, "", log.New(), func(dirs datadir.Dirs) error {
		files, err := pbtAttachFiles(dirs)
		if err != nil {
			return err
		}
		removed := 0
		for _, file := range files {
			if file.domain != kv.AccountsDomain {
				continue
			}
			if err := dir.RemoveFile(file.path); err != nil {
				return err
			}
			removed++
		}
		require.Positive(t, removed)
		return nil
	})
	require.Error(t, err, "C-e accepted output with every accounts file removed; conversion error=%v", err)
}

func TestPBTFileStagesReportIntermediateProgress(t *testing.T) {
	configurePBTGoldenSuite(t, commitment.PBinHashBlake3, true)
	source, _ := newPBTConversionSourceAtWithRows(t, 7, 0, 5000, nil)
	previousHandler := log.Root().GetHandler()
	previousInterval := dbstate.PBTProgressInterval
	var logs pbtLogBuffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	dbstate.PBTProgressInterval = time.Nanosecond
	t.Cleanup(func() {
		log.Root().SetHandler(previousHandler)
		dbstate.PBTProgressInterval = previousInterval
	})
	require.NoError(t, convertPBT(t.Context(), source.DataDir, filepath.Join(t.TempDir(), "published"), true, "", log.New()))
	for _, suffix := range []string{".kv", ".idt", ".kvi"} {
		t.Run(suffix, func(t *testing.T) {
			phase := "range 0-1 " + suffix
			pattern := regexp.MustCompile(`phase="` + regexp.QuoteMeta(phase) + `" done=(\d+) .* total=(\d+)`)
			matches := pattern.FindAllStringSubmatch(logs.String(), -1)
			require.NotEmpty(t, matches, "missing phase %s", phase)
			var intermediate int
			var counts []string
			for _, match := range matches {
				done, err := strconv.ParseUint(match[1], 10, 64)
				require.NoError(t, err)
				total, err := strconv.ParseUint(match[2], 10, 64)
				require.NoError(t, err)
				counts = append(counts, match[1]+"/"+match[2])
				if done > 0 && done < total {
					intermediate++
				}
			}
			require.Positive(t, intermediate, "%s must report during its work loop; saw done/total=%v", phase, counts)
		})
	}
}
