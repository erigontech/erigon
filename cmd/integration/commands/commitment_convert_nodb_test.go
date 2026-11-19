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
	"crypto/sha256"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

func writeErigonDBSettings(t *testing.T, dirs datadir.Dirs, stepSize uint64) {
	t.Helper()
	settings := fmt.Sprintf("step_size = %d\nsteps_in_frozen_file = %d\n", stepSize, config3.DefaultStepsInFrozenFile)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), []byte(settings), 0o644))
}

func legacyDatadirWithoutChaindata(t *testing.T) (dirs datadir.Dirs, stepSize, lastFiledTxNum uint64, roots []common.Hash) {
	t.Helper()
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, dirs, roots := dbWithV3StateRoots(t)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	stepSize, lastFiledTxNum = agg.StepSize(), agg.EndTxNumMinimax()-1
	writeErigonDBSettings(t, dirs, stepSize)
	db.Close()
	require.NoError(t, dir.RemoveAll(dirs.Chaindata))
	return dirs, stepSize, lastFiledTxNum, roots
}

func legacyDatadirWithCommitmentHistory(t *testing.T) datadir.Dirs {
	t.Helper()
	t.Cleanup(statecfg.SetCommitmentV3(false))
	statecfg.EnableHistoricalCommitment()
	db, dirs, _ := dbWithV3StateRoots(t)
	writeErigonDBSettings(t, dirs, db.(state.HasAgg).Agg().(*state.Aggregator).StepSize())
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		enabled, flagged, err := rawdb.ReadDBCommitmentHistoryEnabled(tx)
		require.NoError(t, err)
		require.False(t, enabled || flagged, "the fixture DB carries no commitment history flag")
		return nil
	}))
	db.Close()
	statecfg.SetCommitmentV3(false)
	return dirs
}

func resetConvertFlags() {
	convertSqueeze, convertNibblesV2, convertRestore, convertContinue, convertV3 = false, false, false, false, false
	convertNoDomain, convertNoHistory = false, false
	cmdCommitmentConvert.Flags().Lookup("squeeze").Changed = false
	cmdCommitmentConvert.Flags().Lookup("nibbles.v2").Changed = false
}

func runCommitmentConvert(t *testing.T, dirs datadir.Dirs, flags ...string) error {
	t.Helper()
	previousDatadir, previousChaindata, previousChain := datadirCli, chaindata, chain
	t.Cleanup(func() {
		datadirCli, chaindata, chain = previousDatadir, previousChaindata, previousChain
		resetConvertFlags()
	})
	resetConvertFlags()
	root := integrationRoot()
	cmdCommitmentConvert.SetContext(t.Context())
	root.SetArgs(append([]string{"--log.dir.disable", "commitment", "convert", "--datadir", dirs.DataDir, "--chain", networkname.Test}, flags...))
	return root.ExecuteContext(t.Context())
}

func requireNoThrowawayDB(t *testing.T, dirs datadir.Dirs) {
	t.Helper()
	left, err := filepath.Glob(filepath.Join(dirs.Tmp, "commitment-convert-*"))
	require.NoError(t, err)
	require.Empty(t, left)
}

func requireCommitmentFilesPrefix(t *testing.T, dirPath, glob, prefix string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dirPath, glob))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		require.True(t, strings.HasPrefix(filepath.Base(f), prefix), f)
	}
}

func requireCommitmentKVPrefix(t *testing.T, dirs datadir.Dirs, prefix string) {
	t.Helper()
	requireCommitmentFilesPrefix(t, dirs.SnapDomain, "*-commitment.*.kv", prefix)
}

func snapshotTree(t *testing.T, root string) map[string]string {
	t.Helper()
	tree := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			tree[rel+string(filepath.Separator)] = ""
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		tree[rel] = fmt.Sprintf("%x", sha256.Sum256(data))
		return nil
	}))
	return tree
}

func requireConvertRefusedBeforeWriting(t *testing.T, dirs datadir.Dirs, wantErr string, flags ...string) {
	t.Helper()
	before := snapshotTree(t, dirs.Snap)
	require.ErrorContains(t, runCommitmentConvert(t, dirs, flags...), wantErr)
	require.Equal(t, before, snapshotTree(t, dirs.Snap))
	requireNoThrowawayDB(t, dirs)
}

func TestCommitmentConvertWithoutChaindata(t *testing.T) {
	dirs, stepSize, lastTxNum, roots := legacyDatadirWithoutChaindata(t)

	require.NoError(t, runCommitmentConvert(t, dirs, "--v3"))

	require.NoDirExists(t, dirs.Chaindata)
	requireNoThrowawayDB(t, dirs)
	requireCommitmentKVPrefix(t, dirs, "v3.")

	require.NoError(t, state.MatchCommitmentSchemaToFiles(dirs))
	agg := state.NewTest(dirs).StepSize(stepSize).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder(nil))
	c, err := state.FoldCommitmentV3(t.Context(), agg, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, lastTxNum, c.TxNum)
	require.Equal(t, roots[lastTxNum], common.Hash(c.Root))
}

func TestCommitmentConvertModesWithoutChaindata(t *testing.T) {
	for _, mode := range []struct {
		name, kvPrefix string
		flags          []string
	}{
		{name: "plain", kvPrefix: "v2.2-"},
		{name: "squeeze", kvPrefix: "v2.1-", flags: []string{"--squeeze=true"}},
		{name: "nibbles.v2", kvPrefix: "v2.2-", flags: []string{"--nibbles.v2=true"}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			dirs, _, _, _ := legacyDatadirWithoutChaindata(t)

			require.NoError(t, runCommitmentConvert(t, dirs, mode.flags...))

			require.NoDirExists(t, dirs.Chaindata)
			requireNoThrowawayDB(t, dirs)
			requireCommitmentKVPrefix(t, dirs, mode.kvPrefix)
		})
	}
}

func TestCommitmentConvertRestoreWithoutChaindata(t *testing.T) {
	dirs, _, _, _ := legacyDatadirWithoutChaindata(t)
	require.NoError(t, runCommitmentConvert(t, dirs, "--squeeze=true"))
	requireCommitmentKVPrefix(t, dirs, "v2.1-")

	require.NoError(t, runCommitmentConvert(t, dirs, "--restore"))

	requireCommitmentKVPrefix(t, dirs, "v2.2-")
	require.NoDirExists(t, filepath.Join(dirs.Snap, "backup", "domains"))
}

func TestCommitmentConvertWithoutChaindataRemovesTheThrowawayDBOnError(t *testing.T) {
	dirs, _, _, _ := legacyDatadirWithoutChaindata(t)
	backup := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backup, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(backup, "stale"), []byte{1}, 0o644))

	require.ErrorContains(t, runCommitmentConvert(t, dirs, "--v3"), "already exists with 1 entries")

	require.NoDirExists(t, dirs.Chaindata)
	requireNoThrowawayDB(t, dirs)
}

func TestCommitmentConvertWithoutChaindataOpensV3FilesAtTheirVersions(t *testing.T) {
	dirs, _, _, _ := legacyDatadirWithoutChaindata(t)
	require.NoError(t, runCommitmentConvert(t, dirs, "--v3"))
	statecfg.SetCommitmentV3(false)

	require.ErrorContains(t, runCommitmentConvert(t, dirs, "--squeeze=true"), "apply to legacy commitment files only")

	require.NoDirExists(t, dirs.Chaindata)
	requireNoThrowawayDB(t, dirs)
	requireCommitmentKVPrefix(t, dirs, "v3.")
}

func TestCommitmentConvertWithoutChaindataRefusesMissingSalt(t *testing.T) {
	dirs, _, _, _ := legacyDatadirWithoutChaindata(t)
	require.NoError(t, dir.RemoveFile(filepath.Join(dirs.Snap, "salt-state.txt")))

	require.ErrorIs(t, runCommitmentConvert(t, dirs, "--v3"), state.ErrCannotStartWithoutSaltFiles)

	require.NoDirExists(t, dirs.Chaindata)
	requireNoThrowawayDB(t, dirs)
	requireCommitmentKVPrefix(t, dirs, "v2.2-")
}

func TestCommitmentConvertV3HistoryFromFiles(t *testing.T) {
	dirs := legacyDatadirWithCommitmentHistory(t)
	requireCommitmentFilesPrefix(t, dirs.SnapHistory, "*-commitment.*.v", "v2.")

	require.NoError(t, runCommitmentConvert(t, dirs, "--v3"))

	requireNoThrowawayDB(t, dirs)
	requireCommitmentFilesPrefix(t, dirs.SnapHistory, "*-commitment.*.v", "v3.")
	requireCommitmentKVPrefix(t, dirs, "v3.")
}

func TestCommitmentConvertV3SplitRunFromFiles(t *testing.T) {
	dirs := legacyDatadirWithCommitmentHistory(t)

	require.NoError(t, runCommitmentConvert(t, dirs, "--v3", "--no-domain"))

	requireCommitmentFilesPrefix(t, dirs.SnapHistory, "*-commitment.*.v", "v3.")
	requireCommitmentKVPrefix(t, dirs, "v2.")

	statecfg.SetCommitmentV3(false)
	require.NoError(t, runCommitmentConvert(t, dirs, "--v3", "--no-history"))

	requireNoThrowawayDB(t, dirs)
	requireCommitmentFilesPrefix(t, dirs.SnapHistory, "*-commitment.*.v", "v3.")
	requireCommitmentKVPrefix(t, dirs, "v3.")
}

func TestCommitmentConvertV3EmptySideErrors(t *testing.T) {
	t.Run("no history files", func(t *testing.T) {
		dirs, _, _, _ := legacyDatadirWithoutChaindata(t)
		history, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
		require.NoError(t, err)
		require.Empty(t, history)

		requireConvertRefusedBeforeWriting(t, dirs, "no commitment history files in snapshots/history", "--v3", "--no-domain")
	})

	t.Run("no domain files", func(t *testing.T) {
		dirs, _, _, _ := legacyDatadirWithoutChaindata(t)
		files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*"))
		require.NoError(t, err)
		require.NotEmpty(t, files)
		for _, f := range files {
			require.NoError(t, dir.RemoveFile(f))
		}

		requireConvertRefusedBeforeWriting(t, dirs, "no commitment domain files in snapshots/domain", "--v3", "--no-history")
	})

	t.Run("domain files behind a pending backup", func(t *testing.T) {
		dirs, _, _, _ := legacyDatadirWithoutChaindata(t)
		backup := filepath.Join(dirs.Snap, "backup", "domains")
		require.NoError(t, os.MkdirAll(backup, 0o755))
		files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*"))
		require.NoError(t, err)
		require.NotEmpty(t, files)
		for _, f := range files {
			require.NoError(t, os.Rename(f, filepath.Join(backup, filepath.Base(f))))
		}

		requireConvertRefusedBeforeWriting(t, dirs, "already exists with", "--v3", "--no-history")
	})
}
