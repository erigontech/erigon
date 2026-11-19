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
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

type recordingHandler struct {
	mu      sync.Mutex
	records []log.Record
}

func (h *recordingHandler) Log(r *log.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, *r)
	return nil
}

func (h *recordingHandler) Enabled(context.Context, log.Lvl) bool { return true }

func (h *recordingHandler) withMsg(msg string) []log.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var found []log.Record
	for _, r := range h.records {
		if r.Msg == msg {
			found = append(found, r)
		}
	}
	return found
}

func recordConsoleLogs(t *testing.T) *recordingHandler {
	t.Helper()
	recorder := &recordingHandler{}
	previousConsole, previousRoot := log.StderrHandler, log.Root().GetHandler()
	log.StderrHandler = recorder
	t.Cleanup(func() {
		log.StderrHandler = previousConsole
		log.Root().SetHandler(previousRoot)
	})
	return recorder
}

func legacyDatadirWithoutChaindata(t *testing.T) (dirs datadir.Dirs, stepSize, lastFiledTxNum uint64, roots []common.Hash) {
	t.Helper()
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, dirs, roots := dbWithV3StateRoots(t)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	stepSize, lastFiledTxNum = agg.StepSize(), agg.EndTxNumMinimax()-1
	settings := fmt.Sprintf("step_size = %d\nsteps_in_frozen_file = %d\n", stepSize, config3.DefaultStepsInFrozenFile)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), []byte(settings), 0o644))
	db.Close()
	require.NoError(t, dir.RemoveAll(dirs.Chaindata))
	return dirs, stepSize, lastFiledTxNum, roots
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

func requireCommitmentKVPrefix(t *testing.T, dirs datadir.Dirs, prefix string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		require.True(t, strings.HasPrefix(filepath.Base(f), prefix), f)
	}
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

func TestCommitmentConvertWithoutChaindataWarnsWhenTheThrowawayDBStaysBehind(t *testing.T) {
	dirs, _, _, _ := legacyDatadirWithoutChaindata(t)
	removeErr := errors.New("injected removal failure")
	var attempted []string
	previousRemove := removeThrowawayDB
	t.Cleanup(func() { removeThrowawayDB = previousRemove })
	removeThrowawayDB = func(path string) error {
		attempted = append(attempted, path)
		return removeErr
	}
	logs := recordConsoleLogs(t)

	require.NoError(t, runCommitmentConvert(t, dirs, "--v3"))

	requireCommitmentKVPrefix(t, dirs, "v3.")
	require.Len(t, attempted, 1)
	require.DirExists(t, attempted[0])
	warned := logs.withMsg("[commitment_convert] removing the throwaway DB failed")
	require.Len(t, warned, 1)
	require.Equal(t, log.LvlWarn, warned[0].Lvl)
	require.Equal(t, []any{"dir", attempted[0], "err", removeErr}, warned[0].Ctx)
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
