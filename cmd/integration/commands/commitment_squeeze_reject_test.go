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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/logging"
)

func withRebuildFlags(t *testing.T, set func()) {
	t.Helper()
	previous := struct {
		squeeze, clear, resume, noHistory, reset bool
		datadir, output                          string
	}{squeeze, clearCommitment, resume, noHistory, reset, datadirCli, rebuildOutputDatadir}
	t.Cleanup(func() {
		squeeze, clearCommitment, resume, noHistory, reset, datadirCli, rebuildOutputDatadir = previous.squeeze, previous.clear, previous.resume, previous.noHistory, previous.reset, previous.datadir, previous.output
	})
	squeeze, clearCommitment, resume, noHistory, reset = false, false, false, false, false
	rebuildOutputDatadir = ""
	datadirCli = t.TempDir()
	if set != nil {
		set()
	}
}

func TestCommitmentRebuildRefusesHexBinSourceBeforeAnyWork(t *testing.T) {
	src := hexBinSourceDatadirFixture(t)
	before := snapshotTree(t, src.Snap)
	withRebuildFlags(t, func() { datadirCli = src.DataDir })
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	err := commitmentRebuild(db, context.Background(), log.New(), hexTarget(t), nil)
	require.ErrorContains(t, err, "convert-pbt")
	require.Equal(t, before, snapshotTree(t, src.Snap))
}

func TestCommitmentRebuildRunRefusesHexBinSource(t *testing.T) {
	src := hexBinSourceDatadirFixture(t)
	for _, output := range []bool{false, true} {
		t.Run(fmt.Sprintf("output=%t", output), func(t *testing.T) {
			chainDirs := datadir.New(t.TempDir())
			require.NoError(t, os.MkdirAll(chainDirs.Chaindata, 0o755))
			sourceDB := temporaltest.NewTestDB(nil, chainDirs, temporaltest.WithOpenExisting())
			sourceDB.Close()
			withRebuildFlags(t, func() {
				datadirCli = src.DataDir
				chaindata = chainDirs.Chaindata
				if output {
					noHistory = true
					rebuildOutputDatadir = filepath.Join(t.TempDir(), "output")
				}
			})
			before := snapshotTree(t, src.DataDir)
			outputPath := rebuildOutputDatadir
			cmd := &cobra.Command{Use: "rebuild"}
			utils.CobraFlags(cmd, debug.Flags, utils.MetricFlags, logging.Flags)
			cmd.Flags().AddFlagSet(cmd.PersistentFlags())
			cmd.SetContext(t.Context())
			var err error
			require.NotPanics(t, func() {
				err = runCommitmentRebuild(cmd, nil, log.New(), cmd.Context())
			})
			require.ErrorContains(t, err, "use commitment convert-pbt")
			require.Equal(t, before, snapshotTree(t, src.DataDir))
			if output {
				_, err := os.Stat(outputPath)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

func TestCheckRebuildFlags(t *testing.T) {
	for _, test := range []struct {
		name    string
		set     func()
		output  bool
		wantErr string
	}{
		{name: "output with no-history", set: func() { noHistory = true }, output: true},
		{name: "output without no-history", output: true, wantErr: "--no-history"},
		{name: "output with clear-commitment", set: func() { noHistory, clearCommitment = true, true }, output: true, wantErr: "--clear-commitment"},
		{name: "output with reset", set: func() { noHistory, reset = true, true }, output: true, wantErr: "--reset"},
		{name: "clear-commitment with resume", set: func() { clearCommitment, resume = true, true }, wantErr: "--resume"},
		{name: "clear-commitment with no-history", set: func() { clearCommitment, noHistory = true, true }, wantErr: "--no-history"},
		{name: "in-place plain run"},
	} {
		t.Run(test.name, func(t *testing.T) {
			withRebuildFlags(t, test.set)
			err := checkRebuildFlags(test.output)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}
