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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
)

func pbtR15ResumeFixture(t *testing.T, frozen uint64, mergeBase, mergeSource bool) (*execmoduletester.PBTAcceptanceChain, string) {
	t.Helper()
	selectPBTCommandSuite(t)
	base, err := execmoduletester.NewPBTAcceptanceChainWithTxCounts(t, true, true, 1, frozen, []int{1, 1})
	require.NoError(t, err)
	require.NoError(t, base.Tester.InsertChain(base.Chain))
	base.Tester.Close()
	buildPBTResumeFiles(t, base, pbtAcceptanceLastTxNumRaw(t, base.Tester.Dirs.Chaindata), mergeBase)
	output := filepath.Join(t.TempDir(), "base-output")
	require.NoError(t, convertPBT(t.Context(), base.Tester.Dirs.DataDir, output, true, "", log.New()))
	source, err := execmoduletester.NewPBTAcceptanceChainWithTxCounts(t, true, true, 1, frozen, []int{1, 1, 1, 1})
	require.NoError(t, err)
	source.Tester.Close()
	copyPBTStateSalt(t, base, source)
	source.Tester = execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(source.Tester.Dirs),
		execmoduletester.WithGenesisSpec(source.Genesis),
		execmoduletester.WithKey(source.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
		execmoduletester.WithoutGenesisCommit(),
	)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	source.Tester.Close()
	buildPBTResumeFiles(t, source, pbtAcceptanceLastTxNumRaw(t, source.Tester.Dirs.Chaindata), mergeSource)
	return source, output
}

func TestPBTResumeR15EqualEndPreservesOutput(t *testing.T) {
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

func TestPBTResumeR15SourceMergedBeforeBase(t *testing.T) {
	source, output := pbtR15ResumeFixture(t, 2, false, true)
	err := convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.NoError(t, err, "resume must merge base ranges to match source ranges wholly before X")
}

func TestPBTResumeR15MDBXTombstonePastFiles(t *testing.T) {
	source, output := pbtR15ResumeFixture(t, 1, false, false)
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

func TestPBTResumeR15UnchangedZeroStartBase(t *testing.T) {
	source, output := pbtR15ResumeFixture(t, 8, true, true)
	err := convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.NoError(t, err, "an unchanged zero-start base range contains no gap transactions")
}
