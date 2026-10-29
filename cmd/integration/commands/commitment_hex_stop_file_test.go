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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	execchain "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
)

const hexStopFileName = "commitment-stop.toml"

type hexStopFixture struct {
	dirs     datadir.Dirs
	stopTx   uint64
	lastTx   uint64
	settings *dbstate.ErigonDBSettings
	config   *execchain.Config
}

func newHexStopFixture(t *testing.T, hexStops bool) hexStopFixture {
	t.Helper()
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChainWithStepSizeAndFrozenBlocks(t, true, true, 1, 4, 4)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	lastTx := pbtAcceptanceLastTxNum(t, node.Tester)
	node.Tester.Close()
	dirs := node.Tester.Dirs
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	stopTx := lastTx / 2
	require.Greater(t, lastTx, stopTx)

	rawDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	finality := execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, kv.Step(stopTx/settings.StepSize+1), finality, true))
	agg.WaitForFiles()
	if hexStops {
		agg.StopCommitmentDomain(kv.CommitmentDomain)
	}
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, kv.Step(lastTx/settings.StepSize+1), finality, true))
	agg.WaitForFiles()
	db.Close()
	agg.Close()
	rawDB.Close()
	return hexStopFixture{dirs: dirs, stopTx: stopTx, lastTx: lastTx, settings: settings, config: node.Genesis.Config}
}

func (f hexStopFixture) writeStopFile(t *testing.T, txNum uint64) {
	t.Helper()
	body := fmt.Sprintf("[commitment]\nblock = 1\ntxnum = %d\n", txNum)
	require.NoError(t, os.WriteFile(filepath.Join(f.dirs.DataDir, hexStopFileName), []byte(body), 0o644))
}

func (f hexStopFixture) freshChaindata(t *testing.T) kv.RwDB {
	t.Helper()
	require.NoError(t, os.RemoveAll(f.dirs.Chaindata))
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(f.dirs.Chaindata).MustOpen()
	t.Cleanup(rawDB.Close)
	preActivation := f.config.Copy()
	activation := uint64(1 << 40)
	preActivation.BinaryTrieTime = &activation
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		genesis := common.Hash{1}
		if err := rawdb.WriteCanonicalHash(tx, genesis, 0); err != nil {
			return err
		}
		if err := rawdb.WriteChainConfig(tx, genesis, preActivation); err != nil {
			return err
		}
		return nil
	}))
	return rawDB
}

func (f hexStopFixture) openAgg(t *testing.T, rawDB kv.RwDB) (kv.TemporalRwDB, *dbstate.Aggregator) {
	t.Helper()
	agg := dbstate.New(f.dirs).WithErigonDBSettings(f.settings).Logger(log.New()).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	return db, agg
}

func (f hexStopFixture) seek(t *testing.T, db kv.TemporalRwDB) (txNum, blockNum uint64, err error) {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer sd.Close()
	return sd.SeekCommitment(t.Context(), tx)
}

func TestHexStopFileSurvivesChaindataDrop(t *testing.T) {
	f := newHexStopFixture(t, true)
	f.writeStopFile(t, f.stopTx)
	db, agg := f.openAgg(t, f.freshChaindata(t))

	txNum, _, err := f.seek(t, db)
	require.NoError(t, err)
	require.Equal(t, f.lastTx, txNum)
	require.True(t, agg.CommitmentDomainStopped(kv.CommitmentDomain))
	require.False(t, agg.CommitmentDomainStopped(kv.CommitmentBinDomain))
	require.Equal(t, kv.CommitmentBinDomain, agg.CanonicalCommitmentDomain())
}

func TestHexStopFileSurvivesResetExec(t *testing.T) {
	f := newHexStopFixture(t, true)
	f.writeStopFile(t, f.stopTx)
	rawDB := dbCfg(dbcfg.ChainDB, f.dirs.Chaindata).MustOpen()
	db, agg := f.openAgg(t, rawDB)
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentDomain)
	}))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), db))
	agg.Close()
	rawDB.Close()

	rawDB = dbCfg(dbcfg.ChainDB, f.dirs.Chaindata).MustOpen()
	t.Cleanup(rawDB.Close)
	db, agg = f.openAgg(t, rawDB)
	txNum, _, err := f.seek(t, db)
	require.NoError(t, err)
	require.Equal(t, f.lastTx, txNum)
	require.True(t, agg.CommitmentDomainStopped(kv.CommitmentDomain))
}

func TestHexStopFileDroppedWhenDomainsInLockstep(t *testing.T) {
	f := newHexStopFixture(t, false)
	f.writeStopFile(t, f.lastTx+1)
	db, agg := f.openAgg(t, f.freshChaindata(t))

	txNum, _, err := f.seek(t, db)
	require.NoError(t, err)
	require.Equal(t, f.lastTx, txNum)
	require.False(t, agg.CommitmentDomainStopped(kv.CommitmentDomain))
	require.NoFileExists(t, filepath.Join(f.dirs.DataDir, hexStopFileName))
}

func TestHexStopFileWithDBKeyLeavesFileInPlace(t *testing.T) {
	f := newHexStopFixture(t, true)
	f.writeStopFile(t, f.stopTx)
	rawDB := dbCfg(dbcfg.ChainDB, f.dirs.Chaindata).MustOpen()
	t.Cleanup(rawDB.Close)
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentDomain)
	}))
	db, agg := f.openAgg(t, rawDB)

	txNum, _, err := f.seek(t, db)
	require.NoError(t, err)
	require.Equal(t, f.lastTx, txNum)
	require.True(t, agg.CommitmentDomainStopped(kv.CommitmentDomain))
	require.FileExists(t, filepath.Join(f.dirs.DataDir, hexStopFileName))
}

func TestHexStopFileAppliesWhenStopPointIsPastBinEnd(t *testing.T) {
	f := newHexStopFixture(t, true)
	f.writeStopFile(t, f.lastTx+1)
	db, agg := f.openAgg(t, f.freshChaindata(t))

	txNum, _, err := f.seek(t, db)
	require.NoError(t, err)
	require.Equal(t, f.lastTx, txNum)
	require.True(t, agg.CommitmentDomainStopped(kv.CommitmentDomain))
	require.Equal(t, kv.CommitmentBinDomain, agg.CanonicalCommitmentDomain())
}

func TestHexStopFileUntouchedWithoutDB(t *testing.T) {
	f := newHexStopFixture(t, true)
	f.writeStopFile(t, f.stopTx)
	path := filepath.Join(f.dirs.DataDir, hexStopFileName)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	agg := dbstate.New(f.dirs).WithErigonDBSettings(f.settings).Logger(log.New()).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder(nil))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.False(t, agg.CommitmentDomainStopped(kv.CommitmentDomain))
}

func TestHexStopFileCorruptNamesFile(t *testing.T) {
	f := newHexStopFixture(t, true)
	path := filepath.Join(f.dirs.DataDir, hexStopFileName)
	require.NoError(t, os.WriteFile(path, []byte("[commitment\nblock = "), 0o644))
	rawDB := f.freshChaindata(t)
	agg := dbstate.New(f.dirs).WithErigonDBSettings(f.settings).Logger(log.New()).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	err := agg.OpenFolder(rawDB)
	require.ErrorContains(t, err, hexStopFileName)
}
