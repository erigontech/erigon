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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	chainpkg "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/logging"
)

func TestCommitmentRebuildBinTargetOnExecutedHexBinDatadir(t *testing.T) {
	for _, hash := range []string{commitment.PBinHashKeccak, commitment.PBinHashBlake3} {
		t.Run(hash, func(t *testing.T) {
			fixture := newExecutedHexBinRebuildFixture(t, hash)
			fixture.close()
			before := snapshotTree(t, fixture.dirs.DataDir)
			delete(before, filepath.Join("chaindata", "mdbx.lck"))
			output := filepath.Join(t.TempDir(), "output")

			previousDatadir := datadirCli
			previousChaindata := chaindata
			previousOutput := rebuildOutputDatadir
			previousNoHistory := noHistory
			previousYes := yes
			t.Cleanup(func() {
				datadirCli = previousDatadir
				chaindata = previousChaindata
				rebuildOutputDatadir = previousOutput
				noHistory = previousNoHistory
				yes = previousYes
			})
			datadirCli = fixture.dirs.DataDir
			chaindata = fixture.dirs.Chaindata
			rebuildOutputDatadir = output
			noHistory = true
			yes = true
			statecfg.ExperimentalCommitmentV3 = false

			cmd := &cobra.Command{Use: "rebuild", Run: cmdCommitmentRebuild.Run}
			utils.CobraFlags(cmd, debug.Flags, utils.MetricFlags, logging.Flags)
			cmd.Flags().AddFlagSet(cmd.PersistentFlags())
			cmd.SetContext(t.Context())
			cmd.Run(cmd, nil)

			outputSettings, err := dbstate.ResolveErigonDBSettings(datadir.Open(output), log.New(), false)
			require.NoError(t, err)
			require.Equal(t, dbstate.TrieVariantBin, outputSettings.TrieVariantName())
			require.Equal(t, hash, outputSettings.TrieHashName())

			outputRoot, outputState := reopenBinOutput(t, output, fixture.rawPath)
			require.Equal(t, fixture.root, outputRoot)
			require.Equal(t, fixture.state, outputState)
			after := snapshotTree(t, fixture.dirs.DataDir)
			delete(after, filepath.Join("chaindata", "mdbx.lck"))
			require.Equal(t, before, after)
		})
	}
}

func TestCommitmentRebuildRunResetOnExecutedHexBinDatadir(t *testing.T) {
	fixture := newExecutedHexBinRebuildFixture(t, commitment.PBinHashKeccak)
	fixture.close()
	beforeFiles := snapshotTree(t, fixture.dirs.Snap)
	setExecutionProgress(t, fixture.rawPath, 7)

	previousDatadir := datadirCli
	previousChaindata := chaindata
	previousOutput := rebuildOutputDatadir
	previousNoHistory := noHistory
	previousYes := yes
	previousReset := reset
	t.Cleanup(func() {
		datadirCli = previousDatadir
		chaindata = previousChaindata
		rebuildOutputDatadir = previousOutput
		noHistory = previousNoHistory
		yes = previousYes
		reset = previousReset
	})
	datadirCli = fixture.dirs.DataDir
	chaindata = fixture.dirs.Chaindata
	rebuildOutputDatadir = ""
	noHistory = false
	yes = true
	reset = true

	cmd := &cobra.Command{Use: "rebuild", Run: cmdCommitmentRebuild.Run}
	utils.CobraFlags(cmd, debug.Flags, utils.MetricFlags, logging.Flags)
	cmd.Flags().AddFlagSet(cmd.PersistentFlags())
	cmd.SetContext(t.Context())
	cmd.Run(cmd, nil)

	require.Equal(t, uint64(0), readExecutionStageProgress(t, fixture.rawPath))
	require.Equal(t, beforeFiles, snapshotTree(t, fixture.dirs.Snap))
	root, state := reopenBinSource(t, fixture.dirs.DataDir, fixture.rawPath)
	require.Equal(t, fixture.root, root)
	require.Equal(t, fixture.state, state)
}

func setExecutionProgress(t *testing.T, rawPath string, progress uint64) {
	t.Helper()
	db := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	defer db.Close()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, progress)
	}))
}

func readExecutionStageProgress(t *testing.T, rawPath string) uint64 {
	t.Helper()
	db := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	defer db.Close()
	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	progress, err := stages.GetStageProgress(tx, stages.Execution)
	require.NoError(t, err)
	return progress
}

type executedHexBinRebuildFixture struct {
	dirs    datadir.Dirs
	rawPath string
	root    []byte
	state   []byte
	close   func()
}

func newExecutedHexBinRebuildFixture(t *testing.T, hash string) executedHexBinRebuildFixture {
	t.Helper()
	previousBin := statecfg.ExperimentalBinCommitment
	previousDual := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousDual
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = hash
	require.NoError(t, commitment.SetPBinHashSuite(hash))
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)

	const stepSize = uint64(8)
	const totalTxNum = uint64(32)
	const frozenTxNum = uint64(16)
	dirs := datadir.New(t.TempDir())
	refs := false
	variant := dbstate.TrieVariantHexBin
	require.NoError(t, dbstate.WriteErigonDBSettings(dirs, &dbstate.ErigonDBSettings{
		StepSize:                       stepSize,
		StepsInFrozenFile:              1,
		ReferencesInCommitmentBranches: &refs,
		TrieVariant:                    &variant,
		TrieHash:                       &hash,
	}))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-state.txt"), []byte{1, 2, 3, 4}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-blocks.txt"), []byte("blocks"), 0o644))
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).
		GrowthStep(mdbx.DefaultGrowthStep).MapSize(mdbx.DefaultMapSize).MustOpen()

	rwDB, err := temporal.New(rawDB, dbstate.NewTest(dirs).StepSize(stepSize).StepsInFrozenFile(1).
		WithErigonDBSettings(&dbstate.ErigonDBSettings{
			StepSize:                       stepSize,
			StepsInFrozenFile:              1,
			ReferencesInCommitmentBranches: &refs,
			TrieVariant:                    &variant,
			TrieHash:                       &hash,
		}).Logger(log.New()).MustOpen(t.Context()), nil)
	require.NoError(t, err)
	closed := false
	closeDB := func() {
		if !closed {
			rwDB.Close()
			closed = true
		}
	}
	t.Cleanup(closeDB)

	genesisHash := common.Hash{1}
	firstTx, err := rwDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer firstTx.Rollback()
	require.NoError(t, rawdb.WriteCanonicalHash(firstTx, genesisHash, 0))
	require.NoError(t, rawdb.WriteChainConfig(firstTx, genesisHash, &chainpkg.Config{}))
	for block := uint64(1); block <= 2; block++ {
		require.NoError(t, rawdbv3.TxNums.Append(firstTx, block, block*stepSize))
	}
	sd, err := execctx.NewSharedDomains(t.Context(), firstTx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	updates := commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
	touch := func(updates *commitment.Updates, key []byte) {
		updates.TouchPlainKey(string(key), nil, func(*commitment.KeyUpdate, []byte) {})
	}
	for txNum := range frozenTxNum {
		for accountIndex := range 3 {
			address := make([]byte, length.Addr)
			address[0] = 0x10
			address[length.Addr-1] = byte(accountIndex + 1)
			account := accounts.Account{
				Nonce:    txNum + uint64(accountIndex) + 1,
				Balance:  *uint256.NewInt(txNum*100 + uint64(accountIndex)),
				CodeHash: accounts.EmptyCodeHash,
			}
			previous, _, err := sd.GetLatest(kv.AccountsDomain, firstTx, address)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, firstTx, address, accounts.SerialiseV3(&account), txNum, previous))
			touch(updates, address)
			slot := append(append([]byte{}, address...), make([]byte, length.Hash)...)
			slot[len(slot)-1] = byte(txNum + uint64(accountIndex) + 1)
			previous, _, err = sd.GetLatest(kv.StorageDomain, firstTx, slot)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.StorageDomain, firstTx, slot, []byte{1, byte(txNum + 1)}, txNum, previous))
			touch(updates, slot)
		}
	}
	require.NoError(t, sd.Flush(t.Context(), firstTx))
	hexCtx := sd.GetCommitmentCtxForDomain(kv.CommitmentDomain)
	hexCtx.SetUpdates(updates)
	_, err = hexCtx.ComputeCommitment(t.Context(), firstTx, true, 2, frozenTxNum-1, "fixture", nil)
	require.NoError(t, err, "hex commitment")
	binUpdates := commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
	for txNum := range frozenTxNum {
		for accountIndex := range 3 {
			address := make([]byte, length.Addr)
			address[0] = 0x10
			address[length.Addr-1] = byte(accountIndex + 1)
			touch(binUpdates, address)
			slot := append(append([]byte{}, address...), make([]byte, length.Hash)...)
			slot[len(slot)-1] = byte(txNum + uint64(accountIndex) + 1)
			touch(binUpdates, slot)
		}
	}
	binCtx := sd.GetCommitmentCtxForDomain(kv.CommitmentBinDomain)
	binCtx.SetUpdates(binUpdates)
	_, err = binCtx.ComputeCommitment(t.Context(), firstTx, true, 2, frozenTxNum-1, "fixture", nil)
	require.NoError(t, err)
	require.NoError(t, sd.Flush(t.Context(), firstTx))
	sd.Close()
	require.NoError(t, firstTx.Commit())
	agg := rwDB.Agg().(*dbstate.Aggregator)
	finality := execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)
	require.NoError(t, agg.BuildFiles(rwDB, frozenTxNum, finality))

	secondTx, err := rwDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer secondTx.Rollback()
	for block := uint64(3); block <= 4; block++ {
		require.NoError(t, rawdbv3.TxNums.Append(secondTx, block, block*stepSize))
	}
	sd, err = execctx.NewSharedDomains(t.Context(), secondTx, log.New())
	require.NoError(t, err)
	secondUpdates := commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
	secondBinUpdates := commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
	for txNum := frozenTxNum; txNum < frozenTxNum+stepSize; txNum++ {
		for accountIndex := range 3 {
			address := make([]byte, length.Addr)
			address[0] = 0x10
			address[length.Addr-1] = byte(accountIndex + 1)
			account := accounts.Account{
				Nonce:    txNum + uint64(accountIndex) + 1,
				Balance:  *uint256.NewInt(txNum*100 + uint64(accountIndex)),
				CodeHash: accounts.EmptyCodeHash,
			}
			previous, _, err := sd.GetLatest(kv.AccountsDomain, secondTx, address)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, secondTx, address, accounts.SerialiseV3(&account), txNum, previous))
			touch(secondUpdates, address)
			touch(secondBinUpdates, address)
			slot := append(append([]byte{}, address...), make([]byte, length.Hash)...)
			slot[len(slot)-1] = byte(txNum + uint64(accountIndex) + 1)
			previous, _, err = sd.GetLatest(kv.StorageDomain, secondTx, slot)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.StorageDomain, secondTx, slot, []byte{1, byte(txNum + 1)}, txNum, previous))
			touch(secondUpdates, slot)
			touch(secondBinUpdates, slot)
		}
	}
	require.NoError(t, sd.Flush(t.Context(), secondTx))
	hexCtx = sd.GetCommitmentCtxForDomain(kv.CommitmentDomain)
	hexCtx.SetUpdates(secondUpdates)
	_, err = hexCtx.ComputeCommitment(t.Context(), secondTx, true, 3, frozenTxNum+stepSize-1, "fixture", nil)
	require.NoError(t, err, "hex commitment")
	binCtx = sd.GetCommitmentCtxForDomain(kv.CommitmentBinDomain)
	binCtx.SetUpdates(secondBinUpdates)
	root, err := binCtx.ComputeCommitment(t.Context(), secondTx, true, 3, frozenTxNum+stepSize-1, "fixture", nil)
	require.NoError(t, err)
	require.NoError(t, sd.Flush(t.Context(), secondTx))
	sd.Close()
	require.NoError(t, secondTx.Commit())
	tailTx, err := rwDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tailTx.Rollback()
	sd, err = execctx.NewSharedDomains(t.Context(), tailTx, log.New())
	require.NoError(t, err)
	for txNum := frozenTxNum + stepSize; txNum < totalTxNum; txNum++ {
		for accountIndex := range 3 {
			address := make([]byte, length.Addr)
			address[0] = 0x10
			address[length.Addr-1] = byte(accountIndex + 1)
			account := accounts.Account{
				Nonce:    txNum + uint64(accountIndex) + 1,
				Balance:  *uint256.NewInt(txNum*100 + uint64(accountIndex)),
				CodeHash: accounts.EmptyCodeHash,
			}
			previous, _, err := sd.GetLatest(kv.AccountsDomain, tailTx, address)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, tailTx, address, accounts.SerialiseV3(&account), txNum, previous))
			slot := append(append([]byte{}, address...), make([]byte, length.Hash)...)
			slot[len(slot)-1] = byte(txNum + uint64(accountIndex) + 1)
			previous, _, err = sd.GetLatest(kv.StorageDomain, tailTx, slot)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.StorageDomain, tailTx, slot, []byte{1, byte(txNum + 1)}, txNum, previous))
		}
	}
	require.NoError(t, sd.Flush(t.Context(), tailTx))
	sd.Close()
	require.NoError(t, tailTx.Commit())
	require.NoError(t, agg.BuildFiles(rwDB, totalTxNum, finality))
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	accountFiles := 0
	hexCommitmentFiles := 0
	binCommitmentFiles := 0
	for _, entry := range entries {
		if bytes.Contains([]byte(entry.Name()), []byte("accounts.")) && filepath.Ext(entry.Name()) == ".kv" {
			accountFiles++
		}
		if strings.Contains(entry.Name(), "commitment-bin.") && filepath.Ext(entry.Name()) == ".kv" {
			binCommitmentFiles++
		}
		if strings.Contains(entry.Name(), "commitment.") && !strings.Contains(entry.Name(), "commitment-bin.") && filepath.Ext(entry.Name()) == ".kv" {
			hexCommitmentFiles++
		}
	}
	require.GreaterOrEqual(t, accountFiles, 2)
	require.Greater(t, hexCommitmentFiles, 0)
	require.Greater(t, binCommitmentFiles, 0)
	roTx, err := rwDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	tailAddress := make([]byte, length.Addr)
	tailAddress[0] = 0x10
	tailAddress[length.Addr-1] = 1
	tailAccount, _, err := roTx.GetLatest(kv.AccountsDomain, tailAddress, kv.GetLatestOptions{})
	require.NoError(t, err)
	var account accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&account, tailAccount))
	require.Equal(t, totalTxNum, account.Nonce)
	stateValue, _, err := roTx.GetLatest(kv.CommitmentBinDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	return executedHexBinRebuildFixture{dirs: dirs, rawPath: dirs.Chaindata, root: root, state: bytes.Clone(stateValue), close: closeDB}
}

func reopenBinOutput(t *testing.T, output, rawPath string) ([]byte, []byte) {
	t.Helper()
	dirs := datadir.Open(output)
	settings, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	rawDB := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	t.Cleanup(rawDB.Close)
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	root, err := sd.GetCommitmentCtxForDomain(kv.CommitmentDomain).Trie().RootHash()
	require.NoError(t, err)
	stateValue, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	return root, bytes.Clone(stateValue)
}

func reopenBinSource(t *testing.T, source, rawPath string) ([]byte, []byte) {
	t.Helper()
	dirs := datadir.Open(source)
	settings, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	rawDB := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	t.Cleanup(rawDB.Close)
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	root, err := sd.GetCommitmentCtxForDomain(kv.CommitmentBinDomain).Trie().RootHash()
	require.NoError(t, err)
	state, _, err := tx.GetLatest(kv.CommitmentBinDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	return root, bytes.Clone(state)
}
