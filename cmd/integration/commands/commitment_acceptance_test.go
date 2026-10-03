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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	app "github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPBTAcceptanceChainTxCountsHeaderRoots(t *testing.T) {
	counts := [][]int{{2}, {0}, {1, 1, 1, 1, 1, 1, 2, 0, 1, 1, 1, 1, 1, 1}}
	for _, dual := range []bool{false, true} {
		for _, txCounts := range counts {
			name := fmt.Sprintf("dual=%t-counts=%v", dual, txCounts)
			t.Run(name, func(t *testing.T) {
				if dual {
					selectPBTCommandSuite(t)
				} else {
					selectPBTBinaryCommandSuite(t)
				}
				fixture, err := execmoduletester.NewPBTAcceptanceChainWithTxCounts(t, true, dual, 1, 1, txCounts)
				require.NoError(t, err)
				require.NoError(t, fixture.Tester.InsertChain(fixture.Chain))
				fixture.Tester.Close()
			})
		}
	}
}

func TestPBTAttachAcceptanceAtConversionPoint(t *testing.T) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), node.Tester.DB))
	require.NoError(t, node.Tester.ReExecuteTo(t.Context(), 2))
	buildPBTAcceptanceFilesAt(t, node, 7)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	publishedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	conversionBlock, conversionTx, ok, err := publishedSettings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))
	require.Zero(t, readExecutionStageProgress(t, node.Tester.Dirs.Chaindata))
	require.Equal(t, readPBTFilesRoot(t, published), readPBTFilesRoot(t, node.Tester.Dirs.DataDir))
	dual, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	selectPBTCommandSuite(t)
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	attachedRaw := reopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	var attachedAtConversion, dualAtConversion []byte
	require.NoError(t, attachedRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		attachedAtConversion, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[conversionBlock-1].Hash(), conversionBlock)
		return err
	}))
	require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		dualAtConversion, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[conversionBlock-1].Hash(), conversionBlock)
		return err
	}))
	require.Equal(t, dualAtConversion, attachedAtConversion)
	assertPBTAttachHistory(t, reopened, dual.Tester, conversionTx)
	for block := conversionBlock + 1; block <= node.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
		var attachedRoot []byte
		require.NoError(t, attachedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			attachedRoot, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		var dualRoot []byte
		require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			dualRoot, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.Equal(t, dualRoot, attachedRoot)
	}
	lastTxNum := pbtAcceptanceLastTxNum(t, reopened)
	assertPBTAttachHistory(t, reopened, dual.Tester, lastTxNum)
	attachedAgg := reopened.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	require.NoError(t, attachedAgg.BuildFiles2(t.Context(), reopened.DB, 0, kv.Step(lastTxNum)+1, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), true))
	attachedAgg.WaitForFiles()
	at := attachedAgg.BeginFilesRo()
	wideRange := false
	for _, file := range at.Files(kv.AccountsDomain) {
		if file.EndRootNum()-file.StartRootNum() > 1 {
			wideRange = true
			break
		}
	}
	at.Close()
	require.True(t, wideRange, "the reopened aggregator must merge a range wider than one step")
	reopened.Close()
	selectPBTCommandSuite(t)
	merged := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	assertPBTAttachHistory(t, merged, dual.Tester, lastTxNum)
	merged.Close()
}

func TestPBTAttachPostForkBlockEndShadowRoot(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()

	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	blockNum, txNum, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))

	dual, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	attached := dbCfg(dbcfg.ChainDB, node.Tester.Dirs.Chaindata).MustOpen()
	defer attached.Close()
	dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	var attachedRoot, dualRoot []byte
	require.NoError(t, attached.View(t.Context(), func(tx kv.Tx) error {
		var err error
		attachedRoot, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[blockNum-1].Hash(), blockNum)
		return err
	}))
	require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		dualRoot, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[blockNum-1].Hash(), blockNum)
		return err
	}))
	require.NotEmpty(t, attachedRoot)
	require.Equal(t, dualRoot, attachedRoot)
	require.Equal(t, uint64(7), txNum)
}

func TestPBTAttachRejectsPublishedRootMismatchWithoutMutation(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	hooks := pbtAttachHooks{leafStamps: func(uint64, func(func(dbstate.PBinLeaf) error) error) (common.Hash, error) {
		return common.Hash{0xaa}, nil
	}}
	before := snapshotTree(t, node.Tester.Dirs.DataDir)
	err = attachPBTWithHooks(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New(), hooks)
	require.ErrorContains(t, err, "published binary rows root")
	require.Equal(t, before, snapshotTree(t, node.Tester.Dirs.DataDir))
}

func TestPBTAttachRejectsCorruptedPublishedBinaryRowsWithoutMutation(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	require.NoError(t, corruptPBTOutputRow(t.Context(), datadir.Open(published)))
	setExecutionProgress(t, source.Chaindata, 1)
	before := snapshotTree(t, source.DataDir)
	err := attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
	require.ErrorContains(t, err, "published binary rows")
	require.Equal(t, before, snapshotTree(t, source.DataDir))
}

func TestPBTAttachRejectsNodePBTStateMismatchWithoutMutation(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	node.Tester.Close()
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	before := snapshotTree(t, node.Tester.Dirs.DataDir)
	err = attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "node checkpoint is block")
	require.NotContains(t, err.Error(), "stage_exec")
	require.ErrorContains(t, err, "cannot be loaded by integration")
	require.Equal(t, before, snapshotTree(t, node.Tester.Dirs.DataDir))
	resetPBTAcceptanceExecution(t, node)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))
}

func TestPBTAttachRejectsStateBeyondCheckpointWithoutMutation(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	_, conversionTx, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	m := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	tx, err := m.DB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	fresh := common.Address{0xee}
	code := []byte{0x60, 0x00}
	codeHash := common.Hash(keccak.Sum256(code))
	account := accounts.Account{Balance: *uint256.NewInt(5), CodeHash: accounts.InternCodeHash(codeHash)}
	require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, fresh[:], accounts.SerialiseV3(&account), conversionTx+3, nil))
	slotKey := make([]byte, 20+32)
	copy(slotKey, fresh[:])
	slot := common.Hash{1}
	value := common.Hash{2}
	copy(slotKey[len(fresh):], slot[:])
	require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, slotKey, value[:], conversionTx+3, nil))
	require.NoError(t, doms.DomainPut(kv.CodeDomain, tx, fresh[:], code, conversionTx+3, nil))
	require.NoError(t, doms.Commit(t.Context(), tx))
	doms.Close()
	require.NoError(t, tx.Commit())
	m.Close()
	before := snapshotTree(t, node.Tester.Dirs.DataDir)
	err = attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "node PBT root")
	require.Equal(t, before, snapshotTree(t, node.Tester.Dirs.DataDir))
	resetPBTAcceptanceExecution(t, node)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))
	dual, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	defer reopened.Close()
	require.NoError(t, reopened.ReExecuteTo(t.Context(), node.Chain.TopBlock.NumberU64()))
	attachedRaw := reopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	for block := uint64(1); block <= node.Chain.TopBlock.NumberU64(); block++ {
		var attachedRoot, dualRoot []byte
		require.NoError(t, attachedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			attachedRoot, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			dualRoot, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.Equal(t, dualRoot, attachedRoot, "shadow root at block %d", block)
	}
}

func TestPBTAttachRejectsNodeHexStateMismatchWithoutMutation(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	hooks := pbtAttachHooks{hexRoot: func(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, blockNum, txNum uint64, logger log.Logger) (common.Hash, bool, error) {
		if dirs.DataDir == published {
			return common.Hash{0xaa}, true, nil
		}
		return pbtAttachHexRoot(ctx, dirs, settings, blockNum, txNum, logger)
	}}
	before := snapshotTree(t, node.Tester.Dirs.DataDir)
	err = attachPBTWithHooks(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New(), hooks)
	require.ErrorContains(t, err, "node hex root")
	require.Equal(t, before, snapshotTree(t, node.Tester.Dirs.DataDir))
}

func TestPBTAttachRejectsNodeCheckpointMismatchWithoutMutation(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	overwritePBTImportHexCheckpoint(t, node.Tester.Dirs.DataDir, 1, 6, 8)
	before := snapshotTree(t, node.Tester.Dirs.DataDir)
	err = attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, node.Tester.ChainConfig.ChainName, log.New())
	require.ErrorContains(t, err, "behind conversion point")
	require.ErrorContains(t, err, "txNum 6")
	require.Equal(t, before, snapshotTree(t, node.Tester.Dirs.DataDir))
}

func TestValidatePBTAttachGenesisRejectsMissingHeader(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	published := datadir.New(t.TempDir())
	config := snapcfg.KnownCfgOrDevnet(node.Tester.ChainConfig.ChainName)
	require.NoError(t, os.Link(filepath.Join(node.Tester.Dirs.Snap, "salt-blocks.txt"), filepath.Join(published.Snap, "salt-blocks.txt")))
	require.NoError(t, freezeblocks.DumpBlocks(t.Context(), 0, 3, node.Tester.ChainConfig, node.Tester.Dirs.Tmp, published.Snap, node.Tester.DB, 1, log.LvlInfo, log.New(), node.Tester.BlockReader, config, nil))
	require.NoError(t, filepath.WalkDir(published.Snap, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.Contains(entry.Name(), "headers") {
			return walkErr
		}
		return dir.RemoveFile(path)
	}))
	node.Tester.Close()
	require.ErrorContains(t, validatePBTAttachGenesis(t.Context(), node.Tester.Dirs, published, log.New()), "published genesis is missing")
}

func TestPBTAttachRejectsDifferentPublishedGenesis(t *testing.T) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	published := datadir.New(t.TempDir())
	require.NoError(t, os.Link(filepath.Join(node.Tester.Dirs.Snap, "salt-blocks.txt"), filepath.Join(published.Snap, "salt-blocks.txt")))
	config := snapcfg.KnownCfgOrDevnet(node.Tester.ChainConfig.ChainName)
	require.NoError(t, freezeblocks.DumpBlocks(t.Context(), 0, 3, node.Tester.ChainConfig, node.Tester.Dirs.Tmp, published.Snap, node.Tester.DB, 1, log.LvlInfo, log.New(), node.Tester.BlockReader, config, nil))
	rawDB := node.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	genesisHash := common.Hash{0xff}
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		if err := rawdb.WriteCanonicalHash(tx, genesisHash, 0); err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, genesisHash, node.Tester.ChainConfig)
	}))
	node.Tester.Close()
	require.ErrorContains(t, validatePBTAttachGenesis(t.Context(), node.Tester.Dirs, published, log.New()), "genesis hash")
}

func TestPBTAttachRunsGenesisValidation(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	rawDB := dbCfg(dbcfg.ChainDB, node.Tester.Dirs.Chaindata).MustOpen()
	genesisHash := common.Hash{0xff}
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		if err := rawdb.WriteCanonicalHash(tx, genesisHash, 0); err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, genesisHash, node.Tester.ChainConfig)
	}))
	rawDB.Close()
	before := snapshotTree(t, node.Tester.Dirs.DataDir)
	err = attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "genesis hash")
	require.Equal(t, before, snapshotTree(t, node.Tester.Dirs.DataDir))
}

func TestPBTAttachRejectsMissingPublishedGenesis(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	require.NoError(t, filepath.WalkDir(datadir.Open(published).Snap, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.Contains(entry.Name(), "headers") {
			return walkErr
		}
		return dir.RemoveFile(path)
	}))
	before := snapshotTree(t, node.Tester.Dirs.DataDir)
	err = attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "published genesis is missing")
	require.Equal(t, before, snapshotTree(t, node.Tester.Dirs.DataDir))
}

func TestPBTAttachAcceptanceAtMidBlockConversionPoint(t *testing.T) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), node.Tester.DB))
	require.NoError(t, node.Tester.ReExecuteTo(t.Context(), 3))
	buildPBTAcceptanceFilesAt(t, node, 9)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFilesAt(t, source, 9)
	source.Tester.Close()
	resetNode := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	require.NoError(t, rawdbreset.ResetExec(t.Context(), resetNode.DB))
	resetNode.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	publishedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	conversionBlock, conversionTx, ok, err := publishedSettings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(3), conversionBlock)
	require.Equal(t, uint64(9), conversionTx)
	publishedFiles, err := pbtAttachFiles(datadir.Open(published))
	require.NoError(t, err)
	for _, file := range publishedFiles {
		require.LessOrEqual(t, file.from*publishedSettings.StepSize, conversionTx)
	}
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))

	dual, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	selectPBTCommandSuite(t)
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	assertPBTAttachHistory(t, reopened, dual.Tester, conversionTx)
	for block := conversionBlock; block <= node.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
		attachedRaw := reopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
		var attachedRoot []byte
		require.NoError(t, attachedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			attachedRoot, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
		var dualRoot []byte
		require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			dualRoot, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.Equal(t, dualRoot, attachedRoot)
	}
	assertPBTAttachHistory(t, reopened, dual.Tester, pbtAcceptanceLastTxNum(t, reopened))
	reopened.Close()
}

func TestPBTReplayMatchesConvertedState(t *testing.T) {
	selectPBTHexCommandSuite(t)
	converted, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, converted.Tester.InsertChain(converted.Chain))
	buildPBTAcceptanceFiles(t, converted)
	convertedOutput := filepath.Join(t.TempDir(), "converted")
	selectPBTCommandSuite(t)
	require.NoError(t, convertPBT(t.Context(), converted.Tester.Dirs.DataDir, convertedOutput, true, "", log.New()))

	convertedRaw := dbCfg(dbcfg.ChainDB, converted.Tester.Dirs.Chaindata).MustOpen()
	convertedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(convertedOutput))
	require.NoError(t, err)
	convertedAgg := dbstate.New(datadir.Open(convertedOutput)).WithErigonDBSettings(convertedSettings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, convertedAgg.OpenFolder(convertedRaw))
	convertedDB, err := dbtemporal.New(convertedRaw, convertedAgg, nil)
	require.NoError(t, err)
	convertedTx, err := convertedDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer convertedTx.Rollback()
	convertedExport := filepath.Join(t.TempDir(), "converted-export")
	require.NoError(t, app.RunExportPBT(t.Context(), convertedTx, func(block uint64) (*types.Header, error) {
		return converted.Chain.Headers[block-1], nil
	}, convertedExport, log.New()))
	convertedTx.Rollback()
	convertedDB.Close()
	convertedAgg.Close()
	convertedRaw.Close()

	replayed, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, replayed.Tester.InsertChain(replayed.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), replayed.Tester.DB))
	require.NoError(t, replayed.Tester.ReExecuteTo(t.Context(), replayed.Chain.TopBlock.NumberU64()))
	replayedTx, err := replayed.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer replayedTx.Rollback()
	replayedExport := filepath.Join(t.TempDir(), "replayed-export")
	require.NoError(t, app.RunExportPBT(t.Context(), replayedTx, func(block uint64) (*types.Header, error) {
		return replayed.Chain.Headers[block-1], nil
	}, replayedExport, log.New()))
	replayedTx.Rollback()

	convertedMeta := make(map[string]any)
	replayedMeta := make(map[string]any)
	convertedMetaBytes, err := os.ReadFile(filepath.Join(convertedExport, "pbt-snapshot.meta.json"))
	require.NoError(t, err)
	replayedMetaBytes, err := os.ReadFile(filepath.Join(replayedExport, "pbt-snapshot.meta.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(convertedMetaBytes, &convertedMeta))
	require.NoError(t, json.Unmarshal(replayedMetaBytes, &replayedMeta))
	require.Equal(t, convertedMeta["snapshotDigest"], replayedMeta["snapshotDigest"])
}

func TestPBTAttachedReplayMatchesConvertedState(t *testing.T) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 10)
	resetPBTAcceptanceExecution(t, node)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 3)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	conversionBlock, _, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))

	selectPBTCommandSuite(t)
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	for block := conversionBlock + 1; block <= node.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
	}
	attachedExport := filepath.Join(t.TempDir(), "attached-export")
	attachedTx, err := reopened.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	t.Cleanup(attachedTx.Rollback)
	require.NoError(t, app.RunExportPBT(t.Context(), attachedTx, func(block uint64) (*types.Header, error) {
		return node.Chain.Headers[block-1], nil
	}, attachedExport, log.New()))
	attachedTx.Rollback()
	reopened.Close()

	selectPBTHexCommandSuite(t)
	converted, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, converted.Tester.InsertChain(converted.Chain))
	buildPBTAcceptanceFiles(t, converted)
	converted.Tester.Close()
	selectPBTCommandSuite(t)
	convertedOutput := filepath.Join(t.TempDir(), "converted")
	require.NoError(t, convertPBT(t.Context(), converted.Tester.Dirs.DataDir, convertedOutput, true, "", log.New()))
	convertedDigest := exportConvertedPBTDigest(t, convertedOutput, converted.Tester.Dirs.Chaindata, converted)
	attachedMetaBytes, err := os.ReadFile(filepath.Join(attachedExport, "pbt-snapshot.meta.json"))
	require.NoError(t, err)
	var attachedMeta map[string]any
	require.NoError(t, json.Unmarshal(attachedMetaBytes, &attachedMeta))
	require.Equal(t, convertedDigest, attachedMeta["snapshotDigest"])
}

func exportConvertedPBTDigest(t *testing.T, output, rawPath string, fixture *execmoduletester.PBTAcceptanceChain) any {
	t.Helper()
	convertedRaw := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	convertedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	convertedAgg := dbstate.New(datadir.Open(output)).WithErigonDBSettings(convertedSettings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, convertedAgg.OpenFolder(convertedRaw))
	convertedDB, err := dbtemporal.New(convertedRaw, convertedAgg, nil)
	require.NoError(t, err)
	convertedTx, err := convertedDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer convertedTx.Rollback()
	convertedExport := filepath.Join(t.TempDir(), "converted-export")
	require.NoError(t, app.RunExportPBT(t.Context(), convertedTx, func(block uint64) (*types.Header, error) {
		return fixture.Chain.Headers[block-1], nil
	}, convertedExport, log.New()))
	convertedTx.Rollback()
	convertedDB.Close()
	convertedAgg.Close()
	convertedRaw.Close()
	metaBytes, err := os.ReadFile(filepath.Join(convertedExport, "pbt-snapshot.meta.json"))
	require.NoError(t, err)
	var meta map[string]any
	require.NoError(t, json.Unmarshal(metaBytes, &meta))
	return meta["snapshotDigest"]
}

func copyPBTStateSalt(t *testing.T, node, source *execmoduletester.PBTAcceptanceChain) {
	t.Helper()
	value, err := os.ReadFile(filepath.Join(node.Tester.Dirs.Snap, "salt-state.txt"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source.Tester.Dirs.Snap, "salt-state.txt"), value, 0o644))
}

func buildPBTAcceptanceFiles(t *testing.T, fixture *execmoduletester.PBTAcceptanceChain) {
	t.Helper()
	fixture.Tester.Close()
	rawDB := dbCfg(dbcfg.ChainDB, fixture.Tester.Dirs.Chaindata).MustOpen()
	tx, err := rawDB.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	_, lastTxNum, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	tx.Rollback()
	rawDB.Close()
	buildPBTAcceptanceFilesAt(t, fixture, lastTxNum)
}

func resetPBTAcceptanceExecution(t *testing.T, fixture *execmoduletester.PBTAcceptanceChain) {
	t.Helper()
	settings, err := dbstate.ReadErigonDBSettings(fixture.Tester.Dirs)
	require.NoError(t, err)
	options := []execmoduletester.Option{
		execmoduletester.WithExistingDataDir(fixture.Tester.Dirs),
		execmoduletester.WithGenesisSpec(fixture.Genesis),
		execmoduletester.WithKey(fixture.Key),
		execmoduletester.WithStepSize(settings.StepSize),
		execmoduletester.WithoutGenesisCommit(),
	}
	if settings.TrieVariantName() == dbstate.TrieVariantHexBin {
		options = append(options, execmoduletester.WithEnableDomain(kv.CommitmentBinDomain))
	}
	tester := execmoduletester.New(t, options...)
	require.NoError(t, rawdbreset.ResetExec(t.Context(), tester.DB))
	tester.Close()
}

func buildPBTAcceptanceFilesAt(t *testing.T, fixture *execmoduletester.PBTAcceptanceChain, lastTxNum uint64) {
	buildPBTAcceptanceFilesAtWithMerge(t, fixture, lastTxNum, false)
}

func buildPBTAcceptanceFilesAtWithMerge(t *testing.T, fixture *execmoduletester.PBTAcceptanceChain, lastTxNum uint64, doMerge bool) {
	t.Helper()
	fixture.Tester.Close()
	dirs := fixture.Tester.Dirs
	rawDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	toStep := kv.Step(lastTxNum/settings.StepSize + 1)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, toStep, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), doMerge))
	agg.WaitForFiles()
	lastBlock := uint64(0)
	if headers, globErr := filepath.Glob(filepath.Join(dirs.Snap, "*headers.seg")); globErr != nil {
		require.NoError(t, globErr)
	} else if len(headers) == 0 {
		rawTx, rawErr := rawDB.BeginRo(t.Context())
		require.NoError(t, rawErr)
		func() {
			defer rawTx.Rollback()
			for blockNum := uint64(1); ; blockNum++ {
				if rawdb.ReadHeaderByNumber(rawTx, blockNum) == nil {
					break
				}
				lastBlock = blockNum
			}
		}()
	}
	tx.Rollback()
	db.Close()
	agg.Close()
	rawDB.Close()
	if lastBlock != 0 {
		reopened := execmoduletester.New(t,
			execmoduletester.WithExistingDataDir(dirs),
			execmoduletester.WithGenesisSpec(fixture.Genesis),
			execmoduletester.WithKey(fixture.Key),
			execmoduletester.WithStepSize(1),
			execmoduletester.WithoutGenesisCommit(),
		)
		config := snapcfg.KnownCfgOrDevnet(fixture.Tester.ChainConfig.ChainName)
		require.NoError(t, freezeblocks.DumpBlocks(t.Context(), 0, lastBlock, fixture.Tester.ChainConfig, dirs.Tmp, dirs.Snap, reopened.DB, 1, log.LvlInfo, log.New(), reopened.BlockReader, config, nil))
		reopened.Close()
	}
}

func assertPBTAttachHistory(t *testing.T, attached, dual *execmoduletester.ExecModuleTester, maxTxNum uint64) {
	t.Helper()
	attachedTx, err := attached.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer attachedTx.Rollback()
	dualTx, err := dual.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer dualTx.Rollback()
	for txNum := uint64(0); txNum <= maxTxNum; txNum++ {
		attachedValues := collectPBTStateAt(t, attachedTx, txNum)
		dualValues := collectPBTStateAt(t, dualTx, txNum)
		require.Equalf(t, dualValues, attachedValues, "state at txNum %d", txNum)
	}
}

func collectPBTStateAt(t *testing.T, tx kv.TemporalTx, txNum uint64) map[string][]byte {
	t.Helper()
	values := make(map[string][]byte)
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		it, err := tx.RangeAsOf(domain, nil, nil, txNum+1, order.Asc, kv.Unlim)
		require.NoError(t, err)
		for it.HasNext() {
			key, value, err := it.Next()
			require.NoError(t, err)
			if len(value) == 0 {
				values[domain.String()+"\x00"+string(key)] = []byte{}
			} else {
				values[domain.String()+"\x00"+string(key)] = bytes.Clone(value)
			}
		}
		it.Close()
	}
	return values
}

func pbtAcceptanceLastTxNum(t *testing.T, fixture *execmoduletester.ExecModuleTester) uint64 {
	t.Helper()
	rawDB := fixture.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	var lastTxNum uint64
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		_, lastTxNum, err = rawdbv3.TxNums.Last(tx)
		return err
	}))
	return lastTxNum
}

func readPBTFilesRoot(t *testing.T, output string) common.Hash {
	t.Helper()
	dirs := datadir.Open(output)
	settings, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(nil))
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	builder, err := eip8297.NewStreamRootBuilder(eip8297.SelectedHash())
	require.NoError(t, err)
	require.NoError(t, dbstate.ForEachPBinLeaf(at, nil, true, func(leaf dbstate.PBinLeaf) error {
		return builder.Add(leaf.Key, leaf.Value)
	}))
	root, err := builder.RootHash()
	require.NoError(t, err)
	return root
}

func selectPBTCommandSuite(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func selectPBTBinaryCommandSuite(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func selectPBTHexCommandSuite(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = ""
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
}
