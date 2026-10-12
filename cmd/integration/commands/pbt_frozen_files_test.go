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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

func TestPBTCommandsUseFrozenBlockFiles(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ConfigureCommitmentV3Records(false)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	chain, err := execmoduletester.NewPBTAcceptanceChain(t, true, false)
	require.NoError(t, err)
	require.NoError(t, chain.Tester.InsertChain(chain.Chain))
	config := snapcfg.KnownCfgOrDevnet(chain.Tester.ChainConfig.ChainName)
	err = freezeblocks.DumpBlocks(t.Context(), 0, 3, chain.Tester.ChainConfig, chain.Tester.Dirs.Tmp, chain.Tester.Dirs.Snap, chain.Tester.DB, 1, log.LvlInfo, log.New(), chain.Tester.BlockReader, config, nil)
	require.NoError(t, err)
	buildPBTAcceptanceFilesAt(t, chain, 7)
	block := chain.Chain.Blocks[1]
	blockNum := block.NumberU64()
	settings, err := dbstate.ReadErigonDBSettings(chain.Tester.Dirs)
	require.NoError(t, err)
	point, err := readPBinSourcePoint(t.Context(), chain.Tester.Dirs, settings, false, log.New())
	require.NoError(t, err)
	require.Equal(t, pbinConversionPoint{BlockNum: 2, TxNum: 7}, point)
	rawDB := dbCfg(dbcfg.ChainDB, chain.Tester.Dirs.Chaindata).MustOpen()
	var wantHeader *types.Header
	var wantMaxTx uint64
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		wantHeader = rawdb.ReadHeaderByNumber(tx, blockNum)
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		require.NoError(t, err)
		chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
		require.NoError(t, err)
		require.True(t, chainConfig.IsBinaryTrie(wantHeader.Time))
		var found bool
		var maxErr error
		wantMaxTx, found, maxErr = rawdbv3.TxNums.MaxExact(t.Context(), tx, blockNum)
		require.True(t, found)
		return maxErr
	}))
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		rawdb.DeleteHeader(tx, block.Hash(), blockNum)
		rawdb.DeleteBody(tx, block.Hash(), blockNum)
		if err := rawdb.TruncateCanonicalHash(tx, blockNum, false); err != nil {
			return err
		}
		return rawdbv3.TxNums.Truncate(tx, blockNum)
	}))
	rawDB.Close()
	setExecutionProgress(t, chain.Tester.Dirs.Chaindata, blockNum)
	rawDB = dbCfg(dbcfg.ChainDB, chain.Tester.Dirs.Chaindata).MustOpen()
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		require.Nil(t, rawdb.ReadHeaderByNumber(tx, blockNum))
		canonical, err := rawdb.ReadCanonicalHash(tx, blockNum)
		require.NoError(t, err)
		require.Equal(t, common.Hash{}, canonical)
		_, found, err := rawdbv3.TxNums.MaxExact(t.Context(), tx, blockNum)
		require.NoError(t, err)
		require.False(t, found)
		return nil
	}))
	reader, view, closeReader, err := openPBTBlockReader(t.Context(), chain.Tester.Dirs, rawDB, log.New())
	require.NoError(t, err)
	tx, err := rawDB.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	readerTx := pbtBlockFilesTx{Tx: tx, view: view}
	header, err := reader.HeaderByNumber(t.Context(), readerTx, blockNum)
	require.NoError(t, err)
	require.Equal(t, wantHeader.Hash(), header.Hash())
	maxTx, found, err := reader.TxnumReader().MaxExact(t.Context(), readerTx, blockNum)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wantMaxTx, maxTx)
	canonical, found, err := reader.CanonicalHash(t.Context(), readerTx, blockNum)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wantHeader.Hash(), canonical)
	byHash, err := reader.HeaderByHash(t.Context(), readerTx, wantHeader.Hash())
	require.NoError(t, err)
	require.Equal(t, wantHeader.Hash(), byHash.Hash())
	tx.Rollback()
	closeReader()
	rawDB.Close()
	_, _, _, _, gotMaxTx, err := pbtAttachBlockEnd(t.Context(), chain.Tester.Dirs, blockNum, wantMaxTx)
	require.NoError(t, err)
	require.Equal(t, wantMaxTx, gotMaxTx)
	blockHash, _, blockEnd, _, _, err := pbtAttachBlockEnd(t.Context(), chain.Tester.Dirs, blockNum, wantMaxTx)
	require.NoError(t, err)
	require.Equal(t, wantHeader.Hash(), blockHash)
	require.True(t, blockEnd)
	output := t.TempDir()
	require.NoError(t, convertPBT(t.Context(), chain.Tester.Dirs.DataDir, output, false, "", log.New()))
}

func TestPBTConvertUsesFrozenBlockFilesMidBlock(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ConfigureCommitmentV3Records(false)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	chain, err := execmoduletester.NewPBTAcceptanceChain(t, true, false)
	require.NoError(t, err)
	require.NoError(t, chain.Tester.InsertChain(chain.Chain))
	config := snapcfg.KnownCfgOrDevnet(chain.Tester.ChainConfig.ChainName)
	require.NoError(t, freezeblocks.DumpBlocks(t.Context(), 0, 3, chain.Tester.ChainConfig, chain.Tester.Dirs.Tmp, chain.Tester.Dirs.Snap, chain.Tester.DB, 1, log.LvlInfo, log.New(), chain.Tester.BlockReader, config, nil))
	buildPBTAcceptanceFilesAt(t, chain, 6)
	settings, err := dbstate.ReadErigonDBSettings(chain.Tester.Dirs)
	require.NoError(t, err)
	point, err := readPBinSourcePoint(t.Context(), chain.Tester.Dirs, settings, false, log.New())
	require.NoError(t, err)
	require.Equal(t, pbinConversionPoint{BlockNum: 2, TxNum: 6}, point)
	block := chain.Chain.Blocks[1]
	blockNum := block.NumberU64()
	rawDB := dbCfg(dbcfg.ChainDB, chain.Tester.Dirs.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		rawdb.DeleteHeader(tx, block.Hash(), blockNum)
		rawdb.DeleteBody(tx, block.Hash(), blockNum)
		if err := rawdb.TruncateCanonicalHash(tx, blockNum, false); err != nil {
			return err
		}
		return rawdbv3.TxNums.Truncate(tx, blockNum)
	}))
	rawDB.Close()
	setExecutionProgress(t, chain.Tester.Dirs.Chaindata, blockNum)
	output := t.TempDir()
	require.NoError(t, convertPBT(t.Context(), chain.Tester.Dirs.DataDir, output, false, "", log.New()))
}
