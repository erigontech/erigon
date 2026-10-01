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
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"

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
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestIsCommitmentFileNameAcceptsCommitmentBin(t *testing.T) {
	require.True(t, isCommitmentFileName("v1.0-commitment-bin.0-1024.kv"), "the converter must exclude commitment-bin files from its source links")
}

func TestPBTRealChainTxNumConvention(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
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
	require.ErrorContains(t, requirePBinSourceEnd(agg, lastTxNum-1), "the last file holds writes up to txNum")
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
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	previousDatadir := datadirCli
	previousChaindata := chaindata
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
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

func convertedPBTAcceptanceRows(t *testing.T, sharedCode []byte) (map[string][]byte, common.Hash, common.Hash, string, map[string][]byte, map[string][]byte) {
	return convertedPBTAcceptanceRowsWithLimits(t, sharedCode, &dbstate.PBinRangeWriterLimits{MaxOps: 2, MaxBytes: 1 << 20})
}

func convertedPBTAcceptanceRowsWithLimits(t *testing.T, sharedCode []byte, limits *dbstate.PBinRangeWriterLimits) (map[string][]byte, common.Hash, common.Hash, string, map[string][]byte, map[string][]byte) {
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
	if limits == nil {
		require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New()))
	} else {
		require.NoError(t, convertPBTWithLimits(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), limits))
	}
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	binRoot := readPBTBinRoot(t, output, source.Tester.Dirs.Chaindata)
	referenceRows := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		referenceRows[string(entry.Key)] = bytes.Clone(entry.Value)
	}
	return readPBTBinRows(t, output, source.Tester.Dirs.Chaindata), binRoot, wantRoot, string(entries[len(entries)-1].Key), readPBTBinLeaves(t, output, source.Tester.Dirs.Chaindata), referenceRows
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
		_, _, _, _, leaves, _ := convertedPBTAcceptanceRows(t, code)
		codeHash := crypto.Keccak256Hash(code)
		for index, chunk := range eip8297.ChunkifyCode(code) {
			require.Equal(t, chunk[:], leaves[string(eip8297.TreeKeyCodeChunk(codeHash, index))], "code chunk %d must be present in its stem", index)
		}
	})
	t.Run("shared code chunked once", func(t *testing.T) {
		_, _, _, _, leaves, referenceRows := convertedPBTAcceptanceRows(t, code)
		wantCode := make(map[string][]byte)
		gotCode := make(map[string][]byte)
		for key, value := range referenceRows {
			if len(key) > 0 && key[0] == eip8297.CodeZone {
				wantCode[key] = value
			}
		}
		for key, value := range leaves {
			if len(key) > 0 && key[0] == eip8297.CodeZone {
				gotCode[key] = value
			}
		}
		require.Equal(t, wantCode, gotCode, "shared code chunks must match a single-holder run")
	})
	t.Run("zero chunks absent", func(t *testing.T) {
		_, _, _, _, leaves, _ := convertedPBTAcceptanceRows(t, code)
		zeroCodeHash := crypto.Keccak256Hash(make([]byte, 31))
		for index := range eip8297.ChunkifyCode(make([]byte, 31)) {
			require.NotContains(t, leaves, string(eip8297.TreeKeyCodeChunk(zeroCodeHash, index)), "zero code chunks must be absent")
		}
	})
	t.Run("delegation without code leaves", func(t *testing.T) {
		_, _, _, _, leaves, _ := convertedPBTAcceptanceRows(t, code)
		delegation := append(append([]byte(nil), eip8297.DelegationMarker[:]...), bytes.Repeat([]byte{7}, 20)...)
		delegationHash := crypto.Keccak256Hash(delegation)
		for index := range eip8297.ChunkifyCode(delegation) {
			require.NotContains(t, leaves, string(eip8297.TreeKeyCodeChunk(delegationHash, index)), "delegation must not add code chunks")
		}
	})
	t.Run("root and record parity", func(t *testing.T) {
		tinyRows, _, _, _, _, _ := convertedPBTAcceptanceRows(t, code)
		defaultRows, _, _, _, _, _ := convertedPBTAcceptanceRowsWithLimits(t, code, nil)
		require.Equal(t, defaultRows, tinyRows, "multi-batch rows must match the default writer")
	})
	t.Run("right-edge reads", func(t *testing.T) {
		_, _, _, rightEdge, leaves, _ := convertedPBTAcceptanceRows(t, code)
		require.NotEmpty(t, leaves[rightEdge], "the right-edge row must be present in the output")
	})
}

func TestConvertPBTOutputPassesCommitmentIntegrity(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
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
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
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
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	previousDatadir := datadirCli
	previousChaindata := chaindata
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
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
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
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
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	previousDatadir := datadirCli
	previousChaindata := chaindata
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
		datadirCli = previousDatadir
		chaindata = previousChaindata
	})
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, wantRoot := newPBTConversionSource(t)
	dualPath := filepath.Join(t.TempDir(), "dual")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, dualPath, true, "", log.New()))
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
		header := &types.Header{Number: *uint256.NewInt(1), Time: 1, Root: wantRoot}
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return err
		}
		return rawdb.WriteCanonicalHash(tx, header.Hash(), 1)
	}))
	rawDB.Close()
	require.NoError(t, os.Symlink(source.Chaindata, filepath.Join(dualPath, "chaindata")))
	dual := pbtConversionSource{Dirs: datadir.Open(dualPath)}
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, convertPBT(t.Context(), dual.DataDir, output, false, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantBin, settings.TrieVariantName())
}

func TestConvertPBTHexBinSourceWithOrdinaryHexFlags(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	previousDatadir := datadirCli
	previousChaindata := chaindata
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
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
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHook := convertPBTOutputHook
	t.Cleanup(func() {
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		convertPBTOutputHook = previousHook
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	convertPBTOutputHook = func() error { return errors.New("injected conversion failure") }
	err := convertPBT(t.Context(), source.DataDir, output, true, "", log.New())
	require.ErrorContains(t, err, "injected conversion failure")
	_, statErr := os.Stat(output)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestConvertPBTStandaloneReopenRequiresBothDomains(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	previousHook := convertPBTStandaloneHook
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
		convertPBTStandaloneHook = previousHook
	})
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
			convertPBTStandaloneHook = func(dirs datadir.Dirs) error {
				removePBTOutputDomainFiles(t, dirs.SnapDomain, test.domain)
				return nil
			}
			err := convertPBT(t.Context(), source.DataDir, output, true, "", log.New())
			require.ErrorContains(t, err, "commitment state is missing for one or more domains")
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

func TestConvertPBTRefusesSourceLeafAfterPoint(t *testing.T) {
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	source, _ := newPBTConversionSourceAt(t, 6)
	output := filepath.Join(t.TempDir(), "output")
	err := convertPBT(t.Context(), source.DataDir, output, true, "", log.New())
	require.ErrorContains(t, err, "the last file holds writes up to txNum 7, after conversion txNum 6")
	_, statErr := os.Stat(output)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestConvertPBTIgnoresDatabaseRowsPastFiles(t *testing.T) {
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
	})
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
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	t.Cleanup(func() {
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
	})
	source, _ := newPBTConversionSource(t)
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
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
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, probeTx, address, accounts.SerialiseV3(&account), 1, nil))
	slot := append(bytes.Clone(address), bytes.Repeat([]byte{0x22}, 32)...)
	require.NoError(t, domains.DomainPut(kv.StorageDomain, probeTx, slot, []byte{1}, 1, nil))
	updates := commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
	updates.TouchPlainKey(string(address), nil, func(*commitment.KeyUpdate, []byte) {})
	updates.TouchPlainKey(string(slot), nil, func(*commitment.KeyUpdate, []byte) {})
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
	finalRoot := hexRoot
	finalBlock := uint64(1)
	if multiRange {
		finalBlock = 2
	}
	if multiRange {
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, probeTx, secondAddress, accounts.SerialiseV3(&secondAccount), 9, nil))
		require.NoError(t, domains.DomainPut(kv.StorageDomain, probeTx, secondSlot, []byte{2}, 9, nil))
		updates = commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
		updates.TouchPlainKey(string(secondAddress), nil, func(*commitment.KeyUpdate, []byte) {})
		updates.TouchPlainKey(string(secondSlot), nil, func(*commitment.KeyUpdate, []byte) {})
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
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, &chainpkg.Config{}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 0))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 7))
	if multiRange {
		require.NoError(t, rawdbv3.TxNums.Append(tx, 2, 15))
	}
	domains, err = execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), 1, nil))
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
	updates.TouchPlainKey(string(address), nil, func(*commitment.KeyUpdate, []byte) {})
	updates.TouchPlainKey(string(slot), nil, func(*commitment.KeyUpdate, []byte) {})
	ctx = domains.GetCommitmentCtxForDomain(kv.CommitmentDomain)
	ctx.SetUpdates(updates)
	computedRoot, err := ctx.ComputeCommitment(t.Context(), tx, true, 1, firstCommitmentTx, "conversion-source", nil)
	require.NoError(t, err)
	require.Equal(t, hexRoot, computedRoot)
	if multiRange {
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, secondAddress, accounts.SerialiseV3(&secondAccount), 9, nil))
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, secondSlot, []byte{2}, 9, nil))
		updates = commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
		updates.TouchPlainKey(string(secondAddress), nil, func(*commitment.KeyUpdate, []byte) {})
		updates.TouchPlainKey(string(secondSlot), nil, func(*commitment.KeyUpdate, []byte) {})
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
	builder, err := eip8297.NewStreamRootBuilder(func(data []byte) common.Hash { return common.Hash(blake3.Sum256(data)) })
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
	at.Close()
	db.Close()
	agg.Close()
	rawDB.Close()
	closed = true
	return pbtConversionSource{Dirs: dirs}, pbtRoot
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
	rawDB := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	at := agg.BeginFilesRo()
	defer func() {
		at.Close()
		agg.Close()
		rawDB.Close()
	}()
	iter, err := at.DebugRangeLatestFromFiles(kv.CommitmentBinDomain, nil, nil, kv.Unlim)
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
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	root, err := domains.GetCommitmentCtxForDomain(kv.CommitmentBinDomain).Trie().RootHash()
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

func readPBTBinLeaves(t *testing.T, output, rawPath string) map[string][]byte {
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
	leaves := make(map[string][]byte)
	require.NoError(t, dbstate.ForEachPBinLeaf(at, nil, true, func(leaf dbstate.PBinLeaf) error {
		leaves[string(leaf.Key)] = bytes.Clone(leaf.Value)
		return nil
	}))
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
