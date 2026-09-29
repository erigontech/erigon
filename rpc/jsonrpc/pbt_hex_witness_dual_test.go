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

package jsonrpc

import (
	"encoding/binary"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func configurePBTWitnessGlobals(t *testing.T, commitmentV3, dual bool) {
	t.Helper()
	previousAssert := dbg.AssertEnabled
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		dbg.AssertEnabled = previousAssert
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	dbg.AssertEnabled = true
	statecfg.ExperimentalBinCommitment = dual
	statecfg.ExperimentalHexBinCommitment = dual
	statecfg.ExperimentalParallelCommitment = false
	statecfg.ExperimentalCommitmentV3 = commitmentV3
	if commitmentV3 {
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	}
	statecfg.EnableHistoricalCommitment()
	if dual {
		statecfg.BinCommitmentHash = commitment.PBinHashBlake3
		require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	}
}

func newPBTWitnessModule(t *testing.T, dual bool) *execmoduletester.ExecModuleTester {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	key1, err := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	require.NoError(t, err)
	key2, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	genesis := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			crypto.PubkeyToAddress(key.PublicKey):  {Balance: big.NewInt(9000000000000000000)},
			crypto.PubkeyToAddress(key1.PublicKey): {Balance: big.NewInt(200000000000000000)},
			crypto.PubkeyToAddress(key2.PublicKey): {Balance: big.NewInt(300000000000000000)},
		},
		GasLimit: 10000000,
	}
	options := []execmoduletester.Option{
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(key),
	}
	if dual {
		options = append(options, execmoduletester.WithEnableDomain(kv.CommitmentBinDomain))
	}
	m := execmoduletester.New(t, options...)
	_, testChain := rpcdaemontest.CreateTestExecModuleNoInsert(t)
	if dual {
		seedPBTWitnessGenesis(t, m, genesis)
	}
	require.NoError(t, m.InsertChain(testChain))

	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))
	return m
}

func seedPBTWitnessGenesis(t *testing.T, m *execmoduletester.ExecModuleTester, genesis *types.Genesis) {
	t.Helper()
	tx, err := m.DB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	_, state, err := genesiswrite.ComputeGenesisCommitment(t.Context(), genesis, tx, domains, m.Genesis.Header())
	require.NoError(t, err)
	state.Close()
	require.NoError(t, domains.Commit(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	require.NoError(t, m.ExecModule.ResetCurrentContext(t.Context()))
}

func TestPBTPreForkProofParity(t *testing.T) {
	address := common.Address{1}
	keys := []hexutil.Bytes{{0}}
	selector := rpc.BlockNumberOrHashWithNumber(2)
	var hexOnly, dual any
	t.Run("hex", func(t *testing.T) {
		configurePBTWitnessGlobals(t, true, false)
		m := newPBTWitnessModule(t, false)
		api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
		var err error
		hexOnly, err = api.GetProof(t.Context(), address, keys, &selector)
		require.NoError(t, err)
	})
	t.Run("hex+bin", func(t *testing.T) {
		configurePBTWitnessGlobals(t, true, true)
		m := newPBTWitnessModule(t, true)
		api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
		var err error
		dual, err = api.GetProof(t.Context(), address, keys, &selector)
		require.NoError(t, err)
	})
	require.Equal(t, hexOnly, dual)
}

func TestPBinGetWitnessRefusesBin(t *testing.T) {
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)
	m, _, _, _ := chainWithDeployedContract(t)
	enableCommitmentHistoryFlag(t, m.DB)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	block := rpc.BlockNumber(3)
	_, err := api.GetWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &block})
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
}

func TestPBinHexOnlyCallersStillRefuse(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, rel := range []string{
		"rpc/jsonrpc/eth_call.go",
		"rpc/jsonrpc/receipts/receipts_generator.go",
		"db/integrity/commitment_integrity.go",
	} {
		src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		require.NoError(t, err)
		for i, line := range strings.Split(string(src), "\n") {
			if !strings.Contains(line, "execctx.NewSharedDomains(") {
				continue
			}
			require.Contains(t, line, "execctx.WithHexCommitmentOnly()", "%s:%d", rel, i+1)
		}
	}
	src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash("rpc/jsonrpc/debug_execution_witness.go")))
	require.NoError(t, err)
	require.NotContains(t, string(src), "execctx.WithHexCommitmentOnly()")
}

func TestPBinDualPostFlipProofAndWitnessRefuse(t *testing.T) {
	_, m := pbinWitnessFixture(t, 30)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	for _, block := range []rpc.BlockNumber{3, 4} {
		t.Run(block.String(), func(t *testing.T) {
			selector := rpc.BlockNumberOrHashWithNumber(block)
			_, err := api.GetProof(t.Context(), address, []hexutil.Bytes{{0}}, &selector)
			require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
			_, err = api.GetWitness(t.Context(), selector)
			require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
		})
	}
}

func TestPBinFrozenHexHistoricalWitnessAndProof(t *testing.T) {
	api, m := pbinWitnessFixture(t, 30)
	ethAPI := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	selector := rpc.BlockNumberOrHashWithNumber(2)
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	keys := []hexutil.Bytes{{0}}
	witnessBefore, err := api.ExecutionWitness(t.Context(), selector, nil, nil)
	require.NoError(t, err)
	proofBefore, err := ethAPI.GetProof(t.Context(), address, keys, &selector)
	require.NoError(t, err)
	require.NotEmpty(t, proofBefore.AccountProof)
	require.Len(t, proofBefore.StorageProof, 1)

	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	txNum, err := m.BlockReader.TxnumReader().Max(t.Context(), tx, 2)
	require.NoError(t, err)
	tx.Rollback()
	agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	require.NoError(t, agg.FreezeDomain(kv.CommitmentDomain, txNum))
	settingsPath := filepath.Join(m.Dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE)
	frozenSettings, err := os.ReadFile(settingsPath)
	require.NoError(t, err)

	witnessAfter, err := newDebugApiForTest(m).ExecutionWitness(t.Context(), selector, nil, nil)
	require.NoError(t, err)
	witnessBeforeJSON, err := json.Marshal(witnessBefore)
	require.NoError(t, err)
	witnessAfterJSON, err := json.Marshal(witnessAfter)
	require.NoError(t, err)
	require.Equal(t, witnessBeforeJSON, witnessAfterJSON)
	proofAfter, err := ethAPI.GetProof(t.Context(), address, keys, &selector)
	require.NoError(t, err)
	proofBeforeJSON, err := json.Marshal(proofBefore)
	require.NoError(t, err)
	proofAfterJSON, err := json.Marshal(proofAfter)
	require.NoError(t, err)
	require.Equal(t, proofBeforeJSON, proofAfterJSON)
	currentSettings, err := os.ReadFile(settingsPath)
	require.NoError(t, err)
	require.Equal(t, frozenSettings, currentSettings)
	frozenAt, frozen := agg.IsDomainFrozen(kv.CommitmentDomain)
	require.True(t, frozen)
	require.Equal(t, txNum, frozenAt)
}

func TestDebugExecutionWitnessReportsPrunedCommitmentHistory(t *testing.T) {
	api, m := pbinWitnessFixture(t, 30)
	tx, err := m.DB.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	pruneTo, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, 3)
	require.NoError(t, err)
	cursor, err := tx.RwCursorDupSort(kv.TblCommitmentHistoryKeys)
	require.NoError(t, err)
	defer cursor.Close()
	for {
		key, _, err := cursor.First()
		require.NoError(t, err)
		if key == nil || binary.BigEndian.Uint64(key) >= pruneTo {
			break
		}
		require.NoError(t, cursor.DeleteCurrentDuplicates())
	}
	cursor.Close()
	require.NoError(t, tx.Commit())

	block := rpc.BlockNumber(2)
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &block}, nil, nil)
	require.ErrorContains(t, err, "commitment history pruned")
	require.Nil(t, result)
}
