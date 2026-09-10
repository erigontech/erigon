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
	"bytes"
	"errors"
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

type pbinWitnessWithoutCommitmentHistory struct {
	kv.TemporalTx
}

func (tx pbinWitnessWithoutCommitmentHistory) GetAsOf(domain kv.Domain, key []byte, txNum uint64) ([]byte, bool, error) {
	if domain == kv.CommitmentDomain {
		return nil, false, errors.New("commitment history unavailable")
	}
	return tx.TemporalTx.GetAsOf(domain, key, txNum)
}

func TestPBinHeadCaptureWithoutCommitmentHistory(t *testing.T) {
	withBinCommitmentDatadir(t)
	m, key, from := fundedBankGenesis(t, chain.TestChainBerlinConfig)
	signer := types.LatestSignerForChainID(nil)
	pack, err := m.GenerateChain(6, func(i int, block *blockgen.BlockGen) {
		nonce := block.TxNonce(from)
		var unsigned *types.LegacyTx
		if i == 0 {
			unsigned = types.NewContractCreation(nonce, uint256.NewInt(0), 200_000, uint256.NewInt(1_000_000_000), pbinDeployCode(pbinStoreRuntime))
		} else {
			unsigned = types.NewTransaction(nonce, types.CreateAddress(from, 0), uint256.NewInt(0), 100_000, uint256.NewInt(1_000_000_000), pbinStoreCalldata(common.Hash{}, uint64(i)))
		}
		txn, err := types.SignTx(unsigned, *signer, key)
		require.NoError(t, err)
		block.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack.Slice(0, 5)))
	pin, err := openRollingPin(t.Context(), m.DB)
	require.NoError(t, err)
	defer pin.close()
	require.NoError(t, m.InsertChain(pack.Slice(5, 6)))
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	bn := rpc.BlockNumber(6)
	info, err := api.resolveWitnessBlock(t.Context(), tx, rpc.BlockNumberOrHash{BlockNumber: &bn})
	require.NoError(t, err)
	result, err := api.buildWitnessResultHeadCapture(t.Context(), pbinWitnessWithoutCommitmentHistory{tx}, pin.tx, info, witnessModeLegacy)
	require.NoError(t, err)
	require.NotEmpty(t, result.State)
}

func pbinDualWitnessFixture(t *testing.T) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	t.Helper()
	withBinCommitmentDatadir(t)
	withCommitmentHistory(t)
	previousDual := statecfg.ExperimentalHexBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalHexBinCommitment = previousDual })
	statecfg.ExperimentalHexBinCommitment = true
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	from := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x1000000000000000000000000000000000000001")
	amsterdam, activation := uint64(0), uint64(30)
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime, config.BinaryTrieTime = &amsterdam, &activation
	balance := new(big.Int).Mul(big.NewInt(10), new(big.Int).SetUint64(common.Ether))
	genesis := &types.Genesis{Config: config, Difficulty: uint256.NewInt(0), Alloc: types.GenesisAlloc{from: {Balance: new(big.Int).Set(balance)}, to: {Balance: big.NewInt(0), Nonce: 1, Code: common.FromHex("0x60003560005500")}}, GasLimit: 30_000_000, BaseFee: uint256.NewInt(0)}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key), execmoduletester.WithEnableDomain(kv.CommitmentBinDomain))
	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error { return rawdb.WriteDBCommitmentHistoryEnabled(tx, true) }))
	tx, err := m.DB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	for _, address := range []common.Address{config.GetWithdrawalRequestContract().Value(), config.GetConsolidationRequestContract().Value()} {
		code, _, err := tx.GetLatest(kv.CodeDomain, address[:], kv.GetLatestOptions{})
		require.NoError(t, err)
		require.NotEmpty(t, code)
		genesis.Alloc[address] = types.GenesisAccount{Balance: big.NewInt(0), Nonce: 1, Code: append([]byte(nil), code...)}
	}
	require.NoError(t, tx.Delete(kv.ConfigTable, kv.GenesisKey))
	require.NoError(t, rawdb.WriteGenesisIfNotExist(tx, genesis))
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	_, ibs, err := genesiswrite.ComputeGenesisCommitment(t.Context(), genesis, tx, domains, m.Genesis.Header())
	require.NoError(t, err)
	ibs.Close()
	require.NoError(t, domains.Commit(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	require.NoError(t, m.ExecModule.ResetCurrentContext(t.Context()))
	signer := types.LatestSignerForChainID(config.ChainID)
	pack, err := m.GenerateChain(4, func(i int, b *blockgen.BlockGen) {
		data := common.BigToHash(big.NewInt(int64(i + 1)))
		txn, err := types.SignTx(types.NewTransaction(uint64(i), to, uint256.NewInt(1), 2_000_000, uint256.NewInt(0), data[:]), *signer, key)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	for i, block := range pack.Blocks {
		header := block.Header()
		if i > 0 {
			header.ParentHash = pack.Blocks[i-1].Hash()
		}
		if config.IsBinaryTrie(block.Time()) {
			alloc := maps.Clone(genesis.Alloc)
			alloc[from] = types.GenesisAccount{Balance: new(big.Int).Sub(balance, big.NewInt(int64(i+1))), Nonce: uint64(i + 1)}
			alloc[to] = types.GenesisAccount{Balance: big.NewInt(int64(i + 1)), Nonce: 1, Code: common.FromHex("0x60003560005500"), Storage: map[common.Hash]common.Hash{{}: common.BigToHash(big.NewInt(int64(i + 1)))}}
			address := config.GetBuilderExitContract().Value()
			account := alloc[address]
			account.Storage = nil
			alloc[address] = account
			rootBlock, state, err := genesiswrite.GenesisToBlock(&types.Genesis{Config: config, Difficulty: uint256.NewInt(0), Alloc: alloc, Timestamp: activation}, datadir.New(t.TempDir()), log.New())
			require.NoError(t, err)
			state.Close()
			header.Root = rootBlock.Root()
		}
		pack.Blocks[i] = block.WithSeal(header)
		pack.Headers[i] = pack.Blocks[i].HeaderNoCopy()
	}
	pack.TopBlock = pack.Blocks[len(pack.Blocks)-1]
	require.NoError(t, m.InsertChain(pack))
	api := NewPrivateDebugAPI(NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}), m.DB, nil, &rpccfg.DebugApiConfig{})
	return api, m
}

func TestPBinDualExecutionWitness(t *testing.T) {
	api, m := pbinDualWitnessFixture(t)
	for _, n := range []rpc.BlockNumber{2, 3, 4} {
		t.Run(n.String(), func(t *testing.T) {
			result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &n}, nil)
			require.NoError(t, err)
			require.NotEmpty(t, result.State)
		})
	}
	t.Run("missing_parent_shadow", func(t *testing.T) {
		require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
			parent := rawdb.ReadHeaderByNumber(tx, 2)
			require.NotNil(t, parent)
			return tx.Delete(kv.ShadowStateRoot, dbutils.BlockBodyKey(2, parent.Hash()))
		}))
		n := rpc.BlockNumber(3)
		result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &n}, nil)
		require.ErrorContains(t, err, "binary parent shadow root missing or invalid for block 2")
		require.Nil(t, result)
	})
}

func TestPBinFrozenHexHistoricalWitness(t *testing.T) {
	api, m := pbinDualWitnessFixture(t)
	selector := rpc.BlockNumberOrHashWithNumber(2)
	before, err := api.ExecutionWitness(t.Context(), selector, nil)
	require.NoError(t, err)
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	state, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	state = bytes.Clone(state)
	txNum, _ := commitmentdb.DecodeTxBlockNums(state)
	agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	require.NoError(t, agg.FreezeDomain(kv.CommitmentDomain, txNum))
	settingsPath := filepath.Join(m.Dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE)
	frozenSettings, err := os.ReadFile(settingsPath)
	require.NoError(t, err)
	api = NewPrivateDebugAPI(NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}), m.DB, nil, &rpccfg.DebugApiConfig{})
	after, err := api.ExecutionWitness(t.Context(), selector, nil)
	require.NoError(t, err)
	require.Equal(t, before, after)
	currentSettings, err := os.ReadFile(settingsPath)
	require.NoError(t, err)
	require.Equal(t, frozenSettings, currentSettings)
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	require.ErrorContains(t, domains.DomainPut(kv.CommitmentDomain, tx, []byte("branch"), []byte("value"), txNum+1, nil), "is frozen")
	currentState, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, state, currentState)
}

func TestPBinFrozenHexHistoricalProof(t *testing.T) {
	_, m := pbinDualWitnessFixture(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	selector := rpc.BlockNumberOrHashWithNumber(2)
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	keys := []hexutil.Bytes{{0}}
	before, err := api.GetProof(t.Context(), address, keys, &selector)
	require.NoError(t, err)
	require.NotEmpty(t, before.AccountProof)
	require.Len(t, before.StorageProof, 1)
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	saved, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	saved = bytes.Clone(saved)
	txNum, _ := commitmentdb.DecodeTxBlockNums(saved)
	agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	require.NoError(t, agg.FreezeDomain(kv.CommitmentDomain, txNum))
	after, err := api.GetProof(t.Context(), address, keys, &selector)
	require.NoError(t, err)
	require.Equal(t, before, after)
	current, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, saved, current)
	frozenAt, frozen := agg.IsDomainFrozen(kv.CommitmentDomain)
	require.True(t, frozen)
	require.Equal(t, txNum, frozenAt)
}
