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
	"maps"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func withBinCommitmentDatadir(t *testing.T) {
	t.Helper()
	origBin, origHash, origSuite := statecfg.ExperimentalBinCommitment, statecfg.BinCommitmentHash, commitment.PBinHashSuiteName()
	origParallel := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = origBin
		statecfg.BinCommitmentHash = origHash
		require.NoError(t, commitment.SetPBinHashSuite(origSuite))
		statecfg.ExperimentalParallelCommitment = origParallel
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	statecfg.ExperimentalParallelCommitment = false
}

func withCommitmentHistory(t *testing.T) {
	t.Helper()
	previousSchema := statecfg.Schema
	t.Cleanup(func() { statecfg.Schema = previousSchema })
	statecfg.EnableHistoricalCommitment()
}

func enableCommitmentHistoryFlag(t *testing.T, db kv.TemporalRwDB) {
	t.Helper()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))
}

func pbinWitnessFixture(t *testing.T, activation uint64) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	t.Helper()
	withBinCommitmentDatadir(t)
	withCommitmentHistory(t)
	var options []execmoduletester.Option
	if activation > 0 {
		previousDual := statecfg.ExperimentalHexBinCommitment
		previousV3, previousSchema := statecfg.ExperimentalCommitmentV3, statecfg.Schema
		t.Cleanup(func() {
			statecfg.ExperimentalHexBinCommitment = previousDual
			statecfg.ExperimentalCommitmentV3 = previousV3
			statecfg.Schema = previousSchema
		})
		statecfg.ExperimentalHexBinCommitment = true
		statecfg.ExperimentalCommitmentV3 = true
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
		options = append(options, execmoduletester.WithEnableDomain(kv.CommitmentBinDomain))
	}
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	from := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x1000000000000000000000000000000000000001")
	amsterdam := uint64(0)
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime, config.BinaryTrieTime = &amsterdam, &activation
	balance := new(big.Int).Mul(big.NewInt(10), new(big.Int).SetUint64(common.Ether))
	genesis := &types.Genesis{Config: config, Difficulty: uint256.NewInt(0), Alloc: types.GenesisAlloc{from: {Balance: new(big.Int).Set(balance)}, to: {Balance: big.NewInt(0), Nonce: 1, Code: common.FromHex("0x60003560005500")}}, GasLimit: 30_000_000, BaseFee: uint256.NewInt(0)}
	for i := range 256 {
		genesis.Alloc[common.BytesToAddress([]byte{0x02, byte(i)})] = types.GenesisAccount{Balance: big.NewInt(1)}
	}
	m := execmoduletester.New(t, append(options, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))...)
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
	return newDebugApiForTest(m), m
}

func TestPBinExecutionWitnessRefusesBinOnly(t *testing.T) {
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)
	m, _, _, _ := chainWithDeployedContract(t)
	enableCommitmentHistoryFlag(t, m.DB)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})
	bn := rpc.BlockNumber(2)
	_, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &bn}, nil)
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
}

func TestPBinOnlyProofAndWitnessRefuse(t *testing.T) {
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)
	m, bank, _, _ := chainWithDeployedContract(t)
	enableCommitmentHistoryFlag(t, m.DB)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	selector := rpc.BlockNumberOrHashWithNumber(2)
	_, err := api.GetProof(t.Context(), bank, nil, &selector)
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
	_, err = api.GetWitness(t.Context(), selector)
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
}

func TestPBinDualExecutionWitnessRefusesBin(t *testing.T) {
	api, m := pbinWitnessFixture(t, 30)
	ethAPI := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	for _, n := range []rpc.BlockNumber{3, 4} {
		t.Run(n.String(), func(t *testing.T) {
			selector := rpc.BlockNumberOrHashWithNumber(n)
			_, err := api.ExecutionWitness(t.Context(), selector, nil)
			require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
			_, err = ethAPI.GetProof(t.Context(), address, nil, &selector)
			require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
			_, err = ethAPI.GetWitness(t.Context(), selector)
			require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
		})
	}
}

func TestPBinDualV3HexExecutionWitnessServed(t *testing.T) {
	api, _ := pbinWitnessFixture(t, 30)
	n := rpc.BlockNumber(2)
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &n}, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.State)
}
