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
	"fmt"
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
	pbtengine "github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

var pbinBeaconRootsCode = common.FromHex("0x3373fffffffffffffffffffffffffffffffffffffffe14604d57602036146024575f5ffd5b5f35801560495762001fff810690815414603c575f5ffd5b62001fff01545f5260205ff35b5f5ffd5b62001fff42064281555f359062001fff015500")

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

func pbinWitnessFixture(t *testing.T, activation uint64, dualOption ...bool) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	return pbinWitnessFixtureWithHook(t, activation, nil, dualOption...)
}

func pbinWitnessFixtureWithHook(t *testing.T, activation uint64, beforeInsert func(*execmoduletester.ExecModuleTester, *blockgen.ChainPack) error, dualOption ...bool) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	return pbinWitnessFixtureWithGenerator(t, activation, beforeInsert, nil, dualOption...)
}

type pbinWitnessBlockGenerator func(int, *blockgen.BlockGen, func(common.Address, *uint256.Int, []byte), func(*uint256.Int, []byte), func(types.Transaction), func(common.Address, *uint256.Int, []byte))

func pbinWitnessFixtureWithGenerator(t *testing.T, activation uint64, beforeInsert func(*execmoduletester.ExecModuleTester, *blockgen.ChainPack) error, generator pbinWitnessBlockGenerator, dualOption ...bool) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	return pbinWitnessFixtureWithGeneratorN(t, activation, 4, beforeInsert, generator, dualOption...)
}

func pbinWitnessFixtureWithGeneratorN(t *testing.T, activation uint64, blockCount int, beforeInsert func(*execmoduletester.ExecModuleTester, *blockgen.ChainPack) error, generator pbinWitnessBlockGenerator, dualOption ...bool) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	return pbinWitnessFixtureWithGeneratorNAlloc(t, activation, blockCount, beforeInsert, generator, nil, dualOption...)
}

func pbinWitnessFixtureWithGeneratorNAlloc(t *testing.T, activation uint64, blockCount int, beforeInsert func(*execmoduletester.ExecModuleTester, *blockgen.ChainPack) error, generator pbinWitnessBlockGenerator, extraAlloc types.GenesisAlloc, dualOption ...bool) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	return pbinWitnessFixtureWithGeneratorNConfig(t, activation, blockCount, beforeInsert, generator, extraAlloc, true, dualOption...)
}

func pbinWitnessFixtureWithGeneratorNNoSystemCalls(t *testing.T, activation uint64, blockCount int, generator pbinWitnessBlockGenerator) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	return pbinWitnessFixtureWithGeneratorNConfig(t, activation, blockCount, nil, generator, nil, false)
}

func pbinWitnessFixtureWithGeneratorNAllocNoSystemCalls(t *testing.T, activation uint64, blockCount int, generator pbinWitnessBlockGenerator, extraAlloc types.GenesisAlloc) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	return pbinWitnessFixtureWithGeneratorNConfig(t, activation, blockCount, nil, generator, extraAlloc, false)
}

func pbinWitnessFixtureWithGeneratorNConfig(t *testing.T, activation uint64, blockCount int, beforeInsert func(*execmoduletester.ExecModuleTester, *blockgen.ChainPack) error, generator pbinWitnessBlockGenerator, extraAlloc types.GenesisAlloc, systemCalls bool, dualOption ...bool) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	t.Helper()
	withCommitmentHistory(t)
	dual := activation > 0
	if len(dualOption) > 0 {
		dual = dualOption[0]
	}
	hexOnly := len(dualOption) > 0 && !dual && activation == 0
	var options []execmoduletester.Option
	if (activation == 0 && !hexOnly) || dual {
		withBinCommitmentDatadir(t)
	}
	if activation > 0 || hexOnly {
		previousDual := statecfg.ExperimentalHexBinCommitment
		previousV3, previousSchema := statecfg.ExperimentalCommitmentV3, statecfg.Schema
		previousBin := statecfg.ExperimentalBinCommitment
		t.Cleanup(func() {
			statecfg.ExperimentalHexBinCommitment = previousDual
			statecfg.ExperimentalCommitmentV3 = previousV3
			statecfg.ExperimentalBinCommitment = previousBin
			statecfg.Schema = previousSchema
		})
		statecfg.ExperimentalHexBinCommitment = dual
		statecfg.ExperimentalCommitmentV3 = true
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
		statecfg.ExperimentalBinCommitment = dual
		if dual {
			options = append(options, execmoduletester.WithEnableDomain(kv.CommitmentBinDomain))
		}
	}
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	from := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x1000000000000000000000000000000000000001")
	amsterdam := uint64(0)
	config := chain.AllProtocolChanges.Copy()
	if systemCalls {
		config.AmsterdamTime = &amsterdam
	} else {
		config = chain.AllProtocolChanges.Copy()
		config.ShanghaiTime = nil
		config.CancunTime = nil
		config.PragueTime = nil
		config.OsakaTime = nil
		config.AmsterdamTime = &activation
	}
	if generator != nil {
		if systemCalls {
			require.True(t, config.IsCancun(0))
			require.True(t, config.IsPrague(0))
		}
	}
	if !hexOnly {
		config.BinaryTrieTime = &activation
	}
	balance := new(big.Int).Mul(big.NewInt(10), new(big.Int).SetUint64(common.Ether))
	genesis := &types.Genesis{Config: config, Difficulty: uint256.NewInt(0), Alloc: types.GenesisAlloc{from: {Balance: new(big.Int).Set(balance)}, to: {Balance: big.NewInt(0), Nonce: 1, Code: common.FromHex("0x60003560005500")}}, GasLimit: 30_000_000, BaseFee: uint256.NewInt(0)}
	maps.Copy(genesis.Alloc, extraAlloc)
	for i := range 256 {
		genesis.Alloc[common.BytesToAddress([]byte{0x02, byte(i)})] = types.GenesisAccount{Balance: big.NewInt(1)}
	}
	m := execmoduletester.New(t, append(options, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))...)
	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error { return rawdb.WriteDBCommitmentHistoryEnabled(tx, true) }))
	if systemCalls {
		tx, err := m.DB.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		addresses := []common.Address{
			config.GetWithdrawalRequestContract().Value(),
			config.GetConsolidationRequestContract().Value(),
		}
		if generator != nil {
			addresses = append(addresses, params.BeaconRootsAddress.Value(), params.HistoryStorageAddress.Value())
		}
		for _, address := range addresses {
			code, _, err := tx.GetLatest(kv.CodeDomain, address[:], kv.GetLatestOptions{})
			require.NoError(t, err)
			if len(code) == 0 {
				switch address {
				case params.BeaconRootsAddress.Value():
					code = pbinBeaconRootsCode
				case params.HistoryStorageAddress.Value():
					code = []byte{0}
				}
			}
			require.NotEmpty(t, code)
			genesis.Alloc[address] = types.GenesisAccount{Balance: big.NewInt(0), Nonce: 1, Code: append([]byte(nil), code...)}
		}
		require.NoError(t, tx.Delete(kv.ConfigTable, kv.GenesisKey))
		require.NoError(t, rawdb.WriteGenesisIfNotExist(tx, genesis))
		domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
		require.NoError(t, err)
		root, ibs, err := genesiswrite.ComputeGenesisCommitment(t.Context(), genesis, tx, domains, m.Genesis.Header())
		require.NoError(t, err)
		ibs.Close()
		header := m.Genesis.Header()
		header.Root = common.BytesToHash(root)
		m.Genesis = m.Genesis.WithSeal(header)
		require.NoError(t, rawdb.WriteBlock(tx, m.Genesis))
		require.NoError(t, rawdb.WriteChainConfig(tx, m.Genesis.Hash(), config))
		require.NoError(t, rawdb.WriteTd(tx, m.Genesis.Hash(), 0, *genesis.Difficulty))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, m.Genesis.Hash(), 0))
		rawdb.WriteHeadBlockHash(tx, m.Genesis.Hash())
		require.NoError(t, rawdb.WriteHeadHeaderHash(tx, m.Genesis.Hash()))
		require.NoError(t, domains.Commit(t.Context(), tx))
		domains.Close()
		require.NoError(t, tx.Commit())
	}
	if systemCalls {
		require.NoError(t, m.ExecModule.ResetCurrentContext(t.Context()))
	}
	signer := types.LatestSignerForChainID(config.ChainID)
	pack, err := m.GenerateChain(blockCount, func(i int, b *blockgen.BlockGen) {
		if generator != nil {
			getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
				parent := b.PrevBlock(-1)
				if number == parent.NumberU64() {
					return parent.Header(), nil
				}
				block := b.PrevBlock(int(number) - 1)
				if block.Hash() != hash {
					return nil, fmt.Errorf("unexpected ancestor hash for block %d", number)
				}
				return block.Header(), nil
			}
			addTransaction := func(to common.Address, value *uint256.Int, data []byte) {
				txn, err := types.SignTx(types.NewTransaction(b.TxNonce(from), to, value, 2_000_000, uint256.NewInt(0), data), *signer, key)
				require.NoError(t, err)
				b.AddTx(txn)
			}
			addContract := func(value *uint256.Int, data []byte) {
				txn, err := types.SignTx(types.NewContractCreation(b.TxNonce(from), value, 2_000_000, uint256.NewInt(0), data), *signer, key)
				require.NoError(t, err)
				b.AddTx(txn)
			}
			addSigned := func(tx types.Transaction) {
				signed, err := types.SignTx(tx, *signer, key)
				require.NoError(t, err)
				b.AddTx(signed)
			}
			addTransactionWithChain := func(to common.Address, value *uint256.Int, data []byte) {
				txn, err := types.SignTx(types.NewTransaction(b.TxNonce(from), to, value, 2_000_000, uint256.NewInt(0), data), *signer, key)
				require.NoError(t, err)
				b.AddTxWithChain(getHeader, nil, txn)
			}
			generator(i, b, addTransaction, addContract, addSigned, addTransactionWithChain)
			return
		}
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
		if generator == nil && config.IsBinaryTrie(block.Time()) {
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
	if beforeInsert == nil {
		require.NoError(t, m.InsertChain(pack))
	} else {
		require.NoError(t, beforeInsert(m, pack))
	}
	api := newDebugApiForTest(m)
	api._chainConfig.Store(m.ChainConfig)
	api._genesis.Store(m.Genesis)
	return api, m
}

func repairPBinPreForkShadows(t *testing.T, m *execmoduletester.ExecModuleTester, activation uint64) {
	t.Helper()
	commitmentDomain := kv.CommitmentBinDomain
	if !statecfg.ExperimentalHexBinCommitment {
		commitmentDomain = kv.CommitmentDomain
	}
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	roots := make(map[uint64]common.Hash)
	for blockNum := uint64(0); ; blockNum++ {
		header := rawdb.ReadHeaderByNumber(tx, blockNum)
		if header == nil {
			break
		}
		if header.Time >= activation {
			continue
		}
		maxTxNum, err := m.BlockReader.TxnumReader().Max(t.Context(), tx, blockNum)
		require.NoError(t, err)
		data, ok, err := tx.GetAsOf(commitmentDomain, pbtengine.GlobalRootKey(), maxTxNum+1)
		require.NoError(t, err)
		require.True(t, ok, "missing binary root record for block %d", blockNum)
		record, err := pbtengine.DecodeRecord(pbtengine.GlobalRootKey(), data)
		require.NoError(t, err)
		root, err := pbtengine.Fold(pbtengine.GlobalRootKey(), &record)
		require.NoError(t, err)
		roots[blockNum] = root
	}
	tx.Rollback()
	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		for blockNum, root := range roots {
			header := rawdb.ReadHeaderByNumber(tx, blockNum)
			if err := rawdb.WriteShadowStateRoot(tx, header.Hash(), blockNum, root[:]); err != nil {
				return err
			}
		}
		return nil
	}))
}

func TestPBinExecutionWitnessServedBinOnly(t *testing.T) {
	api, _ := pbinWitnessFixture(t, 0)
	bn := rpc.BlockNumber(2)
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &bn}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.State)
}

func TestPBinExecutionWitnessFreshContractAndEmptyTouch(t *testing.T) {
	empty := common.HexToAddress("0x7300000000000000000000000000000000000000")
	initCode := common.FromHex("0x6001600c60003960016000f36000")
	api, _ := pbinWitnessFixtureWithGenerator(t, 0, nil, func(i int, _ *blockgen.BlockGen, addTransaction func(common.Address, *uint256.Int, []byte), addContract func(*uint256.Int, []byte), _ func(types.Transaction), _ func(common.Address, *uint256.Int, []byte)) {
		switch i {
		case 1:
			addContract(uint256.NewInt(0), initCode)
		case 2:
			addTransaction(empty, uint256.NewInt(0), nil)
		default:
			addTransaction(common.HexToAddress("0x1000000000000000000000000000000000000001"), uint256.NewInt(1), nil)
		}
	})
	for _, block := range []rpc.BlockNumber{2, 3} {
		result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(block), nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
	}
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

func TestPBinDualExecutionWitnessServedBin(t *testing.T) {
	api, m := pbinWitnessFixture(t, 2)
	ethAPI := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	for _, n := range []rpc.BlockNumber{3, 4} {
		t.Run(n.String(), func(t *testing.T) {
			selector := rpc.BlockNumberOrHashWithNumber(n)
			result, err := api.ExecutionWitness(t.Context(), selector, nil, nil)
			require.NoError(t, err)
			require.NotEmpty(t, result.State)
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
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &n}, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.State)
}
