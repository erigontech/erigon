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

package executiontests

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestPBTDualCommitmentFlipAndReorg(t *testing.T) {
	for _, parallel := range []bool{true, false} {
		t.Run(fmt.Sprintf("exec3_parallel=%t", parallel), func(t *testing.T) {
			testPBTDualCommitmentFlipAndReorg(t, parallel)
		})
	}
}

func testPBTDualCommitmentFlipAndReorg(t *testing.T, parallel bool) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	previousExec3Parallel := dbg.Exec3Parallel
	previousBatchCommitments := dbg.BatchCommitments
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.BinCommitmentHash = previousHash
		dbg.Exec3Parallel = previousExec3Parallel
		dbg.BatchCommitments = previousBatchCommitments
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = ""
	dbg.Exec3Parallel = parallel
	dbg.BatchCommitments = false

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	from := crypto.PubkeyToAddress(key.PublicKey)
	amsterdamTime := uint64(0)
	activationTime := uint64(30)
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = &amsterdamTime
	config.BinaryTrieTime = &activationTime
	contract := common.HexToAddress("0x1000000000000000000000000000000000000001")
	code := common.FromHex("0x60003560005500")
	initialBalance := new(big.Int).Mul(big.NewInt(10), new(big.Int).SetUint64(common.Ether))
	genesis := &types.Genesis{
		Config: config,
		Alloc: types.GenesisAlloc{
			from:     {Balance: new(big.Int).Set(initialBalance)},
			contract: {Balance: big.NewInt(0), Nonce: 1, Code: code},
		},
		GasLimit: 30_000_000,
		BaseFee:  uint256.NewInt(0),
	}

	options := []execmoduletester.Option{
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(key),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	}
	if parallel {
		options = append(options, execmoduletester.WithExperimentalBAL())
	} else {
		options = append(options, execmoduletester.WithoutExperimentalBAL())
	}
	m := execmoduletester.New(t, options...)
	seedDualGenesis(t, m, genesis)
	require.NoError(t, m.ExecModule.ResetCurrentContext(t.Context()))

	signer := types.LatestSignerForChainID(config.ChainID)
	generate := func(base byte) func(int, *blockgen.BlockGen) {
		return func(i int, b *blockgen.BlockGen) {
			value := uint64(base) + uint64(i) + 1
			data := common.BigToHash(new(big.Int).SetUint64(value))
			txn, signErr := types.SignTx(types.NewTransaction(b.TxNonce(from), contract,
				uint256.NewInt(value), 2_000_000, uint256.NewInt(0), data[:]), *signer, key)
			require.NoError(t, signErr)
			b.AddTx(txn)
		}
	}
	expected := func(pack *blockgen.ChainPack, nonce, total uint64) []pbtBlockRoots {
		roots := make([]pbtBlockRoots, len(pack.Blocks))
		for i, block := range pack.Blocks {
			require.Len(t, pack.Receipts[i], 1)
			require.Equal(t, uint64(1), pack.Receipts[i][0].Status)
			require.Greater(t, pack.Receipts[i][0].GasUsed, uint64(21_000))
			value := block.Transactions()[0].GetValue().Uint64()
			nonce++
			total += value
			alloc := types.GenesisAlloc{
				from: {Balance: new(big.Int).Sub(initialBalance, new(big.Int).SetUint64(total)), Nonce: nonce},
				contract: {Balance: new(big.Int).SetUint64(total), Nonce: 1, Code: code,
					Storage: map[common.Hash]common.Hash{{}: common.BigToHash(new(big.Int).SetUint64(value))}},
			}
			for address, account := range genesis.Alloc {
				if address != from && address != contract {
					alloc[address] = account
				}
			}
			builderExit := config.GetBuilderExitContract().Value()
			builderAccount := alloc[builderExit]
			builderAccount.Storage = nil
			alloc[builderExit] = builderAccount
			roots[i] = pbtRootsFromAllocation(t, config, alloc, activationTime)
			require.Equal(t, roots[i].hex, block.Root(), "generated hex root at block %d", block.NumberU64())
			if i > 0 {
				require.NotEqual(t, roots[i-1], roots[i])
			}
		}
		setPBTRoots(pack, roots, config)
		return roots
	}
	mainChain, err := m.GenerateChain(4, generate(0x10))
	require.NoError(t, err)
	mainRoots := expected(mainChain, 0, 0)
	prefix := mainChain.Slice(0, 2)
	for i := range prefix.Blocks {
		require.NoError(t, m.InsertChain(prefix.Slice(i, i+1)))
		assertBlockCommitments(t, m, prefix.Blocks[i], mainRoots[i])
	}
	alternateChain, err := m.GenerateChainFrom(prefix.TopBlock, 3, generate(0x30))
	require.NoError(t, err)
	alternateRoots := expected(alternateChain, 2, 0x11+0x12)
	require.NotEqual(t, mainRoots[2], alternateRoots[0])

	debugAPI := jsonrpc.NewPrivateDebugAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, &rpccfg.DebugApiConfig{},
	)
	for i := 2; i < len(mainChain.Blocks); i++ {
		require.NoError(t, m.InsertChain(mainChain.Slice(i, i+1)))
		assertBlockCommitments(t, m, mainChain.Blocks[i], mainRoots[i])
		require.NoError(t, m.ExecModule.ResetCurrentContext(t.Context()))
	}
	for i, block := range mainChain.Blocks {
		assertShadowRoot(t, m, debugAPI, block, mainRoots[i])
	}
	progress, err := debugAPI.MigrationProgress(context.Background())
	require.NoError(t, err)
	require.True(t, progress.Flipped)
	require.False(t, progress.ShadowStopped)

	require.NoError(t, m.InsertChain(alternateChain))
	require.Equal(t, alternateChain.TopBlock.Hash(), currentHead(t, m))
	assertBlockCommitments(t, m, alternateChain.TopBlock, alternateRoots[len(alternateRoots)-1])
	for i, block := range alternateChain.Blocks {
		assertShadowRoot(t, m, debugAPI, block, alternateRoots[i])
	}
	require.NoError(t, m.ExecModule.ResetCurrentContext(t.Context()))
	assertBlockCommitments(t, m, alternateChain.TopBlock, alternateRoots[len(alternateRoots)-1])
}

func seedDualGenesis(t *testing.T, m *execmoduletester.ExecModuleTester, genesis *types.Genesis) {
	t.Helper()
	tx, err := m.DB.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	_, statedb, err := genesiswrite.ComputeGenesisCommitment(context.Background(), genesis, tx, domains, m.Genesis.Header())
	require.NoError(t, err)
	statedb.Close()
	require.NoError(t, domains.Commit(context.Background(), tx))
	require.NoError(t, tx.Commit())
}

type pbtBlockRoots struct {
	hex common.Hash
	bin common.Hash
}

func pbtRootsFromAllocation(t *testing.T, config *chain.Config, alloc types.GenesisAlloc, activationTime uint64) pbtBlockRoots {
	t.Helper()
	root := func(timestamp uint64) common.Hash {
		block, state, err := genesiswrite.GenesisToBlock(&types.Genesis{Config: config, Alloc: alloc, Timestamp: timestamp}, datadir.New(t.TempDir()), log.New())
		require.NoError(t, err)
		state.Close()
		return block.Root()
	}
	roots := pbtBlockRoots{hex: root(0), bin: root(activationTime)}
	require.NotEqual(t, roots.hex, roots.bin)
	return roots
}

func setPBTRoots(pack *blockgen.ChainPack, roots []pbtBlockRoots, config *chain.Config) {
	for i, block := range pack.Blocks {
		header := block.Header()
		if i > 0 {
			header.ParentHash = pack.Blocks[i-1].Hash()
		}
		if config.IsBinaryTrie(block.Time()) {
			header.Root = roots[i].bin
		}
		pack.Blocks[i] = block.WithSeal(header)
		pack.Headers[i] = pack.Blocks[i].HeaderNoCopy()
	}
	pack.TopBlock = pack.Blocks[len(pack.Blocks)-1]
}

func assertBlockCommitments(t *testing.T, m *execmoduletester.ExecModuleTester, block *types.Block, want pbtBlockRoots) {
	t.Helper()
	ctx := context.Background()
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	hexRoot, err := domains.GetCommitmentCtxForDomain(kv.CommitmentDomain).Trie().RootHash()
	require.NoError(t, err)
	binRoot, err := domains.GetCommitmentCtxForDomain(kv.CommitmentBinDomain).Trie().RootHash()
	require.NoError(t, err)
	require.Equal(t, want.hex, common.BytesToHash(hexRoot))
	require.Equal(t, want.bin, common.BytesToHash(binRoot))
	blockRoot := block.Root()
	if m.ChainConfig.IsBinaryTrie(block.Time()) {
		require.Equal(t, blockRoot[:], binRoot)
		require.NotEqual(t, blockRoot[:], hexRoot)
	} else {
		require.Equal(t, blockRoot[:], hexRoot)
		require.NotEqual(t, blockRoot[:], binRoot)
	}
}

func assertShadowRoot(t *testing.T, m *execmoduletester.ExecModuleTester, api *jsonrpc.DebugAPIImpl, block *types.Block, roots pbtBlockRoots) {
	t.Helper()
	want := roots.bin
	if m.ChainConfig.IsBinaryTrie(block.Time()) {
		want = roots.hex
	}
	got, err := api.ShadowStateRoot(context.Background(), block.Hash())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, want, *got)
	require.NotEqual(t, block.Root(), *got)
}

func currentHead(t *testing.T, m *execmoduletester.ExecModuleTester) common.Hash {
	t.Helper()
	tx, err := m.DB.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	head, err := m.BlockReader.CurrentBlock(tx)
	require.NoError(t, err)
	return head.Hash()
}
