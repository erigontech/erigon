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
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
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
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestPBTDualCommitmentFlipAndReorg(t *testing.T) {
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
	dbg.Exec3Parallel = true
	dbg.BatchCommitments = false

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	from := crypto.PubkeyToAddress(key.PublicKey)
	amsterdamTime := uint64(0)
	activationTime := uint64(30)
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = &amsterdamTime
	config.BinaryTrieTime = &activationTime
	genesis := &types.Genesis{
		Config: config,
		Alloc: types.GenesisAlloc{
			from: {Balance: new(big.Int).Mul(big.NewInt(10), new(big.Int).SetUint64(common.Ether))},
		},
		GasLimit: 30_000_000,
	}

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(key),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
		execmoduletester.WithExperimentalBAL(),
	)
	seedDualGenesis(t, m, genesis)
	m.ExecModule.ResetCurrentContext()

	prefix, err := m.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
		b.SetExtra([]byte{0x10, byte(i)})
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(prefix))

	mainChain, err := m.GenerateChainFrom(prefix.TopBlock, 2, func(i int, b *blockgen.BlockGen) {
		b.SetExtra([]byte{0x20, byte(i)})
	})
	require.NoError(t, err)
	alternateChain, err := m.GenerateChainFrom(prefix.TopBlock, 3, func(i int, b *blockgen.BlockGen) {
		b.SetExtra([]byte{0x30, byte(i)})
	})
	require.NoError(t, err)
	_, binRoot := dualRoots(t, m)
	setBinaryRoots(alternateChain, binRoot)
	setBinaryRoots(mainChain, binRoot)

	require.NoError(t, m.InsertChain(mainChain))
	for _, block := range append(prefix.Blocks, mainChain.Blocks...) {
		assertBlockCommitments(t, m, block)
	}

	debugAPI := jsonrpc.NewPrivateDebugAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB,
		nil,
		&rpccfg.DebugApiConfig{},
	)
	for _, block := range append(prefix.Blocks, mainChain.Blocks...) {
		assertShadowRoot(t, m, debugAPI, block)
	}
	progress, err := debugAPI.MigrationProgress(context.Background())
	require.NoError(t, err)
	require.True(t, progress.Flipped)
	require.False(t, progress.ShadowStopped)
	dbstateAgg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	dbstateAgg.SetCanonicalCommitmentDomain(kv.CommitmentDomain)
	m.ExecModule.ResetCurrentContext()

	require.NoError(t, m.InsertChain(alternateChain))
	require.Equal(t, alternateChain.TopBlock.Hash(), currentHead(t, m))
	assertBlockCommitments(t, m, alternateChain.TopBlock)
	assertShadowRoot(t, m, debugAPI, alternateChain.Blocks[0])
	assertShadowRoot(t, m, debugAPI, alternateChain.TopBlock)

	for _, block := range append(prefix.Blocks[1:], alternateChain.Blocks[0]) {
		root := readShadowRoot(t, m, block)
		require.NotEmpty(t, root)
	}
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

func setBinaryRoots(chainPack *blockgen.ChainPack, root []byte) {
	for i, block := range chainPack.Blocks {
		header := block.Header()
		changed := false
		if i > 0 {
			header.ParentHash = chainPack.Blocks[i-1].Hash()
			changed = true
		}
		if block.Time() >= 30 {
			header.Root = common.BytesToHash(root)
			changed = true
		}
		if !changed {
			continue
		}
		chainPack.Blocks[i] = block.WithSeal(header)
		chainPack.Headers[i] = chainPack.Blocks[i].HeaderNoCopy()
	}
	chainPack.TopBlock = chainPack.Blocks[len(chainPack.Blocks)-1]
}

func dualRoots(t *testing.T, m *execmoduletester.ExecModuleTester) ([]byte, []byte) {
	t.Helper()
	tx, err := m.DB.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	hexRoot, err := domains.GetCommitmentCtxForDomain(kv.CommitmentDomain).Trie().RootHash()
	require.NoError(t, err)
	binRoot, err := domains.GetCommitmentCtxForDomain(kv.CommitmentBinDomain).Trie().RootHash()
	require.NoError(t, err)
	return bytes.Clone(hexRoot), bytes.Clone(binRoot)
}

func assertBlockCommitments(t *testing.T, m *execmoduletester.ExecModuleTester, block *types.Block) {
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
	blockRoot := block.Root()
	if m.ChainConfig.IsBinaryTrie(block.Time()) {
		require.Equal(t, blockRoot[:], binRoot)
		require.NotEqual(t, blockRoot[:], hexRoot)
	} else {
		require.Equal(t, blockRoot[:], hexRoot)
		require.NotEqual(t, blockRoot[:], binRoot)
	}
}

func assertShadowRoot(t *testing.T, m *execmoduletester.ExecModuleTester, api *jsonrpc.DebugAPIImpl, block *types.Block) {
	t.Helper()
	want := readShadowRoot(t, m, block)
	require.NotEmpty(t, want)
	got, err := api.ShadowStateRoot(context.Background(), block.Hash())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, common.BytesToHash(want), *got)
	require.NotEqual(t, block.Root(), *got)
}

func readShadowRoot(t *testing.T, m *execmoduletester.ExecModuleTester, block *types.Block) []byte {
	t.Helper()
	tx, err := m.DB.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	root, err := rawdb.ReadShadowStateRoot(tx, block.Hash(), block.NumberU64())
	require.NoError(t, err)
	return bytes.Clone(root)
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
