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
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

const pbtForkActivationTime = uint64(30)

type pbtForkChain struct {
	t              *testing.T
	m              *execmoduletester.ExecModuleTester
	api            *jsonrpc.DebugAPIImpl
	config         *chain.Config
	genesis        *types.Genesis
	key            *ecdsa.PrivateKey
	from           common.Address
	contract       common.Address
	code           []byte
	initialBalance *big.Int
}

func newPBTForkChain(t *testing.T, options ...execmoduletester.Option) *pbtForkChain {
	commitmentflags.Restore(t)
	previousExec3Parallel := dbg.Exec3Parallel
	previousBatchCommitments := dbg.BatchCommitments
	t.Cleanup(func() {
		dbg.Exec3Parallel = previousExec3Parallel
		dbg.BatchCommitments = previousBatchCommitments
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	dbg.Exec3Parallel = true
	dbg.BatchCommitments = false

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	amsterdamTime := uint64(0)
	activationTime := pbtForkActivationTime
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = &amsterdamTime
	config.BinaryTrieTime = &activationTime
	c := &pbtForkChain{
		t:              t,
		config:         config,
		key:            key,
		from:           crypto.PubkeyToAddress(key.PublicKey),
		contract:       common.HexToAddress("0x1000000000000000000000000000000000000001"),
		code:           common.FromHex("0x60003560005500"),
		initialBalance: new(big.Int).Mul(big.NewInt(10), new(big.Int).SetUint64(common.Ether)),
	}
	c.genesis = &types.Genesis{
		Config: config,
		Alloc: types.GenesisAlloc{
			c.from:     {Balance: new(big.Int).Set(c.initialBalance)},
			c.contract: {Balance: big.NewInt(0), Nonce: 1, Code: c.code},
		},
		GasLimit: 30_000_000,
		BaseFee:  uint256.NewInt(0),
	}
	c.m = execmoduletester.New(t, append([]execmoduletester.Option{
		execmoduletester.WithGenesisSpec(c.genesis),
		execmoduletester.WithKey(key),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
		execmoduletester.WithExperimentalBAL(),
	}, options...)...)
	seedDualGenesis(t, c.m, c.genesis)
	require.NoError(t, c.m.ExecModule.ResetCurrentContext(t.Context()))
	c.api = jsonrpc.NewPrivateDebugAPI(
		jsonrpc.NewBaseApi(nil, c.m.StateCache, c.m.BlockReader, c.m.Engine, &rpccfg.BaseApiConfig{Dirs: c.m.Dirs}),
		c.m.DB, nil, &rpccfg.DebugApiConfig{},
	)
	return c
}

func (c *pbtForkChain) generate(parent *types.Block, n int, base byte, firstBlockOffset int64) *blockgen.ChainPack {
	signer := types.LatestSignerForChainID(c.config.ChainID)
	pack, err := c.m.GenerateChainFrom(parent, n, func(i int, b *blockgen.BlockGen) {
		if i == 0 && firstBlockOffset != 0 {
			b.OffsetTime(firstBlockOffset)
		}
		value := uint64(base) + uint64(i) + 1
		data := common.BigToHash(new(big.Int).SetUint64(value))
		txn, err := types.SignTx(types.NewTransaction(b.TxNonce(c.from), c.contract,
			uint256.NewInt(value), 2_000_000, uint256.NewInt(0), data[:]), *signer, c.key)
		require.NoError(c.t, err)
		b.AddTx(txn)
	})
	require.NoError(c.t, err)
	return pack
}

func (c *pbtForkChain) roots(pack *blockgen.ChainPack, nonce, total uint64) []pbtBlockRoots {
	roots := make([]pbtBlockRoots, len(pack.Blocks))
	for i, block := range pack.Blocks {
		require.Len(c.t, pack.Receipts[i], 1)
		require.Equal(c.t, uint64(1), pack.Receipts[i][0].Status)
		value := block.Transactions()[0].GetValue().Uint64()
		nonce++
		total += value
		alloc := types.GenesisAlloc{
			c.from: {Balance: new(big.Int).Sub(c.initialBalance, new(big.Int).SetUint64(total)), Nonce: nonce},
			c.contract: {
				Balance: new(big.Int).SetUint64(total), Nonce: 1, Code: c.code,
				Storage: map[common.Hash]common.Hash{{}: common.BigToHash(new(big.Int).SetUint64(value))},
			},
		}
		for address, account := range c.genesis.Alloc {
			if address != c.from && address != c.contract {
				alloc[address] = account
			}
		}
		builderExit := c.config.GetBuilderExitContract().Value()
		builderAccount := alloc[builderExit]
		builderAccount.Storage = nil
		alloc[builderExit] = builderAccount
		roots[i] = pbtRootsFromAllocation(c.t, c.config, alloc, pbtForkActivationTime)
		require.Equal(c.t, roots[i].hex, block.Root(), "generated hex root at block %d", block.NumberU64())
	}
	setPBTRoots(pack, roots, c.config)
	return roots
}

func (c *pbtForkChain) insert(blocks []*types.Block, finalized common.Hash) execmodule.ForkChoiceResult {
	status, err := c.m.InsertBlocks(c.t.Context(), blocks)
	require.NoError(c.t, err)
	require.Equal(c.t, execmodule.ExecutionStatusSuccess, status)
	fcu, err := c.m.UpdateForkChoice(c.t.Context(), blocks[len(blocks)-1].Header(),
		execmoduletester.WithSafeHash(finalized), execmoduletester.WithFinalisedHash(finalized))
	require.NoError(c.t, err)
	return fcu
}

func (c *pbtForkChain) requireInserted(blocks []*types.Block, finalized common.Hash) {
	fcu := c.insert(blocks, finalized)
	require.Equal(c.t, execmodule.ExecutionStatusSuccess, fcu.Status, fcu.ValidationError)
	require.Equal(c.t, blocks[len(blocks)-1].Hash(), currentHead(c.t, c.m))
}

func (c *pbtForkChain) requireShadowLive(flipped bool) {
	progress, err := c.api.MigrationProgress(c.t.Context())
	require.NoError(c.t, err)
	require.Equal(c.t, flipped, progress.Flipped)
	require.False(c.t, progress.ShadowStopped)
}

func firstPostForkHeight(config *chain.Config, blocks ...*types.Block) uint64 {
	for _, block := range blocks {
		if config.IsBinaryTrie(block.Time()) {
			return block.NumberU64()
		}
	}
	return 0
}

func TestPBTReorgBetweenBranchesActivatingAtDifferentHeights(t *testing.T) {
	c := newPBTForkChain(t)
	main := c.generate(c.m.Genesis, 4, 0x10, 0)
	mainRoots := c.roots(main, 0, 0)
	c.requireInserted(main.Blocks[:1], common.Hash{})
	early := c.generate(main.Blocks[0], 3, 0x30, 10)
	earlyRoots := c.roots(early, 1, 0x11)
	c.requireInserted(main.Blocks[1:2], common.Hash{})
	late := c.generate(main.Blocks[1], 3, 0x50, -5)
	lateRoots := c.roots(late, 2, 0x11+0x12)

	require.Equal(t, uint64(3), firstPostForkHeight(c.config, main.Blocks...))
	require.Equal(t, uint64(2), firstPostForkHeight(c.config, early.Blocks...))
	require.Equal(t, uint64(4), firstPostForkHeight(c.config, append(main.Blocks[:2:2], late.Blocks...)...))

	for i := 2; i < len(main.Blocks); i++ {
		c.requireInserted(main.Blocks[i:i+1], common.Hash{})
		assertBlockCommitments(t, c.m, main.Blocks[i], mainRoots[i])
	}
	c.requireShadowLive(true)

	c.requireInserted(early.Blocks, common.Hash{})
	assertBlockCommitments(t, c.m, early.TopBlock, earlyRoots[len(earlyRoots)-1])
	for i, block := range early.Blocks {
		assertShadowRoot(t, c.m, c.api, block, earlyRoots[i])
	}
	c.requireShadowLive(true)

	c.requireInserted(late.Blocks, common.Hash{})
	assertBlockCommitments(t, c.m, late.TopBlock, lateRoots[len(lateRoots)-1])
	assertShadowRoot(t, c.m, c.api, main.Blocks[1], mainRoots[1])
	for i, block := range late.Blocks {
		assertShadowRoot(t, c.m, c.api, block, lateRoots[i])
	}
	c.requireShadowLive(true)

	c.requireInserted(main.Blocks[2:], common.Hash{})
	assertBlockCommitments(t, c.m, main.TopBlock, mainRoots[len(mainRoots)-1])
	require.NoError(t, c.m.ExecModule.ResetCurrentContext(t.Context()))
	assertBlockCommitments(t, c.m, main.TopBlock, mainRoots[len(mainRoots)-1])
}

func TestPBTRejectsTheOtherTreesRootAroundActivation(t *testing.T) {
	c := newPBTForkChain(t)
	pack := c.generate(c.m.Genesis, 5, 0x10, 0)
	roots := c.roots(pack, 0, 0)
	require.Equal(t, uint64(3), firstPostForkHeight(c.config, pack.Blocks...))
	c.requireInserted(pack.Blocks[:1], common.Hash{})

	for _, tc := range []struct {
		index int
		root  common.Hash
	}{
		{index: 1, root: roots[1].bin},
		{index: 2, root: roots[2].hex},
		{index: 3, root: roots[3].hex},
	} {
		block := pack.Blocks[tc.index]
		header := block.Header()
		header.Root = tc.root
		forged := block.WithSeal(header)
		fcu := c.insert([]*types.Block{forged}, common.Hash{})
		require.Equal(t, execmodule.ExecutionStatusBadBlock, fcu.Status, "block %d carrying the other tree's root", block.NumberU64())
		require.Contains(t, fcu.ValidationError, "invalid state root")
		require.Equal(t, pack.Blocks[tc.index-1].Hash(), currentHead(t, c.m))

		c.requireInserted([]*types.Block{block}, common.Hash{})
		assertBlockCommitments(t, c.m, block, roots[tc.index])
	}
	c.requireInserted(pack.Blocks[4:], common.Hash{})
	assertBlockCommitments(t, c.m, pack.TopBlock, roots[4])
	for i, block := range pack.Blocks {
		assertShadowRoot(t, c.m, c.api, block, roots[i])
	}
}

func TestPBTReorgBelowActivationWhileFinalityStalls(t *testing.T) {
	const maxReorgDepth = 4
	c := newPBTForkChain(t, execmoduletester.WithMaxReorgDepth(maxReorgDepth))
	window := maxReorgDepth + 32
	main := c.generate(c.m.Genesis, 3+window+5, 0x10, 0)
	mainRoots := c.roots(main, 0, 0)
	require.Equal(t, uint64(3), firstPostForkHeight(c.config, main.Blocks...))
	finalized := main.Blocks[0].Hash()

	c.requireInserted(main.Blocks[:1], finalized)
	c.requireInserted(main.Blocks[1:2], finalized)
	alternate := c.generate(main.Blocks[1], len(main.Blocks), 0x70, 0)
	alternateRoots := c.roots(alternate, 2, 0x11+0x12)
	for i := 2; i < len(main.Blocks); i++ {
		c.requireInserted(main.Blocks[i:i+1], finalized)
	}
	assertBlockCommitments(t, c.m, main.TopBlock, mainRoots[len(mainRoots)-1])
	c.requireShadowLive(true)

	c.requireInserted(alternate.Blocks, finalized)
	assertBlockCommitments(t, c.m, alternate.TopBlock, alternateRoots[len(alternateRoots)-1])
	for i, block := range alternate.Blocks {
		assertShadowRoot(t, c.m, c.api, block, alternateRoots[i])
	}
	c.requireShadowLive(true)
}
