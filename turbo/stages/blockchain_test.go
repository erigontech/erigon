// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package stages

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
)

// So we can deterministically seed different blockchains
var (
	canonicalSeed = 1
	forkSeed      = 2
)

// makeHeaderChain creates a deterministic chain of headers rooted at parent.
func makeHeaderChain(parent *types.Header, n int, engine consensus.Engine, db ethdb.RwKV, seed int) []*types.Header {
	blocks := makeBlockChain(types.NewBlockWithHeader(parent), n, engine, db, seed)
	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	return headers
}

// makeBlockChain creates a deterministic chain of blocks rooted at parent.
func makeBlockChain(parent *types.Block, n int, engine consensus.Engine, db ethdb.RwKV, seed int) []*types.Block {
	chain, _ := core.GenerateChain(params.TestChainConfig, parent, engine, db, n, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{0: byte(seed), 19: byte(i)})
	}, false /* intermediateHashes */)
	return chain.Blocks
}

// newCanonical creates a chain database, and injects a deterministic canonical
// chain. Depending on the full flag, if creates either a full block chain or a
// header only chain.
func newCanonical(t *testing.T, engine consensus.Engine, n int, full bool) (*ethdb.ObjectDatabase, *types.Block) {
	db := ethdb.NewTestDB(t)
	genesis, _, err := new(core.Genesis).Commit(db, true /* history */)
	if err != nil {
		panic(err)
	}

	// Create and inject the requested chain
	if n == 0 {
		return db, genesis
	}

	if full {
		// Full block-chain requested
		blocks := makeBlockChain(genesis, n, engine, db.RwKV(), canonicalSeed)
		_, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.AllEthashProtocolChanges, &vm.Config{}, engine, blocks, true /* checkRoot */)
		if err != nil {
			t.Fatal(err)
		}
		return db, genesis
	}
	// Header-only chain requested
	headers := makeHeaderChain(genesis.Header(), n, engine, db.RwKV(), canonicalSeed)
	_, _, _, err = stagedsync.InsertHeadersInStages(db, params.AllEthashProtocolChanges, ethash.NewFaker(), headers)
	if err != nil {
		t.Fatal(err)
	}
	return db, genesis
}

// Test fork of length N starting from block i
func testFork(t *testing.T, chainDb ethdb.Database, i, n int, full bool, comparator func(td1, td2 *big.Int)) {
	// Copy old chain up to #i into a new db
	db, _ := newCanonical(t, ethash.NewFaker(), i, true)
	var err error
	// Assert the chains have the same header/block at #i
	var hash1, hash2 common.Hash
	if hash1, err = rawdb.ReadCanonicalHash(chainDb, uint64(i)); err != nil {
		t.Fatalf("Failed to read canonical hash: %v", err)
	}
	if hash2, err = rawdb.ReadCanonicalHash(db, uint64(i)); err != nil {
		t.Fatalf("Failed to read canonical hash 2: %v", err)
	}
	if full {
		if block1 := rawdb.ReadBlockDeprecated(chainDb, hash1, uint64(i)); block1 == nil {
			t.Fatalf("Did not find canonical block")
		}
		if block2 := rawdb.ReadBlockDeprecated(db, hash2, uint64(i)); block2 == nil {
			t.Fatalf("Did not find canonical block 2")
		}
	} else {
		if header1 := rawdb.ReadHeader(chainDb, hash1, uint64(i)); header1 == nil {
			t.Fatalf("Did not find canonical header")
		}
		if header2 := rawdb.ReadHeader(db, hash2, uint64(i)); header2 == nil {
			t.Fatalf("Did not find canonical header 2")
		}
	}
	if hash1 != hash2 {
		t.Errorf("chain content mismatch at %d: have hash %v, want hash %v", i, hash2, hash1)
	}
	// Extend the newly created chain
	var (
		blockChainB  []*types.Block
		headerChainB []*types.Header
	)
	var tdPre, tdPost *big.Int
	if full {
		currentBlockHash := rawdb.ReadHeadBlockHash(chainDb)
		currentBlock, err1 := rawdb.ReadBlockByHashDeprecated(chainDb, currentBlockHash)
		if err1 != nil {
			t.Fatalf("Failed to read current bock: %v", err1)
		}
		currentBlockB, err2 := rawdb.ReadBlockByHashDeprecated(db, rawdb.ReadHeadBlockHash(db))
		if err2 != nil {
			t.Fatalf("Failed to read current bock: %v", err2)
		}
		blockChainB = makeBlockChain(currentBlockB, n, ethash.NewFaker(), db.RwKV(), forkSeed)
		tdPre, err = rawdb.ReadTd(chainDb, currentBlockHash, currentBlock.NumberU64())
		if err != nil {
			t.Fatalf("Failed to read TD for current block: %v", err)
		}
		if _, err = stagedsync.InsertBlocksInStages(chainDb, ethdb.DefaultStorageMode, params.AllEthashProtocolChanges, &vm.Config{}, ethash.NewFaker(), blockChainB, true /* checkRoot */); err != nil {
			t.Fatalf("failed to insert forking chain: %v", err)
		}
		currentBlockHash = blockChainB[len(blockChainB)-1].Hash()
		currentBlock, err1 = rawdb.ReadBlockByHashDeprecated(chainDb, currentBlockHash)
		if err1 != nil {
			t.Fatalf("Failed to read last header: %v", err1)
		}
		tdPost, err = rawdb.ReadTd(chainDb, currentBlockHash, currentBlock.NumberU64())
		if err != nil {
			t.Fatalf("Failed to read TD for current header: %v", err)
		}
	} else {
		currentHeaderHash := rawdb.ReadHeadHeaderHash(chainDb)
		currentHeader, err1 := rawdb.ReadHeaderByHash(chainDb, currentHeaderHash)
		if err1 != nil {
			t.Fatalf("Failed to read current header: %v", err1)
		}
		currentHeaderB, err2 := rawdb.ReadHeaderByHash(db, rawdb.ReadHeadHeaderHash(db))
		if err2 != nil {
			t.Fatalf("Failed to read current header: %v", err2)
		}
		headerChainB = makeHeaderChain(currentHeaderB, n, ethash.NewFaker(), db.RwKV(), forkSeed)
		tdPre, err = rawdb.ReadTd(chainDb, currentHeaderHash, currentHeader.Number.Uint64())
		if err != nil {
			t.Fatalf("Failed to read TD for current header: %v", err)
		}
		if _, _, _, err = stagedsync.InsertHeadersInStages(chainDb, params.AllEthashProtocolChanges, ethash.NewFaker(), headerChainB); err != nil {
			t.Fatalf("failed to insert forking chain: %v", err)
		}
		currentHeader = headerChainB[len(headerChainB)-1]
		tdPost, err = rawdb.ReadTd(chainDb, currentHeader.Hash(), currentHeader.Number.Uint64())
		if err != nil {
			t.Fatalf("Failed to read TD for current header: %v", err)
		}
	}
	// Sanity check that the forked chain can be imported into the original
	if full {
		if err := testBlockChainImport(blockChainB, db); err != nil {
			t.Fatalf("failed to import forked block chain: %v", err)
		}
	} else {
		if err := testHeaderChainImport(headerChainB, db); err != nil {
			t.Fatalf("failed to import forked header chain: %v", err)
		}
	}
	// Compare the total difficulties of the chains
	comparator(tdPre, tdPost)
}

// testBlockChainImport tries to process a chain of blocks, writing them into
// the database if successful.
func testBlockChainImport(chain types.Blocks, db ethdb.Database) error {
	if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.AllEthashProtocolChanges, &vm.Config{}, ethash.NewFaker(), chain, true /* checkRoot */); err != nil {
		return err
	}
	return nil
}

// testHeaderChainImport tries to process a chain of header, writing them into
// the database if successful.
func testHeaderChainImport(chain []*types.Header, db ethdb.Database) error {
	if _, _, _, err := stagedsync.InsertHeadersInStages(db, params.AllEthashProtocolChanges, ethash.NewFaker(), chain); err != nil {
		return err
	}
	return nil
}

func TestLastBlock(t *testing.T) {
	db, _ := newCanonical(t, ethash.NewFaker(), 0, true)
	var err error

	blocks := makeBlockChain(rawdb.ReadCurrentBlockDeprecated(db), 1, ethash.NewFullFaker(), db.RwKV(), 0)
	engine := ethash.NewFaker()
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, blocks, true /* checkRoot */); err != nil {
		t.Fatalf("Failed to insert block: %v", err)
	}
	if blocks[len(blocks)-1].Hash() != rawdb.ReadHeadBlockHash(db) {
		t.Fatalf("Write/Get HeadBlockHash failed")
	}
}

// Tests that given a starting canonical chain of a given size, it can be extended
// with various length chains.
func TestExtendCanonicalHeaders(t *testing.T) { testExtendCanonical(t, false) }
func TestExtendCanonicalBlocks(t *testing.T)  { testExtendCanonical(t, true) }

func testExtendCanonical(t *testing.T, full bool) {
	length := 5

	// Make first chain starting from genesis
	db, _ := newCanonical(t, ethash.NewFaker(), length, full)

	// Define the difficulty comparator
	better := func(td1, td2 *big.Int) {
		if td2.Cmp(td1) <= 0 {
			t.Errorf("total difficulty mismatch: have %v, expected more than %v", td2, td1)
		}
	}
	// Start fork from current height
	testFork(t, db, length, 1, full, better)
	testFork(t, db, length, 2, full, better)
	testFork(t, db, length, 5, full, better)
	testFork(t, db, length, 10, full, better)
}

// Tests that given a starting canonical chain of a given size, creating shorter
// forks do not take canonical ownership.
func TestShorterForkHeaders(t *testing.T) { testShorterFork(t, false) }
func TestShorterForkBlocks(t *testing.T) {
	t.Skip("Erigon does not insert shorter forks")
	testShorterFork(t, true)
}

func testShorterFork(t *testing.T, full bool) {
	length := 10

	// Make first chain starting from genesis
	db, _ := newCanonical(t, ethash.NewFaker(), length, full)

	// Define the difficulty comparator
	worse := func(td1, td2 *big.Int) {
		if td2.Cmp(td1) >= 0 {
			t.Errorf("total difficulty mismatch: have %v, expected less than %v", td2, td1)
		}
	}
	// Sum of numbers must be less than `length` for this to be a shorter fork
	testFork(t, db, 0, 3, full, worse)
	testFork(t, db, 0, 7, full, worse)
	testFork(t, db, 1, 1, full, worse)
	testFork(t, db, 1, 7, full, worse)
	testFork(t, db, 5, 3, full, worse)
	testFork(t, db, 5, 4, full, worse)
}

// Tests that given a starting canonical chain of a given size, creating longer
// forks do take canonical ownership.
func TestLongerForkHeaders(t *testing.T) { testLongerFork(t, false) }
func TestLongerForkBlocks(t *testing.T)  { testLongerFork(t, true) }

func testLongerFork(t *testing.T, full bool) {
	length := 10

	// Make first chain starting from genesis
	db, _ := newCanonical(t, ethash.NewFaker(), length, full)

	// Define the difficulty comparator
	better := func(td1, td2 *big.Int) {
		if td2.Cmp(td1) <= 0 {
			t.Errorf("total difficulty mismatch: have %v, expected more than %v", td2, td1)
		}
	}
	// Sum of numbers must be greater than `length` for this to be a longer fork
	testFork(t, db, 5, 6, full, better)
	testFork(t, db, 5, 8, full, better)
	testFork(t, db, 1, 13, full, better)
	testFork(t, db, 1, 14, full, better)
	testFork(t, db, 0, 16, full, better)
	testFork(t, db, 0, 17, full, better)
}

// Tests that given a starting canonical chain of a given size, creating equal
// forks do take canonical ownership.
func TestEqualForkHeaders(t *testing.T) { testEqualFork(t, false) }
func TestEqualForkBlocks(t *testing.T) {
	t.Skip("Erigon does not insert equal forks")
	testEqualFork(t, true)
}

func testEqualFork(t *testing.T, full bool) {
	length := 10

	// Make first chain starting from genesis
	db, _ := newCanonical(t, ethash.NewFaker(), length, full)

	// Define the difficulty comparatorc
	equal := func(td1, td2 *big.Int) {
		if td2.Cmp(td1) != 0 {
			t.Errorf("total difficulty mismatch: have %v, want %v", td2, td1)
		}
	}
	// Sum of numbers must be equal to `length` for this to be an equal fork
	testFork(t, db, 9, 1, full, equal)
	testFork(t, db, 6, 4, full, equal)
	testFork(t, db, 5, 5, full, equal)
	testFork(t, db, 2, 8, full, equal)
	testFork(t, db, 1, 9, full, equal)
	testFork(t, db, 0, 10, full, equal)
}

// Tests that chains missing links do not get accepted by the processor.
func TestBrokenHeaderChain(t *testing.T) { testBrokenChain(t, false) }
func TestBrokenBlockChain(t *testing.T)  { testBrokenChain(t, true) }

func testBrokenChain(t *testing.T, full bool) {
	// Make chain starting from genesis
	db, _ := newCanonical(t, ethash.NewFaker(), 10, true)

	// Create a forked chain, and try to insert with a missing link
	if full {
		chain := makeBlockChain(rawdb.ReadCurrentBlockDeprecated(db), 5, ethash.NewFaker(), db.RwKV(), forkSeed)[1:]
		if err := testBlockChainImport(chain, db); err == nil {
			t.Errorf("broken block chain not reported")
		}
	} else {
		chain := makeHeaderChain(rawdb.ReadCurrentHeader(db), 5, ethash.NewFaker(), db.RwKV(), forkSeed)[1:]
		if err := testHeaderChainImport(chain, db); err == nil {
			t.Errorf("broken header chain not reported")
		}
	}
}

// Tests that reorganising a long difficult chain after a short easy one
// overwrites the canonical numbers and links in the database.
func TestReorgLongHeaders(t *testing.T) { testReorgLong(t, false) }
func TestReorgLongBlocks(t *testing.T)  { testReorgLong(t, true) }

func testReorgLong(t *testing.T, full bool) {
	testReorg(t, []int64{0, 0, -9}, []int64{0, 0, 0, -9}, 393280, full)
}

// Tests that reorganising a short difficult chain after a long easy one
// overwrites the canonical numbers and links in the database.
func TestReorgShortHeaders(t *testing.T) { testReorgShort(t, false) }
func TestReorgShortBlocks(t *testing.T)  { testReorgShort(t, true) }

func testReorgShort(t *testing.T, full bool) {
	// Create a long easy chain vs. a short heavy one. Due to difficulty adjustment
	// we need a fairly long chain of blocks with different difficulties for a short
	// one to become heavyer than a long one. The 96 is an empirical value.
	easy := make([]int64, 96)
	for i := 0; i < len(easy); i++ {
		easy[i] = 60
	}
	diff := make([]int64, len(easy)-1)
	for i := 0; i < len(diff); i++ {
		diff[i] = -9
	}
	testReorg(t, easy, diff, 12615120, full)
}

func testReorg(t *testing.T, first, second []int64, td int64, full bool) {
	// Create a pristine chain and database
	db, genesis := newCanonical(t, ethash.NewFaker(), 0, full)

	// Insert an easy and a difficult chain afterwards
	easyChain, err := core.GenerateChain(params.TestChainConfig, rawdb.ReadCurrentBlockDeprecated(db), ethash.NewFaker(), db.RwKV(), len(first), func(i int, b *core.BlockGen) {
		b.OffsetTime(first[i])
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	diffChain, err := core.GenerateChain(params.TestChainConfig, rawdb.ReadCurrentBlockDeprecated(db), ethash.NewFaker(), db.RwKV(), len(second), func(i int, b *core.BlockGen) {
		b.OffsetTime(second[i])
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	if full {
		if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, ethash.NewFaker(), easyChain.Blocks, true /* checkRoot */); err != nil {
			t.Fatalf("failed to insert easy chain: %v", err)
		}
		if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, ethash.NewFaker(), diffChain.Blocks, true /* checkRoot */); err != nil {
			t.Fatalf("failed to insert difficult chain: %v", err)
		}
	} else {
		if _, _, _, err = stagedsync.InsertHeadersInStages(db, params.TestChainConfig, ethash.NewFaker(), easyChain.Headers); err != nil {
			t.Fatalf("failed to insert easy chain: %v", err)
		}
		if _, _, _, err = stagedsync.InsertHeadersInStages(db, params.TestChainConfig, ethash.NewFaker(), diffChain.Headers); err != nil {
			t.Fatalf("failed to insert difficult chain: %v", err)
		}
	}
	// Check that the chain is valid number and link wise
	if full {
		prev := rawdb.ReadCurrentBlockDeprecated(db)
		block, err := rawdb.ReadBlockByNumberDeprecated(db, rawdb.ReadCurrentHeader(db).Number.Uint64()-1)
		if err != nil {
			t.Fatal(err)
		}
		for block.NumberU64() != 0 {
			if prev.ParentHash() != block.Hash() {
				t.Errorf("parent block hash mismatch: have %x, want %x", prev.ParentHash(), block.Hash())
			}
			prev = block
			block, err = rawdb.ReadBlockByNumberDeprecated(db, block.NumberU64()-1)
			if err != nil {
				t.Fatal(err)
			}
		}
	} else {
		prev := rawdb.ReadCurrentHeader(db)
		for header := rawdb.ReadHeaderByNumber(db, rawdb.ReadCurrentHeader(db).Number.Uint64()-1); header != nil && header.Number.Uint64() != 0; {
			if prev.ParentHash != header.Hash() {
				t.Errorf("parent header hash mismatch: have %x, want %x", prev.ParentHash, header.Hash())
			}
			prev, header = header, rawdb.ReadHeaderByNumber(db, header.Number.Uint64()-1)

		}
	}
	// Make sure the chain total difficulty is the correct one
	want := new(big.Int).Add(genesis.Difficulty(), big.NewInt(td))
	if full {
		have, err := rawdb.ReadTdByHash(db, rawdb.ReadCurrentHeader(db).Hash())
		if err != nil {
			t.Fatal(err)
		}
		if have.Cmp(want) != 0 {
			t.Errorf("total difficulty mismatch: have %v, want %v", have, want)
		}
	} else {
		have, err := rawdb.ReadTdByHash(db, rawdb.ReadCurrentHeader(db).Hash())
		if err != nil {
			t.Fatal(err)
		}
		if have.Cmp(want) != 0 {
			t.Errorf("total difficulty mismatch: have %v, want %v", have, want)
		}
	}
}

// Tests that the insertion functions detect banned hashes.
func TestBadHeaderHashes(t *testing.T) { testBadHashes(t, false) }
func TestBadBlockHashes(t *testing.T)  { testBadHashes(t, true) }

func testBadHashes(t *testing.T, full bool) {

	t.Skip("to support this error in Erigon")
	// Create a pristine chain and database
	db, _ := newCanonical(t, ethash.NewFaker(), 0, full)
	var err error
	// Create a chain, ban a hash and try to import
	if full {
		blocks := makeBlockChain(rawdb.ReadCurrentBlockDeprecated(db), 3, ethash.NewFaker(), db.RwKV(), 10)

		core.BadHashes[blocks[2].Header().Hash()] = true
		defer func() { delete(core.BadHashes, blocks[2].Header().Hash()) }()

		_, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.AllEthashProtocolChanges, &vm.Config{}, ethash.NewFaker(), blocks, true /* checkRoot */)
	} else {
		headers := makeHeaderChain(rawdb.ReadCurrentHeader(db), 3, ethash.NewFaker(), db.RwKV(), 10)

		core.BadHashes[headers[2].Hash()] = true
		defer func() { delete(core.BadHashes, headers[2].Hash()) }()
		_, _, _, err = stagedsync.InsertHeadersInStages(db, params.AllEthashProtocolChanges, ethash.NewFaker(), headers)
	}
	if !errors.Is(err, core.ErrBlacklistedHash) {
		t.Errorf("error mismatch: have: %v, want: %v", err, core.ErrBlacklistedHash)
	}
}

// Tests that chain reorganisations handle transaction removals and reinsertions.
func TestChainTxReorgs(t *testing.T) {
	db, db2 := ethdb.NewTestDB(t), ethdb.NewTestDB(t)
	var (
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key2, _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		key3, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		addr2   = crypto.PubkeyToAddress(key2.PublicKey)
		addr3   = crypto.PubkeyToAddress(key3.PublicKey)
		gspec   = &core.Genesis{
			Config:   params.TestChainConfig,
			GasLimit: 3141592,
			Alloc: core.GenesisAlloc{
				addr1: {Balance: big.NewInt(1000000)},
				addr2: {Balance: big.NewInt(1000000)},
				addr3: {Balance: big.NewInt(1000000)},
			},
		}
		genesis = gspec.MustCommit(db)
		_       = gspec.MustCommit(db2)
		signer  = types.LatestSigner(gspec.Config)
	)

	// Create two transactions shared between the chains:
	//  - postponed: transaction included at a later block in the forked chain
	//  - swapped: transaction included at the same block number in the forked chain
	postponed, _ := types.SignTx(types.NewTransaction(0, addr1, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), *signer, key1)
	swapped, _ := types.SignTx(types.NewTransaction(1, addr1, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), *signer, key1)

	// Create two transactions that will be dropped by the forked chain:
	//  - pastDrop: transaction dropped retroactively from a past block
	//  - freshDrop: transaction dropped exactly at the block where the reorg is detected
	var pastDrop, freshDrop types.Transaction

	// Create three transactions that will be added in the forked chain:
	//  - pastAdd:   transaction added before the reorganization is detected
	//  - freshAdd:  transaction added at the exact block the reorg is detected
	//  - futureAdd: transaction added after the reorg has already finished
	var pastAdd, freshAdd, futureAdd types.Transaction

	chain, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db.RwKV(), 3, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			pastDrop, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr2, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), *signer, key2)

			gen.AddTx(pastDrop)  // This transaction will be dropped in the fork from below the split point
			gen.AddTx(postponed) // This transaction will be postponed till block #3 in the fork

		case 2:
			freshDrop, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr2, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), *signer, key2)

			gen.AddTx(freshDrop) // This transaction will be dropped in the fork from exactly at the split point
			gen.AddTx(swapped)   // This transaction will be swapped out at the exact height

			gen.OffsetTime(9) // Lower the block difficulty to simulate a weaker chain
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	// Import the chain. This runs all block validation rules.
	if _, err1 := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks, true /* checkRoot */); err1 != nil {
		t.Fatalf("failed to insert original chain: %v", err1)
	}

	// overwrite the old chain
	chain, err = core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db2.RwKV(), 5, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			pastAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), *signer, key3)
			gen.AddTx(pastAdd) // This transaction needs to be injected during reorg

		case 2:
			gen.AddTx(postponed) // This transaction was postponed from block #1 in the original chain
			gen.AddTx(swapped)   // This transaction was swapped from the exact current spot in the original chain

			freshAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), *signer, key3)
			gen.AddTx(freshAdd) // This transaction will be added exactly at reorg time

		case 3:
			futureAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), *signer, key3)
			gen.AddTx(futureAdd) // This transaction will be added after a full reorg
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}

	// removed tx
	txs := types.Transactions{pastDrop, freshDrop}
	for i, tx := range txs {
		if txn, _, _, _ := rawdb.ReadTransactionDeprecated(db, tx.Hash()); txn != nil {
			t.Errorf("drop %d: tx %v found while shouldn't have been", i, txn)
		}
		if rcpt, _, _, _ := rawdb.ReadReceipt(db, tx.Hash()); rcpt != nil {
			t.Errorf("drop %d: receipt %v found while shouldn't have been", i, rcpt)
		}
	}
	// added tx
	txs = types.Transactions{pastAdd, freshAdd, futureAdd}
	for i, tx := range txs {
		if txn, _, _, _ := rawdb.ReadTransactionDeprecated(db, tx.Hash()); txn == nil {
			t.Errorf("add %d: expected tx to be found", i)
		}
		if rcpt, _, _, _ := rawdb.ReadReceipt(db, tx.Hash()); rcpt == nil {
			t.Errorf("add %d: expected receipt to be found", i)
		}
	}
	// shared tx
	txs = types.Transactions{postponed, swapped}
	for i, tx := range txs {
		if txn, _, _, _ := rawdb.ReadTransactionDeprecated(db, tx.Hash()); txn == nil {
			t.Errorf("share %d: expected tx to be found", i)
		}
		if rcpt, _, _, _ := rawdb.ReadReceipt(db, tx.Hash()); rcpt == nil {
			t.Errorf("share %d: expected receipt to be found", i)
		}
	}
}

// Tests if the canonical block can be fetched from the database during chain insertion.
func TestCanonicalBlockRetrieval(t *testing.T) {
	db, genesis := newCanonical(t, ethash.NewFaker(), 0, true)

	chain, err2 := core.GenerateChain(params.AllEthashProtocolChanges, genesis, ethash.NewFaker(), db.RwKV(), 10, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
	if err2 != nil {
		t.Fatalf("generate chain: %v", err2)
	}

	ok, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.AllEthashProtocolChanges, &vm.Config{}, ethash.NewFaker(), chain.Blocks, true)
	require.NoError(t, err)
	require.True(t, ok)

	for _, block := range chain.Blocks {
		// try to retrieve a block by its canonical hash and see if the block data can be retrieved.
		ch, err := rawdb.ReadCanonicalHash(db, block.NumberU64())
		require.NoError(t, err)
		if err != nil {
			panic(err)
		}
		if ch == (common.Hash{}) {
			continue // busy wait for canonical hash to be written
		}
		if ch != block.Hash() {
			t.Errorf("unknown canonical hash, want %s, got %s", block.Hash().Hex(), ch.Hex())
			return
		}
		fb := rawdb.ReadBlockDeprecated(db, ch, block.NumberU64())
		if fb == nil {
			t.Errorf("unable to retrieve block %d for canonical hash: %s", block.NumberU64(), ch.Hex())
			return
		}
		if fb.Hash() != block.Hash() {
			t.Errorf("invalid block hash for block %d, want %s, got %s", block.NumberU64(), block.Hash().Hex(), fb.Hash().Hex())
			return
		}
	}
}

func TestEIP155Transition(t *testing.T) {
	// Configure and generate a sample block chain
	db := ethdb.NewTestDB(t)

	var (
		key, _     = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address    = crypto.PubkeyToAddress(key.PublicKey)
		funds      = big.NewInt(1000000000)
		deleteAddr = common.Address{1}
		gspec      = &core.Genesis{
			Config: &params.ChainConfig{ChainID: big.NewInt(1), EIP150Block: big.NewInt(0), EIP155Block: big.NewInt(2), HomesteadBlock: new(big.Int)},
			Alloc:  core.GenesisAlloc{address: {Balance: funds}, deleteAddr: {Balance: new(big.Int)}},
		}
		genesis = gspec.MustCommit(db)
	)

	chain, chainErr := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db.RwKV(), 4, func(i int, block *core.BlockGen) {
		var (
			tx      types.Transaction
			err     error
			basicTx = func(signer types.Signer) (types.Transaction, error) {
				return types.SignTx(types.NewTransaction(block.TxNonce(address), common.Address{}, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
			}
		)
		switch i {
		case 0:
			tx, err = basicTx(*types.LatestSignerForChainID(nil))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 2:
			tx, err = basicTx(*types.LatestSignerForChainID(nil))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)

			tx, err = basicTx(*types.LatestSigner(gspec.Config))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 3:
			tx, err = basicTx(*types.LatestSignerForChainID(nil))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)

			tx, err = basicTx(*types.LatestSigner(gspec.Config))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
	}, false /* intermediateHashes */)
	if chainErr != nil {
		t.Fatalf("generate chain: %v", chainErr)
	}

	if _, chainErr = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks, true /* checkRoot */); chainErr != nil {
		t.Fatal(chainErr)
	}
	block, _ := rawdb.ReadBlockByNumberDeprecated(db, 1)
	if block.Transactions()[0].Protected() {
		t.Error("Expected block[0].txs[0] to not be replay protected")
	}

	block, _ = rawdb.ReadBlockByNumberDeprecated(db, 3)
	if block.Transactions()[0].Protected() {
		t.Error("Expected block[3].txs[0] to not be replay protected")
	}
	if !block.Transactions()[1].Protected() {
		t.Error("Expected block[3].txs[1] to be replay protected")
	}
	if _, chainErr = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks[4:], true /* checkRoot */); chainErr != nil {
		t.Fatal(chainErr)
	}

	// generate an invalid chain id transaction
	config := &params.ChainConfig{ChainID: big.NewInt(2), EIP150Block: big.NewInt(0), EIP155Block: big.NewInt(2), HomesteadBlock: new(big.Int)}
	chain, chainErr = core.GenerateChain(config, chain.TopBlock, ethash.NewFaker(), db.RwKV(), 4, func(i int, block *core.BlockGen) {
		var (
			basicTx = func(signer types.Signer) (types.Transaction, error) {
				return types.SignTx(types.NewTransaction(block.TxNonce(address), common.Address{}, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
			}
		)
		if i == 0 {
			tx, txErr := basicTx(*types.LatestSigner(config))
			if txErr != nil {
				t.Fatal(txErr)
			}
			block.AddTx(tx)
		}
	}, false /* intemediateHashes */)
	if chainErr != nil {
		t.Fatalf("generate blocks: %v", chainErr)
	}
	if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks, true /* checkRoot */); !errors.Is(err, types.ErrInvalidChainId) {
		t.Errorf("expected error: %v, got %v", types.ErrInvalidChainId, err)
	}
}

func TestModes(t *testing.T) {
	// run test on all combination of flags
	runWithModesPermuations(
		t,
		doModesTest,
	)
}

func doModesTest(t *testing.T, history, preimages, receipts, txlookup bool) error {
	fmt.Printf("h=%v, p=%v, r=%v, t=%v\n", history, preimages, receipts, txlookup)
	// Configure and generate a sample block chain
	db := ethdb.NewTestDB(t)
	defer db.Close()
	var (
		key, _     = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address    = crypto.PubkeyToAddress(key.PublicKey)
		funds      = big.NewInt(1000000000)
		deleteAddr = common.Address{1}
		gspec      = &core.Genesis{
			Config: &params.ChainConfig{ChainID: big.NewInt(1), EIP150Block: big.NewInt(0), EIP155Block: big.NewInt(2), HomesteadBlock: new(big.Int)},
			Alloc:  core.GenesisAlloc{address: {Balance: funds}, deleteAddr: {Balance: new(big.Int)}},
		}
		genesis, _, _ = gspec.Commit(db, history)
	)

	chain, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db.RwKV(), 4, func(i int, block *core.BlockGen) {
		var (
			tx      types.Transaction
			err     error
			basicTx = func(signer types.Signer) (types.Transaction, error) {
				return types.SignTx(types.NewTransaction(block.TxNonce(address), common.Address{}, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
			}
		)
		switch i {
		case 0:
			tx, err = basicTx(*types.LatestSignerForChainID(nil))
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 2:
			tx, err = basicTx(*types.LatestSignerForChainID(nil))
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)

			tx, err = basicTx(*types.LatestSignerForChainID(gspec.Config.ChainID))
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 3:
			tx, err = basicTx(*types.LatestSignerForChainID(nil))
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)

			tx, err = basicTx(*types.LatestSignerForChainID(gspec.Config.ChainID))
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		}
	}, false /* intemediateHashes */)
	if err != nil {
		return fmt.Errorf("generate blocks: %v", err)
	}

	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.StorageMode{History: history, Receipts: receipts, TxIndex: txlookup}, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks, true /* checkRoot */); err != nil {
		return err
	}

	for bucketName, shouldBeEmpty := range map[string]bool{
		dbutils.AccountsHistoryBucket: !history,
		dbutils.PreimagePrefix:        !preimages,
		dbutils.BlockReceiptsPrefix:   !receipts,
		dbutils.TxLookupPrefix:        !txlookup,
	} {
		numberOfEntries := 0

		err := db.Walk(bucketName, nil, 0, func(k, v []byte) (bool, error) {
			// we ignore empty account history
			//nolint:scopelint
			if bucketName == dbutils.AccountsHistoryBucket && len(v) == 0 {
				return true, nil
			}

			numberOfEntries++
			return true, nil
		})
		if err != nil {
			return err
		}

		if bucketName == dbutils.BlockReceiptsPrefix {
			// we will always have a receipt for genesis
			numberOfEntries--
		}

		if bucketName == dbutils.PreimagePrefix {
			// we will always have 2 preimages because core.GenerateChain interface does not
			// allow us to set it to ignore them
			// but if the preimages are enabled in BlockChain, we will have more than 2.
			// TODO: with a better interface to core.GenerateChain allow to check preimages
			numberOfEntries -= 2
		}

		if (shouldBeEmpty && numberOfEntries > 0) || (!shouldBeEmpty && numberOfEntries == 0) {
			return fmt.Errorf("bucket '%s' should be empty? %v (actually %d entries)", bucketName, shouldBeEmpty, numberOfEntries)
		}
	}

	return nil
}

func runWithModesPermuations(t *testing.T, testFunc func(*testing.T, bool, bool, bool, bool) error) {
	err := runPermutation(t, testFunc, 0, true, true, true, true)
	if err != nil {
		t.Errorf("error while testing stuff: %v", err)
	}
}

func runPermutation(t *testing.T, testFunc func(*testing.T, bool, bool, bool, bool) error, current int, history, preimages, receipts, txlookup bool) error {
	if current == 4 {
		return testFunc(t, history, preimages, receipts, txlookup)
	}
	if err := runPermutation(t, testFunc, current+1, history, preimages, receipts, txlookup); err != nil {
		return err
	}
	switch current {
	case 0:
		history = !history
	case 1:
		preimages = !preimages
	case 2:
		receipts = !receipts
	case 3:
		txlookup = !txlookup
	default:
		panic("unexpected current item")
	}

	return runPermutation(t, testFunc, current+1, history, preimages, receipts, txlookup)
}

func TestEIP161AccountRemoval(t *testing.T) {
	// Configure and generate a sample block chain
	db := ethdb.NewTestDB(t)
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		theAddr = common.Address{1}
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:        big.NewInt(1),
				HomesteadBlock: new(big.Int),
				EIP155Block:    new(big.Int),
				EIP150Block:    new(big.Int),
				EIP158Block:    big.NewInt(2),
			},
			Alloc: core.GenesisAlloc{address: {Balance: funds}},
		}
		genesis = gspec.MustCommit(db)
	)

	chain, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db.RwKV(), 3, func(i int, block *core.BlockGen) {
		var (
			txn    types.Transaction
			err    error
			signer = types.LatestSigner(gspec.Config)
		)
		switch i {
		case 0:
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, new(uint256.Int), 21000, new(uint256.Int), nil), *signer, key)
		case 1:
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, new(uint256.Int), 21000, new(uint256.Int), nil), *signer, key)
		case 2:
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, new(uint256.Int), 21000, new(uint256.Int), nil), *signer, key)
		}
		if err != nil {
			t.Fatal(err)
		}
		block.AddTx(txn)
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// account must exist pre eip 161
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks[0], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	if st := state.New(state.NewPlainStateReader(db)); !st.Exist(theAddr) {
		t.Error("expected account to exist")
	}

	// account needs to be deleted post eip 161
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks[1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	if st := state.New(state.NewPlainStateReader(db)); st.Exist(theAddr) {
		t.Error("account should not exist")
	}

	// account mustn't be created post eip 161
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks[2], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	if st := state.New(state.NewPlainStateReader(db)); st.Exist(theAddr) {
		t.Error("account should not exist")
	}
}

func TestDoubleAccountRemoval(t *testing.T) {
	db := ethdb.NewTestDB(t)
	var (
		signer      = types.LatestSignerForChainID(nil)
		bankKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		bankAddress = crypto.PubkeyToAddress(bankKey.PublicKey)
		bankFunds   = big.NewInt(1e9)
		contract    = hexutil.MustDecode("0x60606040526040516102eb3803806102eb8339016040526060805160600190602001505b33600060006101000a81548173ffffffffffffffffffffffffffffffffffffffff02191690830217905550806001600050908051906020019082805482825590600052602060002090601f01602090048101928215609c579182015b82811115609b578251826000505591602001919060010190607f565b5b50905060c3919060a7565b8082111560bf576000818150600090555060010160a7565b5090565b50505b50610215806100d66000396000f30060606040526000357c01000000000000000000000000000000000000000000000000000000009004806341c0e1b51461004f578063adbd84651461005c578063cfae32171461007d5761004d565b005b61005a6004506100f6565b005b610067600450610208565b6040518082815260200191505060405180910390f35b61008860045061018a565b60405180806020018281038252838181518152602001915080519060200190808383829060006004602084601f0104600302600f01f150905090810190601f1680156100e85780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141561018757600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16ff5b5b565b60206040519081016040528060008152602001506001600050805480601f016020809104026020016040519081016040528092919081815260200182805480156101f957820191906000526020600020905b8154815290600101906020018083116101dc57829003601f168201915b50505050509050610205565b90565b6000439050610212565b90560000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000d5468697320697320437972757300000000000000000000000000000000000000")
		input       = hexutil.MustDecode("0xadbd8465")
		kill        = hexutil.MustDecode("0x41c0e1b5")
		gspec       = &core.Genesis{
			Config: params.AllEthashProtocolChanges,
			Alloc:  core.GenesisAlloc{bankAddress: {Balance: bankFunds}},
		}
		genesis = gspec.MustCommit(db)
	)

	var theAddr common.Address

	chain, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db.RwKV(), 3, func(i int, block *core.BlockGen) {
		nonce := block.TxNonce(bankAddress)
		switch i {
		case 0:
			tx, err := types.SignTx(types.NewContractCreation(nonce, new(uint256.Int), 1e6, new(uint256.Int), contract), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
			theAddr = crypto.CreateAddress(bankAddress, nonce)
		case 1:
			txn, err := types.SignTx(types.NewTransaction(nonce, theAddr, new(uint256.Int), 90000, new(uint256.Int), input), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(txn)
		case 2:
			txn, err := types.SignTx(types.NewTransaction(nonce, theAddr, new(uint256.Int), 90000, new(uint256.Int), kill), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(txn)

			// sending kill messsage to an already suicided account
			txn, err = types.SignTx(types.NewTransaction(nonce+1, theAddr, new(uint256.Int), 90000, new(uint256.Int), kill), *signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(txn)
		}
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	_, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain.Blocks, true /* checkRoot */)
	assert.NoError(t, err)

	st := state.New(state.NewDbStateReader(db))
	assert.NoError(t, err)
	assert.False(t, st.Exist(theAddr), "Contract should've been removed")

	dbTx, err := db.RwKV().BeginRo(context.Background())
	if err != nil {
		t.Fatalf("read only db tx to read state: %v", err)
	}
	defer dbTx.Rollback()
	st = state.New(state.NewPlainKvState(dbTx, 0))
	assert.NoError(t, err)
	assert.False(t, st.Exist(theAddr), "Contract should not exist at block #0")

	st = state.New(state.NewPlainKvState(dbTx, 1))
	assert.NoError(t, err)
	assert.True(t, st.Exist(theAddr), "Contract should exist at block #1")

	st = state.New(state.NewPlainKvState(dbTx, 2))
	assert.NoError(t, err)
	assert.True(t, st.Exist(theAddr), "Contract should exist at block #2")
}

// This is a regression test (i.e. as weird as it is, don't delete it ever), which
// tests that under weird reorg conditions the blockchain and its internal header-
// chain return the same latest block/header.
//
// https://github.com/ethereum/go-ethereum/pull/15941
func TestBlockchainHeaderchainReorgConsistency(t *testing.T) {
	// Generate a canonical chain to act as the main dataset
	engine := ethash.NewFaker()

	db := ethdb.NewTestDB(t)
	genesis := (&core.Genesis{Config: params.TestChainConfig}).MustCommit(db)

	diskdb := ethdb.NewTestDB(t)
	(&core.Genesis{Config: params.TestChainConfig}).MustCommit(diskdb)

	chain, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 64, func(i int, b *core.BlockGen) { b.SetCoinbase(common.Address{1}) }, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Generate a bunch of fork blocks, each side forking from the canonical chain
	forks := make([]*types.Block, chain.Length)
	for i := 0; i < len(forks); i++ {
		fork, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), i+1, func(j int, b *core.BlockGen) {
			//nolint:scopelint
			if j == i {
				b.SetCoinbase(common.Address{2})
				b.OffsetTime(-2) // By reducing time, we increase difficulty of the fork, so that it can overwrite the canonical chain
			} else {
				b.SetCoinbase(common.Address{1})
			}
		}, false /* intemediateHashes */)
		if err != nil {
			t.Fatalf("generate fork %d: %v", i, err)
		}
		forks[i] = fork.TopBlock
	}
	// Import the canonical and fork chain side by side, verifying the current block
	// and current header consistency
	for i := 0; i < chain.Length; i++ {
		if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, chain.Blocks[i:i+1], true /* checkRoot */); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", i, err)
		}

		b, h := rawdb.ReadCurrentBlockDeprecated(diskdb), rawdb.ReadCurrentHeader(diskdb)
		if b.Hash() != h.Hash() {
			t.Errorf("block %d: current block/header mismatch: block #%d [%x…], header #%d [%x…]", i, b.Number(), b.Hash().Bytes()[:4], h.Number, h.Hash().Bytes()[:4])
		}
		if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, forks[i:i+1], true /* checkRoot */); err != nil {
			t.Fatalf(" fork %d: failed to insert into chain: %v", i, err)
		}
		b, h = rawdb.ReadCurrentBlockDeprecated(diskdb), rawdb.ReadCurrentHeader(diskdb)
		if b.Hash() != h.Hash() {
			t.Errorf(" fork %d: current block/header mismatch: block #%d [%x…], header #%d [%x…]", i, b.Number(), b.Hash().Bytes()[:4], h.Number, h.Hash().Bytes()[:4])
		}
	}
}

// Tests that doing large reorgs works even if the state associated with the
// forking point is not available any more.
func TestLargeReorgTrieGC(t *testing.T) {
	// Generate the original common chain segment and the two competing forks
	engine := ethash.NewFaker()

	diskdb := ethdb.NewTestDB(t)
	(&core.Genesis{Config: params.TestChainConfig}).MustCommit(diskdb)

	db := ethdb.NewTestDB(t)
	genesis := (&core.Genesis{Config: params.TestChainConfig}).MustCommit(db)

	shared, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 64, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate shared chain: %v", err)
	}
	original, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 64+2*core.TriesInMemory, func(i int, b *core.BlockGen) {
		if i < 64 {
			b.SetCoinbase(common.Address{1})
		} else {
			b.SetCoinbase(common.Address{2})
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate original chain: %v", err)
	}
	competitor, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 64+2*core.TriesInMemory+1, func(i int, b *core.BlockGen) {
		if i < 64 {
			b.SetCoinbase(common.Address{1})
		} else {
			b.SetCoinbase(common.Address{3})
			b.OffsetTime(-2)
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate competitor chain: %v", err)
	}

	// Import the shared chain and the original canonical one
	if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, shared.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert shared chain: %v", err)
	}
	if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, original.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert original chain: %v", err)
	}
	// Import the competitor chain without exceeding the canonical's TD and ensure
	// we have not processed any of the blocks (protection against malicious blocks)
	if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, competitor.Blocks[:competitor.Length-2], true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert competitor chain: %v", err)
	}
	// Import the head of the competitor chain, triggering the reorg and ensure we
	// successfully reprocess all the stashed away blocks.
	if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, competitor.Blocks[competitor.Length-2:], true /* checkRoot */); err != nil {
		t.Fatalf("failed to finalize competitor chain: %v", err)
	}
}

/*

func TestBlockchainRecovery(t *testing.T) {
	t.Skip("should be restored. skipped for Erigon. tag: reorg")
	// Configure and generate a sample block chain
	gendb := ethdb.NewMemDatabase()
	defer gendb.Close()
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{Config: params.TestChainConfig, Alloc: core.GenesisAlloc{address: {Balance: funds}}}
		genesis = gspec.MustCommit(gendb)
	)
	height := uint64(1024)
	blocks, receipts, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), gendb, int(height), nil, false)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Import the chain as a ancient-first node and ensure all pointers are updated
	frdir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("failed to create temp freezer dir: %v", err)
	}
	defer os.Remove(frdir)

	ancientDb, err := ethdb.NewDatabaseWithFreezer(ethdb.NewMemDatabase(), frdir, "")
	if err != nil {
		t.Fatalf("failed to create temp freezer db: %v", err)
	}
	defer ancientDb.Close()

	gspec.MustCommit(ancientDb)

	txCacher := core.NewTxSenderCacher(1)
	ancient, _ := core.NewBlockChain(ancientDb, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)

	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	if n, err := ancient.InsertHeaderChain(headers, 1); err != nil {
		t.Fatalf("failed to insert header %d: %v", n, err)
	}

	if n, err := ancient.InsertReceiptChain(blocks, receipts, uint64(3*len(blocks)/4)); err != nil {
		t.Fatalf("failed to insert receipt %d: %v", n, err)
	}
	ancient.Stop()

	// Destroy head fast block manually
	midBlock := blocks[len(blocks)/2]
	rawdb.WriteHeadFastBlockHash(ancientDb, midBlock.Hash())

	// Reopen broken blockchain again
	txCacher = core.NewTxSenderCacher(1)
	ancient, _ = core.NewBlockChain(ancientDb, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer ancient.Stop()
	if num := ancient.CurrentBlock().NumberU64(); num != 0 {
		t.Errorf("head block mismatch: have #%v, want #%v", num, 0)
	}
	if num := ancient.CurrentFastBlock().NumberU64(); num != midBlock.NumberU64() {
		t.Errorf("head fast-block mismatch: have #%v, want #%v", num, midBlock.NumberU64())
	}
	if num := ancient.CurrentHeader().Number.Uint64(); num != midBlock.NumberU64() {
		t.Errorf("head header mismatch: have #%v, want #%v", num, midBlock.NumberU64())
	}
}

func TestIncompleteAncientReceiptChainInsertion(t *testing.T) {
	t.Skip("should be restored. skipped for Erigon. tag: fast-sync")
	// Configure and generate a sample block chain
	gendb := ethdb.NewMemDatabase()
	defer gendb.Close()

	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{Config: params.TestChainConfig, Alloc: core.GenesisAlloc{address: {Balance: funds}}}
		genesis = gspec.MustCommit(gendb)
	)
	height := uint64(1024)
	blocks, receipts, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), gendb, int(height), nil, false)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Import the chain as a ancient-first node and ensure all pointers are updated
	frdir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("failed to create temp freezer dir: %v", err)
	}
	defer os.Remove(frdir)
	ancientDb, err := ethdb.NewDatabaseWithFreezer(ethdb.NewMemDatabase(), frdir, "")
	if err != nil {
		t.Fatalf("failed to create temp freezer db: %v", err)
	}
	defer ancientDb.Close()
	gspec.MustCommit(ancientDb)
	txCacher := core.NewTxSenderCacher(1)
	ancient, _ := core.NewBlockChain(ancientDb, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer ancient.Stop()

	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	if n, err := ancient.InsertHeaderChain(headers, 1); err != nil {
		t.Fatalf("failed to insert header %d: %v", n, err)
	}
	// Abort ancient receipt chain insertion deliberately
	ancient.TerminateInsert = func(hash common.Hash, number uint64) bool {
		return number == blocks[len(blocks)/2].NumberU64()
	}
	previousFastBlock := ancient.CurrentFastBlock()
	if n, err := ancient.InsertReceiptChain(blocks, receipts, uint64(3*len(blocks)/4)); err == nil {
		t.Fatalf("failed to insert receipt %d: %v", n, err)
	}
	if ancient.CurrentFastBlock().NumberU64() != previousFastBlock.NumberU64() {
		t.Fatalf("failed to rollback ancient data, want %d, have %d", previousFastBlock.NumberU64(), ancient.CurrentFastBlock().NumberU64())
	}
	ancient.TerminateInsert = nil
	if n, err := ancient.InsertReceiptChain(blocks, receipts, uint64(3*len(blocks)/4)); err != nil {
		t.Fatalf("failed to insert receipt %d: %v", n, err)
	}
	if ancient.CurrentFastBlock().NumberU64() != blocks[len(blocks)-1].NumberU64() {
		t.Fatalf("failed to insert ancient recept chain after rollback")
	}
}
*/

// Tests that importing a very large side fork, which is larger than the canon chain,
// but where the difficulty per block is kept low: this means that it will not
// overtake the 'canon' chain until after it's passed canon by about 200 blocks.
//
// Details at:
//  - https://github.com/ethereum/go-ethereum/issues/18977
//  - https://github.com/ethereum/go-ethereum/pull/18988
func TestLowDiffLongChain(t *testing.T) {
	// Generate a canonical chain to act as the main dataset
	engine := ethash.NewFaker()
	db := ethdb.NewTestDB(t)
	genesis := new(core.Genesis).MustCommit(db)

	// We must use a pretty long chain to ensure that the fork doesn't overtake us
	// until after at least 128 blocks post tip
	chain, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 6*core.TriesInMemory, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		b.OffsetTime(-9)
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Generate fork chain, starting from an early block
	fork, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 11+8*core.TriesInMemory, func(i int, b *core.BlockGen) {
		if i < 11 {
			b.SetCoinbase(common.Address{1})
			b.OffsetTime(-9)
		} else {
			b.SetCoinbase(common.Address{2})
		}
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate fork: %v", err)
	}

	// Import the canonical chain
	diskDB := ethdb.NewTestDB(t)
	new(core.Genesis).MustCommit(diskDB)

	if _, err := stagedsync.InsertBlocksInStages(diskDB, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, chain.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	// And now import the fork
	if _, err := stagedsync.InsertBlocksInStages(diskDB, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, fork.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	head := rawdb.ReadCurrentBlockDeprecated(diskDB)
	if got := fork.TopBlock.Hash(); got != head.Hash() {
		t.Fatalf("head wrong, expected %x got %x", head.Hash(), got)
	}
	// Sanity check that all the canonical numbers are present
	header := rawdb.ReadCurrentHeader(diskDB)
	for number := head.NumberU64(); number > 0; number-- {
		if hash := rawdb.ReadHeaderByNumber(diskDB, number).Hash(); hash != header.Hash() {
			t.Fatalf("header %d: canonical hash mismatch: have %x, want %x", number, hash, header.Hash())
		}

		header = rawdb.ReadHeader(diskDB, header.ParentHash, number-1)
	}
}

// TestDeleteCreateRevert tests a weird state transition corner case that we hit
// while changing the internals of statedb. The workflow is that a contract is
// self destructed, then in a followup transaction (but same block) it's created
// again and the transaction reverted.
//
// The original statedb implementation flushed dirty objects to the tries after
// each transaction, so this works ok. The rework accumulated writes in memory
// first, but the journal wiped the entire state object on create-revert.
func TestDeleteCreateRevert(t *testing.T) {
	db := ethdb.NewTestDB(t)
	var (
		aa = common.HexToAddress("0x000000000000000000000000000000000000aaaa")
		bb = common.HexToAddress("0x000000000000000000000000000000000000bbbb")
		// Generate a canonical chain to act as the main dataset
		engine = ethash.NewFaker()

		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: params.TestChainConfig,
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
				// The address 0xAAAAA selfdestructs if called
				aa: {
					// Code needs to just selfdestruct
					Code:    []byte{byte(vm.PC), byte(vm.SELFDESTRUCT)},
					Nonce:   1,
					Balance: big.NewInt(0),
				},
				// The address 0xBBBB send 1 wei to 0xAAAA, then reverts
				bb: {
					Code: []byte{
						byte(vm.PC),          // [0]
						byte(vm.DUP1),        // [0,0]
						byte(vm.DUP1),        // [0,0,0]
						byte(vm.DUP1),        // [0,0,0,0]
						byte(vm.PUSH1), 0x01, // [0,0,0,0,1] (value)
						byte(vm.PUSH2), 0xaa, 0xaa, // [0,0,0,0,1, 0xaaaa]
						byte(vm.GAS),
						byte(vm.CALL),
						byte(vm.REVERT),
					},
					Balance: big.NewInt(1),
				},
			},
		}
		genesis = gspec.MustCommit(db)
	)

	chain, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to AAAA
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		// One transaction to BBBB
		tx, _ = types.SignTx(types.NewTransaction(1, bb,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Import the canonical chain
	diskdb := ethdb.NewTestDB(t)
	gspec.MustCommit(diskdb)

	if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, chain.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}
}

// TestDeleteRecreateSlots tests a state-transition that contains both deletion
// and recreation of contract state.
// Contract A exists, has slots 1 and 2 set
// Tx 1: Selfdestruct A
// Tx 2: Re-create A, set slots 3 and 4
// Expected outcome is that _all_ slots are cleared from A, due to the selfdestruct,
// and then the new slots exist
func TestDeleteRecreateSlots(t *testing.T) {
	db := ethdb.NewTestDB(t)

	var (
		// Generate a canonical chain to act as the main dataset
		engine = ethash.NewFaker()
		// A sender who makes transactions, has some funds
		key, _    = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address   = crypto.PubkeyToAddress(key.PublicKey)
		funds     = big.NewInt(1000000000)
		bb        = common.HexToAddress("0x000000000000000000000000000000000000bbbb")
		aaStorage = make(map[common.Hash]common.Hash)          // Initial storage in AA
		aaCode    = []byte{byte(vm.PC), byte(vm.SELFDESTRUCT)} // Code for AA (simple selfdestruct)
	)
	// Populate two slots
	aaStorage[common.HexToHash("01")] = common.HexToHash("01")
	aaStorage[common.HexToHash("02")] = common.HexToHash("02")

	// The bb-code needs to CREATE2 the aa contract. It consists of
	// both initcode and deployment code
	// initcode:
	// 1. Set slots 3=3, 4=4,
	// 2. Return aaCode

	initCode := []byte{
		byte(vm.PUSH1), 0x3, // value
		byte(vm.PUSH1), 0x3, // location
		byte(vm.SSTORE),     // Set slot[3] = 3
		byte(vm.PUSH1), 0x4, // value
		byte(vm.PUSH1), 0x4, // location
		byte(vm.SSTORE), // Set slot[4] = 4
		// Slots are set, now return the code
		byte(vm.PUSH2), byte(vm.PC), byte(vm.SELFDESTRUCT), // Push code on stack
		byte(vm.PUSH1), 0x0, // memory start on stack
		byte(vm.MSTORE),
		// Code is now in memory.
		byte(vm.PUSH1), 0x2, // size
		byte(vm.PUSH1), byte(32 - 2), // offset
		byte(vm.RETURN),
	}
	if l := len(initCode); l > 32 {
		t.Fatalf("init code is too long for a pushx, need a more elaborate deployer")
	}
	bbCode := []byte{
		// Push initcode onto stack
		byte(vm.PUSH1) + byte(len(initCode)-1)}
	bbCode = append(bbCode, initCode...)
	bbCode = append(bbCode, []byte{
		byte(vm.PUSH1), 0x0, // memory start on stack
		byte(vm.MSTORE),
		byte(vm.PUSH1), 0x00, // salt
		byte(vm.PUSH1), byte(len(initCode)), // size
		byte(vm.PUSH1), byte(32 - len(initCode)), // offset
		byte(vm.PUSH1), 0x00, // endowment
		byte(vm.CREATE2),
	}...)

	initHash := crypto.Keccak256Hash(initCode)
	aa := crypto.CreateAddress2(bb, [32]byte{}, initHash[:])
	t.Logf("Destination address: %x\n", aa)

	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc: core.GenesisAlloc{
			address: {Balance: funds},
			// The address 0xAAAAA selfdestructs if called
			aa: {
				// Code needs to just selfdestruct
				Code:    aaCode,
				Nonce:   1,
				Balance: big.NewInt(0),
				Storage: aaStorage,
			},
			// The contract BB recreates AA
			bb: {
				Code:    bbCode,
				Balance: big.NewInt(1),
			},
		},
	}
	genesis := gspec.MustCommit(db)
	chain, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to AA, to kill it
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		// One transaction to BB, to recreate AA
		tx, _ = types.SignTx(types.NewTransaction(1, bb,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, chain.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}
	statedb := state.New(state.NewDbStateReader(db))

	// If all is correct, then slot 1 and 2 are zero
	key1 := common.HexToHash("01")
	var got uint256.Int
	statedb.GetState(aa, &key1, &got)
	if !got.IsZero() {
		t.Errorf("got %d exp %d", got.Uint64(), 0)
	}
	key2 := common.HexToHash("02")
	statedb.GetState(aa, &key2, &got)
	if !got.IsZero() {
		t.Errorf("got %d exp %d", got.Uint64(), 0)
	}
	// Also, 3 and 4 should be set
	key3 := common.HexToHash("03")
	statedb.GetState(aa, &key3, &got)
	if got.Uint64() != 3 {
		t.Errorf("got %d exp %d", got.Uint64(), 3)
	}
	key4 := common.HexToHash("04")
	statedb.GetState(aa, &key4, &got)
	if got.Uint64() != 4 {
		t.Errorf("got %d exp %d", got.Uint64(), 4)
	}
}

// TestDeleteRecreateAccount tests a state-transition that contains deletion of a
// contract with storage, and a recreate of the same contract via a
// regular value-transfer
// Expected outcome is that _all_ slots are cleared from A
func TestDeleteRecreateAccount(t *testing.T) {
	db := ethdb.NewTestDB(t)
	var (
		// Generate a canonical chain to act as the main dataset
		engine = ethash.NewFaker()
		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)

		aa        = common.HexToAddress("0x7217d81b76bdd8707601e959454e3d776aee5f43")
		aaStorage = make(map[common.Hash]common.Hash)          // Initial storage in AA
		aaCode    = []byte{byte(vm.PC), byte(vm.SELFDESTRUCT)} // Code for AA (simple selfdestruct)
	)
	// Populate two slots
	aaStorage[common.HexToHash("01")] = common.HexToHash("01")
	aaStorage[common.HexToHash("02")] = common.HexToHash("02")

	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc: core.GenesisAlloc{
			address: {Balance: funds},
			// The address 0xAAAAA selfdestructs if called
			aa: {
				// Code needs to just selfdestruct
				Code:    aaCode,
				Nonce:   1,
				Balance: big.NewInt(0),
				Storage: aaStorage,
			},
		},
	}
	genesis := gspec.MustCommit(db)

	chain, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to AA, to kill it
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		// One transaction to AA, to recreate it (but without storage
		tx, _ = types.SignTx(types.NewTransaction(1, aa,
			u256.Num1, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, chain.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}
	statedb := state.New(state.NewDbStateReader(db))

	// If all is correct, then both slots are zero
	key1 := common.HexToHash("01")
	var got uint256.Int
	statedb.GetState(aa, &key1, &got)
	if !got.IsZero() {
		t.Errorf("got %x exp %x", got, 0)
	}
	key2 := common.HexToHash("02")
	statedb.GetState(aa, &key2, &got)
	if !got.IsZero() {
		t.Errorf("got %x exp %x", got, 0)
	}
}

// TestDeleteRecreateSlotsAcrossManyBlocks tests multiple state-transition that contains both deletion
// and recreation of contract state.
// Contract A exists, has slots 1 and 2 set
// Tx 1: Selfdestruct A
// Tx 2: Re-create A, set slots 3 and 4
// Expected outcome is that _all_ slots are cleared from A, due to the selfdestruct,
// and then the new slots exist
func TestDeleteRecreateSlotsAcrossManyBlocks(t *testing.T) {
	db := ethdb.NewTestDB(t)
	var (
		// Generate a canonical chain to act as the main dataset
		engine = ethash.NewFaker()
		// A sender who makes transactions, has some funds
		key, _    = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address   = crypto.PubkeyToAddress(key.PublicKey)
		funds     = big.NewInt(1000000000)
		bb        = common.HexToAddress("0x000000000000000000000000000000000000bbbb")
		aaStorage = make(map[common.Hash]common.Hash)          // Initial storage in AA
		aaCode    = []byte{byte(vm.PC), byte(vm.SELFDESTRUCT)} // Code for AA (simple selfdestruct)
	)
	// Populate two slots
	aaStorage[common.HexToHash("01")] = common.HexToHash("01")
	aaStorage[common.HexToHash("02")] = common.HexToHash("02")

	// The bb-code needs to CREATE2 the aa contract. It consists of
	// both initcode and deployment code
	// initcode:
	// 1. Set slots 3=blocknum+1, 4=4,
	// 2. Return aaCode

	initCode := []byte{
		byte(vm.PUSH1), 0x1, //
		byte(vm.NUMBER),     // value = number + 1
		byte(vm.ADD),        //
		byte(vm.PUSH1), 0x3, // location
		byte(vm.SSTORE),     // Set slot[3] = number + 1
		byte(vm.PUSH1), 0x4, // value
		byte(vm.PUSH1), 0x4, // location
		byte(vm.SSTORE), // Set slot[4] = 4
		// Slots are set, now return the code
		byte(vm.PUSH2), byte(vm.PC), byte(vm.SELFDESTRUCT), // Push code on stack
		byte(vm.PUSH1), 0x0, // memory start on stack
		byte(vm.MSTORE),
		// Code is now in memory.
		byte(vm.PUSH1), 0x2, // size
		byte(vm.PUSH1), byte(32 - 2), // offset
		byte(vm.RETURN),
	}
	if l := len(initCode); l > 32 {
		t.Fatalf("init code is too long for a pushx, need a more elaborate deployer")
	}
	bbCode := []byte{
		// Push initcode onto stack
		byte(vm.PUSH1) + byte(len(initCode)-1)}
	bbCode = append(bbCode, initCode...)
	bbCode = append(bbCode, []byte{
		byte(vm.PUSH1), 0x0, // memory start on stack
		byte(vm.MSTORE),
		byte(vm.PUSH1), 0x00, // salt
		byte(vm.PUSH1), byte(len(initCode)), // size
		byte(vm.PUSH1), byte(32 - len(initCode)), // offset
		byte(vm.PUSH1), 0x00, // endowment
		byte(vm.CREATE2),
	}...)

	initHash := crypto.Keccak256Hash(initCode)
	aa := crypto.CreateAddress2(bb, [32]byte{}, initHash[:])
	t.Logf("Destination address: %x\n", aa)
	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc: core.GenesisAlloc{
			address: {Balance: funds},
			// The address 0xAAAAA selfdestructs if called
			aa: {
				// Code needs to just selfdestruct
				Code:    aaCode,
				Nonce:   1,
				Balance: big.NewInt(0),
				Storage: aaStorage,
			},
			// The contract BB recreates AA
			bb: {
				Code:    bbCode,
				Balance: big.NewInt(1),
			},
		},
	}
	genesis := gspec.MustCommit(db)
	var nonce uint64

	type expectation struct {
		exist    bool
		blocknum int
		values   map[int]int
	}
	var current = &expectation{
		exist:    true, // exists in genesis
		blocknum: 0,
		values:   map[int]int{1: 1, 2: 2},
	}
	var expectations []*expectation
	var newDestruct = func(e *expectation) types.Transaction {
		tx, _ := types.SignTx(types.NewTransaction(nonce, aa,
			u256.Num0, 50000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		nonce++
		if e.exist {
			e.exist = false
			e.values = nil
		}
		//t.Logf("block %d; adding destruct\n", e.blocknum)
		return tx
	}
	var newResurrect = func(e *expectation) types.Transaction {
		tx, _ := types.SignTx(types.NewTransaction(nonce, bb,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		nonce++
		if !e.exist {
			e.exist = true
			e.values = map[int]int{3: e.blocknum + 1, 4: 4}
		}
		//t.Logf("block %d; adding resurrect\n", e.blocknum)
		return tx
	}

	chain, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 150, func(i int, b *core.BlockGen) {
		var exp = new(expectation)
		exp.blocknum = i + 1
		exp.values = make(map[int]int)
		for k, v := range current.values {
			exp.values[k] = v
		}
		exp.exist = current.exist

		b.SetCoinbase(common.Address{1})
		if i%2 == 0 {
			b.AddTx(newDestruct(exp))
		}
		if i%3 == 0 {
			b.AddTx(newResurrect(exp))
		}
		if i%5 == 0 {
			b.AddTx(newDestruct(exp))
		}
		if i%7 == 0 {
			b.AddTx(newResurrect(exp))
		}
		expectations = append(expectations, exp)
		current = exp
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	var asHash = func(num int) common.Hash {
		return common.BytesToHash([]byte{byte(num)})
	}
	for i, block := range chain.Blocks {
		blockNum := i + 1
		if _, err := stagedsync.InsertBlockInStages(db, params.TestChainConfig, &vm.Config{}, engine, block, true /* checkRoot */); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", i, err)
		}
		statedb := state.New(state.NewDbStateReader(db))
		// If all is correct, then slot 1 and 2 are zero
		key1 := common.HexToHash("01")
		var got uint256.Int
		statedb.GetState(aa, &key1, &got)
		if !got.IsZero() {
			t.Errorf("block %d, got %x exp %x", blockNum, got, 0)
		}
		key2 := common.HexToHash("02")
		statedb.GetState(aa, &key2, &got)
		if !got.IsZero() {
			t.Errorf("block %d, got %x exp %x", blockNum, got, 0)
		}
		exp := expectations[i]
		if exp.exist {
			if !statedb.Exist(aa) {
				t.Fatalf("block %d, expected %x to exist, it did not", blockNum, aa)
			}
			for slot, val := range exp.values {
				key := asHash(slot)
				var gotValue uint256.Int
				statedb.GetState(aa, &key, &gotValue)
				if gotValue.Uint64() != uint64(val) {
					t.Fatalf("block %d, slot %d, got %x exp %x", blockNum, slot, gotValue, val)
				}
			}
		} else {
			if statedb.Exist(aa) {
				t.Fatalf("block %d, expected %x to not exist, it did", blockNum, aa)
			}
		}
	}
}

// TestInitThenFailCreateContract tests a pretty notorious case that happened
// on mainnet over blocks 7338108, 7338110 and 7338115.
// - Block 7338108: address e771789f5cccac282f23bb7add5690e1f6ca467c is initiated
//   with 0.001 ether (thus created but no code)
// - Block 7338110: a CREATE2 is attempted. The CREATE2 would deploy code on
//   the same address e771789f5cccac282f23bb7add5690e1f6ca467c. However, the
//   deployment fails due to OOG during initcode execution
// - Block 7338115: another tx checks the balance of
//   e771789f5cccac282f23bb7add5690e1f6ca467c, and the snapshotter returned it as
//   zero.
//
// The problem being that the snapshotter maintains a destructset, and adds items
// to the destructset in case something is created "onto" an existing item.
// We need to either roll back the snapDestructs, or not place it into snapDestructs
// in the first place.
//
func TestInitThenFailCreateContract(t *testing.T) {
	db := ethdb.NewTestDB(t)
	var (
		// Generate a canonical chain to act as the main dataset
		engine = ethash.NewFaker()
		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		bb      = common.HexToAddress("0x000000000000000000000000000000000000bbbb")
	)

	// The bb-code needs to CREATE2 the aa contract. It consists of
	// both initcode and deployment code
	// initcode:
	// 1. If blocknum < 1, error out (e.g invalid opcode)
	// 2. else, return a snippet of code
	initCode := []byte{
		byte(vm.PUSH1), 0x1, // y (2)
		byte(vm.NUMBER), // x (number)
		byte(vm.GT),     // x > y?
		byte(vm.PUSH1), byte(0x8),
		byte(vm.JUMPI), // jump to label if number > 2
		byte(0xFE),     // illegal opcode
		byte(vm.JUMPDEST),
		byte(vm.PUSH1), 0x2, // size
		byte(vm.PUSH1), 0x0, // offset
		byte(vm.RETURN), // return 2 bytes of zero-code
	}
	if l := len(initCode); l > 32 {
		t.Fatalf("init code is too long for a pushx, need a more elaborate deployer")
	}
	bbCode := []byte{
		// Push initcode onto stack
		byte(vm.PUSH1) + byte(len(initCode)-1)}
	bbCode = append(bbCode, initCode...)
	bbCode = append(bbCode, []byte{
		byte(vm.PUSH1), 0x0, // memory start on stack
		byte(vm.MSTORE),
		byte(vm.PUSH1), 0x00, // salt
		byte(vm.PUSH1), byte(len(initCode)), // size
		byte(vm.PUSH1), byte(32 - len(initCode)), // offset
		byte(vm.PUSH1), 0x00, // endowment
		byte(vm.CREATE2),
	}...)

	initHash := crypto.Keccak256Hash(initCode)
	aa := crypto.CreateAddress2(bb, [32]byte{}, initHash[:])
	t.Logf("Destination address: %x\n", aa)

	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc: core.GenesisAlloc{
			address: {Balance: funds},
			// The address aa has some funds
			aa: {Balance: big.NewInt(100000)},
			// The contract BB tries to create code onto AA
			bb: {
				Code:    bbCode,
				Balance: big.NewInt(1),
			},
		},
	}
	genesis := gspec.MustCommit(db)
	nonce := uint64(0)

	chain, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 4, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to BB
		tx, _ := types.SignTx(types.NewTransaction(nonce, bb,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		nonce++
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Import the canonical chain
	diskdb := ethdb.NewTestDB(t)
	gspec.MustCommit(diskdb)
	statedb := state.New(state.NewPlainStateReader(diskdb))
	if got, exp := statedb.GetBalance(aa), uint64(100000); got.Uint64() != exp {
		t.Fatalf("Genesis err, got %v exp %v", got, exp)
	}
	// First block tries to create, but fails
	{
		block := chain.Blocks[0]
		if _, err := stagedsync.InsertBlockInStages(diskdb, params.TestChainConfig, &vm.Config{}, engine, chain.Blocks[0], true /* checkRoot */); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", block.NumberU64(), err)
		}
		statedb = state.New(state.NewPlainStateReader(diskdb))
		if got, exp := statedb.GetBalance(aa), uint64(100000); got.Uint64() != exp {
			t.Fatalf("block %d: got %v exp %v", block.NumberU64(), got, exp)
		}
	}
	// Import the rest of the blocks
	for _, block := range chain.Blocks[1:] {
		if _, err := stagedsync.InsertBlockInStages(diskdb, params.TestChainConfig, &vm.Config{}, engine, chain.Blocks[0], true /* checkRoot */); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", block.NumberU64(), err)
		}
	}
}

// TestEIP2718Transition tests that an EIP-2718 transaction will be accepted
// after the fork block has passed. This is verified by sending an EIP-2930
// access list transaction, which specifies a single slot access, and then
// checking that the gas usage of a hot SLOAD and a cold SLOAD are calculated
// correctly.
func TestEIP2718Transition(t *testing.T) {
	var (
		aa = common.HexToAddress("0x000000000000000000000000000000000000aaaa")

		// Generate a canonical chain to act as the main dataset
		engine = ethash.NewFaker()
		db     = ethdb.NewTestDB(t)

		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: params.AllEthashProtocolChanges,
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
				// The address 0xAAAA sloads 0x00 and 0x01
				aa: {
					Code: []byte{
						byte(vm.PC),
						byte(vm.PC),
						byte(vm.SLOAD),
						byte(vm.SLOAD),
					},
					Nonce:   0,
					Balance: big.NewInt(0),
				},
			},
		}
		genesis = gspec.MustCommit(db)
	)

	chain, _ := core.GenerateChain(gspec.Config, genesis, engine, db.RwKV(), 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		gasPrice, _ := uint256.FromBig(big.NewInt(1))
		chainID, _ := uint256.FromBig(gspec.Config.ChainID)

		// One transaction to 0xAAAA
		signer := types.LatestSigner(gspec.Config)
		tx, _ := types.SignNewTx(key, *signer, &types.AccessListTx{
			ChainID: chainID,
			LegacyTx: types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce: 0,
					To:    &aa,
					Gas:   30000,
				},
				GasPrice: gasPrice,
			},
			AccessList: types.AccessList{{
				Address:     aa,
				StorageKeys: []common.Hash{{0}},
			}},
		})
		b.AddTx(tx)
	}, false /*intermediateHashes*/)

	// Import the canonical chain
	diskdb := ethdb.NewTestDB(t)
	gspec.MustCommit(diskdb)

	if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, chain.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	block, _ := rawdb.ReadBlockByNumberDeprecated(diskdb, 1)

	// Expected gas is intrinsic + 2 * pc + hot load + cold load, since only one load is in the access list
	expected := params.TxGas + params.TxAccessListAddressGas + params.TxAccessListStorageKeyGas +
		vm.GasQuickStep*2 + params.WarmStorageReadCostEIP2929 + params.ColdSloadCostEIP2929
	if block.GasUsed() != expected {
		t.Fatalf("incorrect amount of gas spent: expected %d, got %d", expected, block.GasUsed())

	}
}

// TestEIP1559Transition tests the following:
//
// 1. A tranaction whose feeCap is greater than the baseFee is valid.
// 2. Gas accounting for access lists on EIP-1559 transactions is correct.
// 3. Only the transaction's tip will be received by the coinbase.
// 4. The transaction sender pays for both the tip and baseFee.
// 5. The coinbase receives only the partially realized tip when
//    feeCap - tip < baseFee.
// 6. Legacy transaction behave as expected (e.g. gasPrice = feeCap = tip).
func TestEIP1559Transition(t *testing.T) {
	t.Skip("needs fixing")
	var (
		aa = common.HexToAddress("0x000000000000000000000000000000000000aaaa")

		// Generate a canonical chain to act as the main dataset
		engine = ethash.NewFaker()
		db     = ethdb.NewTestDB(t)

		// A sender who makes transactions, has some funds
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key2, _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		addr2   = crypto.PubkeyToAddress(key2.PublicKey)
		funds   = new(uint256.Int).Mul(u256.Num1, new(uint256.Int).SetUint64(params.Ether))
		gspec   = &core.Genesis{
			Config: params.BaikalChainConfig,
			Alloc: core.GenesisAlloc{
				addr1: {Balance: funds.ToBig()},
				addr2: {Balance: funds.ToBig()},
				// The address 0xAAAA sloads 0x00 and 0x01
				aa: {
					Code: []byte{
						byte(vm.PC),
						byte(vm.PC),
						byte(vm.SLOAD),
						byte(vm.SLOAD),
					},
					Nonce:   0,
					Balance: big.NewInt(0),
				},
			},
		}
		genesis = gspec.MustCommit(db)
		signer  = types.LatestSigner(gspec.Config)
	)

	chain, err := core.GenerateChain(gspec.Config, genesis, engine, db.RwKV(), 501, func(i int, b *core.BlockGen) {
		if i == 500 {
			b.SetCoinbase(common.Address{1})
		} else {
			b.SetCoinbase(common.Address{0})
		}
		if i == 500 {
			// One transaction to 0xAAAA
			accesses := types.AccessList{types.AccessTuple{
				Address:     aa,
				StorageKeys: []common.Hash{{0}},
			}}

			var chainID uint256.Int
			chainID.SetFromBig(gspec.Config.ChainID)
			var tx types.Transaction = &types.DynamicFeeTransaction{
				ChainID: &chainID,
				CommonTx: types.CommonTx{
					Nonce: 0,
					To:    &aa,
					Gas:   30000,
					Data:  []byte{},
				},
				FeeCap:     new(uint256.Int).Mul(new(uint256.Int).SetUint64(5), new(uint256.Int).SetUint64(params.GWei)),
				Tip:        u256.Num2,
				AccessList: accesses,
			}
			tx, _ = types.SignTx(tx, *signer, key1)

			b.AddTx(tx)
		}
	}, false /* intermediate hashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	diskdb := ethdb.NewTestDB(t)
	gspec.MustCommit(diskdb)

	if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, chain.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	block := chain.Blocks[500]

	// 1+2: Ensure EIP-1559 access lists are accounted for via gas usage.
	expectedGas := params.TxGas + params.TxAccessListAddressGas + params.TxAccessListStorageKeyGas + vm.GasQuickStep*2 + params.WarmStorageReadCostEIP2929 + params.ColdSloadCostEIP2929
	if block.GasUsed() != expectedGas {
		t.Fatalf("incorrect amount of gas spent: expected %d, got %d", expectedGas, block.GasUsed())
	}

	statedb := state.New(state.NewPlainStateReader(diskdb))

	// 3: Ensure that miner received only the tx's tip.
	actual := statedb.GetBalance(block.Coinbase())
	expected := new(uint256.Int).Add(
		new(uint256.Int).SetUint64(block.GasUsed()*block.Transactions()[0].GetPrice().Uint64()),
		ethash.ConstantinopleBlockReward,
	)
	if actual.Cmp(expected) != 0 {
		t.Fatalf("miner balance incorrect: expected %d, got %d", expected, actual)
	}

	// 4: Ensure the tx sender paid for the gasUsed * (tip + block baseFee).
	actual = new(uint256.Int).Sub(funds, statedb.GetBalance(addr1))
	expected = new(uint256.Int).SetUint64(block.GasUsed() * (block.Transactions()[0].GetPrice().Uint64() + block.BaseFee().Uint64()))
	if actual.Cmp(expected) != 0 {
		t.Fatalf("sender expenditure incorrect: expected %d, got %d", expected, actual)
	}

	chain, err = core.GenerateChain(gspec.Config, block, engine, diskdb.RwKV(), 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{2})

		var tx types.Transaction = types.NewTransaction(0, aa, u256.Num0, 30000, new(uint256.Int).Mul(new(uint256.Int).SetUint64(5), new(uint256.Int).SetUint64(params.GWei)), nil)
		tx, _ = types.SignTx(tx, *signer, key2)

		b.AddTx(tx)
	}, false /* intermediate hashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	if _, err := stagedsync.InsertBlocksInStages(diskdb, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, chain.Blocks, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	block = chain.Blocks[0]
	statedb = state.New(state.NewPlainStateReader(diskdb))
	effectiveTip := block.Transactions()[0].GetPrice().Uint64() - block.BaseFee().Uint64()

	// 6+5: Ensure that miner received only the tx's effective tip.
	actual = statedb.GetBalance(block.Coinbase())
	expected = new(uint256.Int).Add(
		new(uint256.Int).SetUint64(block.GasUsed()*effectiveTip),
		ethash.ConstantinopleBlockReward,
	)
	if actual.Cmp(expected) != 0 {
		t.Fatalf("miner balance incorrect: expected %d, got %d", expected, actual)
	}

	// 4: Ensure the tx sender paid for the gasUsed * (effectiveTip + block baseFee).
	actual = new(uint256.Int).Sub(funds, statedb.GetBalance(addr2))
	expected = new(uint256.Int).SetUint64(block.GasUsed() * (effectiveTip + block.BaseFee().Uint64()))
	if actual.Cmp(expected) != 0 {
		t.Fatalf("sender balance incorrect: expected %d, got %d", expected, actual)
	}
}
