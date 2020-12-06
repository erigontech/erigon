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
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/common/u256"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// So we can deterministically seed different blockchains
var (
	canonicalSeed = 1
	forkSeed      = 2
)

// makeHeaderChain creates a deterministic chain of headers rooted at parent.
func makeHeaderChain(parent *types.Header, n int, engine consensus.Engine, db *ethdb.ObjectDatabase, seed int) []*types.Header {
	blocks := makeBlockChain(types.NewBlockWithHeader(parent), n, engine, db, seed)
	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	return headers
}

// makeBlockChain creates a deterministic chain of blocks rooted at parent.
func makeBlockChain(parent *types.Block, n int, engine consensus.Engine, db *ethdb.ObjectDatabase, seed int) []*types.Block {
	blocks, _, _ := core.GenerateChain(params.TestChainConfig, parent, engine, db, n, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{0: byte(seed), 19: byte(i)})
	}, false /* intermediateHashes */)
	return blocks
}

// newCanonical creates a chain database, and injects a deterministic canonical
// chain. Depending on the full flag, if creates either a full block chain or a
// header only chain.
func newCanonical(engine consensus.Engine, n int, full bool) (*ethdb.ObjectDatabase, *core.BlockChain, error) {
	db := ethdb.NewMemDatabase()
	genesis, _, err := new(core.Genesis).Commit(db, true /* history */)
	if err != nil {
		panic(err)
	}

	// Initialize a fresh chain with only a genesis block
	cacheConfig := &core.CacheConfig{
		TrieCleanLimit: 256,
		TrieDirtyLimit: 256,
		TrieTimeLimit:  5 * time.Minute,
		NoHistory:      false,
		Pruning:        false,
	}
	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, cacheConfig, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, txCacher)

	// Create and inject the requested chain
	if n == 0 {
		return db, blockchain, nil
	}

	if full {
		// Full block-chain requested
		blocks := makeBlockChain(genesis, n, engine, db, canonicalSeed)
		_, err = stagedsync.InsertBlocksInStages(db, params.AllEthashProtocolChanges, &vm.Config{}, engine, blocks, true /* checkRoot */)
		return db, blockchain, err
	}
	// Header-only chain requested
	headers := makeHeaderChain(genesis.Header(), n, engine, db, canonicalSeed)
	_, _, _, err = stagedsync.InsertHeadersInStages(db, params.AllEthashProtocolChanges, ethash.NewFaker(), headers)
	return db, blockchain, err
}

// Test fork of length N starting from block i
func testFork(t *testing.T, chainDb ethdb.Database, i, n int, full bool, comparator func(td1, td2 *big.Int)) {
	// Copy old chain up to #i into a new db
	db, blockchain2, err := newCanonical(ethash.NewFaker(), i, true)
	if err != nil {
		t.Fatal("could not make new canonical in testFork", err)
	}
	defer blockchain2.Stop()
	defer db.Close()

	// Assert the chains have the same header/block at #i
	var hash1, hash2 common.Hash
	if hash1, err = rawdb.ReadCanonicalHash(chainDb, uint64(i)); err != nil {
		t.Fatalf("Failed to read canonical hash: %v", err)
	}
	if hash2, err = rawdb.ReadCanonicalHash(db, uint64(i)); err != nil {
		t.Fatalf("Failed to read canonical hash 2: %v", err)
	}
	if full {
		if block1 := rawdb.ReadBlock(chainDb, hash1, uint64(i)); block1 == nil {
			t.Fatalf("Did not find canonical block")
		}
		if block2 := rawdb.ReadBlock(db, hash2, uint64(i)); block2 == nil {
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
		currentBlock, err1 := rawdb.ReadBlockByHash(chainDb, currentBlockHash)
		if err1 != nil {
			t.Fatalf("Failed to read current bock: %v", err1)
		}
		currentBlockB, err2 := rawdb.ReadBlockByHash(db, rawdb.ReadHeadBlockHash(db))
		if err2 != nil {
			t.Fatalf("Failed to read current bock: %v", err2)
		}
		blockChainB = makeBlockChain(currentBlockB, n, ethash.NewFaker(), db, forkSeed)
		tdPre, err = rawdb.ReadTd(chainDb, currentBlockHash, currentBlock.NumberU64())
		if err != nil {
			t.Fatalf("Failed to read TD for current block: %v", err)
		}
		if _, err = stagedsync.InsertBlocksInStages(chainDb, params.AllEthashProtocolChanges, &vm.Config{}, ethash.NewFaker(), blockChainB, true /* checkRoot */); err != nil {
			t.Fatalf("failed to insert forking chain: %v", err)
		}
		currentBlockHash = blockChainB[len(blockChainB)-1].Hash()
		currentBlock, err1 = rawdb.ReadBlockByHash(chainDb, currentBlockHash)
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
		headerChainB = makeHeaderChain(currentHeaderB, n, ethash.NewFaker(), db, forkSeed)
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
		if err := testBlockChainImport(blockChainB, blockchain2); err != nil {
			t.Fatalf("failed to import forked block chain: %v", err)
		}
	} else {
		if err := testHeaderChainImport(headerChainB, blockchain2); err != nil {
			t.Fatalf("failed to import forked header chain: %v", err)
		}
	}
	// Compare the total difficulties of the chains
	comparator(tdPre, tdPost)
}

// testBlockChainImport tries to process a chain of blocks, writing them into
// the database if successful.
func testBlockChainImport(chain types.Blocks, blockchain *core.BlockChain) error {
	if _, err := stagedsync.InsertBlocksInStages(blockchain.ChainDb(), blockchain.Config(), &vm.Config{}, blockchain.Engine(), chain, true /* checkRoot */); err != nil {
		return err
	}
	return nil
}

// testHeaderChainImport tries to process a chain of header, writing them into
// the database if successful.
func testHeaderChainImport(chain []*types.Header, blockchain *core.BlockChain) error {
	if _, _, _, err := stagedsync.InsertHeadersInStages(blockchain.ChainDb(), blockchain.Config(), blockchain.Engine(), chain); err != nil {
		return err
	}
	return nil
}

func TestLastBlock(t *testing.T) {
	db, blockchain, err := newCanonical(ethash.NewFaker(), 0, true)
	if err != nil {
		t.Fatalf("failed to create pristine chain: %v", err)
	}
	defer blockchain.Stop()
	defer db.Close()

	blocks := makeBlockChain(blockchain.CurrentBlock(), 1, ethash.NewFullFaker(), db, 0)
	if _, err := blockchain.InsertChain(context.Background(), blocks); err != nil {
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
	db, processor, err := newCanonical(ethash.NewFaker(), length, full)
	if err != nil {
		t.Fatalf("failed to make new canonical chain: %v", err)
	}
	defer processor.Stop()
	defer db.Close()

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
	t.Skip("turbo-geth does not insert shorter forks")
	testShorterFork(t, true)
}

func testShorterFork(t *testing.T, full bool) {
	length := 10

	// Make first chain starting from genesis
	db, processor, err := newCanonical(ethash.NewFaker(), length, full)
	if err != nil {
		t.Fatalf("failed to make new canonical chain: %v", err)
	}
	defer processor.Stop()
	defer db.Close()

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
	db, processor, err := newCanonical(ethash.NewFaker(), length, full)
	if err != nil {
		t.Fatalf("failed to make new canonical chain: %v", err)
	}
	defer processor.Stop()
	defer db.Close()

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
	t.Skip("turbo-geth does not insert equal forks")
	testEqualFork(t, true)
}

func testEqualFork(t *testing.T, full bool) {
	length := 10

	// Make first chain starting from genesis
	db, processor, err := newCanonical(ethash.NewFaker(), length, full)
	if err != nil {
		t.Fatalf("failed to make new canonical chain: %v", err)
	}
	defer processor.Stop()
	defer db.Close()

	// Define the difficulty comparator
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
	db, blockchain, err := newCanonical(ethash.NewFaker(), 10, true)
	if err != nil {
		t.Fatalf("failed to make new canonical chain: %v", err)
	}
	defer blockchain.Stop()
	defer db.Close()

	// Create a forked chain, and try to insert with a missing link
	if full {
		chain := makeBlockChain(blockchain.CurrentBlock(), 5, ethash.NewFaker(), db, forkSeed)[1:]
		if err := testBlockChainImport(chain, blockchain); err == nil {
			t.Errorf("broken block chain not reported")
		}
	} else {
		chain := makeHeaderChain(blockchain.CurrentHeader(), 5, ethash.NewFaker(), db, forkSeed)[1:]
		if err := testHeaderChainImport(chain, blockchain); err == nil {
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
	db, blockchain, err := newCanonical(ethash.NewFaker(), 0, full)
	if err != nil {
		t.Fatalf("failed to create pristine chain: %v", err)
	}
	defer blockchain.Stop()
	defer db.Close()

	// Insert an easy and a difficult chain afterwards
	easyBlocks, _, err := core.GenerateChain(params.TestChainConfig, blockchain.CurrentBlock(), ethash.NewFaker(), db, len(first), func(i int, b *core.BlockGen) {
		b.OffsetTime(first[i])
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	diffBlocks, _, err := core.GenerateChain(params.TestChainConfig, blockchain.CurrentBlock(), ethash.NewFaker(), db, len(second), func(i int, b *core.BlockGen) {
		b.OffsetTime(second[i])
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	if full {
		if _, err = stagedsync.InsertBlocksInStages(db, params.TestChainConfig, &vm.Config{}, ethash.NewFaker(), easyBlocks, true /* checkRoot */); err != nil {
			t.Fatalf("failed to insert easy chain: %v", err)
		}
		if _, err = stagedsync.InsertBlocksInStages(db, params.TestChainConfig, &vm.Config{}, ethash.NewFaker(), diffBlocks, true /* checkRoot */); err != nil {
			t.Fatalf("failed to insert difficult chain: %v", err)
		}
	} else {
		easyHeaders := make([]*types.Header, len(easyBlocks))
		for i, block := range easyBlocks {
			easyHeaders[i] = block.Header()
		}
		diffHeaders := make([]*types.Header, len(diffBlocks))
		for i, block := range diffBlocks {
			diffHeaders[i] = block.Header()
		}
		if _, _, _, err = stagedsync.InsertHeadersInStages(db, params.TestChainConfig, ethash.NewFaker(), easyHeaders); err != nil {
			t.Fatalf("failed to insert easy chain: %v", err)
		}
		if _, _, _, err = stagedsync.InsertHeadersInStages(db, params.TestChainConfig, ethash.NewFaker(), diffHeaders); err != nil {
			t.Fatalf("failed to insert difficult chain: %v", err)
		}
	}
	// Check that the chain is valid number and link wise
	if full {
		prev := blockchain.CurrentBlock()
		for block := blockchain.GetBlockByNumber(blockchain.CurrentBlock().NumberU64() - 1); block.NumberU64() != 0; prev, block = block, blockchain.GetBlockByNumber(block.NumberU64()-1) {
			if prev.ParentHash() != block.Hash() {
				t.Errorf("parent block hash mismatch: have %x, want %x", prev.ParentHash(), block.Hash())
			}
		}
	} else {
		prev := blockchain.CurrentHeader()
		for header := blockchain.GetHeaderByNumber(blockchain.CurrentHeader().Number.Uint64() - 1); header.Number.Uint64() != 0; prev, header = header, blockchain.GetHeaderByNumber(header.Number.Uint64()-1) {
			if prev.ParentHash != header.Hash() {
				t.Errorf("parent header hash mismatch: have %x, want %x", prev.ParentHash, header.Hash())
			}
		}
	}
	// Make sure the chain total difficulty is the correct one
	want := new(big.Int).Add(blockchain.Genesis().Difficulty(), big.NewInt(td))
	if full {
		if have := blockchain.GetTdByHash(blockchain.CurrentBlock().Hash()); have.Cmp(want) != 0 {
			t.Errorf("total difficulty mismatch: have %v, want %v", have, want)
		}
	} else {
		if have := blockchain.GetTdByHash(blockchain.CurrentHeader().Hash()); have.Cmp(want) != 0 {
			t.Errorf("total difficulty mismatch: have %v, want %v", have, want)
		}
	}
}

// Tests that the insertion functions detect banned hashes.
func TestBadHeaderHashes(t *testing.T) { testBadHashes(t, false) }
func TestBadBlockHashes(t *testing.T)  { testBadHashes(t, true) }

func testBadHashes(t *testing.T, full bool) {
	// Create a pristine chain and database
	db, blockchain, err := newCanonical(ethash.NewFaker(), 0, full)
	if err != nil {
		t.Fatalf("failed to create pristine chain: %v", err)
	}
	defer db.Close()
	defer blockchain.Stop()

	// Create a chain, ban a hash and try to import
	if full {
		blocks := makeBlockChain(blockchain.CurrentBlock(), 3, ethash.NewFaker(), db, 10)

		core.BadHashes[blocks[2].Header().Hash()] = true
		defer func() { delete(core.BadHashes, blocks[2].Header().Hash()) }()

		_, err = blockchain.InsertChain(context.Background(), blocks)
	} else {
		headers := makeHeaderChain(blockchain.CurrentHeader(), 3, ethash.NewFaker(), db, 10)

		core.BadHashes[headers[2].Hash()] = true
		defer func() { delete(core.BadHashes, headers[2].Hash()) }()

		_, err = blockchain.InsertHeaderChain(headers, 1)
	}
	if !errors.Is(err, core.ErrBlacklistedHash) {
		t.Errorf("error mismatch: have: %v, want: %v", err, core.ErrBlacklistedHash)
	}
}

// Tests that bad hashes are detected on boot, and the chain rolled back to a
// good state prior to the bad hash.
func TestReorgBadHeaderHashes(t *testing.T) { testReorgBadHashes(t, false) }
func TestReorgBadBlockHashes(t *testing.T)  { testReorgBadHashes(t, true) }

func testReorgBadHashes(t *testing.T, full bool) {
	t.Skip("Broken by removal BadHashes check at the creation of blockchain")
	// Create a pristine chain and database
	db, blockchain, err := newCanonical(ethash.NewFaker(), 0, full)
	if err != nil {
		t.Fatalf("failed to create pristine chain: %v", err)
	}
	defer db.Close()
	defer blockchain.Stop()

	// Create a chain, import and ban afterwards
	headers := makeHeaderChain(blockchain.CurrentHeader(), 4, ethash.NewFaker(), db, 10)
	blocks := makeBlockChain(blockchain.CurrentBlock(), 4, ethash.NewFaker(), db, 10)

	if full {
		if _, err = blockchain.InsertChain(context.Background(), blocks); err != nil {
			t.Errorf("failed to import blocks: %v", err)
		}
		if blockchain.CurrentBlock().Hash() != blocks[3].Hash() {
			t.Errorf("last block hash mismatch: have: %x, want %x", blockchain.CurrentBlock().Hash(), blocks[3].Header().Hash())
		}
		core.BadHashes[blocks[3].Header().Hash()] = true
		defer func() { delete(core.BadHashes, blocks[3].Header().Hash()) }()
	} else {
		if _, err = blockchain.InsertHeaderChain(headers, 1); err != nil {
			t.Errorf("failed to import headers: %v", err)
		}
		if blockchain.CurrentHeader().Hash() != headers[3].Hash() {
			t.Errorf("last header hash mismatch: have: %x, want %x", blockchain.CurrentHeader().Hash(), headers[3].Hash())
		}
		core.BadHashes[headers[3].Hash()] = true
		defer func() { delete(core.BadHashes, headers[3].Hash()) }()
	}
	blockchain.Stop()

	// Create a new BlockChain and check that it rolled back the state.
	cacheConfig := &core.CacheConfig{
		TrieCleanLimit: 256,
		TrieDirtyLimit: 256,
		TrieTimeLimit:  5 * time.Minute,
		NoHistory:      false,
		Pruning:        false,
	}
	txCacher := core.NewTxSenderCacher(1)
	ncm, err := core.NewBlockChain(blockchain.ChainDb(), cacheConfig, blockchain.Config(), ethash.NewFaker(), vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create new chain manager: %v", err)
	}
	defer ncm.Stop()
	if full {
		if ncm.CurrentBlock().Hash() != blocks[2].Header().Hash() {
			t.Errorf("last block hash mismatch: have: %x, want %x", ncm.CurrentBlock().Hash(), blocks[2].Header().Hash())
		}
		if blocks[2].Header().GasLimit != ncm.GasLimit() {
			t.Errorf("last  block gasLimit mismatch: have: %d, want %d", ncm.GasLimit(), blocks[2].Header().GasLimit)
		}
	} else {
		if ncm.CurrentHeader().Hash() != headers[2].Hash() {
			t.Errorf("last header hash mismatch: have: %x, want %x", ncm.CurrentHeader().Hash(), headers[2].Hash())
		}
	}
}

// Tests chain insertions in the face of one entity containing an invalid nonce.
func TestHeadersInsertNonceError(t *testing.T) { testInsertNonceError(t, false) }
func TestBlocksInsertNonceError(t *testing.T)  { testInsertNonceError(t, true) }

func testInsertNonceError(t *testing.T, full bool) {
	for i := 1; i < 25 && !t.Failed(); i++ {
		i := i
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			// Create a pristine chain and database
			db, blockchain, err := newCanonical(ethash.NewFaker(), 0, full)
			if err != nil {
				t.Fatalf("failed to create pristine chain: %v", err)
			}

			defer func(db ethdb.Database) {
				// could not close db because .ValidateHeaderChain could not wait for ethash.VerifyHeaders finish
				time.Sleep(time.Millisecond)
				db.Close()
			}(db)
			defer blockchain.Stop()

			// Create and insert a chain with a failing nonce
			var (
				failAt  int
				failRes int
				failNum uint64
			)
			if full {
				blocks := makeBlockChain(blockchain.CurrentBlock(), i, ethash.NewFaker(), db, 0)

				failAt = rand.Int() % len(blocks) // nolint:gosec
				failNum = blocks[failAt].NumberU64()

				blockchain.SetEngine(ethash.NewFakeFailer(failNum))
				failRes, err = blockchain.InsertChain(context.Background(), blocks)
			} else {
				headers := makeHeaderChain(blockchain.CurrentHeader(), i, ethash.NewFaker(), db, 0)

				failAt = rand.Int() % len(headers) // nolint:gosec
				failNum = headers[failAt].Number.Uint64()

				blockchain.SetEngine(ethash.NewFakeFailer(failNum))
				blockchain.HeaderChain().SetEngine(blockchain.Engine())
				failRes, err = blockchain.InsertHeaderChain(headers, 1)
			}
			// Check that the returned error indicates the failure
			if failRes != failAt {
				t.Errorf("test %d: failure (%v) index mismatch: have %d, want %d", i, err, failRes, failAt)
			}
			// Check that all blocks after the failing block have been inserted
			for j := 0; j < i-failAt; j++ {
				if full {
					if block := blockchain.GetBlockByNumber(failNum + uint64(j)); block != nil {
						t.Errorf("test %d: invalid block in chain: %v", i, block)
					}
				} else {
					if header := blockchain.GetHeaderByNumber(failNum + uint64(j)); header != nil {
						t.Errorf("test %d: invalid header in chain: %v", i, header)
					}
				}
			}
		})
	}
}

// Tests that fast importing a block chain produces the same chain data as the
// classical full block processing.
func TestFastVsFullChains(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")

	// Configure and generate a sample block chain
	gendb := ethdb.NewMemDatabase()
	defer gendb.Close()
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: params.TestChainConfig,
			Alloc:  core.GenesisAlloc{address: {Balance: funds}},
		}
		genesis = gspec.MustCommit(gendb)
		signer  = types.NewEIP155Signer(gspec.Config.ChainID)
	)
	blocks, receipts, err1 := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), gendb, 1024, func(i int, block *core.BlockGen) {
		block.SetCoinbase(common.Address{0x00})

		// If the block number is multiple of 3, send a few bonus transactions to the miner
		if i%3 == 2 {
			for j := 0; j < i%4+1; j++ {
				tx, err := types.SignTx(types.NewTransaction(block.TxNonce(address), common.Address{0x00}, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key)
				if err != nil {
					panic(err)
				}
				block.AddTx(tx)
			}
		}
		// If the block number is a multiple of 5, add a few bonus uncles to the block
		if i%5 == 5 {
			block.AddUncle(&types.Header{ParentHash: block.PrevBlock(i - 1).Hash(), Number: big.NewInt(int64(i - 1))})
		}
	}, false /* intemediateHashes */)
	if err1 != nil {
		t.Fatalf("generate chain: %v", err1)
	}
	// Import the chain as an archive node for the comparison baseline
	archiveDb := ethdb.NewMemDatabase()
	gspec.MustCommit(archiveDb)
	cacheConfig := &core.CacheConfig{
		TrieCleanLimit: 256,
		TrieDirtyLimit: 256,
		TrieTimeLimit:  5 * time.Minute,
		NoHistory:      false,
		Pruning:        false,
	}
	txCacher := core.NewTxSenderCacher(1)
	archive, _ := core.NewBlockChain(archiveDb, cacheConfig, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer archive.Stop()

	if n, err := archive.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("failed to process block %d: %v", n, err)
	}
	// Fast import the chain as a non-archive node to test
	fastDb := ethdb.NewMemDatabase()
	defer fastDb.Close()
	gspec.MustCommit(fastDb)
	txCacherFast := core.NewTxSenderCacher(1)
	fast, _ := core.NewBlockChain(fastDb, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacherFast)
	defer fast.Stop()

	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	if n, err := fast.InsertHeaderChain(headers, 1); err != nil {
		t.Fatalf("failed to insert header %d: %v", n, err)
	}
	if n, err := fast.InsertReceiptChain(blocks, receipts, 0); err != nil {
		t.Fatalf("failed to insert receipt %d: %v", n, err)
	}
	// Freezer style fast import the chain.
	frdir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("failed to create temp freezer dir: %v", err)
	}
	defer os.Remove(frdir)
	ancientDb, err := ethdb.NewDatabaseWithFreezer(ethdb.NewMemDatabase(), frdir, "")
	if err != nil {
		t.Fatalf("failed to create temp freezer db: %v", err)
	}
	gspec.MustCommit(ancientDb)
	txCacherAncient := core.NewTxSenderCacher(1)
	ancient, _ := core.NewBlockChain(ancientDb, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacherAncient)
	defer ancient.Stop()

	if n, err := ancient.InsertHeaderChain(headers, 1); err != nil {
		t.Fatalf("failed to insert header %d: %v", n, err)
	}
	if n, err := ancient.InsertReceiptChain(blocks, receipts, uint64(len(blocks)/2)); err != nil {
		t.Fatalf("failed to insert receipt %d: %v", n, err)
	}
	// Iterate over all chain data components, and cross reference
	for i := 0; i < len(blocks); i++ {
		num, hash := blocks[i].NumberU64(), blocks[i].Hash()

		if ftd, atd := fast.GetTdByHash(hash), archive.GetTdByHash(hash); ftd.Cmp(atd) != 0 {
			t.Errorf("block #%d [%x]: td mismatch: fastdb %v, archivedb %v", num, hash, ftd, atd)
		}
		if antd, artd := ancient.GetTdByHash(hash), archive.GetTdByHash(hash); antd.Cmp(artd) != 0 {
			t.Errorf("block #%d [%x]: td mismatch: ancientdb %v, archivedb %v", num, hash, antd, artd)
		}
		if fheader, aheader := fast.GetHeaderByHash(hash), archive.GetHeaderByHash(hash); fheader.Hash() != aheader.Hash() {
			t.Errorf("block #%d [%x]: header mismatch: fastdb %v, archivedb %v", num, hash, fheader, aheader)
		}
		if anheader, arheader := ancient.GetHeaderByHash(hash), archive.GetHeaderByHash(hash); anheader.Hash() != arheader.Hash() {
			t.Errorf("block #%d [%x]: header mismatch: ancientdb %v, archivedb %v", num, hash, anheader, arheader)
		}
		if fblock, arblock, anblock := fast.GetBlockByHash(hash), archive.GetBlockByHash(hash), ancient.GetBlockByHash(hash); fblock.Hash() != arblock.Hash() || anblock.Hash() != arblock.Hash() {
			t.Errorf("block #%d [%x]: block mismatch: fastdb %v, ancientdb %v, archivedb %v", num, hash, fblock, anblock, arblock)
		} else if types.DeriveSha(fblock.Transactions()) != types.DeriveSha(arblock.Transactions()) || types.DeriveSha(anblock.Transactions()) != types.DeriveSha(arblock.Transactions()) {
			t.Errorf("block #%d [%x]: transactions mismatch: fastdb %v, ancientdb %v, archivedb %v", num, hash, fblock.Transactions(), anblock.Transactions(), arblock.Transactions())
		} else if types.CalcUncleHash(fblock.Uncles()) != types.CalcUncleHash(arblock.Uncles()) || types.CalcUncleHash(anblock.Uncles()) != types.CalcUncleHash(arblock.Uncles()) {
			t.Errorf("block #%d [%x]: uncles mismatch: fastdb %v, ancientdb %v, archivedb %v", num, hash, fblock.Uncles(), anblock, arblock.Uncles())
		}
		if freceipts, anreceipts, areceipts := rawdb.ReadReceipts(fastDb, hash, *rawdb.ReadHeaderNumber(fastDb, hash)), rawdb.ReadReceipts(ancientDb, hash, *rawdb.ReadHeaderNumber(ancientDb, hash)), rawdb.ReadReceipts(archiveDb, hash, *rawdb.ReadHeaderNumber(archiveDb, hash)); types.DeriveSha(freceipts) != types.DeriveSha(areceipts) {
			t.Errorf("block #%d [%x]: receipts mismatch: fastdb %v, ancientdb %v, archivedb %v", num, hash, freceipts, anreceipts, areceipts)
		}
	}
	// Check that the canonical chains are the same between the databases
	for i := 0; i < len(blocks)+1; i++ {
		fhash, err := rawdb.ReadCanonicalHash(fastDb, uint64(i))
		if err != nil {
			panic(err)
		}
		ahash, err := rawdb.ReadCanonicalHash(archiveDb, uint64(i))
		if err != nil {
			panic(err)
		}
		if fhash != ahash {
			t.Errorf("block #%d: canonical hash mismatch: fastdb %v, archivedb %v", i, fhash, ahash)
		}
		anhash, err := rawdb.ReadCanonicalHash(ancientDb, uint64(i))
		if err != nil {
			panic(err)
		}
		arhash, err := rawdb.ReadCanonicalHash(archiveDb, uint64(i))
		if err != nil {
			panic(err)
		}
		if anhash != arhash {
			t.Errorf("block #%d: canonical hash mismatch: ancientdb %v, archivedb %v", i, anhash, arhash)
		}
	}
}

// Tests that various import methods move the chain head pointers to the correct
// positions.
func TestLightVsFastVsFullChainHeads(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")
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
	blocks, receipts, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), gendb, int(height), nil, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	// makeDb creates a db instance for testing.
	makeDb := func() (*ethdb.ObjectDatabase, func()) {
		dir, err := ioutil.TempDir("", "")
		if err != nil {
			t.Fatalf("failed to create temp freezer dir: %v", err)
		}
		defer os.Remove(dir)
		db, err := ethdb.NewDatabaseWithFreezer(ethdb.NewMemDatabase(), dir, "")
		if err != nil {
			t.Fatalf("failed to create temp freezer db: %v", err)
		}
		gspec.MustCommit(db)
		return db, func() { os.RemoveAll(dir) }
	}
	// Configure a subchain to roll back
	remove := blocks[height/2].NumberU64()

	// Create a small assertion method to check the three heads
	assert := func(t *testing.T, kind string, chain *core.BlockChain, header uint64, fast uint64, block uint64) {
		t.Helper()

		if num := chain.CurrentBlock().NumberU64(); num != block {
			t.Errorf("%s head block mismatch: have #%v, want #%v", kind, num, block)
		}
		if num := chain.CurrentFastBlock().NumberU64(); num != fast {
			t.Errorf("%s head fast-block mismatch: have #%v, want #%v", kind, num, fast)
		}
		if num := chain.CurrentHeader().Number.Uint64(); num != header {
			t.Errorf("%s head header mismatch: have #%v, want #%v", kind, num, header)
		}
	}
	// Import the chain as an archive node and ensure all pointers are updated
	archiveDb := ethdb.NewMemDatabase()
	defer archiveDb.Close()

	gspec.MustCommit(archiveDb)

	cacheConfig := &core.CacheConfig{
		TrieCleanLimit: 256,
		TrieDirtyLimit: 256,
		TrieTimeLimit:  5 * time.Minute,
		NoHistory:      false,
	}
	txCacherArchive := core.NewTxSenderCacher(1)
	archive, _ := core.NewBlockChain(archiveDb, cacheConfig, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacherArchive)
	if n, err := archive.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("failed to process block %d: %v", n, err)
	}
	defer archive.Stop()

	assert(t, "archive", archive, height, height, height)
	archive.SetHead(remove - 1) //nolint:errcheck
	assert(t, "archive", archive, height/2, height/2, height/2)

	// Import the chain as a non-archive node and ensure all pointers are updated
	fastDb, delfn := makeDb()
	defer delfn()
	txCacherFast := core.NewTxSenderCacher(1)
	fast, _ := core.NewBlockChain(fastDb, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacherFast)
	defer fast.Stop()

	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	if n, err := fast.InsertHeaderChain(headers, 1); err != nil {
		t.Fatalf("failed to insert header %d: %v", n, err)
	}
	if n, err := fast.InsertReceiptChain(blocks, receipts, 0); err != nil {
		t.Fatalf("failed to insert receipt %d: %v", n, err)
	}
	assert(t, "fast", fast, height, height, 0)
	fast.SetHead(remove - 1) //nolint:errcheck
	assert(t, "fast", fast, height/2, height/2, 0)

	// Import the chain as a ancient-first node and ensure all pointers are updated
	ancientDb, delfn := makeDb()
	defer delfn()
	txCacherAncient := core.NewTxSenderCacher(1)
	ancient, _ := core.NewBlockChain(ancientDb, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacherAncient)
	defer ancient.Stop()

	if n, err := ancient.InsertHeaderChain(headers, 1); err != nil {
		t.Fatalf("failed to insert header %d: %v", n, err)
	}
	if n, err := ancient.InsertReceiptChain(blocks, receipts, uint64(3*len(blocks)/4)); err != nil {
		t.Fatalf("failed to insert receipt %d: %v", n, err)
	}
	assert(t, "ancient", ancient, height, height, 0)
	ancient.SetHead(remove - 1) //nolint:errcheck
	assert(t, "ancient", ancient, 0, 0, 0)

	if frozen, err := ancientDb.Ancients(); err != nil || frozen != 1 {
		t.Fatalf("failed to truncate ancient store, want %v, have %v", 1, frozen)
	}
	// Import the chain as a light node and ensure all pointers are updated
	lightDb, delfn := makeDb()
	defer delfn()
	txCacherLight := core.NewTxSenderCacher(1)
	light, _ := core.NewBlockChain(lightDb, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacherLight)
	if n, err := light.InsertHeaderChain(headers, 1); err != nil {
		t.Fatalf("failed to insert header %d: %v", n, err)
	}
	defer light.Stop()

	assert(t, "light", light, height, 0, 0)
	light.SetHead(remove - 1) //nolint:errcheck
	assert(t, "light", light, height/2, 0, 0)
}

// Tests that chain reorganisations handle transaction removals and reinsertions.
func TestChainTxReorgs(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
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
		signer  = types.NewEIP155Signer(gspec.Config.ChainID)
	)

	genesisDb := db.MemCopy()
	defer genesisDb.Close()

	// Create two transactions shared between the chains:
	//  - postponed: transaction included at a later block in the forked chain
	//  - swapped: transaction included at the same block number in the forked chain
	postponed, _ := types.SignTx(types.NewTransaction(0, addr1, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key1)
	swapped, _ := types.SignTx(types.NewTransaction(1, addr1, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key1)

	// Create two transactions that will be dropped by the forked chain:
	//  - pastDrop: transaction dropped retroactively from a past block
	//  - freshDrop: transaction dropped exactly at the block where the reorg is detected
	var pastDrop, freshDrop *types.Transaction

	// Create three transactions that will be added in the forked chain:
	//  - pastAdd:   transaction added before the reorganization is detected
	//  - freshAdd:  transaction added at the exact block the reorg is detected
	//  - futureAdd: transaction added after the reorg has already finished
	var pastAdd, freshAdd, futureAdd *types.Transaction

	cacheConfig := &core.CacheConfig{
		TrieCleanLimit: 256,
		TrieDirtyLimit: 256,
		TrieTimeLimit:  5 * time.Minute,
		NoHistory:      false,
		Pruning:        false,
	}
	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, cacheConfig, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	blockchain.EnableReceipts(true)

	chain, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), genesisDb, 3, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			pastDrop, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr2, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key2)

			gen.AddTx(pastDrop)  // This transaction will be dropped in the fork from below the split point
			gen.AddTx(postponed) // This transaction will be postponed till block #3 in the fork

		case 2:
			freshDrop, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr2, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key2)

			gen.AddTx(freshDrop) // This transaction will be dropped in the fork from exactly at the split point
			gen.AddTx(swapped)   // This transaction will be swapped out at the exact height

			gen.OffsetTime(9) // Lower the block difficulty to simulate a weaker chain
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	// Import the chain. This runs all block validation rules.
	if i, err1 := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain, true /* checkRoot */); err1 != nil {
		t.Fatalf("failed to insert original chain[%d]: %v", i, err1)
	}
	defer blockchain.Stop()

	// overwrite the old chain
	chain, _, err = core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), genesisDb, 5, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			pastAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key3)
			gen.AddTx(pastAdd) // This transaction needs to be injected during reorg

		case 2:
			gen.AddTx(postponed) // This transaction was postponed from block #1 in the original chain
			gen.AddTx(swapped)   // This transaction was swapped from the exact current spot in the original chain

			freshAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key3)
			gen.AddTx(freshAdd) // This transaction will be added exactly at reorg time

		case 3:
			futureAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key3)
			gen.AddTx(futureAdd) // This transaction will be added after a full reorg
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	if _, err := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}

	// removed tx
	txs := types.Transactions{pastDrop, freshDrop}
	for i, tx := range txs {
		if txn, _, _, _ := rawdb.ReadTransaction(db, tx.Hash()); txn != nil {
			t.Errorf("drop %d: tx %v found while shouldn't have been", i, txn)
		}
		if rcpt, _, _, _ := rawdb.ReadReceipt(db, tx.Hash()); rcpt != nil {
			t.Errorf("drop %d: receipt %v found while shouldn't have been", i, rcpt)
		}
	}
	// added tx
	txs = types.Transactions{pastAdd, freshAdd, futureAdd}
	for i, tx := range txs {
		if txn, _, _, _ := rawdb.ReadTransaction(db, tx.Hash()); txn == nil {
			t.Errorf("add %d: expected tx to be found", i)
		}
		if rcpt, _, _, _ := rawdb.ReadReceipt(db, tx.Hash()); rcpt == nil {
			t.Errorf("add %d: expected receipt to be found", i)
		}
	}
	// shared tx
	txs = types.Transactions{postponed, swapped}
	for i, tx := range txs {
		if txn, _, _, _ := rawdb.ReadTransaction(db, tx.Hash()); txn == nil {
			t.Errorf("share %d: expected tx to be found", i)
		}
		if rcpt, _, _, _ := rawdb.ReadReceipt(db, tx.Hash()); rcpt == nil {
			t.Errorf("share %d: expected receipt to be found", i)
		}
	}
}

func TestLogReorgs(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		// this code generates a log
		code    = common.Hex2Bytes("60606040525b7f24ec1d3ff24c2f6ff210738839dbc339cd45a5294d85c79361016243157aae7b60405180905060405180910390a15b600a8060416000396000f360606040526008565b00")
		gspec   = &core.Genesis{Config: params.TestChainConfig, Alloc: core.GenesisAlloc{addr1: {Balance: big.NewInt(10000000000000)}}}
		genesis = gspec.MustCommit(db)
		signer  = types.NewEIP155Signer(gspec.Config.ChainID)
	)
	genesisDB := db.MemCopy()
	defer genesisDB.Close()

	cacheConfig := &core.CacheConfig{
		TrieCleanLimit: 256,
		TrieDirtyLimit: 256,
		TrieTimeLimit:  5 * time.Minute,
		NoHistory:      false,
		Pruning:        false,
	}
	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, cacheConfig, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	blockchain.EnableReceipts(true)
	defer blockchain.Stop()

	rmLogsCh := make(chan core.RemovedLogsEvent, 10)
	blockchain.SubscribeRemovedLogsEvent(rmLogsCh)
	chain, _, err := core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), genesisDB, 2, func(i int, gen *core.BlockGen) {
		if i == 1 {
			tx, err := types.SignTx(types.NewContractCreation(gen.TxNonce(addr1), new(uint256.Int), 1000000, new(uint256.Int), code), signer, key1)
			if err != nil {
				t.Fatalf("failed to create tx: %v", err)
			}
			gen.AddTx(tx)
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	if _, err1 := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain, true /* checkRoot */); err1 != nil {
		t.Fatalf("failed to insert chain: %v", err1)
	}

	chain, _, err = core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), genesisDB, 3, func(i int, gen *core.BlockGen) {}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	done := make(chan struct{})
	go func() {
		ev := <-rmLogsCh
		if len(ev.Logs) == 0 {
			t.Error("expected logs")
		}
		close(done)
	}()
	if _, err := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}
	// In turbo-geth, RemoveLogsEvent is not working yet
	/*
		timeout := time.NewTimer(1 * time.Second)
		defer timeout.Stop()
		select {
		case <-done:
		case <-timeout.C:
			t.Fatal("Timeout. There is no RemovedLogsEvent has been sent.")
		}
	*/
}

// This EVM code generates a log when the contract is created.
var logCode = common.Hex2Bytes("60606040525b7f24ec1d3ff24c2f6ff210738839dbc339cd45a5294d85c79361016243157aae7b60405180905060405180910390a15b600a8060416000396000f360606040526008565b00")

// This test checks that log events and RemovedLogsEvent are sent
// when the chain reorganizes.
func TestLogRebirth(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	txCacher := core.NewTxSenderCacher(1)
	var (
		key1, _       = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr1         = crypto.PubkeyToAddress(key1.PublicKey)
		gspec         = &core.Genesis{Config: params.TestChainConfig, Alloc: core.GenesisAlloc{addr1: {Balance: big.NewInt(10000000000000)}}}
		genesis       = gspec.MustCommit(db)
		signer        = types.NewEIP155Signer(gspec.Config.ChainID)
		engine        = ethash.NewFaker()
		blockchain, _ = core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil, txCacher)
	)
	blockchain.EnableReceipts(true)
	defer blockchain.Stop()

	// The event channels.
	newLogCh := make(chan []*types.Log, 10)
	rmLogsCh := make(chan core.RemovedLogsEvent, 10)
	blockchain.SubscribeLogsEvent(newLogCh)
	blockchain.SubscribeRemovedLogsEvent(rmLogsCh)

	// This chain contains a single log.
	chain, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 2, func(i int, gen *core.BlockGen) {
		if i == 1 {
			tx, err := types.SignTx(types.NewContractCreation(gen.TxNonce(addr1), new(uint256.Int), 1000000, new(uint256.Int), logCode), signer, key1)
			if err != nil {
				t.Fatalf("failed to create tx: %v", err)
			}
			gen.AddTx(tx)
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	// Generate long reorg chain containing another log. Inserting the
	// chain removes one log and adds one.
	forkChain, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 2, func(i int, gen *core.BlockGen) {
		if i == 1 {
			tx, err := types.SignTx(types.NewContractCreation(gen.TxNonce(addr1), new(uint256.Int), 1000000, new(uint256.Int), logCode), signer, key1)
			if err != nil {
				t.Fatalf("failed to create tx: %v", err)
			}
			gen.AddTx(tx)
			gen.OffsetTime(-9) // higher block difficulty
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate fork chain: %v", err)
	}
	// This chain segment is rooted in the original chain, but doesn't contain any logs.
	// When inserting it, the canonical chain switches away from forkChain and re-emits
	// the log event for the old chain, as well as a RemovedLogsEvent for forkChain.
	newBlocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 3, func(i int, gen *core.BlockGen) {
		if i == 1 {
			tx, err1 := types.SignTx(types.NewContractCreation(gen.TxNonce(addr1), new(uint256.Int), 1000000, new(uint256.Int), logCode), signer, key1)
			if err1 != nil {
				t.Fatalf("failed to create tx: %v", err1)
			}
			gen.AddTx(tx)
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate new blocks: %v", err)
	}

	if _, err := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert chain: %v", err)
	}
	checkLogEvents(t, newLogCh, rmLogsCh, 1, 0)

	if _, err := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), forkChain, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}
	checkLogEvents(t, newLogCh, rmLogsCh, 1, 1)

	if _, err := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), newBlocks[2:], true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}
	checkLogEvents(t, newLogCh, rmLogsCh, 1, 1)
}

// This test is a variation of TestLogRebirth. It verifies that log events are emitted
// when a side chain containing log events overtakes the canonical chain.
func TestSideLogRebirth(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()

	var (
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		gspec   = &core.Genesis{Config: params.TestChainConfig, Alloc: core.GenesisAlloc{addr1: {Balance: big.NewInt(10000000000000)}}}
		genesis = gspec.MustCommit(db)
		signer  = types.NewEIP155Signer(gspec.Config.ChainID)
	)

	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer blockchain.Stop()

	newLogCh := make(chan []*types.Log, 10)
	rmLogsCh := make(chan core.RemovedLogsEvent, 10)
	blockchain.SubscribeLogsEvent(newLogCh)
	blockchain.SubscribeRemovedLogsEvent(rmLogsCh)

	// Generate main chain
	chain, _, err := core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db, 2, func(i int, gen *core.BlockGen) {
		if i == 1 {
			gen.OffsetTime(-9) // higher block difficulty
		}
	}, false /* intemediateHahes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	// Generate side chain with lower difficulty
	sideChain, _, err := core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db, 3, func(i int, gen *core.BlockGen) {
		if i == 1 {
			tx, err := types.SignTx(types.NewContractCreation(gen.TxNonce(addr1), new(uint256.Int), 1000000, new(uint256.Int), logCode), signer, key1)
			if err != nil {
				t.Fatalf("failed to create tx: %v", err)
			}
			gen.AddTx(tx)
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate side chain: %v", err)
	}
	if _, err := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}
	checkLogEvents(t, newLogCh, rmLogsCh, 0, 0)
	if _, err := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), sideChain[:2], true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}
	checkLogEvents(t, newLogCh, rmLogsCh, 0, 0)

	if _, err := stagedsync.InsertBlocksInStages(db, gspec.Config, &vm.Config{}, ethash.NewFaker(), sideChain[2:], true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}
	checkLogEvents(t, newLogCh, rmLogsCh, 1, 0)
}

func checkLogEvents(t *testing.T, logsCh <-chan []*types.Log, rmLogsCh <-chan core.RemovedLogsEvent, wantNew, wantRemoved int) {
	t.Helper()

	if len(logsCh) != wantNew {
		//t.Fatalf("wrong number of log events: got %d, want %d", len(logsCh), wantNew)
	}
	if len(rmLogsCh) != wantRemoved {
		//t.Fatalf("wrong number of removed log events: got %d, want %d", len(rmLogsCh), wantRemoved)
	}
	// Drain events.
	for i := 0; i < len(logsCh); i++ {
		<-logsCh
	}
	for i := 0; i < len(rmLogsCh); i++ {
		<-rmLogsCh
	}
}

func TestReorgSideEvent(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth. tag: reorg")
	db := ethdb.NewMemDatabase()
	defer db.Close()

	var (
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		gspec   = &core.Genesis{
			Config: params.TestChainConfig,
			Alloc:  core.GenesisAlloc{addr1: {Balance: big.NewInt(10000000000000)}},
		}
		genesis = gspec.MustCommit(db)
		signer  = types.NewEIP155Signer(gspec.Config.ChainID)
	)
	genesisDb := db.MemCopy()
	defer genesisDb.Close()

	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer blockchain.Stop()

	chain, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), genesisDb, 3, func(i int, gen *core.BlockGen) {}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	if _, err = blockchain.InsertChain(context.Background(), chain); err != nil {
		t.Fatalf("failed to insert chain: %v", err)
	}

	replacementBlocks, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), genesisDb, 4, func(i int, gen *core.BlockGen) {
		tx, err := types.SignTx(types.NewContractCreation(gen.TxNonce(addr1), new(uint256.Int), 1000000, new(uint256.Int), nil), signer, key1)
		if i == 2 {
			gen.OffsetTime(-9)
		}
		if err != nil {
			t.Fatalf("failed to create tx: %v", err)
		}
		gen.AddTx(tx)
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate replacement chain: %v", err)
	}
	chainSideCh := make(chan core.ChainSideEvent, 64)
	blockchain.SubscribeChainSideEvent(chainSideCh)
	if _, err := blockchain.InsertChain(context.Background(), replacementBlocks); err != nil {
		t.Fatalf("failed to insert chain: %v", err)
	}

	// first two block of the secondary chain are for a brief moment considered
	// side chains because up to that point the first one is considered the
	// heavier chain.
	expectedSideHashes := map[common.Hash]bool{
		replacementBlocks[0].Hash(): true,
		replacementBlocks[1].Hash(): true,
		chain[0].Hash():             true,
		chain[1].Hash():             true,
		chain[2].Hash():             true,
	}

	i := 0

	const timeoutDura = 10 * time.Second
	timeout := time.NewTimer(timeoutDura)
done:
	for {
		select {
		case ev := <-chainSideCh:
			block := ev.Block
			if _, ok := expectedSideHashes[block.Hash()]; !ok {
				t.Errorf("%d: didn't expect %x to be in side chain", i, block.Hash())
			}
			i++

			if i == len(expectedSideHashes) {
				timeout.Stop()

				break done
			}
			timeout.Reset(timeoutDura)

		case <-timeout.C:
			t.Fatal("Timeout. Possibly not all blocks were triggered for sideevent")
		}
	}

	// make sure no more events are fired
	select {
	case e := <-chainSideCh:
		t.Errorf("unexpected event fired: %v", e)
	case <-time.After(250 * time.Millisecond):
	}

}

// Tests if the canonical block can be fetched from the database during chain insertion.
func TestCanonicalBlockRetrieval(t *testing.T) {
	db, blockchain, err := newCanonical(ethash.NewFaker(), 0, true)
	if err != nil {
		t.Fatalf("failed to create pristine chain: %v", err)
	}
	defer db.Close()
	defer blockchain.Stop()

	chain, _, err := core.GenerateChain(blockchain.Config(), blockchain.Genesis(), ethash.NewFaker(), db, 10, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	var pend sync.WaitGroup
	pend.Add(len(chain))

	for i := range chain {
		go func(block *types.Block) {
			defer pend.Done()

			// try to retrieve a block by its canonical hash and see if the block data can be retrieved.
			for {
				ch, err := rawdb.ReadCanonicalHash(blockchain.ChainDb(), block.NumberU64())
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
				fb := rawdb.ReadBlock(blockchain.ChainDb(), ch, block.NumberU64())
				if fb == nil {
					t.Errorf("unable to retrieve block %d for canonical hash: %s", block.NumberU64(), ch.Hex())
					return
				}
				if fb.Hash() != block.Hash() {
					t.Errorf("invalid block hash for block %d, want %s, got %s", block.NumberU64(), block.Hash().Hex(), fb.Hash().Hex())
					return
				}
				return
			}
		}(chain[i])

		if _, err := blockchain.InsertChain(context.Background(), types.Blocks{chain[i]}); err != nil {
			t.Fatalf("failed to insert block %d: %v", i, err)
		}
	}
	pend.Wait()
}

func TestEIP155Transition(t *testing.T) {
	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
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
		genesis = gspec.MustCommit(db)
	)

	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer blockchain.Stop()

	blocks, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 4, func(i int, block *core.BlockGen) {
		var (
			tx      *types.Transaction
			err     error
			basicTx = func(signer types.Signer) (*types.Transaction, error) {
				return types.SignTx(types.NewTransaction(block.TxNonce(address), common.Address{}, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
			}
		)
		switch i {
		case 0:
			tx, err = basicTx(types.HomesteadSigner{})
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 2:
			tx, err = basicTx(types.HomesteadSigner{})
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)

			tx, err = basicTx(types.NewEIP155Signer(gspec.Config.ChainID))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 3:
			tx, err = basicTx(types.HomesteadSigner{})
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)

			tx, err = basicTx(types.NewEIP155Signer(gspec.Config.ChainID))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	if _, err = blockchain.InsertChain(context.Background(), blocks); err != nil {
		t.Fatal(err)
	}
	block := blockchain.GetBlockByNumber(1)
	if block.Transactions()[0].Protected() {
		t.Error("Expected block[0].txs[0] to not be replay protected")
	}

	block = blockchain.GetBlockByNumber(3)
	if block.Transactions()[0].Protected() {
		t.Error("Expected block[3].txs[0] to not be replay protected")
	}
	if !block.Transactions()[1].Protected() {
		t.Error("Expected block[3].txs[1] to be replay protected")
	}
	if _, err = blockchain.InsertChain(context.Background(), blocks[4:]); err != nil {
		t.Fatal(err)
	}

	// generate an invalid chain id transaction
	config := &params.ChainConfig{ChainID: big.NewInt(2), EIP150Block: big.NewInt(0), EIP155Block: big.NewInt(2), HomesteadBlock: new(big.Int)}
	blocks, _, err = core.GenerateChain(config, blocks[len(blocks)-1], ethash.NewFaker(), db, 4, func(i int, block *core.BlockGen) {
		var (
			tx      *types.Transaction
			err     error
			basicTx = func(signer types.Signer) (*types.Transaction, error) {
				return types.SignTx(types.NewTransaction(block.TxNonce(address), common.Address{}, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
			}
		)
		if i == 0 {
			tx, err = basicTx(types.NewEIP155Signer(big.NewInt(2)))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	_, err = blockchain.InsertChain(context.Background(), blocks)
	if err != types.ErrInvalidChainId {
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

func doModesTest(history, preimages, receipts, txlookup bool) error {
	fmt.Printf("h=%v, p=%v, r=%v, t=%v\n", history, preimages, receipts, txlookup)
	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
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

	cacheConfig := &core.CacheConfig{
		Pruning:             false,
		BlocksBeforePruning: 1024,
		TrieCleanLimit:      256,
		TrieDirtyLimit:      256,
		TrieTimeLimit:       5 * time.Minute,
		DownloadOnly:        false,
		NoHistory:           !history,
	}

	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, cacheConfig, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	blockchain.EnableReceipts(receipts)
	blockchain.EnablePreimages(preimages)
	blockchain.EnableTxLookupIndex(txlookup)
	defer blockchain.Stop()

	blocks, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 4, func(i int, block *core.BlockGen) {
		var (
			tx      *types.Transaction
			err     error
			basicTx = func(signer types.Signer) (*types.Transaction, error) {
				return types.SignTx(types.NewTransaction(block.TxNonce(address), common.Address{}, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
			}
		)
		switch i {
		case 0:
			tx, err = basicTx(types.HomesteadSigner{})
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 2:
			tx, err = basicTx(types.HomesteadSigner{})
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)

			tx, err = basicTx(types.NewEIP155Signer(gspec.Config.ChainID))
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 3:
			tx, err = basicTx(types.HomesteadSigner{})
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)

			tx, err = basicTx(types.NewEIP155Signer(gspec.Config.ChainID))
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		}
	}, false /* intemediateHashes */)
	if err != nil {
		return fmt.Errorf("generate blocks: %v", err)
	}

	if _, err := blockchain.InsertChain(context.Background(), blocks); err != nil {
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
			if bucketName == string(dbutils.AccountsHistoryBucket) && len(v) == 0 {
				return true, nil
			}

			numberOfEntries++
			return true, nil
		})
		if err != nil {
			return err
		}

		if bucketName == string(dbutils.BlockReceiptsPrefix) {
			// we will always have a receipt for genesis
			numberOfEntries--
		}

		if bucketName == string(dbutils.PreimagePrefix) {
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

func runWithModesPermuations(t *testing.T, testFunc func(bool, bool, bool, bool) error) {
	err := runPermutation(testFunc, 0, true, true, true, true)
	if err != nil {
		t.Errorf("error while testing stuff: %v", err)
	}
}

func runPermutation(testFunc func(bool, bool, bool, bool) error, current int, history, preimages, receipts, txlookup bool) error {
	if current == 4 {
		return testFunc(history, preimages, receipts, txlookup)
	}
	if err := runPermutation(testFunc, current+1, history, preimages, receipts, txlookup); err != nil {
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

	return runPermutation(testFunc, current+1, history, preimages, receipts, txlookup)
}

func TestEIP161AccountRemoval(t *testing.T) {
	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
	defer db.Close()
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
	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer blockchain.Stop()

	blocks, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 3, func(i int, block *core.BlockGen) {
		var (
			tx     *types.Transaction
			err    error
			signer = types.NewEIP155Signer(gspec.Config.ChainID)
		)
		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
		case 2:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
		}
		if err != nil {
			t.Fatal(err)
		}
		block.AddTx(tx)
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// account must exist pre eip 161
	if _, err := blockchain.InsertChain(context.Background(), types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}
	if st := state.New(state.NewDbStateReader(db)); !st.Exist(theAddr) {
		t.Error("expected account to exist")
	}

	// account needs to be deleted post eip 161
	if _, err := blockchain.InsertChain(context.Background(), types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}
	if st := state.New(state.NewDbStateReader(db)); st.Exist(theAddr) {
		t.Error("account should not exist")
	}

	// account mustn't be created post eip 161
	if _, err := blockchain.InsertChain(context.Background(), types.Blocks{blocks[2]}); err != nil {
		t.Fatal(err)
	}
	if st := state.New(state.NewDbStateReader(db)); st.Exist(theAddr) {
		t.Error("account should not exist")
	}
}

func TestDoubleAccountRemoval(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		signer      = types.HomesteadSigner{}
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

	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, nil, gspec.Config, ethash.NewFaker(), vm.Config{}, nil, txCacher)
	defer blockchain.Stop()

	var theAddr common.Address

	blocks, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 3, func(i int, block *core.BlockGen) {
		nonce := block.TxNonce(bankAddress)
		switch i {
		case 0:
			tx, err := types.SignTx(types.NewContractCreation(nonce, new(uint256.Int), 1e6, new(uint256.Int), contract), signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
			theAddr = crypto.CreateAddress(bankAddress, nonce)
		case 1:
			tx, err := types.SignTx(types.NewTransaction(nonce, theAddr, new(uint256.Int), 90000, new(uint256.Int), input), signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		case 2:
			tx, err := types.SignTx(types.NewTransaction(nonce, theAddr, new(uint256.Int), 90000, new(uint256.Int), kill), signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(tx)

			// sending kill messsage to an already suicided account
			tx, err = types.SignTx(types.NewTransaction(nonce+1, theAddr, new(uint256.Int), 90000, new(uint256.Int), kill), signer, bankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		}
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	_, err = blockchain.InsertChain(context.Background(), blocks)
	assert.NoError(t, err)

	st := state.New(state.NewDbStateReader(db))
	assert.NoError(t, err)
	assert.False(t, st.Exist(theAddr), "Contract should've been removed")

	/*
		st = state.New(state.NewDbState(db.KV(), 0))
		assert.NoError(t, err)
		assert.False(t, st.Exist(theAddr), "Contract should not exist at block #0")

		st = state.New(state.NewDbState(db.KV(), 1))
		assert.NoError(t, err)
		assert.True(t, st.Exist(theAddr), "Contract should exist at block #1")

		st = state.New(state.NewDbState(db.KV(), 2))
		assert.NoError(t, err)
		assert.True(t, st.Exist(theAddr), "Contract should exist at block #2")
	*/
}

// This is a regression test (i.e. as weird as it is, don't delete it ever), which
// tests that under weird reorg conditions the blockchain and its internal header-
// chain return the same latest block/header.
//
// https://github.com/ethereum/go-ethereum/pull/15941
func TestBlockchainHeaderchainReorgConsistency(t *testing.T) {
	// Generate a canonical chain to act as the main dataset
	engine := ethash.NewFaker()

	db := ethdb.NewMemDatabase()
	defer db.Close()
	genesis := (&core.Genesis{Config: params.TestChainConfig}).MustCommit(db)

	diskdb := ethdb.NewMemDatabase()
	defer diskdb.Close()
	(&core.Genesis{Config: params.TestChainConfig}).MustCommit(diskdb)

	cacheConfig := &core.CacheConfig{
		TrieCleanLimit: 256,
		TrieDirtyLimit: 256,
		TrieTimeLimit:  5 * time.Minute,
		NoHistory:      false,
		Pruning:        false,
	}
	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(diskdb, cacheConfig, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()

	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 64, func(i int, b *core.BlockGen) { b.SetCoinbase(common.Address{1}) }, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Generate a bunch of fork blocks, each side forking from the canonical chain
	forks := make([]*types.Block, len(blocks))
	for i := 0; i < len(forks); i++ {
		fork, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, i+1, func(j int, b *core.BlockGen) {
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
		forks[i] = fork[len(fork)-1]
	}
	// Import the canonical and fork chain side by side, verifying the current block
	// and current header consistency
	for i := 0; i < len(blocks); i++ {
		if _, err := stagedsync.InsertBlocksInStages(diskdb, params.TestChainConfig, &vm.Config{}, engine, blocks[i:i+1], true /* checkRoot */); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", i, err)
		}
		if chain.CurrentBlock().Hash() != chain.CurrentHeader().Hash() {
			t.Errorf("block %d: current block/header mismatch: block #%d [%x…], header #%d [%x…]", i, chain.CurrentBlock().Number(), chain.CurrentBlock().Hash().Bytes()[:4], chain.CurrentHeader().Number, chain.CurrentHeader().Hash().Bytes()[:4])
		}
		if _, err := stagedsync.InsertBlocksInStages(diskdb, params.TestChainConfig, &vm.Config{}, engine, forks[i:i+1], true /* checkRoot */); err != nil {
			t.Fatalf(" fork %d: failed to insert into chain: %v", i, err)
		}
		if chain.CurrentBlock().Hash() != chain.CurrentHeader().Hash() {
			t.Errorf(" fork %d: current block/header mismatch: block #%d [%x…], header #%d [%x…]", i, chain.CurrentBlock().Number(), chain.CurrentBlock().Hash().Bytes()[:4], chain.CurrentHeader().Number, chain.CurrentHeader().Hash().Bytes()[:4])
		}
	}
}

// Tests that doing large reorgs works even if the state associated with the
// forking point is not available any more.
func TestLargeReorgTrieGC(t *testing.T) {
	// Generate the original common chain segment and the two competing forks
	engine := ethash.NewFaker()

	diskdb := ethdb.NewMemDatabase()
	defer diskdb.Close()
	(&core.Genesis{Config: params.TestChainConfig}).MustCommit(diskdb)
	cacheConfig := &core.CacheConfig{
		TrieCleanLimit: 256,
		TrieDirtyLimit: 256,
		TrieTimeLimit:  5 * time.Minute,
		NoHistory:      false,
		Pruning:        false,
	}
	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(diskdb, cacheConfig, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()

	db := ethdb.NewMemDatabase()
	defer db.Close()
	genesis := (&core.Genesis{Config: params.TestChainConfig}).MustCommit(db)

	shared, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 64, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate shared chain: %v", err)
	}
	original, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 64+2*core.TriesInMemory, func(i int, b *core.BlockGen) {
		if i < 64 {
			b.SetCoinbase(common.Address{1})
		} else {
			b.SetCoinbase(common.Address{2})
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate original chain: %v", err)
	}
	competitor, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 64+2*core.TriesInMemory+1, func(i int, b *core.BlockGen) {
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
	if _, err := stagedsync.InsertBlocksInStages(diskdb, params.TestChainConfig, &vm.Config{}, engine, shared, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert shared chain: %v", err)
	}
	if _, err := stagedsync.InsertBlocksInStages(diskdb, params.TestChainConfig, &vm.Config{}, engine, original, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert original chain: %v", err)
	}
	// Import the competitor chain without exceeding the canonical's TD and ensure
	// we have not processed any of the blocks (protection against malicious blocks)
	if _, err := stagedsync.InsertBlocksInStages(diskdb, params.TestChainConfig, &vm.Config{}, engine, competitor[:len(competitor)-2], true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert competitor chain: %v", err)
	}
	// Import the head of the competitor chain, triggering the reorg and ensure we
	// successfully reprocess all the stashed away blocks.
	if _, err := stagedsync.InsertBlocksInStages(diskdb, params.TestChainConfig, &vm.Config{}, engine, competitor[len(competitor)-2:], true /* checkRoot */); err != nil {
		t.Fatalf("failed to finalize competitor chain: %v", err)
	}
}

func TestBlockchainRecovery(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth. tag: reorg")
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
	blocks, receipts, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), gendb, int(height), nil, false /* intermediateHashes */)
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
	t.Skip("should be restored. skipped for turbo-geth. tag: fast-sync")
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
	blocks, receipts, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), gendb, int(height), nil, false /* intermediateHashes */)
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
	if frozen, err := ancient.ChainDb().Ancients(); err != nil || frozen != 1 {
		t.Fatalf("failed to truncate ancient data")
	}
	ancient.TerminateInsert = nil
	if n, err := ancient.InsertReceiptChain(blocks, receipts, uint64(3*len(blocks)/4)); err != nil {
		t.Fatalf("failed to insert receipt %d: %v", n, err)
	}
	if ancient.CurrentFastBlock().NumberU64() != blocks[len(blocks)-1].NumberU64() {
		t.Fatalf("failed to insert ancient recept chain after rollback")
	}
}

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
	db := ethdb.NewMemDatabase()
	defer db.Close()
	genesis := new(core.Genesis).MustCommit(db)

	// We must use a pretty long chain to ensure that the fork doesn't overtake us
	// until after at least 128 blocks post tip
	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 6*core.TriesInMemory, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		b.OffsetTime(-9)
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Generate fork chain, starting from an early block
	fork, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 11+8*core.TriesInMemory, func(i int, b *core.BlockGen) {
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
	diskDB := ethdb.NewMemDatabase()
	new(core.Genesis).MustCommit(diskDB)
	defer diskDB.Close()

	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(diskDB, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()
	if n, err := stagedsync.InsertBlocksInStages(diskDB, params.TestChainConfig, &vm.Config{}, engine, blocks, true /* checkRoot */); err != nil {
		t.Fatalf("block %d: failed to insert into chain: %v", n, err)
	}

	// And now import the fork
	if i, err := stagedsync.InsertBlocksInStages(diskDB, params.TestChainConfig, &vm.Config{}, engine, fork, true /* checkRoot */); err != nil {
		t.Fatalf("block %d: failed to insert into chain: %v", i, err)
	}
	head := chain.CurrentBlock()
	if got := fork[len(fork)-1].Hash(); got != head.Hash() {
		t.Fatalf("head wrong, expected %x got %x", head.Hash(), got)
	}
	// Sanity check that all the canonical numbers are present
	header := chain.CurrentHeader()
	for number := head.NumberU64(); number > 0; number-- {
		if hash := chain.GetHeaderByNumber(number).Hash(); hash != header.Hash() {
			t.Fatalf("header %d: canonical hash mismatch: have %x, want %x", number, hash, header.Hash())
		}
		header = chain.GetHeader(header.ParentHash, number-1)
	}
}

// Tests that importing a sidechain (S), where
// - S is sidechain, containing blocks [Sn...Sm]
// - C is canon chain, containing blocks [G..Cn..Cm]
// - A common ancestor is placed at prune-point + blocksBetweenCommonAncestorAndPruneblock
// - The sidechain S is prepended with numCanonBlocksInSidechain blocks from the canon chain
func testSideImport(t *testing.T, numCanonBlocksInSidechain, blocksBetweenCommonAncestorAndPruneblock int) {
	t.Skip("should be restored. skipped for turbo-geth")

	// Generate a canonical chain to act as the main dataset
	engine := ethash.NewFaker()
	db := ethdb.NewMemDatabase()
	defer db.Close()
	genesis := new(core.Genesis).MustCommit(db)

	blocksNum := 2 * core.TriesInMemory
	lastPrunedIndex := blocksNum - core.TriesInMemory - 1
	parentIndex := lastPrunedIndex + blocksBetweenCommonAncestorAndPruneblock

	var side *ethdb.ObjectDatabase
	// Generate and import the canonical chain
	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, blocksNum, func(i int, gen *core.BlockGen) {
		if i == parentIndex+1 {
			side = db.MemCopy()
		}
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	defer side.Close()

	diskdb := ethdb.NewMemDatabase()
	defer diskdb.Close()
	new(core.Genesis).MustCommit(diskdb)
	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(diskdb, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()
	var n int
	if n, err = chain.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("block %d: failed to insert into chain: %v", n, err)
	}

	lastPrunedBlock := blocks[lastPrunedIndex]
	firstNonPrunedBlock := blocks[len(blocks)-core.TriesInMemory]

	// Verify pruning of lastPrunedBlock
	if chain.HasBlockAndState(lastPrunedBlock.Hash(), lastPrunedBlock.NumberU64()) {
		t.Errorf("Block %d not pruned", lastPrunedBlock.NumberU64())
	}
	// Verify firstNonPrunedBlock is not pruned
	if !chain.HasBlockAndState(firstNonPrunedBlock.Hash(), firstNonPrunedBlock.NumberU64()) {
		t.Errorf("Block %d pruned", firstNonPrunedBlock.NumberU64())
	}
	// Generate the sidechain
	// First block should be a known block, block after should be a pruned block. So
	// canon(pruned), side, side...

	// Generate fork chain, make it longer than canon
	parent := blocks[parentIndex]
	fork, _, err := core.GenerateChain(params.TestChainConfig, parent, engine, side, 2*core.TriesInMemory, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{2})
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate fork: %v", err)
	}
	// Prepend the parent(s)
	var sidechain []*types.Block
	for i := numCanonBlocksInSidechain; i > 0; i-- {
		sidechain = append(sidechain, blocks[parentIndex+1-i])
	}
	sidechain = append(sidechain, fork...)
	_, err = chain.InsertChain(context.Background(), sidechain)
	if err != nil {
		t.Errorf("Got error, %v", err)
	}
	head := chain.CurrentBlock()
	if got := fork[len(fork)-1].Hash(); got != head.Hash() {
		t.Fatalf("head wrong, expected %x got %x", head.Hash(), got)
	}
}

// Tests that importing a sidechain (S), where
// - S is sidechain, containing blocks [Sn...Sm]
// - C is canon chain, containing blocks [G..Cn..Cm]
// - The common ancestor Cc is pruned
// - The first block in S: Sn, is == Cn
// That is: the sidechain for import contains some blocks already present in canon chain.
// So the blocks are
// [ Cn, Cn+1, Cc, Sn+3 ... Sm]
//   ^    ^    ^  pruned
func TestPrunedImportSide(t *testing.T) {
	//glogger := log.NewGlogHandler(log.StreamHandler(os.Stdout, log.TerminalFormat(false)))
	//glogger.Verbosity(3)
	//log.Root().SetHandler(log.Handler(glogger))
	testSideImport(t, 3, 3)
	testSideImport(t, 3, -3)
	testSideImport(t, 10, 0)
	testSideImport(t, 1, 10)
	testSideImport(t, 1, -10)
}

func TestInsertKnownHeaders(t *testing.T)      { testInsertKnownChainData(t, "headers") }
func TestInsertKnownReceiptChain(t *testing.T) { testInsertKnownChainData(t, "receipts") }
func TestInsertKnownBlocks(t *testing.T)       { testInsertKnownChainData(t, "blocks") }

func testInsertKnownChainData(t *testing.T, typ string) {
	t.Skip("should be restored. skipped for turbo-geth. tag: reorg")
	engine := ethash.NewFaker()

	db := ethdb.NewMemDatabase()
	defer db.Close()
	genesis := new(core.Genesis).MustCommit(db)

	blocks, receipts, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 32, func(i int, b *core.BlockGen) { b.SetCoinbase(common.Address{1}) }, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// A longer chain but total difficulty is lower.
	blocks2, receipts2, err := core.GenerateChain(params.TestChainConfig, blocks[len(blocks)-1], engine, db, 65, func(i int, b *core.BlockGen) { b.SetCoinbase(common.Address{1}) }, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks2: %v", err)
	}
	// A shorter chain but total difficulty is higher.
	blocks3, receipts3, err := core.GenerateChain(params.TestChainConfig, blocks[len(blocks)-1], engine, db, 64, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		b.OffsetTime(-9) // A higher difficulty
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the shared chain and the original canonical one
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("failed to create temp freezer dir: %v", err)
	}
	defer os.Remove(dir)
	chaindb, err := ethdb.NewDatabaseWithFreezer(ethdb.NewMemDatabase(), dir, "")
	if err != nil {
		t.Fatalf("failed to create temp freezer db: %v", err)
	}
	new(core.Genesis).MustCommit(chaindb)
	defer os.RemoveAll(dir)

	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(chaindb, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}

	var (
		inserter func(blocks []*types.Block, receipts []types.Receipts) error
		asserter func(t *testing.T, block *types.Block)
	)
	if typ == "headers" {
		inserter = func(blocks []*types.Block, receipts []types.Receipts) error {
			headers := make([]*types.Header, 0, len(blocks))
			for _, block := range blocks {
				headers = append(headers, block.Header())
			}
			_, err := chain.InsertHeaderChain(headers, 1)
			return err
		}
		asserter = func(t *testing.T, block *types.Block) {
			if chain.CurrentHeader().Hash() != block.Hash() {
				t.Fatalf("current head header mismatch, have %v, want %v", chain.CurrentHeader().Hash().Hex(), block.Hash().Hex())
			}
		}
	} else if typ == "receipts" {
		inserter = func(blocks []*types.Block, receipts []types.Receipts) error {
			headers := make([]*types.Header, 0, len(blocks))
			for _, block := range blocks {
				headers = append(headers, block.Header())
			}
			_, err := chain.InsertHeaderChain(headers, 1)
			if err != nil {
				return err
			}
			_, err = chain.InsertReceiptChain(blocks, receipts, 0)
			return err
		}
		asserter = func(t *testing.T, block *types.Block) {
			if chain.CurrentFastBlock().Hash() != block.Hash() {
				t.Fatalf("current head fast block mismatch, have %v, want %v", chain.CurrentFastBlock().Hash().Hex(), block.Hash().Hex())
			}
		}
	} else {
		inserter = func(blocks []*types.Block, receipts []types.Receipts) error {
			_, err := chain.InsertChain(context.Background(), blocks)
			return err
		}
		asserter = func(t *testing.T, block *types.Block) {
			if chain.CurrentBlock().Hash() != block.Hash() {
				t.Fatalf("current head block mismatch, have %v, want %v", chain.CurrentBlock().Hash().Hex(), block.Hash().Hex())
			}
		}
	}

	if err := inserter(blocks, receipts); err != nil {
		t.Fatalf("failed to insert chain data: %v", err)
	}

	// Reimport the chain data again. All the imported
	// chain data are regarded "known" data.
	if err := inserter(blocks, receipts); err != nil {
		t.Fatalf("failed to insert chain data: %v", err)
	}
	asserter(t, blocks[len(blocks)-1])

	// Import a long canonical chain with some known data as prefix.
	rollback := blocks[len(blocks)/2].NumberU64()

	chain.SetHead(rollback - 1) //nolint:errcheck
	if err := inserter(append(blocks, blocks2...), append(receipts, receipts2...)); err != nil {
		t.Fatalf("failed to insert chain data: %v", err)
	}
	asserter(t, blocks2[len(blocks2)-1])

	// Import a heavier shorter but higher total difficulty chain with some known data as prefix.
	if err := inserter(append(blocks, blocks3...), append(receipts, receipts3...)); err != nil {
		t.Fatalf("failed to insert chain data: %v", err)
	}
	asserter(t, blocks3[len(blocks3)-1])

	// Import a longer but lower total difficulty chain with some known data as prefix.
	if err := inserter(append(blocks, blocks2...), append(receipts, receipts2...)); err != nil {
		t.Fatalf("failed to insert chain data: %v", err)
	}
	// The head shouldn't change.
	asserter(t, blocks3[len(blocks3)-1])

	// Rollback the heavier chain and re-insert the longer chain again
	chain.SetHead(rollback - 1) //nolint:errcheck
	if err := inserter(append(blocks, blocks2...), append(receipts, receipts2...)); err != nil {
		t.Fatalf("failed to insert chain data: %v", err)
	}
	asserter(t, blocks2[len(blocks2)-1])
}

// getLongAndShortChains returns two chains,
// A is longer, B is heavier
func getLongAndShortChains() (*core.BlockChain, []*types.Block, []*types.Block, error) {
	// Generate a canonical chain to act as the main dataset
	engine := ethash.NewFaker()
	db := ethdb.NewMemDatabase()
	defer db.Close()
	genesis := new(core.Genesis).MustCommit(db)

	// Generate and import the canonical chain,
	// Offset the time, to keep the difficulty low
	longChain, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 80, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intermediateHashes */)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate long chain: %v", err)
	}
	diskdb := ethdb.NewMemDatabase()
	defer diskdb.Close()
	new(core.Genesis).MustCommit(diskdb)

	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(diskdb, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()

	// Generate fork chain, make it shorter than canon, with common ancestor pretty early
	parentIndex := 3
	parent := longChain[parentIndex]
	heavyChain, _, err := core.GenerateChain(params.TestChainConfig, parent, engine, db, 75, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{2})
		b.OffsetTime(-9)
	}, false /* intermediateHashes */)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate heavy chain: %v", err)
	}
	// Verify that the test is sane
	var (
		longerTd  = new(big.Int)
		shorterTd = new(big.Int)
	)
	for index, b := range longChain {
		longerTd.Add(longerTd, b.Difficulty())
		if index <= parentIndex {
			shorterTd.Add(shorterTd, b.Difficulty())
		}
	}
	for _, b := range heavyChain {
		shorterTd.Add(shorterTd, b.Difficulty())
	}
	if shorterTd.Cmp(longerTd) <= 0 {
		return nil, nil, nil, fmt.Errorf("Test is moot, heavyChain td (%v) must be larger than canon td (%v)", shorterTd, longerTd)
	}
	longerNum := longChain[len(longChain)-1].NumberU64()
	shorterNum := heavyChain[len(heavyChain)-1].NumberU64()
	if shorterNum >= longerNum {
		return nil, nil, nil, fmt.Errorf("Test is moot, heavyChain num (%v) must be lower than canon num (%v)", shorterNum, longerNum)
	}
	return chain, longChain, heavyChain, nil
}

// TestReorgToShorterRemovesCanonMapping tests that if we
// 1. Have a chain [0 ... N .. X]
// 2. Reorg to shorter but heavier chain [0 ... N ... Y]
// 3. Then there should be no canon mapping for the block at height X
func TestReorgToShorterRemovesCanonMapping(t *testing.T) {
	t.Skip("TestReorgToShorterRemovesCanonMapping")
	chain, canonblocks, sideblocks, err := getLongAndShortChains()
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if n, err = chain.InsertChain(context.Background(), canonblocks); err != nil {
		t.Fatalf("block %d: failed to insert into chain: %v", n, err)
	}
	canonNum := chain.CurrentBlock().NumberU64()
	_, err = chain.InsertChain(context.Background(), sideblocks)
	if err != nil {
		t.Errorf("Got error, %v", err)
	}
	head := chain.CurrentBlock()
	if got := sideblocks[len(sideblocks)-1].Hash(); got != head.Hash() {
		t.Fatalf("head wrong, expected %x got %x", head.Hash(), got)
	}
	// We have now inserted a sidechain.
	if blockByNum := chain.GetBlockByNumber(canonNum); blockByNum != nil {
		t.Errorf("expected block to be gone: %v", blockByNum.NumberU64())
	}
	if headerByNum := chain.GetHeaderByNumber(canonNum); headerByNum != nil {
		t.Errorf("expected header to be gone: %v", headerByNum.Number.Uint64())
	}
}

// TestReorgToShorterRemovesCanonMappingHeaderChain is the same scenario
// as TestReorgToShorterRemovesCanonMapping, but applied on headerchain
// imports -- that is, for fast sync
func TestReorgToShorterRemovesCanonMappingHeaderChain(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")
	chain, canonblocks, sideblocks, err := getLongAndShortChains()
	if err != nil {
		t.Fatal(err)
	}
	// Convert into headers
	canonHeaders := make([]*types.Header, len(canonblocks))
	for i, block := range canonblocks {
		canonHeaders[i] = block.Header()
	}
	if n, err := chain.InsertHeaderChain(canonHeaders, 0); err != nil {
		t.Fatalf("header %d: failed to insert into chain: %v", n, err)
	}
	canonNum := chain.CurrentHeader().Number.Uint64()
	sideHeaders := make([]*types.Header, len(sideblocks))
	for i, block := range sideblocks {
		sideHeaders[i] = block.Header()
	}
	if n, err := chain.InsertHeaderChain(sideHeaders, 0); err != nil {
		t.Fatalf("header %d: failed to insert into chain: %v", n, err)
	}
	head := chain.CurrentHeader()
	if got := sideblocks[len(sideblocks)-1].Hash(); got != head.Hash() {
		t.Fatalf("head wrong, expected %x got %x", head.Hash(), got)
	}
	// We have now inserted a sidechain.
	if blockByNum := chain.GetBlockByNumber(canonNum); blockByNum != nil {
		t.Errorf("expected block to be gone: %v", blockByNum.NumberU64())
	}
	if headerByNum := chain.GetHeaderByNumber(canonNum); headerByNum != nil {
		t.Errorf("expected header to be gone: %v", headerByNum.Number.Uint64())
	}
}

func TestTransactionIndices(t *testing.T) {
	t.Skip("skipped for turbo-geth")
}

func TestSkipStaleTxIndicesInFastSync(t *testing.T) {
	t.Skip("skipped for turbo-geth")
}

// Benchmarks large blocks with value transfers to non-existing accounts
func benchmarkLargeNumberOfValueToNonexisting(b *testing.B, numTxs, numBlocks int, recipientFn func(uint64) common.Address, dataFn func(uint64) []byte) {
	var (
		signer          = types.HomesteadSigner{}
		testBankKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		testBankAddress = crypto.PubkeyToAddress(testBankKey.PublicKey)
		bankFunds       = big.NewInt(100000000000000000)
		gspec           = core.Genesis{
			Config: params.TestChainConfig,
			Alloc: core.GenesisAlloc{
				testBankAddress: {Balance: bankFunds},
				common.HexToAddress("0xc0de"): {
					Code:    []byte{0x60, 0x01, 0x50},
					Balance: big.NewInt(0),
				}, // push 1, pop
			},
			GasLimit: 100e6, // 100 M
		}
	)
	// Generate the original common chain segment and the two competing forks
	engine := ethash.NewFaker()
	db := ethdb.NewMemDatabase()
	defer db.Close()
	genesis := gspec.MustCommit(db)

	blockGenerator := func(i int, block *core.BlockGen) {
		block.SetCoinbase(common.Address{1})
		for txi := 0; txi < numTxs; txi++ {
			uniq := uint64(i*numTxs + txi)
			recipient := recipientFn(uniq)
			tx, err := types.SignTx(types.NewTransaction(uniq, recipient, u256.Num1, params.TxGas, u256.Num1, nil), signer, testBankKey)
			if err != nil {
				b.Error(err)
			}
			block.AddTx(tx)
		}
	}

	diskdb := ethdb.NewMemDatabase()
	defer diskdb.Close()
	gspec.MustCommit(diskdb)
	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(diskdb, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		b.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()

	shared, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, numBlocks, blockGenerator, false /* intermediateHashes */)
	if err != nil {
		b.Fatalf("generate shared chain: %v", err)
	}
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Import the shared chain and the original canonical one
		diskdb := ethdb.NewMemDatabase()
		gspec.MustCommit(diskdb)

		txCacher := core.NewTxSenderCacher(1)
		chain, err := core.NewBlockChain(diskdb, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
		if err != nil {
			b.Fatalf("failed to create tester chain: %v", err)
		}
		b.StartTimer()
		if _, err := chain.InsertChain(context.Background(), shared); err != nil {
			b.Fatalf("failed to insert shared chain: %v", err)
		}
		b.StopTimer()
		if got := chain.CurrentBlock().Transactions().Len(); got != numTxs*numBlocks {
			b.Fatalf("Transactions were not included, expected %d, got %d", numTxs*numBlocks, got)

		}
		diskdb.Close()
		chain.Stop()
	}
}

func BenchmarkBlockChain_1x1000ValueTransferToNonexisting(b *testing.B) {
	var (
		numTxs    = 1000
		numBlocks = 1
	)
	recipientFn := func(nonce uint64) common.Address {
		return common.BigToAddress(big.NewInt(0).SetUint64(1337 + nonce))
	}
	dataFn := func(nonce uint64) []byte {
		return nil
	}
	benchmarkLargeNumberOfValueToNonexisting(b, numTxs, numBlocks, recipientFn, dataFn)
}

func BenchmarkBlockChain_1x1000ValueTransferToExisting(b *testing.B) {
	var (
		numTxs    = 1000
		numBlocks = 1
	)
	b.StopTimer()
	b.ResetTimer()

	recipientFn := func(nonce uint64) common.Address {
		return common.BigToAddress(big.NewInt(0).SetUint64(1337))
	}
	dataFn := func(nonce uint64) []byte {
		return nil
	}
	benchmarkLargeNumberOfValueToNonexisting(b, numTxs, numBlocks, recipientFn, dataFn)
}

func BenchmarkBlockChain_1x1000Executions(b *testing.B) {
	var (
		numTxs    = 1000
		numBlocks = 1
	)
	b.StopTimer()
	b.ResetTimer()

	recipientFn := func(nonce uint64) common.Address {
		return common.BigToAddress(big.NewInt(0).SetUint64(0xc0de))
	}
	dataFn := func(nonce uint64) []byte {
		return nil
	}
	benchmarkLargeNumberOfValueToNonexisting(b, numTxs, numBlocks, recipientFn, dataFn)
}

// Tests that importing a some old blocks, where all blocks are before the
// pruning point.
// This internally leads to a sidechain import, since the blocks trigger an
// ErrPrunedAncestor error.
// This may e.g. happen if
//   1. Downloader rollbacks a batch of inserted blocks and exits
//   2. Downloader starts to sync again
//   3. The blocks fetched are all known and canonical blocks
func TestSideImportPrunedBlocks(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth. tag: reorg")
	// Generate a canonical chain to act as the main dataset
	engine := ethash.NewFaker()
	db := ethdb.NewMemDatabase()
	defer db.Close()
	genesis := new(core.Genesis).MustCommit(db)

	// Generate and import the canonical chain
	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 2*core.TriesInMemory, nil, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	diskdb := ethdb.NewMemDatabase()
	new(core.Genesis).MustCommit(diskdb)
	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(diskdb, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()
	var n int
	if n, err = chain.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("block %d: failed to insert into chain: %v", n, err)
	}

	lastPrunedIndex := len(blocks) - core.TriesInMemory - 1
	lastPrunedBlock := blocks[lastPrunedIndex]

	// Verify pruning of lastPrunedBlock
	if chain.HasBlockAndState(lastPrunedBlock.Hash(), lastPrunedBlock.NumberU64()) {
		t.Errorf("Block %d not pruned", lastPrunedBlock.NumberU64())
	}
	firstNonPrunedBlock := blocks[len(blocks)-core.TriesInMemory]
	// Verify firstNonPrunedBlock is not pruned
	if !chain.HasBlockAndState(firstNonPrunedBlock.Hash(), firstNonPrunedBlock.NumberU64()) {
		t.Errorf("Block %d pruned", firstNonPrunedBlock.NumberU64())
	}
	// Now re-import some old blocks
	blockToReimport := blocks[5:8]
	_, err = chain.InsertChain(context.Background(), blockToReimport)
	if err != nil {
		t.Errorf("Got error, %v", err)
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
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

	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to AAAA
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), types.HomesteadSigner{}, key)
		b.AddTx(tx)
		// One transaction to BBBB
		tx, _ = types.SignTx(types.NewTransaction(1, bb,
			u256.Num0, 100000, u256.Num1, nil), types.HomesteadSigner{}, key)
		b.AddTx(tx)
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	diskdb := ethdb.NewMemDatabase()
	defer diskdb.Close()
	gspec.MustCommit(diskdb)

	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(diskdb, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()
	if n, err := chain.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("block %d: failed to insert into chain: %v", n, err)
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
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
	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to AA, to kill it
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), types.HomesteadSigner{}, key)
		b.AddTx(tx)
		// One transaction to BB, to recreate AA
		tx, _ = types.SignTx(types.NewTransaction(1, bb,
			u256.Num0, 100000, u256.Num1, nil), types.HomesteadSigner{}, key)
		b.AddTx(tx)
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain

	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(db, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()
	if n, err := chain.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("block %d: failed to insert into chain: %v", n, err)
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
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

	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to AA, to kill it
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), types.HomesteadSigner{}, key)
		b.AddTx(tx)
		// One transaction to AA, to recreate it (but without storage
		tx, _ = types.SignTx(types.NewTransaction(1, aa,
			u256.Num1, 100000, u256.Num1, nil), types.HomesteadSigner{}, key)
		b.AddTx(tx)
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	txCacher1 := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(db, nil, params.TestChainConfig, engine, vm.Config{}, nil, txCacher1)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()
	if n, err := chain.InsertChain(context.Background(), blocks); err != nil {
		t.Fatalf("block %d: failed to insert into chain: %v", n, err)
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
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
	var newDestruct = func(e *expectation) *types.Transaction {
		tx, _ := types.SignTx(types.NewTransaction(nonce, aa,
			u256.Num0, 50000, u256.Num1, nil), types.HomesteadSigner{}, key)
		nonce++
		if e.exist {
			e.exist = false
			e.values = nil
		}
		//t.Logf("block %d; adding destruct\n", e.blocknum)
		return tx
	}
	var newResurrect = func(e *expectation) *types.Transaction {
		tx, _ := types.SignTx(types.NewTransaction(nonce, bb,
			u256.Num0, 100000, u256.Num1, nil), types.HomesteadSigner{}, key)
		nonce++
		if !e.exist {
			e.exist = true
			e.values = map[int]int{3: e.blocknum + 1, 4: 4}
		}
		//t.Logf("block %d; adding resurrect\n", e.blocknum)
		return tx
	}

	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 150, func(i int, b *core.BlockGen) {
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
	txCacher := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(db, nil, params.TestChainConfig, engine, vm.Config{
		//Debug:  true,
		//Tracer: vm.NewJSONLogger(nil, os.Stdout),
	}, nil, txCacher)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	defer chain.Stop()
	var asHash = func(num int) common.Hash {
		return common.BytesToHash([]byte{byte(num)})
	}
	for i, block := range blocks {
		blockNum := i + 1
		if n, err := chain.InsertChain(context.Background(), []*types.Block{block}); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", n, err)
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
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
	cacheConfig := &core.CacheConfig{
		TrieCleanLimit: 256,
		TrieDirtyLimit: 256,
		TrieTimeLimit:  5 * time.Minute,
		NoHistory:      false,
		Pruning:        false,
	}

	txCacher := core.NewTxSenderCacher(1)
	blockchain, _ := core.NewBlockChain(db, cacheConfig, params.AllEthashProtocolChanges, engine, vm.Config{}, nil, txCacher)
	defer blockchain.Stop()

	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 4, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to BB
		tx, _ := types.SignTx(types.NewTransaction(nonce, bb,
			u256.Num0, 100000, u256.Num1, nil), types.HomesteadSigner{}, key)
		b.AddTx(tx)
		nonce++
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Import the canonical chain
	diskdb := ethdb.NewMemDatabase()
	defer diskdb.Close()
	gspec.MustCommit(diskdb)
	txCacherChain := core.NewTxSenderCacher(1)
	chain, err := core.NewBlockChain(diskdb, nil, params.TestChainConfig, engine, vm.Config{
		//Debug:  true,
		//Tracer: vm.NewJSONLogger(nil, os.Stdout),
	}, nil, txCacherChain)
	if err != nil {
		t.Fatalf("failed to create tester chain: %v", err)
	}
	statedb := state.New(state.NewDbStateReader(db))
	if got, exp := statedb.GetBalance(aa), uint64(100000); got.Uint64() != exp {
		t.Fatalf("Genesis err, got %v exp %v", got, exp)
	}
	// First block tries to create, but fails
	{
		block := blocks[0]
		if _, err := chain.InsertChain(context.Background(), []*types.Block{blocks[0]}); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", block.NumberU64(), err)
		}
		statedb = state.New(state.NewDbStateReader(db))
		if got, exp := statedb.GetBalance(aa), uint64(100000); got.Uint64() != exp {
			t.Fatalf("block %d: got %v exp %v", block.NumberU64(), got, exp)
		}
	}
	// Import the rest of the blocks
	for _, block := range blocks[1:] {
		if _, err := chain.InsertChain(context.Background(), []*types.Block{block}); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", block.NumberU64(), err)
		}
	}
}
