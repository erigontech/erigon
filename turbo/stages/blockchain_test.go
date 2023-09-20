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

package stages_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/chain"
	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
)

// So we can deterministically seed different blockchains
var (
	canonicalSeed = 1
	forkSeed      = 2
)

// makeBlockChain creates a deterministic chain of blocks rooted at parent.
func makeBlockChain(parent *types.Block, n int, m *mock.MockSentry, seed int) *core.ChainPack {
	chain, _ := core.GenerateChain(m.ChainConfig, parent, m.Engine, m.DB, n, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{0: byte(seed), 19: byte(i)})
	})
	return chain
}

// newCanonical creates a chain database, and injects a deterministic canonical
// chain. Depending on the full flag, if creates either a full block chain or a
// header only chain.
func newCanonical(t *testing.T, n int) *mock.MockSentry {
	m := mock.Mock(t)

	// Create and inject the requested chain
	if n == 0 {
		return m
	}

	// Full block-chain requested
	chain := makeBlockChain(m.Genesis, n, m, canonicalSeed)
	if err := m.InsertChain(chain, nil); err != nil {
		t.Fatal(err)
	}
	return m
}

// Test fork of length N starting from block i
func testFork(t *testing.T, m *mock.MockSentry, i, n int, comparator func(td1, td2 *big.Int)) {
	// Copy old chain up to #i into a new db
	canonicalMock := newCanonical(t, i)
	var err error
	ctx := context.Background()

	// Assert the chains have the same header/block at #i
	var hash1, hash2 libcommon.Hash
	err = m.DB.View(m.Ctx, func(tx kv.Tx) error {
		if hash1, err = m.BlockReader.CanonicalHash(m.Ctx, tx, uint64(i)); err != nil {
			t.Fatalf("Failed to read canonical hash: %v", err)
		}
		if block1, _, _ := m.BlockReader.BlockWithSenders(ctx, tx, hash1, uint64(i)); block1 == nil {
			t.Fatalf("Did not find canonical block")
		}
		return nil
	})
	require.NoError(t, err)

	canonicalMock.DB.View(ctx, func(tx kv.Tx) error {
		if hash2, err = m.BlockReader.CanonicalHash(m.Ctx, tx, uint64(i)); err != nil {
			t.Fatalf("Failed to read canonical hash: %v", err)
		}
		if block2, _, _ := m.BlockReader.BlockWithSenders(ctx, tx, hash2, uint64(i)); block2 == nil {
			t.Fatalf("Did not find canonical block 2")
		}
		return nil
	})
	require.NoError(t, err)

	if hash1 != hash2 {
		t.Errorf("chain content mismatch at %d: have hash %v, want hash %v", i, hash2, hash1)
	}
	// Extend the newly created chain
	var blockChainB *core.ChainPack
	var tdPre, tdPost *big.Int
	var currentBlockB *types.Block

	err = canonicalMock.DB.View(context.Background(), func(tx kv.Tx) error {
		currentBlockB, err = m.BlockReader.CurrentBlock(tx)
		return err
	})
	require.NoError(t, err)

	blockChainB = makeBlockChain(currentBlockB, n, canonicalMock, forkSeed)

	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		currentBlock, err := m.BlockReader.CurrentBlock(tx)
		if err != nil {
			return err
		}
		tdPre, err = rawdb.ReadTd(tx, currentBlock.Hash(), currentBlock.NumberU64())
		if err != nil {
			t.Fatalf("Failed to read TD for current block: %v", err)
		}
		return nil
	})
	require.NoError(t, err)

	if err = m.InsertChain(blockChainB, nil); err != nil {
		t.Fatalf("failed to insert forking chain: %v", err)
	}
	currentBlockHash := blockChainB.TopBlock.Hash()
	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		number := rawdb.ReadHeaderNumber(tx, currentBlockHash)
		currentBlock, _, _ := m.BlockReader.BlockWithSenders(ctx, tx, currentBlockHash, *number)
		tdPost, err = rawdb.ReadTd(tx, currentBlockHash, currentBlock.NumberU64())
		if err != nil {
			t.Fatalf("Failed to read TD for current header: %v", err)
		}
		return nil
	})
	require.NoError(t, err)

	// Sanity check that the forked chain can be imported into the original
	if err := canonicalMock.InsertChain(blockChainB, nil); err != nil {
		t.Fatalf("failed to import forked block chain: %v", err)
	}
	// Compare the total difficulties of the chains
	comparator(tdPre, tdPost)
}

func TestLastBlock(t *testing.T) {
	m := newCanonical(t, 0)
	var err error

	chain := makeBlockChain(current(m, nil), 1, m, 0)
	if err = m.InsertChain(chain, nil); err != nil {
		t.Fatalf("Failed to insert block: %v", err)
	}

	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	if chain.TopBlock.Hash() != rawdb.ReadHeadBlockHash(tx) {
		t.Fatalf("Write/Get HeadBlockHash failed")
	}
}

// Tests that given a starting canonical chain of a given size, it can be extended
// with various length chains.
func TestExtendCanonicalBlocks(t *testing.T) { testExtendCanonical(t) }

func testExtendCanonical(t *testing.T) {
	length := 5

	// Make first chain starting from genesis
	m := newCanonical(t, length)

	// Define the difficulty comparator
	better := func(td1, td2 *big.Int) {
		if td2.Cmp(td1) <= 0 {
			t.Errorf("total difficulty mismatch: have %v, expected more than %v", td2, td1)
		}
	}
	// Start fork from current height
	testFork(t, m, length, 1, better)
	testFork(t, m, length, 2, better)
	testFork(t, m, length, 5, better)
	testFork(t, m, length, 10, better)
}

// Tests that given a starting canonical chain of a given size, creating shorter
// forks do not take canonical ownership.
func TestShorterForkBlocks(t *testing.T) {
	t.Skip("Erigon does not insert shorter forks")
	testShorterFork(t)
}

func testShorterFork(t *testing.T) {
	length := 10

	// Make first chain starting from genesis
	m := newCanonical(t, length)

	// Define the difficulty comparator
	worse := func(td1, td2 *big.Int) {
		if td2.Cmp(td1) >= 0 {
			t.Errorf("total difficulty mismatch: have %v, expected less than %v", td2, td1)
		}
	}
	// Sum of numbers must be less than `length` for this to be a shorter fork
	testFork(t, m, 0, 3, worse)
	testFork(t, m, 0, 7, worse)
	testFork(t, m, 1, 1, worse)
	testFork(t, m, 1, 7, worse)
	testFork(t, m, 5, 3, worse)
	testFork(t, m, 5, 4, worse)
}

// Tests that given a starting canonical chain of a given size, creating longer
// forks do take canonical ownership.
func TestLongerForkHeaders(t *testing.T) { testLongerFork(t, false) }
func TestLongerForkBlocks(t *testing.T)  { testLongerFork(t, true) }

func testLongerFork(t *testing.T, full bool) {
	length := 10

	// Make first chain starting from genesis
	m := newCanonical(t, length)

	// Define the difficulty comparator
	better := func(td1, td2 *big.Int) {
		if td2.Cmp(td1) <= 0 {
			t.Errorf("total difficulty mismatch: have %v, expected more than %v", td2, td1)
		}
	}
	// Sum of numbers must be greater than `length` for this to be a longer fork
	testFork(t, m, 5, 6, better)
	testFork(t, m, 5, 8, better)
	testFork(t, m, 1, 13, better)
	testFork(t, m, 1, 14, better)
	testFork(t, m, 0, 16, better)
	testFork(t, m, 0, 17, better)
}

// Tests that chains missing links do not get accepted by the processor.
func TestBrokenBlockChain(t *testing.T) { testBrokenChain(t) }

func testBrokenChain(t *testing.T) {
	// Make chain starting from genesis
	m := newCanonical(t, 10)

	// Create a forked chain, and try to insert with a missing link
	chain := makeBlockChain(current(m, nil), 5, m, forkSeed)
	brokenChain := chain.Slice(1, chain.Length())

	if err := m.InsertChain(brokenChain, nil); err == nil {
		t.Errorf("broken block chain not reported")
	}
}

// Tests that reorganising a long difficult chain after a short easy one
// overwrites the canonical numbers and links in the database.
func TestReorgLongBlocks(t *testing.T) { testReorgLong(t) }

func testReorgLong(t *testing.T) {
	testReorg(t, []int64{0, 0, -9}, []int64{0, 0, 0, -9}, 393280)
}

// Tests that reorganising a short difficult chain after a long easy one
// overwrites the canonical numbers and links in the database.
func TestReorgShortBlocks(t *testing.T) { testReorgShort(t) }

func testReorgShort(t *testing.T) {
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
	testReorg(t, easy, diff, 12615120)
}

func testReorg(t *testing.T, first, second []int64, td int64) {
	require := require.New(t)
	// Create a pristine chain and database
	m := newCanonical(t, 0)
	// Insert an easy and a difficult chain afterwards
	easyChain, err := core.GenerateChain(m.ChainConfig, current(m, nil), m.Engine, m.DB, len(first), func(i int, b *core.BlockGen) {
		b.OffsetTime(first[i])
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	diffChain, err := core.GenerateChain(m.ChainConfig, current(m, nil), m.Engine, m.DB, len(second), func(i int, b *core.BlockGen) {
		b.OffsetTime(second[i])
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	tx, err := m.DB.BeginRw(m.Ctx)
	if err != nil {
		fmt.Printf("beginro error: %v\n", err)
		return
	}
	defer tx.Rollback()

	if err = m.InsertChain(easyChain, tx); err != nil {
		t.Fatalf("failed to insert easy chain: %v", err)
	}
	if err = m.InsertChain(diffChain, tx); err != nil {
		t.Fatalf("failed to insert difficult chain: %v", err)
	}

	// Check that the chain is valid number and link wise
	prev, err := m.BlockReader.CurrentBlock(tx)
	require.NoError(err)
	block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, rawdb.ReadCurrentHeader(tx).Number.Uint64()-1)
	if err != nil {
		t.Fatal(err)
	}
	for block.NumberU64() != 0 {
		if prev.ParentHash() != block.Hash() {
			t.Errorf("parent block hash mismatch: have %x, want %x", prev.ParentHash(), block.Hash())
		}
		prev = block
		block, err = m.BlockReader.BlockByNumber(m.Ctx, tx, block.NumberU64()-1)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Make sure the chain total difficulty is the correct one
	want := new(big.Int).Add(m.Genesis.Difficulty(), big.NewInt(td))
	have, err := rawdb.ReadTdByHash(tx, rawdb.ReadCurrentHeader(tx).Hash())
	require.NoError(err)
	if have.Cmp(want) != 0 {
		t.Errorf("total difficulty mismatch: have %v, want %v", have, want)
	}
}

// Tests that the insertion functions detect banned hashes.
func TestBadBlockHashes(t *testing.T) { testBadHashes(t) }

func testBadHashes(t *testing.T) {
	t.Skip("to support this error in Erigon")
	// Create a pristine chain and database
	m := newCanonical(t, 0)
	var err error

	// Create a chain, ban a hash and try to import
	chain := makeBlockChain(current(m, nil), 3, m, 10)

	core.BadHashes[chain.Headers[2].Hash()] = true
	defer func() { delete(core.BadHashes, chain.Headers[2].Hash()) }()

	err = m.InsertChain(chain, nil)
	if !errors.Is(err, core.ErrBlacklistedHash) {
		t.Errorf("error mismatch: have: %v, want: %v", err, core.ErrBlacklistedHash)
	}
}

// Tests that chain reorganisations handle transaction removals and reinsertions.
func TestChainTxReorgs(t *testing.T) {
	var (
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key2, _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		key3, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		addr2   = crypto.PubkeyToAddress(key2.PublicKey)
		addr3   = crypto.PubkeyToAddress(key3.PublicKey)
		gspec   = &types.Genesis{
			Config:   params.TestChainConfig,
			GasLimit: 3141592,
			Alloc: types.GenesisAlloc{
				addr1: {Balance: big.NewInt(1000000)},
				addr2: {Balance: big.NewInt(1000000)},
				addr3: {Balance: big.NewInt(1000000)},
			},
		}
		signer = types.LatestSigner(gspec.Config)
	)

	m := mock.MockWithGenesis(t, gspec, key1, false)
	m2 := mock.MockWithGenesis(t, gspec, key1, false)
	defer m2.DB.Close()

	// Create two transactions shared between the chains:
	//  - postponed: transaction included at a later block in the forked chain
	//  - swapped: transaction included at the same block number in the forked chain
	postponed, _ := types.SignTx(types.NewTransaction(0, addr1, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key1)
	swapped, _ := types.SignTx(types.NewTransaction(1, addr1, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key1)

	// Create two transactions that will be dropped by the forked chain:
	//  - pastDrop: transaction dropped retroactively from a past block
	//  - freshDrop: transaction dropped exactly at the block where the reorg is detected
	var pastDrop, freshDrop types.Transaction

	// Create three transactions that will be added in the forked chain:
	//  - pastAdd:   transaction added before the reorganization is detected
	//  - freshAdd:  transaction added at the exact block the reorg is detected
	//  - futureAdd: transaction added after the reorg has already finished
	var pastAdd, freshAdd, futureAdd types.Transaction

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			pastDrop, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr2, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key2)

			gen.AddTx(pastDrop)  // This transaction will be dropped in the fork from below the split point
			gen.AddTx(postponed) // This transaction will be postponed till block #3 in the fork

		case 2:
			freshDrop, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr2, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key2)

			gen.AddTx(freshDrop) // This transaction will be dropped in the fork from exactly at the split point
			gen.AddTx(swapped)   // This transaction will be swapped out at the exact height

			gen.OffsetTime(9) // Lower the block difficulty to simulate a weaker chain
		}
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	// Import the chain. This runs all block validation rules.
	if err1 := m.InsertChain(chain, nil); err1 != nil {
		t.Fatalf("failed to insert original chain: %v", err1)
	}

	// overwrite the old chain
	chain, err = core.GenerateChain(m2.ChainConfig, m2.Genesis, m2.Engine, m2.DB, 5, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			pastAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key3)
			gen.AddTx(pastAdd) // This transaction needs to be injected during reorg

		case 2:
			gen.AddTx(postponed) // This transaction was postponed from block #1 in the original chain
			gen.AddTx(swapped)   // This transaction was swapped from the exact current spot in the original chain

			freshAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key3)
			gen.AddTx(freshAdd) // This transaction will be added exactly at reorg time

		case 3:
			futureAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key3)
			gen.AddTx(futureAdd) // This transaction will be added after a full reorg
		}
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	if err := m.InsertChain(chain, nil); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}
	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// removed tx
	txs := types.Transactions{pastDrop, freshDrop}
	for i, txn := range txs {
		if bn, _ := rawdb.ReadTxLookupEntry(tx, txn.Hash()); bn != nil {
			t.Errorf("drop %d: tx %v found while shouldn't have been", i, txn)
		}
		if rcpt, _, _, _, _ := readReceipt(tx, txn.Hash(), m.BlockReader); rcpt != nil {
			t.Errorf("drop %d: receipt %v found while shouldn't have been", i, rcpt)
		}
	}

	// added tx
	txs = types.Transactions{pastAdd, freshAdd, futureAdd}
	for i, txn := range txs {
		_, found, err := m.BlockReader.TxnLookup(m.Ctx, tx, txn.Hash())
		require.NoError(t, err)
		require.True(t, found)

		if m.HistoryV3 {
			// m.HistoryV3 doesn't store
		} else {
			if rcpt, _, _, _, _ := readReceipt(tx, txn.Hash(), m.BlockReader); rcpt == nil {
				t.Errorf("add %d: expected receipt to be found", i)
			}
		}
	}
	// shared tx
	txs = types.Transactions{postponed, swapped}
	for i, txn := range txs {
		if bn, _ := rawdb.ReadTxLookupEntry(tx, txn.Hash()); bn == nil {
			t.Errorf("drop %d: tx %v found while shouldn't have been", i, txn)
		}
		if m.HistoryV3 {
			// m.HistoryV3 doesn't store
		} else {
			if rcpt, _, _, _, _ := readReceipt(tx, txn.Hash(), m.BlockReader); rcpt == nil {
				t.Errorf("share %d: expected receipt to be found", i)
			}
		}
	}
}

func readReceipt(db kv.Tx, txHash libcommon.Hash, br services.FullBlockReader) (*types.Receipt, libcommon.Hash, uint64, uint64, error) {
	// Retrieve the context of the receipt based on the transaction hash
	blockNumber, err := rawdb.ReadTxLookupEntry(db, txHash)
	if err != nil {
		return nil, libcommon.Hash{}, 0, 0, err
	}
	if blockNumber == nil {
		return nil, libcommon.Hash{}, 0, 0, nil
	}
	blockHash, err := br.CanonicalHash(context.Background(), db, *blockNumber)
	if err != nil {
		return nil, libcommon.Hash{}, 0, 0, err
	}
	if blockHash == (libcommon.Hash{}) {
		return nil, libcommon.Hash{}, 0, 0, nil
	}
	b, senders, err := br.BlockWithSenders(context.Background(), db, blockHash, *blockNumber)
	if err != nil {
		return nil, libcommon.Hash{}, 0, 0, err
	}
	// Read all the receipts from the block and return the one with the matching hash
	receipts := rawdb.ReadReceipts(db, b, senders)
	for receiptIndex, receipt := range receipts {
		if receipt.TxHash == txHash {
			return receipt, blockHash, *blockNumber, uint64(receiptIndex), nil
		}
	}
	log.Error("Receipt not found", "number", blockNumber, "hash", blockHash, "txhash", txHash)
	return nil, libcommon.Hash{}, 0, 0, nil
}

// Tests if the canonical block can be fetched from the database during chain insertion.
func TestCanonicalBlockRetrieval(t *testing.T) {
	m := newCanonical(t, 0)

	chain, err2 := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, gen *core.BlockGen) {})
	if err2 != nil {
		t.Fatalf("generate chain: %v", err2)
	}
	err := m.InsertChain(chain, nil)
	require.NoError(t, err)

	tx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	for _, block := range chain.Blocks {
		// try to retrieve a block by its canonical hash and see if the block data can be retrieved.
		ch, err := m.BlockReader.CanonicalHash(m.Ctx, tx, block.NumberU64())
		require.NoError(t, err)
		if err != nil {
			panic(err)
		}
		if ch == (libcommon.Hash{}) {
			continue // busy wait for canonical hash to be written
		}
		if ch != block.Hash() {
			t.Errorf("unknown canonical hash, want %s, got %s", block.Hash().Hex(), ch.Hex())
			return
		}
		fb, _ := m.BlockReader.Header(m.Ctx, tx, ch, block.NumberU64())
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
	// Configure and generate a sample block chai

	var (
		key, _     = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address    = crypto.PubkeyToAddress(key.PublicKey)
		funds      = big.NewInt(1000000000)
		deleteAddr = libcommon.Address{1}
		gspec      = &types.Genesis{
			Config: &chain.Config{ChainID: big.NewInt(1), TangerineWhistleBlock: big.NewInt(0), SpuriousDragonBlock: big.NewInt(2), HomesteadBlock: new(big.Int)},
			Alloc:  types.GenesisAlloc{address: {Balance: funds}, deleteAddr: {Balance: new(big.Int)}},
		}
	)
	m := mock.MockWithGenesis(t, gspec, key, false)

	chain, chainErr := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, block *core.BlockGen) {
		var (
			tx      types.Transaction
			err     error
			basicTx = func(signer types.Signer) (types.Transaction, error) {
				return types.SignTx(types.NewTransaction(block.TxNonce(address), libcommon.Address{}, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
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
	})
	if chainErr != nil {
		t.Fatalf("generate chain: %v", chainErr)
	}

	if chainErr = m.InsertChain(chain, nil); chainErr != nil {
		t.Fatal(chainErr)
	}
	if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
		block, _ := m.BlockReader.BlockByNumber(m.Ctx, tx, 1)
		if block.Transactions()[0].Protected() {
			t.Error("Expected block[0].txs[0] to not be replay protected")
		}

		block, _ = m.BlockReader.BlockByNumber(m.Ctx, tx, 3)
		if block.Transactions()[0].Protected() {
			t.Error("Expected block[3].txs[0] to not be replay protected")
		}
		if !block.Transactions()[1].Protected() {
			t.Error("Expected block[3].txs[1] to be replay protected")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// generate an invalid chain id transaction
	config := &chain2.Config{ChainID: big.NewInt(2), TangerineWhistleBlock: big.NewInt(0), SpuriousDragonBlock: big.NewInt(2), HomesteadBlock: new(big.Int)}
	chain, chainErr = core.GenerateChain(config, chain.TopBlock, m.Engine, m.DB, 4, func(i int, block *core.BlockGen) {
		var (
			basicTx = func(signer types.Signer) (types.Transaction, error) {
				return types.SignTx(types.NewTransaction(block.TxNonce(address), libcommon.Address{}, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
			}
		)
		if i == 0 {
			tx, txErr := basicTx(*types.LatestSigner(config))
			if txErr != nil {
				t.Fatal(txErr)
			}
			block.AddTx(tx)
		}
	})
	if chainErr != nil {
		t.Fatalf("generate blocks: %v", chainErr)
	}
	if err := m.InsertChain(chain, nil); err == nil {
		t.Errorf("expected error")
	}
}

func TestModes(t *testing.T) {
	// run test on all combination of flags
	runWithModesPermuations(
		t,
		doModesTest,
	)
}

func TestBeforeModes(t *testing.T) {
	mode := prune.DefaultMode
	mode.History = prune.Before(0)
	mode.Receipts = prune.Before(1)
	mode.TxIndex = prune.Before(2)
	mode.CallTraces = prune.Before(3)
	doModesTest(t, mode)
}

func doModesTest(t *testing.T, pm prune.Mode) error {
	fmt.Printf("h=%v, r=%v, t=%v\n", pm.History.Enabled(), pm.Receipts.Enabled(), pm.TxIndex.Enabled())
	require := require.New(t)
	// Configure and generate a sample block chain
	var (
		key, _     = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address    = crypto.PubkeyToAddress(key.PublicKey)
		funds      = big.NewInt(1000000000)
		deleteAddr = libcommon.Address{1}
		gspec      = &types.Genesis{
			Config: &chain.Config{ChainID: big.NewInt(1), TangerineWhistleBlock: big.NewInt(0), SpuriousDragonBlock: big.NewInt(2), HomesteadBlock: new(big.Int)},
			Alloc:  types.GenesisAlloc{address: {Balance: funds}, deleteAddr: {Balance: new(big.Int)}},
		}
	)
	m := mock.MockWithGenesisPruneMode(t, gspec, key, 128, pm, false)

	head := uint64(4)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, int(head), func(i int, block *core.BlockGen) {
		var (
			tx      types.Transaction
			err     error
			basicTx = func(signer types.Signer) (types.Transaction, error) {
				return types.SignTx(types.NewTransaction(block.TxNonce(address), libcommon.Address{}, new(uint256.Int), 21000, new(uint256.Int), nil), signer, key)
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
	})
	if err != nil {
		return fmt.Errorf("generate blocks: %w", err)
	}

	if err = m.InsertChain(chain, nil); err != nil {
		return err
	}

	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	if pm.Receipts.Enabled() {
		receiptsAvailable, err := rawdb.ReceiptsAvailableFrom(tx)
		require.NoError(err)
		found := uint64(0)
		err = tx.ForEach(kv.Receipts, nil, func(k, v []byte) error {
			found++
			return nil
		})
		require.NoError(err)
		if m.HistoryV3 {
			// receipts are not stored in erigon3
		} else {
			require.GreaterOrEqual(receiptsAvailable, pm.Receipts.PruneTo(head))
			require.Greater(found, uint64(0))
		}
	} else {
		receiptsAvailable, err := rawdb.ReceiptsAvailableFrom(tx)
		require.NoError(err)
		require.Equal(uint64(0), receiptsAvailable)
	}

	if m.HistoryV3 {
		//TODO: e3 not implemented Prune feature yet
		/*
			if pm.History.Enabled() {
				it, err := tx.(kv.TemporalTx).HistoryRange(temporal.AccountsHistory, 0, int(pm.History.PruneTo(head)), order.Asc, -1)
				require.NoError(err)
				count, err := iter.CountKV(it)
				require.NoError(err)
				require.Zero(count)

				it, err = tx.(kv.TemporalTx).HistoryRange(temporal.AccountsHistory, int(pm.History.PruneTo(head)), -1, order.Asc, -1)
				require.NoError(err)
				count, err = iter.CountKV(it)
				require.NoError(err)
				require.Equal(3, count)
			} else {
				it, err := tx.(kv.TemporalTx).HistoryRange(temporal.AccountsHistory, 0, -1, order.Asc, -1)
				require.NoError(err)
				count, err := iter.CountKV(it)
				require.NoError(err)
				require.Equal(3, count)
			}
		*/
	} else {
		if pm.History.Enabled() {
			afterPrune := uint64(0)
			err := tx.ForEach(kv.E2AccountsHistory, nil, func(k, _ []byte) error {
				n := binary.BigEndian.Uint64(k[length.Addr:])
				require.Greater(n, pm.History.PruneTo(head))
				afterPrune++
				return nil
			})
			require.Greater(afterPrune, uint64(0))
			assert.NoError(t, err)
		} else {
			found, err := bitmapdb.Get64(tx, kv.E2AccountsHistory, address[:], 0, 1024)
			require.NoError(err)
			require.Equal(uint64(0), found.Minimum())
		}
	}

	if pm.TxIndex.Enabled() {
		b, err := m.BlockReader.BlockByNumber(m.Ctx, tx, 1)
		require.NoError(err)
		for _, txn := range b.Transactions() {
			found, err := rawdb.ReadTxLookupEntry(tx, txn.Hash())
			require.NoError(err)
			require.Nil(found)
		}
	} else {
		b, err := m.BlockReader.BlockByNumber(m.Ctx, tx, 1)
		require.NoError(err)
		for _, txn := range b.Transactions() {
			foundBlockNum, found, err := m.BlockReader.TxnLookup(context.Background(), tx, txn.Hash())
			require.NoError(err)
			require.True(found)
			require.Equal(uint64(1), foundBlockNum)
		}
	}
	/*
		for bucketName, shouldBeEmpty := range map[string]bool{
			//dbutils.AccountsHistory: pm.History.Enabled(),
			dbutils.Receipts: pm.Receipts.Enabled(),
			//dbutils.TxLookup: pm.TxIndex.Enabled(),
		} {
			numberOfEntries := 0

			err := tx.ForEach(bucketName, nil, func(k, v []byte) error {
				// we ignore empty account history
				//nolint:scopelint
				if bucketName == dbutils.AccountsHistory && len(v) == 0 {
					return nil
				}

				numberOfEntries++
				return nil
			})
			if err != nil {
				return err
			}

			if bucketName == dbutils.Receipts {
				// we will always have a receipt for genesis
				numberOfEntries--
			}

			if (shouldBeEmpty && numberOfEntries > 0) || (!shouldBeEmpty && numberOfEntries == 0) {
				return fmt.Errorf("bucket '%s' should be empty? %v (actually %d entries)", bucketName, shouldBeEmpty, numberOfEntries)
			}
			}
	*/
	return nil
}

func runWithModesPermuations(t *testing.T, testFunc func(*testing.T, prune.Mode) error) {
	err := runPermutation(t, testFunc, 0, prune.DefaultMode)
	if err != nil {
		t.Errorf("error while testing stuff: %v", err)
	}
}

func runPermutation(t *testing.T, testFunc func(*testing.T, prune.Mode) error, current int, pm prune.Mode) error {
	if current == 3 {
		return testFunc(t, pm)
	}
	if err := runPermutation(t, testFunc, current+1, pm); err != nil {
		return err
	}
	invert := func(a prune.BlockAmount) prune.Distance {
		if a.Enabled() {
			return math.MaxUint64
		}
		return 2
	}
	switch current {
	case 0:
		pm.History = invert(pm.History)
	case 1:
		pm.Receipts = invert(pm.Receipts)
	case 2:
		pm.TxIndex = invert(pm.TxIndex)
	default:
		panic("unexpected current item")
	}

	return runPermutation(t, testFunc, current+1, pm)
}

func TestEIP161AccountRemoval(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		theAddr = libcommon.Address{1}
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(2),
			},
			Alloc: types.GenesisAlloc{address: {Balance: funds}},
		}
	)
	m := mock.MockWithGenesis(t, gspec, key, false)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
		var (
			txn    types.Transaction
			err    error
			signer = types.MakeFrontierSigner()
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
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	tx, err := m.DB.BeginRw(m.Ctx)
	if err != nil {
		fmt.Printf("beginro error: %v\n", err)
		return
	}
	defer tx.Rollback()

	// account must exist pre eip 161
	if err = m.InsertChain(chain.Slice(0, 1), tx); err != nil {
		t.Fatal(err)
	}
	if st := state.New(m.NewStateReader(tx)); !st.Exist(theAddr) {
		t.Error("expected account to exist")
	}

	// account needs to be deleted post eip 161
	if err = m.InsertChain(chain.Slice(1, 2), tx); err != nil {
		t.Fatal(err)
	}
	if st := state.New(m.NewStateReader(tx)); st.Exist(theAddr) {
		t.Error("account should not exist")
	}

	// account mustn't be created post eip 161
	if err = m.InsertChain(chain.Slice(2, 3), tx); err != nil {
		t.Fatal(err)
	}
	if st := state.New(m.NewStateReader(tx)); st.Exist(theAddr) {
		t.Error("account should not exist")
	}
	require.NoError(t, err)
}

func TestDoubleAccountRemoval(t *testing.T) {
	var (
		signer      = types.LatestSignerForChainID(nil)
		bankKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		bankAddress = crypto.PubkeyToAddress(bankKey.PublicKey)
		bankFunds   = big.NewInt(1e9)
		contract    = hexutil.MustDecode("0x60606040526040516102eb3803806102eb8339016040526060805160600190602001505b33600060006101000a81548173ffffffffffffffffffffffffffffffffffffffff02191690830217905550806001600050908051906020019082805482825590600052602060002090601f01602090048101928215609c579182015b82811115609b578251826000505591602001919060010190607f565b5b50905060c3919060a7565b8082111560bf576000818150600090555060010160a7565b5090565b50505b50610215806100d66000396000f30060606040526000357c01000000000000000000000000000000000000000000000000000000009004806341c0e1b51461004f578063adbd84651461005c578063cfae32171461007d5761004d565b005b61005a6004506100f6565b005b610067600450610208565b6040518082815260200191505060405180910390f35b61008860045061018a565b60405180806020018281038252838181518152602001915080519060200190808383829060006004602084601f0104600302600f01f150905090810190601f1680156100e85780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141561018757600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16ff5b5b565b60206040519081016040528060008152602001506001600050805480601f016020809104026020016040519081016040528092919081815260200182805480156101f957820191906000526020600020905b8154815290600101906020018083116101dc57829003601f168201915b50505050509050610205565b90565b6000439050610212565b90560000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000d5468697320697320437972757300000000000000000000000000000000000000")
		input       = hexutil.MustDecode("0xadbd8465")
		kill        = hexutil.MustDecode("0x41c0e1b5")
		gspec       = &types.Genesis{
			Config: params.TestChainConfig,
			Alloc:  types.GenesisAlloc{bankAddress: {Balance: bankFunds}},
		}
	)
	m := mock.MockWithGenesis(t, gspec, bankKey, false)

	var theAddr libcommon.Address

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
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
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	tx, err := m.DB.BeginRw(m.Ctx)
	if err != nil {
		fmt.Printf("beginro error: %v\n", err)
		return
	}
	defer tx.Rollback()
	err = m.InsertChain(chain, tx)
	assert.NoError(t, err)

	st := state.New(m.NewStateReader(tx))
	assert.NoError(t, err)
	assert.False(t, st.Exist(theAddr), "Contract should've been removed")

	st = state.New(m.NewHistoryStateReader(1, tx))
	assert.NoError(t, err)
	assert.False(t, st.Exist(theAddr), "Contract should not exist at block #0")

	st = state.New(m.NewHistoryStateReader(2, tx))
	assert.NoError(t, err)
	assert.True(t, st.Exist(theAddr), "Contract should exist at block #1")

	st = state.New(m.NewHistoryStateReader(3, tx))
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
	m, m2 := mock.Mock(t), mock.Mock(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 64, func(i int, b *core.BlockGen) { b.SetCoinbase(libcommon.Address{1}) })
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Generate a bunch of fork blocks, each side forking from the canonical chain
	forks := make([]*core.ChainPack, chain.Length())
	for i := 0; i < len(forks); i++ {
		fork, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, i+1, func(j int, b *core.BlockGen) {
			//nolint:scopelint
			if j == i {
				b.SetCoinbase(libcommon.Address{2})
				b.OffsetTime(-2) // By reducing time, we increase difficulty of the fork, so that it can overwrite the canonical chain
			} else {
				b.SetCoinbase(libcommon.Address{1})
			}
		})
		if err != nil {
			t.Fatalf("generate fork %d: %v", i, err)
		}
		forks[i] = fork.Slice(i, i+1)
	}
	// Import the canonical and fork chain side by side, verifying the current block
	// and current header consistency
	for i := 0; i < chain.Length(); i++ {
		if err := m2.InsertChain(chain.Slice(i, i+1), nil); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", i, err)
		}

		if err := m2.DB.View(m2.Ctx, func(tx kv.Tx) error {
			b, err := m.BlockReader.CurrentBlock(tx)
			if err != nil {
				return err
			}
			h := rawdb.ReadCurrentHeader(tx)
			if b.Hash() != h.Hash() {
				t.Errorf("block %d: current block/header mismatch: block #%d [%x…], header #%d [%x…]", i, b.Number(), b.Hash().Bytes()[:4], h.Number, h.Hash().Bytes()[:4])
			}
			if err := m2.InsertChain(forks[i], nil); err != nil {
				t.Fatalf(" fork %d: failed to insert into chain: %v", i, err)
			}
			b, err = m.BlockReader.CurrentBlock(tx)
			if err != nil {
				return err
			}
			h = rawdb.ReadCurrentHeader(tx)
			if b.Hash() != h.Hash() {
				t.Errorf(" fork %d: current block/header mismatch: block #%d [%x…], header #%d [%x…]", i, b.Number(), b.Hash().Bytes()[:4], h.Number, h.Hash().Bytes()[:4])
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// Tests that doing large reorgs works even if the state associated with the
// forking point is not available any more.
func TestLargeReorgTrieGC(t *testing.T) {
	// Generate the original common chain segment and the two competing forks

	m, m2 := mock.Mock(t), mock.Mock(t)

	shared, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 64, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	if err != nil {
		t.Fatalf("generate shared chain: %v", err)
	}
	original, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 64+2*core.TriesInMemory, func(i int, b *core.BlockGen) {
		if i < 64 {
			b.SetCoinbase(libcommon.Address{1})
		} else {
			b.SetCoinbase(libcommon.Address{2})
		}
	})
	if err != nil {
		t.Fatalf("generate original chain: %v", err)
	}
	competitor, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 64+2*core.TriesInMemory+1, func(i int, b *core.BlockGen) {
		if i < 64 {
			b.SetCoinbase(libcommon.Address{1})
		} else {
			b.SetCoinbase(libcommon.Address{3})
			b.OffsetTime(-2)
		}
	})
	if err != nil {
		t.Fatalf("generate competitor chain: %v", err)
	}

	// Import the shared chain and the original canonical one
	if err := m2.InsertChain(shared, nil); err != nil {
		t.Fatalf("failed to insert shared chain: %v", err)
	}
	if err := m2.InsertChain(original, nil); err != nil {
		t.Fatalf("failed to insert original chain: %v", err)
	}
	// Import the competitor chain without exceeding the canonical's TD and ensure
	// we have not processed any of the blocks (protection against malicious blocks)
	if err := m2.InsertChain(competitor.Slice(0, competitor.Length()-2), nil); err != nil {
		t.Fatalf("failed to insert competitor chain: %v", err)
	}
	// Import the head of the competitor chain, triggering the reorg and ensure we
	// successfully reprocess all the stashed away blocks.
	if err := m2.InsertChain(competitor.Slice(competitor.Length()-2, competitor.Length()), nil); err != nil {
		t.Fatalf("failed to finalize competitor chain: %v", err)
	}
}

// Tests that importing a very large side fork, which is larger than the canon chain,
// but where the difficulty per block is kept low: this means that it will not
// overtake the 'canon' chain until after it's passed canon by about 200 blocks.
//
// Details at:
//   - https://github.com/ethereum/go-ethereum/issues/18977
//   - https://github.com/ethereum/go-ethereum/pull/18988
func TestLowDiffLongChain(t *testing.T) {
	// Generate a canonical chain to act as the main dataset
	m := mock.Mock(t)

	// We must use a pretty long chain to ensure that the fork doesn't overtake us
	// until after at least 128 blocks post tip
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 6*core.TriesInMemory, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		b.OffsetTime(-9)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Generate fork chain, starting from an early block
	fork, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 11+8*core.TriesInMemory, func(i int, b *core.BlockGen) {
		if i < 11 {
			b.SetCoinbase(libcommon.Address{1})
			b.OffsetTime(-9)
		} else {
			b.SetCoinbase(libcommon.Address{2})
		}
	})
	if err != nil {
		t.Fatalf("generate fork: %v", err)
	}

	// Import the canonical chain
	m2 := mock.Mock(t)

	if err := m2.InsertChain(chain, nil); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	// And now import the fork
	if err := m2.InsertChain(fork, nil); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	if err := m2.DB.View(context.Background(), func(tx kv.Tx) error {
		head, err := m.BlockReader.CurrentBlock(tx)
		if err != nil {
			return err
		}
		if got := fork.TopBlock.Hash(); got != head.Hash() {
			t.Fatalf("head wrong, expected %x got %x", head.Hash(), got)
		}

		// Sanity check that all the canonical numbers are present
		header := rawdb.ReadCurrentHeader(tx)
		for number := head.NumberU64(); number > 0; number-- {
			hh, _ := m.BlockReader.HeaderByNumber(m.Ctx, tx, number)
			if hash := hh.Hash(); hash != header.Hash() {
				t.Fatalf("header %d: canonical hash mismatch: have %x, want %x", number, hash, header.Hash())
			}

			header, _ = m.BlockReader.Header(m.Ctx, tx, header.ParentHash, number-1)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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
	var (
		aa = libcommon.HexToAddress("0x000000000000000000000000000000000000aaaa")
		bb = libcommon.HexToAddress("0x000000000000000000000000000000000000bbbb")
		// Generate a canonical chain to act as the main dataset

		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: params.TestChainConfig,
			Alloc: types.GenesisAlloc{
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
	)
	m := mock.MockWithGenesis(t, gspec, key, false)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		// One transaction to AAAA
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		// One transaction to BBBB
		tx, _ = types.SignTx(types.NewTransaction(1, bb,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	if err := m.InsertChain(chain, nil); err != nil {
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
	var (
		// Generate a canonical chain to act as the main dataset
		// A sender who makes transactions, has some funds
		key, _    = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address   = crypto.PubkeyToAddress(key.PublicKey)
		funds     = big.NewInt(1000000000)
		bb        = libcommon.HexToAddress("0x000000000000000000000000000000000000bbbb")
		aaStorage = make(map[libcommon.Hash]libcommon.Hash)    // Initial storage in AA
		aaCode    = []byte{byte(vm.PC), byte(vm.SELFDESTRUCT)} // Code for AA (simple selfdestruct)
	)
	// Populate two slots
	aaStorage[libcommon.HexToHash("01")] = libcommon.HexToHash("01")
	aaStorage[libcommon.HexToHash("02")] = libcommon.HexToHash("02")

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

	gspec := &types.Genesis{
		Config: params.TestChainConfig,
		Alloc: types.GenesisAlloc{
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
	m := mock.MockWithGenesis(t, gspec, key, false)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		// One transaction to AA, to kill it
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		// One transaction to BB, to recreate AA
		tx, _ = types.SignTx(types.NewTransaction(1, bb,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	if err := m.InsertChain(chain, nil); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}
	err = m.DB.View(m.Ctx, func(tx kv.Tx) error {
		statedb := state.New(m.NewHistoryStateReader(2, tx))

		// If all is correct, then slot 1 and 2 are zero
		key1 := libcommon.HexToHash("01")
		var got uint256.Int
		statedb.GetState(aa, &key1, &got)
		if !got.IsZero() {
			t.Errorf("got %d exp %d", got.Uint64(), 0)
		}
		key2 := libcommon.HexToHash("02")
		statedb.GetState(aa, &key2, &got)
		if !got.IsZero() {
			t.Errorf("got %d exp %d", got.Uint64(), 0)
		}
		// Also, 3 and 4 should be set
		key3 := libcommon.HexToHash("03")
		statedb.GetState(aa, &key3, &got)
		if got.Uint64() != 3 {
			t.Errorf("got %d exp %d", got.Uint64(), 3)
		}
		key4 := libcommon.HexToHash("04")
		statedb.GetState(aa, &key4, &got)
		if got.Uint64() != 4 {
			t.Errorf("got %d exp %d", got.Uint64(), 4)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestCVE2020_26265(t *testing.T) {
	var (
		// Generate a canonical chain to act as the main dataset
		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)

		aa        = libcommon.HexToAddress("0x000000000000000000000000000000000000aaaa")
		aaStorage = make(map[libcommon.Hash]libcommon.Hash) // Initial storage in AA
		aaCode    = []byte{
			byte(vm.CALLVALUE),
			byte(vm.PUSH1), 0x06, // Destination for JUMPI
			byte(vm.JUMPI),
			byte(vm.ADDRESS),
			byte(vm.SELFDESTRUCT),
			byte(vm.JUMPDEST),
			byte(vm.SELFBALANCE),
			byte(vm.PUSH1), 0x00,
			byte(vm.SSTORE),
		} // Code for AAAA (selfdestruct to itself, but only when CALLVALUE is 0)

		caller        = libcommon.HexToAddress("0x000000000000000000000000000000000000bbbb")
		callerStorage = make(map[libcommon.Hash]libcommon.Hash) // Initial storage in CALLER
		callerCode    = []byte{
			byte(vm.PC),          // [0]
			byte(vm.DUP1),        // [0,0]
			byte(vm.DUP1),        // [0,0,0]
			byte(vm.DUP1),        // [0,0,0,0]
			byte(vm.PUSH1), 0x00, // [0,0,0,0,1] (value)
			byte(vm.PUSH2), 0xaa, 0xaa, // [0,0,0,0,1, 0xaaaa]
			byte(vm.GAS),
			byte(vm.CALL), // Cause self-destruct of aa

			byte(vm.PC),          // [0]
			byte(vm.DUP1),        // [0,0]
			byte(vm.DUP1),        // [0,0,0]
			byte(vm.DUP1),        // [0,0,0,0]
			byte(vm.PUSH1), 0x01, // [0,0,0,0,1] (value)
			byte(vm.PUSH2), 0xaa, 0xaa, // [0,0,0,0,1, 0xaaaa]
			byte(vm.GAS),
			byte(vm.CALL), // Send 1 wei to add

			byte(vm.RETURN),
		} // Code for CALLER
	)
	gspec := &types.Genesis{
		Config: params.TestChainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
			// The address 0xAAAAA selfdestructs if called
			aa: {
				// Code needs to just selfdestruct
				Code:    aaCode,
				Nonce:   1,
				Balance: big.NewInt(3),
				Storage: aaStorage,
			},
			caller: {
				// Code needs to just selfdestruct
				Code:    callerCode,
				Nonce:   1,
				Balance: big.NewInt(10),
				Storage: callerStorage,
			},
		},
	}
	m := mock.MockWithGenesis(t, gspec, key, false)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		// One transaction to AA, to kill it
		tx, _ := types.SignTx(types.NewTransaction(0, caller,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		// One transaction to AA, to recreate it (but without storage
		tx, _ = types.SignTx(types.NewTransaction(1, aa,
			new(uint256.Int).SetUint64(5), 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	if err := m.InsertChain(chain, nil); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}
	err = m.DB.View(m.Ctx, func(tx kv.Tx) error {
		reader := m.NewHistoryStateReader(2, tx)
		statedb := state.New(reader)

		got := statedb.GetBalance(aa)
		if !got.Eq(new(uint256.Int).SetUint64(5)) {
			t.Errorf("got %x exp %x", got, 5)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestDeleteRecreateAccount tests a state-transition that contains deletion of a
// contract with storage, and a recreate of the same contract via a
// regular value-transfer
// Expected outcome is that _all_ slots are cleared from A
func TestDeleteRecreateAccount(t *testing.T) {
	var (
		// Generate a canonical chain to act as the main dataset
		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)

		aa        = libcommon.HexToAddress("0x7217d81b76bdd8707601e959454e3d776aee5f43")
		aaStorage = make(map[libcommon.Hash]libcommon.Hash)    // Initial storage in AA
		aaCode    = []byte{byte(vm.PC), byte(vm.SELFDESTRUCT)} // Code for AA (simple selfdestruct)
	)
	// Populate two slots
	aaStorage[libcommon.HexToHash("01")] = libcommon.HexToHash("01")
	aaStorage[libcommon.HexToHash("02")] = libcommon.HexToHash("02")

	gspec := &types.Genesis{
		Config: params.TestChainConfig,
		Alloc: types.GenesisAlloc{
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
	m := mock.MockWithGenesis(t, gspec, key, false)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		// One transaction to AA, to kill it
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		// One transaction to AA, to recreate it (but without storage
		tx, _ = types.SignTx(types.NewTransaction(1, aa,
			u256.Num1, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	if err := m.InsertChain(chain, nil); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}
	err = m.DB.View(m.Ctx, func(tx kv.Tx) error {
		statedb := state.New(m.NewHistoryStateReader(2, tx))

		// If all is correct, then both slots are zero
		key1 := libcommon.HexToHash("01")
		var got uint256.Int
		statedb.GetState(aa, &key1, &got)
		if !got.IsZero() {
			t.Errorf("got %x exp %x", got, 0)
		}
		key2 := libcommon.HexToHash("02")
		statedb.GetState(aa, &key2, &got)
		if !got.IsZero() {
			t.Errorf("got %x exp %x", got, 0)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestDeleteRecreateSlotsAcrossManyBlocks tests multiple state-transition that contains both deletion
// and recreation of contract state.
// Contract A exists, has slots 1 and 2 set
// Tx 1: Selfdestruct A
// Tx 2: Re-create A, set slots 3 and 4
// Expected outcome is that _all_ slots are cleared from A, due to the selfdestruct,
// and then the new slots exist
func TestDeleteRecreateSlotsAcrossManyBlocks(t *testing.T) {
	var (
		// Generate a canonical chain to act as the main dataset
		// A sender who makes transactions, has some funds
		key, _    = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address   = crypto.PubkeyToAddress(key.PublicKey)
		funds     = big.NewInt(1000000000)
		bb        = libcommon.HexToAddress("0x000000000000000000000000000000000000bbbb")
		aaStorage = make(map[libcommon.Hash]libcommon.Hash)    // Initial storage in AA
		aaCode    = []byte{byte(vm.PC), byte(vm.SELFDESTRUCT)} // Code for AA (simple selfdestruct)
	)

	// Populate two slots
	aaStorage[libcommon.HexToHash("01")] = libcommon.HexToHash("01")
	aaStorage[libcommon.HexToHash("02")] = libcommon.HexToHash("02")

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
	gspec := &types.Genesis{
		Config: params.TestChainConfig,
		Alloc: types.GenesisAlloc{
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
	m := mock.MockWithGenesis(t, gspec, key, false)
	var nonce uint64

	type expectation struct {
		values   map[int]int
		exist    bool
		blocknum int
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

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 150, func(i int, b *core.BlockGen) {
		var exp = new(expectation)
		exp.blocknum = i + 1
		exp.values = make(map[int]int)
		for k, v := range current.values {
			exp.values[k] = v
		}
		exp.exist = current.exist

		b.SetCoinbase(libcommon.Address{1})
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
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	var asHash = func(num int) libcommon.Hash {
		return libcommon.BytesToHash([]byte{byte(num)})
	}
	for i := range chain.Blocks {
		blockNum := i + 1
		if err := m.InsertChain(chain.Slice(i, i+1), nil); err != nil {
			t.Fatalf("block %d: failed to insert into chain: %v", i, err)
		}
		err = m.DB.View(m.Ctx, func(tx kv.Tx) error {

			statedb := state.New(m.NewStateReader(tx))
			// If all is correct, then slot 1 and 2 are zero
			key1 := libcommon.HexToHash("01")
			var got uint256.Int
			statedb.GetState(aa, &key1, &got)
			if !got.IsZero() {
				t.Errorf("block %d, got %x exp %x", blockNum, got, 0)
			}
			key2 := libcommon.HexToHash("02")
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
			return nil
		})
		require.NoError(t, err)
	}
}

// TestInitThenFailCreateContract tests a pretty notorious case that happened
// on mainnet over blocks 7338108, 7338110 and 7338115.
//   - Block 7338108: address e771789f5cccac282f23bb7add5690e1f6ca467c is initiated
//     with 0.001 ether (thus created but no code)
//   - Block 7338110: a CREATE2 is attempted. The CREATE2 would deploy code on
//     the same address e771789f5cccac282f23bb7add5690e1f6ca467c. However, the
//     deployment fails due to OOG during initcode execution
//   - Block 7338115: another tx checks the balance of
//     e771789f5cccac282f23bb7add5690e1f6ca467c, and the snapshotter returned it as
//     zero.
//
// The problem being that the snapshotter maintains a destructset, and adds items
// to the destructset in case something is created "onto" an existing item.
// We need to either roll back the snapDestructs, or not place it into snapDestructs
// in the first place.
func TestInitThenFailCreateContract(t *testing.T) {
	var (
		// Generate a canonical chain to act as the main dataset
		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		bb      = libcommon.HexToAddress("0x000000000000000000000000000000000000bbbb")
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

	gspec := &types.Genesis{
		Config: params.TestChainConfig,
		Alloc: types.GenesisAlloc{
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
	m := mock.MockWithGenesis(t, gspec, key, false)
	nonce := uint64(0)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		// One transaction to BB
		tx, _ := types.SignTx(types.NewTransaction(nonce, bb,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		nonce++
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	err = m.DB.View(m.Ctx, func(tx kv.Tx) error {

		// Import the canonical chain
		statedb := state.New(m.NewHistoryStateReader(2, tx))
		if got, exp := statedb.GetBalance(aa), uint64(100000); got.Uint64() != exp {
			t.Fatalf("Genesis err, got %v exp %v", got, exp)
		}
		// First block tries to create, but fails
		{
			block := chain.Blocks[0]
			if err := m.InsertChain(chain.Slice(0, 1), nil); err != nil {
				t.Fatalf("block %d: failed to insert into chain: %v", block.NumberU64(), err)
			}
			statedb = state.New(m.NewHistoryStateReader(1, tx))
			if got, exp := statedb.GetBalance(aa), uint64(100000); got.Uint64() != exp {
				t.Fatalf("block %d: got %v exp %v", block.NumberU64(), got, exp)
			}
		}
		// Import the rest of the blocks
		for i, block := range chain.Blocks[1:] {
			if err := m.InsertChain(chain.Slice(1+i, 2+i), nil); err != nil {
				t.Fatalf("block %d: failed to insert into chain: %v", block.NumberU64(), err)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// TestEIP2718Transition tests that an EIP-2718 transaction will be accepted
// after the fork block has passed. This is verified by sending an EIP-2930
// access list transaction, which specifies a single slot access, and then
// checking that the gas usage of a hot SLOAD and a cold SLOAD are calculated
// correctly.
func TestEIP2718Transition(t *testing.T) {
	var (
		aa = libcommon.HexToAddress("0x000000000000000000000000000000000000aaaa")

		// Generate a canonical chain to act as the main dataset

		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: params.TestChainConfig,
			Alloc: types.GenesisAlloc{
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
	)
	m := mock.MockWithGenesis(t, gspec, key, false)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
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
			AccessList: types2.AccessList{{
				Address:     aa,
				StorageKeys: []libcommon.Hash{{0}},
			}},
		})
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	// Import the canonical chain

	if err = m.InsertChain(chain, nil); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	tx, err := m.DB.BeginRo(m.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	block, _ := m.BlockReader.BlockByNumber(m.Ctx, tx, 1)

	// Expected gas is intrinsic + 2 * pc + hot load + cold load, since only one load is in the access list
	expected := params.TxGas + params.TxAccessListAddressGas + params.TxAccessListStorageKeyGas +
		vm.GasQuickStep*2 + params.WarmStorageReadCostEIP2929 + params.ColdSloadCostEIP2929
	if block.GasUsed() != expected {
		t.Fatalf("incorrect amount of gas spent: expected %d, got %d", expected, block.GasUsed())

	}
}

// TestEIP1559Transition tests the following:
//
//  1. A tranaction whose feeCap is greater than the baseFee is valid.
//  2. Gas accounting for access lists on EIP-1559 transactions is correct.
//  3. Only the transaction's tip will be received by the coinbase.
//  4. The transaction sender pays for both the tip and baseFee.
//  5. The coinbase receives only the partially realized tip when
//     feeCap - tip < baseFee.
//  6. Legacy transaction behave as expected (e.g. gasPrice = feeCap = tip).
func TestEIP1559Transition(t *testing.T) {
	t.Skip("needs fixing")
	var (
		aa = libcommon.HexToAddress("0x000000000000000000000000000000000000aaaa")

		// Generate a canonical chain to act as the main dataset

		// A sender who makes transactions, has some funds
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key2, _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		addr2   = crypto.PubkeyToAddress(key2.PublicKey)
		funds   = new(uint256.Int).Mul(u256.Num1, new(uint256.Int).SetUint64(params.Ether))
		gspec   = &types.Genesis{
			Config: params.GoerliChainConfig,
			Alloc: types.GenesisAlloc{
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
		signer = types.LatestSigner(gspec.Config)
	)
	m := mock.MockWithGenesis(t, gspec, key1, false)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 501, func(i int, b *core.BlockGen) {
		if i == 500 {
			b.SetCoinbase(libcommon.Address{1})
		} else {
			b.SetCoinbase(libcommon.Address{0})
		}
		if i == 500 {
			// One transaction to 0xAAAA
			accesses := types2.AccessList{types2.AccessTuple{
				Address:     aa,
				StorageKeys: []libcommon.Hash{{0}},
			}}

			var chainID uint256.Int
			chainID.SetFromBig(gspec.Config.ChainID)
			var tx types.Transaction = &types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce: 0,
					To:    &aa,
					Gas:   30000,
					Data:  []byte{},
				},
				ChainID:    &chainID,
				FeeCap:     new(uint256.Int).Mul(new(uint256.Int).SetUint64(5), new(uint256.Int).SetUint64(params.GWei)),
				Tip:        u256.Num2,
				AccessList: accesses,
			}
			tx, _ = types.SignTx(tx, *signer, key1)

			b.AddTx(tx)
		}
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain

	if err := m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		if err = m.InsertChain(chain, tx); err != nil {
			t.Fatalf("failed to insert into chain: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	block := chain.Blocks[500]

	// 1+2: Ensure EIP-1559 access lists are accounted for via gas usage.
	expectedGas := params.TxGas + params.TxAccessListAddressGas + params.TxAccessListStorageKeyGas + vm.GasQuickStep*2 + params.WarmStorageReadCostEIP2929 + params.ColdSloadCostEIP2929
	if block.GasUsed() != expectedGas {
		t.Fatalf("incorrect amount of gas spent: expected %d, got %d", expectedGas, block.GasUsed())
	}

	err = m.DB.View(m.Ctx, func(tx kv.Tx) error {
		statedb := state.New(m.NewHistoryStateReader(1, tx))

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

		return nil
	})
	require.NoError(t, err)

	chain, err = core.GenerateChain(m.ChainConfig, block, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{2})

		var tx types.Transaction = types.NewTransaction(0, aa, u256.Num0, 30000, new(uint256.Int).Mul(new(uint256.Int).SetUint64(5), new(uint256.Int).SetUint64(params.GWei)), nil)
		tx, _ = types.SignTx(tx, *signer, key2)

		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	if err = m.InsertChain(chain, nil); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	block = chain.Blocks[0]
	err = m.DB.View(m.Ctx, func(tx kv.Tx) error {
		statedb := state.New(m.NewHistoryStateReader(1, tx))
		effectiveTip := block.Transactions()[0].GetPrice().Uint64() - block.BaseFee().Uint64()

		// 6+5: Ensure that miner received only the tx's effective tip.
		actual := statedb.GetBalance(block.Coinbase())
		expected := new(uint256.Int).Add(
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
		return nil
	})
	require.NoError(t, err)
}

func current(m *mock.MockSentry, tx kv.Tx) *types.Block {
	if tx != nil {
		b, err := m.BlockReader.CurrentBlock(tx)
		if err != nil {
			panic(err)
		}
		return b
	}
	tx, err := m.DB.BeginRo(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	b, err := m.BlockReader.CurrentBlock(tx)
	if err != nil {
		panic(err)
	}
	return b
}
