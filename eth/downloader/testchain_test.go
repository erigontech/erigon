// Copyright 2018 The go-ethereum Authors
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

package downloader

import (
	"math/big"
	"os"
	"sync"
	"testing"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// Test chain parameters.
var (
	testKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddress = crypto.PubkeyToAddress(testKey.PublicKey)

	testDb      = ethdb.NewMemDatabase()
	testGenesis = core.GenesisWithAccounts(testDb, []core.GenAccount{
		{Addr: testAddress,
			Balance: big.NewInt(1000000000),
		},
	})

	// The common prefix of all test chains:
	// Different forks on top of the base chain:
	testChainBase   *testChain
	testChainBaseMu sync.Mutex

	testChainForkLightA   *testChain
	testChainForkLightAMu sync.Mutex

	testChainForkHeavy   *testChain
	testChainForkHeavyMu sync.Mutex
)

func getTestChainForkLightA() *testChain {
	testChainForkLightAMu.Lock()
	defer testChainForkLightAMu.Unlock()
	if testChainForkLightA == nil {
		testChainForkLightA = getTestChainBase().makeFork(getForkLen(), false, 1)
	}
	return testChainForkLightA
}
func getTestChainForkHeavy() *testChain {
	testChainForkHeavyMu.Lock()
	defer testChainForkHeavyMu.Unlock()
	if testChainForkHeavy == nil {
		testChainForkHeavy = getTestChainBase().makeFork(getForkLen()+1, true, 3)
	}
	return testChainForkHeavy
}
func getTestChainBase() *testChain {
	testChainBaseMu.Lock()
	defer testChainBaseMu.Unlock()
	if testChainBase == nil {
		testChainBase = newTestChain(OverwriteBlockCacheItems+200, testDb, testGenesis)
	}
	return testChainBase
}

func getForkLen() int {
	return int(fullMaxForkAncestry + 50)
}

func TestMain(m *testing.M) {
	result := m.Run()

	// teardown
	testDb.Close()

	os.Exit(result)
}

type testChain struct {
	db       *ethdb.ObjectDatabase
	genesis  *types.Block
	chain    []common.Hash
	headerm  map[common.Hash]*types.Header
	blockm   map[common.Hash]*types.Block
	receiptm map[common.Hash][]*types.Receipt
	tdm      map[common.Hash]*big.Int
	cpyLock  sync.Mutex
}

// newTestChain creates a blockchain of the given length.
func newTestChain(length int, db *ethdb.ObjectDatabase, genesis *types.Block) *testChain {
	tc := new(testChain).copy(length)
	tc.db = db
	tc.genesis = genesis
	tc.chain = append(tc.chain, genesis.Hash())
	tc.headerm[tc.genesis.Hash()] = tc.genesis.Header()
	tc.tdm[tc.genesis.Hash()] = tc.genesis.Difficulty()
	tc.blockm[tc.genesis.Hash()] = tc.genesis
	tc.generate(length-1, 0, genesis, false)
	return tc
}

// makeFork creates a fork on top of the test chain.
func (tc *testChain) makeFork(length int, heavy bool, seed byte) *testChain {
	fork := tc.copy(tc.len() + length)
	fork.generate(length, seed, tc.headBlock(), heavy)
	return fork
}

func (tc *testChain) copy(newlen int) *testChain {
	tc.cpyLock.Lock()
	defer tc.cpyLock.Unlock()
	db := tc.db
	if tc.db != nil {
		db = tc.db.MemCopy()
	}
	cpy := &testChain{
		genesis:  tc.genesis,
		db:       db,
		headerm:  make(map[common.Hash]*types.Header, newlen),
		blockm:   make(map[common.Hash]*types.Block, newlen),
		receiptm: make(map[common.Hash][]*types.Receipt, newlen),
		tdm:      make(map[common.Hash]*big.Int, newlen),
	}
	for i := 0; i < len(tc.chain) && i < newlen; i++ {
		hash := tc.chain[i]
		cpy.chain = append(cpy.chain, tc.chain[i])
		cpy.tdm[hash] = tc.tdm[hash]
		cpy.blockm[hash] = tc.blockm[hash]
		cpy.headerm[hash] = tc.headerm[hash]
		cpy.receiptm[hash] = tc.receiptm[hash]
	}
	return cpy
}

// generate creates a chain of n blocks starting at and including parent.
// the returned hash chain is ordered head->parent. In addition, every 22th block
// contains a transaction and every 5th an uncle to allow testing correct block
// reassembly.
func (tc *testChain) generate(n int, seed byte, parent *types.Block, heavy bool) {
	tc.cpyLock.Lock()
	defer tc.cpyLock.Unlock()

	zeroAddress := common.Address{0}
	seedAddress := common.Address{seed}
	amount := uint256.NewInt().SetUint64(1000)

	var parentBlock *types.Block
	existingLen := len(tc.chain) - 1
	totalLen := existingLen + n
	signer := types.MakeSigner(params.TestChainConfig, 1)

	testGenesisNonce := int64(-1)
	blocks, receipts, err := core.GenerateChain(params.TestChainConfig, tc.genesis, ethash.NewFaker(), tc.db, totalLen, func(i int, block *core.BlockGen) {
		if i < existingLen || existingLen == 0 {
			block.SetCoinbase(zeroAddress)

			// Include transactions to the miner to make blocks more interesting.
			if i%22 == 0 {
				if testGenesisNonce == -1 {
					testGenesisNonce = int64(block.TxNonce(testAddress))
				}
				tx, err := types.SignTx(types.NewTransaction(uint64(testGenesisNonce), zeroAddress, amount, params.TxGas, nil, nil), *signer, testKey)
				if err != nil {
					panic(err)
				}

				block.AddTx(tx)
				testGenesisNonce++
			}

			// if the block number is a multiple of 5, add a bonus uncle to the block
			if i > 0 && i%5 == 0 {
				parentBlock = block.GetParent()
				block.AddUncle(&types.Header{
					ParentHash: parentBlock.Hash(),
					Number:     parentBlock.Number(),
				})
			}
		} else {
			block.SetCoinbase(seedAddress)

			// If a heavy chain is requested, delay blocks to raise difficulty
			if heavy {
				block.OffsetTime(-1)
			}

			// if the block number is a multiple of 5, add a bonus uncle to the block
			if i > existingLen && (i-existingLen)%5 == 0 {
				parentBlock = block.GetParent()
				block.AddUncle(&types.Header{
					ParentHash: parentBlock.Hash(),
					Number:     parentBlock.Number(),
				})
			}
		}
	}, false /* intermediateHashes */)
	if err != nil {
		panic(err)
	}

	// Convert the block-chain into a hash-chain and header/block maps
	td := new(big.Int).Set(tc.td(parent.Hash()))
	for i, b := range blocks[existingLen:] {
		td = td.Add(td, b.Difficulty())
		hash := b.Hash()
		tc.chain = append(tc.chain, hash)
		tc.blockm[hash] = b
		tc.headerm[hash] = b.Header()
		tc.receiptm[hash] = receipts[existingLen+i]
		tc.tdm[hash] = new(big.Int).Set(td)
	}
}

// len returns the total number of blocks in the chain.
func (tc *testChain) len() int {
	return len(tc.chain)
}

// headBlock returns the head of the chain.
func (tc *testChain) headBlock() *types.Block {
	return tc.blockm[tc.chain[len(tc.chain)-1]]
}

// td returns the total difficulty of the given block.
func (tc *testChain) td(hash common.Hash) *big.Int {
	return tc.tdm[hash]
}

// headersByHash returns headers in order from the given hash.
func (tc *testChain) headersByHash(origin common.Hash, amount int, skip int, reverse bool) []*types.Header {
	num, _ := tc.hashToNumber(origin)
	return tc.headersByNumber(num, amount, skip, reverse)
}

// headersByNumber returns headers from the given number.
func (tc *testChain) headersByNumber(origin uint64, amount int, skip int, reverse bool) []*types.Header {
	result := make([]*types.Header, 0, amount)

	if !reverse {
		for num := origin; num < uint64(len(tc.chain)) && len(result) < amount; num += uint64(skip) + 1 {
			if header, ok := tc.headerm[tc.chain[int(num)]]; ok {
				result = append(result, header)
			}
		}
	} else {
		for num := int64(origin); num >= 0 && len(result) < amount; num -= int64(skip) + 1 {
			if header, ok := tc.headerm[tc.chain[int(num)]]; ok {
				result = append(result, header)
			}
		}
	}
	return result
}

// bodies returns the block bodies of the given block hashes.
func (tc *testChain) bodies(hashes []common.Hash) ([][]types.Transaction, [][]*types.Header) {
	transactions := make([][]types.Transaction, 0, len(hashes))
	uncles := make([][]*types.Header, 0, len(hashes))
	for _, hash := range hashes {
		if block, ok := tc.blockm[hash]; ok {
			transactions = append(transactions, block.Transactions())
			uncles = append(uncles, block.Uncles())
		}
	}
	return transactions, uncles
}

func (tc *testChain) hashToNumber(target common.Hash) (uint64, bool) {
	for num, hash := range tc.chain {
		if hash == target {
			return uint64(num), true
		}
	}
	return 0, false
}
