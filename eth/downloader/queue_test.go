// Copyright 2019 The go-ethereum Authors
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
	"fmt"
	"math/big"
	"sync"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

var (
	testdb  = ethdb.NewMemDatabase()
	genesis = core.GenesisBlockForTesting(testdb, testAddress, big.NewInt(1000000000))
)

// makeChain creates a chain of n blocks starting at and including parent.
// the returned hash chain is ordered head->parent. In addition, every 3rd block
// contains a transaction and every 5th an uncle to allow testing correct block
// reassembly.
func makeChain(n int, seed byte, parent *types.Block, empty bool) ([]*types.Block, []types.Receipts) { //nolint:unparam
	blocks, receipts, err := core.GenerateChain(params.TestChainConfig, parent, ethash.NewFaker(), testdb, n, func(i int, block *core.BlockGen) {
		block.SetCoinbase(common.Address{seed})
		// Add one tx to every secondblock
		if !empty && i%2 == 0 {
			signer := types.MakeSigner(params.TestChainConfig, block.Number())
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testAddress), common.Address{seed}, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, testKey)
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		}
	}, false)

	if err != nil {
		panic(err)
	}

	return blocks, receipts
}

type chainData struct {
	blocks []*types.Block
	offset int
}

var chain *chainData
var emptyChain *chainData
var chainsMu sync.Mutex

const targetTestBlocks = 128

func getEmptyChain() *chainData {
	chainsMu.Lock()
	defer chainsMu.Unlock()
	if emptyChain == nil {
		blocks, _ := makeChain(targetTestBlocks, 0, genesis, true)
		emptyChain = &chainData{blocks, 0}
	}
	return emptyChain
}
func getChain() *chainData {
	chainsMu.Lock()
	defer chainsMu.Unlock()
	if chain == nil {
		blocks, _ := makeChain(targetTestBlocks, 0, genesis, false)
		chain = &chainData{blocks, 0}
	}
	return chain
}

func (chain *chainData) headers() []*types.Header {
	hdrs := make([]*types.Header, len(chain.blocks))
	for i, b := range chain.blocks {
		hdrs[i] = b.Header()
	}
	return hdrs
}

func (chain *chainData) Len() int {
	return len(chain.blocks)
}

func dummyPeer(id string) *peerConnection {
	p := &peerConnection{
		id:      id,
		lacking: make(map[common.Hash]struct{}),
	}
	return p
}

func newNetwork() *network {
	var l sync.RWMutex
	return &network{
		cond:   sync.NewCond(&l),
		offset: 1, // block 1 is at blocks[0]
	}
}

// represents the network
type network struct {
	offset   int
	chain    []*types.Block
	receipts []types.Receipts
	lock     sync.RWMutex
	cond     *sync.Cond
}

func (n *network) getTransactions(blocknum uint64) types.Transactions {
	index := blocknum - uint64(n.offset)
	return n.chain[index].Transactions()
}
func (n *network) getReceipts(blocknum uint64) types.Receipts {
	index := blocknum - uint64(n.offset)
	if got := n.chain[index].Header().Number.Uint64(); got != blocknum {
		fmt.Printf("Err, got %d exp %d\n", got, blocknum)
		panic("sd")
	}
	return n.receipts[index]
}

func (n *network) forget(blocknum uint64) {
	index := blocknum - uint64(n.offset)
	n.chain = n.chain[index:]
	n.receipts = n.receipts[index:]
	n.offset = int(blocknum)

}
func (n *network) progress(numBlocks int) {

	n.lock.Lock()
	defer n.lock.Unlock()
	//fmt.Printf("progressing...\n")
	newBlocks, newR := makeChain(numBlocks, 0, n.chain[len(n.chain)-1], false)
	n.chain = append(n.chain, newBlocks...)
	n.receipts = append(n.receipts, newR...)
	n.cond.Broadcast()

}

func (n *network) headers(from int) []*types.Header {
	numHeaders := 128
	var hdrs []*types.Header //nolint:prealloc
	index := from - n.offset

	for index >= len(n.chain) {
		// wait for progress
		n.cond.L.Lock()
		//fmt.Printf("header going into wait\n")
		n.cond.Wait()
		index = from - n.offset
		n.cond.L.Unlock()
	}
	n.lock.RLock()
	defer n.lock.RUnlock()
	for i, b := range n.chain[index:] {
		hdrs = append(hdrs, b.Header())
		if i >= numHeaders {
			break
		}
	}
	return hdrs
}
