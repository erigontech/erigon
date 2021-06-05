// Copyright 2015 The go-ethereum Authors
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

package eth

import (
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
)

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)
)

// testBackend is a mock implementation of the live Ethereum message handler. Its
// purpose is to allow testing the request/reply workflows and wire serialization
// in the `eth` protocol without actually doing any data processing.
type testBackend struct {
	db          ethdb.Database
	txpool      *core.TxPool
	headBlock   *types.Block
	genesis     *types.Block
	chainConfig *params.ChainConfig
}

// newTestBackend creates an empty chain and wraps it into a mock backend.
func newTestBackend(t *testing.T, blocks int) *testBackend {
	return newTestBackendWithGenerator(t, blocks, nil)
}

// newTestBackend creates a chain with a number of explicitly defined blocks and
// wraps it into a mock backend.
func newTestBackendWithGenerator(t *testing.T, blocks int, generator func(int, *core.BlockGen)) *testBackend {
	// Create a database pre-initialize with a genesis block
	db := ethdb.NewTestDB(t)
	genesis := (&core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{testAddr: {Balance: big.NewInt(1000000)}},
	}).MustCommit(db)

	headBlock := genesis
	if blocks > 0 {
		chain, _ := core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db.RwKV(), blocks, generator, true)
		if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, ethash.NewFaker(), chain.Blocks, true /* checkRoot */); err != nil {
			panic(err)
		}
		headBlock = chain.TopBlock
	}
	txconfig := core.DefaultTxPoolConfig
	txconfig.Journal = "" // Don't litter the disk with test journals

	b := &testBackend{
		db:          db,
		txpool:      core.NewTxPool(txconfig, params.TestChainConfig, db),
		headBlock:   headBlock,
		genesis:     genesis,
		chainConfig: params.TestChainConfig,
	}
	t.Cleanup(func() {
		b.txpool.Stop()
	})
	return b
}

func (b *testBackend) DB() ethdb.RwKV { return b.db.(ethdb.HasRwKV).RwKV() }
func (b *testBackend) TxPool() TxPool { return b.txpool }
func (b *testBackend) RunPeer(peer *Peer, handler Handler) error {
	// Normally the backend would do peer mainentance and handshakes. All that
	// is omitted and we will just give control back to the handler.
	return handler(peer)
}
func (b *testBackend) PeerInfo(enode.ID) interface{} { panic("not implemented") }

func (b *testBackend) AcceptTxs() bool {
	panic("data processing tests should be done in the handler package")
}
func (b *testBackend) Handle(*Peer, Packet) error {
	panic("data processing tests should be done in the handler package")
}
func (b *testBackend) GetBlockHashesFromHash(hash common.Hash, max uint64) []common.Hash {
	// Get the origin header from which to fetch
	header, _ := rawdb.ReadHeaderByHash(b.db, hash)
	if header == nil {
		return nil
	}
	// Iterate the headers until enough is collected or the genesis reached
	chain := make([]common.Hash, 0, max)
	for i := uint64(0); i < max; i++ {
		next := header.ParentHash
		if header = rawdb.ReadHeader(b.db, next, header.Number.Uint64()-1); header == nil {
			break
		}
		chain = append(chain, next)
		if header.Number.Sign() == 0 {
			break
		}
	}
	return chain
}

// Tests that block headers can be retrieved from a remote chain based on user queries.
func TestGetBlockHeaders64(t *testing.T) { testGetBlockHeaders(t, 64) }
func TestGetBlockHeaders65(t *testing.T) { testGetBlockHeaders(t, 65) }

func testGetBlockHeaders(t *testing.T, protocol uint) {
	backend := newTestBackend(t, maxHeadersServe+15)

	peer, _ := newTestPeer("peer", protocol, backend)
	defer peer.close()

	// Create a "random" unknown hash for testing
	var unknown common.Hash
	for i := range unknown {
		unknown[i] = byte(i)
	}
	getBlockHash := func(n uint64) common.Hash {
		h, _ := rawdb.ReadCanonicalHash(backend.db, n)
		return h

	}
	// Create a batch of tests for various scenarios
	limit := uint64(maxHeadersServe)
	tests := []struct {
		query  *GetBlockHeadersPacket // The query to execute for header retrieval
		expect []common.Hash          // The hashes of the block whose headers are expected
	}{
		// A single random block should be retrievable by hash and number too
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Hash: getBlockHash(limit / 2)}, Amount: 1},
			[]common.Hash{getBlockHash(limit / 2)},
		}, {
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: limit / 2}, Amount: 1},
			[]common.Hash{getBlockHash(limit / 2)},
		},
		// Multiple headers should be retrievable in both directions
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: limit / 2}, Amount: 3},
			[]common.Hash{
				getBlockHash(limit / 2),
				getBlockHash(limit/2 + 1),
				getBlockHash(limit/2 + 2),
			},
		}, {
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: limit / 2}, Amount: 3, Reverse: true},
			[]common.Hash{
				getBlockHash(limit / 2),
				getBlockHash(limit/2 - 1),
				getBlockHash(limit/2 - 2),
			},
		},
		// Multiple headers with skip lists should be retrievable
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: limit / 2}, Skip: 3, Amount: 3},
			[]common.Hash{
				getBlockHash(limit / 2),
				getBlockHash(limit/2 + 4),
				getBlockHash(limit/2 + 8),
			},
		}, {
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: limit / 2}, Skip: 3, Amount: 3, Reverse: true},
			[]common.Hash{
				getBlockHash(limit / 2),
				getBlockHash(limit/2 - 4),
				getBlockHash(limit/2 - 8),
			},
		},
		// The chain endpoints should be retrievable
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: 0}, Amount: 1},
			[]common.Hash{getBlockHash(0)},
		}, {
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: rawdb.ReadCurrentHeader(backend.db).Number.Uint64()}, Amount: 1},
			[]common.Hash{rawdb.ReadCurrentHeader(backend.db).Hash()},
		},
		// Ensure protocol limits are honored
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: rawdb.ReadCurrentHeader(backend.db).Number.Uint64() - 1}, Amount: limit + 10, Reverse: true},
			backend.GetBlockHashesFromHash(rawdb.ReadCurrentHeader(backend.db).Hash(), limit),
		},
		// Check that requesting more than available is handled gracefully
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: rawdb.ReadCurrentHeader(backend.db).Number.Uint64() - 4}, Skip: 3, Amount: 3},
			[]common.Hash{
				getBlockHash(rawdb.ReadCurrentHeader(backend.db).Number.Uint64() - 4),
				getBlockHash(rawdb.ReadCurrentHeader(backend.db).Number.Uint64()),
			},
		}, {
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: 4}, Skip: 3, Amount: 3, Reverse: true},
			[]common.Hash{
				getBlockHash(4),
				getBlockHash(0),
			},
		},
		// Check that requesting more than available is handled gracefully, even if mid skip
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: rawdb.ReadCurrentHeader(backend.db).Number.Uint64() - 4}, Skip: 2, Amount: 3},
			[]common.Hash{
				getBlockHash(rawdb.ReadCurrentHeader(backend.db).Number.Uint64() - 4),
				getBlockHash(rawdb.ReadCurrentHeader(backend.db).Number.Uint64() - 1),
			},
		}, {
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: 4}, Skip: 2, Amount: 3, Reverse: true},
			[]common.Hash{
				getBlockHash(4),
				getBlockHash(1),
			},
		},
		// Check a corner case where requesting more can iterate past the endpoints
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: 2}, Amount: 5, Reverse: true},
			[]common.Hash{
				getBlockHash(2),
				getBlockHash(1),
				getBlockHash(0),
			},
		},
		// Check a corner case where skipping overflow loops back into the chain start
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Hash: getBlockHash(3)}, Amount: 2, Reverse: false, Skip: math.MaxUint64 - 1},
			[]common.Hash{
				getBlockHash(3),
			},
		},
		// Check a corner case where skipping overflow loops back to the same header
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Hash: getBlockHash(1)}, Amount: 2, Reverse: false, Skip: math.MaxUint64},
			[]common.Hash{
				getBlockHash(1),
			},
		},
		// Check that non existing headers aren't returned
		{
			&GetBlockHeadersPacket{Origin: HashOrNumber{Hash: unknown}, Amount: 1},
			[]common.Hash{},
		}, {
			&GetBlockHeadersPacket{Origin: HashOrNumber{Number: rawdb.ReadCurrentHeader(backend.db).Number.Uint64() + 1}, Amount: 1},
			[]common.Hash{},
		},
	}
	// Run each of the tests and verify the results against the chain
	for i, tt := range tests {
		// Collect the headers to expect in the response
		var headers []*types.Header
		for _, hash := range tt.expect {
			h, _ := rawdb.ReadHeaderByHash(backend.db, hash)

			headers = append(headers, h)
		}
		// Send the hash request and verify the response
		if err := p2p.Send(peer.app, GetBlockHeadersMsg, tt.query); err != nil {
			t.Fatal(err)
		}
		if err := p2p.ExpectMsg(peer.app, BlockHeadersMsg, headers); err != nil {
			t.Errorf("test %d: headers mismatch: %v", i, err)
		}
		// If the test used number origins, repeat with hashes as the too
		if tt.query.Origin.Hash == (common.Hash{}) {
			if origin := rawdb.ReadHeaderByNumber(backend.db, tt.query.Origin.Number); origin != nil {
				tt.query.Origin.Hash, tt.query.Origin.Number = origin.Hash(), 0

				if err := p2p.Send(peer.app, GetBlockHeadersMsg, tt.query); err != nil {
					t.Fatal(err)
				}
				if err := p2p.ExpectMsg(peer.app, BlockHeadersMsg, headers); err != nil {
					t.Errorf("test %d: headers mismatch: %v", i, err)
				}
			}
		}
	}
}

// Tests that block contents can be retrieved from a remote chain based on their hashes.
func TestGetBlockBodies64(t *testing.T) { testGetBlockBodies(t, 64) }
func TestGetBlockBodies65(t *testing.T) { testGetBlockBodies(t, 65) }

func testGetBlockBodies(t *testing.T, protocol uint) {
	backend := newTestBackend(t, maxBodiesServe+15)

	peer, _ := newTestPeer("peer", protocol, backend)
	defer peer.close()

	block1 := rawdb.ReadHeaderByNumber(backend.db, 1)
	block10 := rawdb.ReadHeaderByNumber(backend.db, 10)
	block100 := rawdb.ReadHeaderByNumber(backend.db, 100)

	// Create a batch of tests for various scenarios
	limit := maxBodiesServe
	tests := []struct {
		random    int           // Number of blocks to fetch randomly from the chain
		explicit  []common.Hash // Explicitly requested blocks
		available []bool        // Availability of explicitly requested blocks
		expected  int           // Total number of existing blocks to expect
	}{
		{1, nil, nil, 1},             // A single random block should be retrievable
		{10, nil, nil, 10},           // Multiple random blocks should be retrievable
		{limit, nil, nil, limit},     // The maximum possible blocks should be retrievable
		{limit + 1, nil, nil, limit}, // No more than the possible block count should be returned
		{0, []common.Hash{backend.genesis.Hash()}, []bool{true}, 1},                     // The genesis block should be retrievable
		{0, []common.Hash{rawdb.ReadCurrentHeader(backend.db).Hash()}, []bool{true}, 1}, // The chains head block should be retrievable
		{0, []common.Hash{{}}, []bool{false}, 0},                                        // A non existent block should not be returned

		// Existing and non-existing blocks interleaved should not cause problems
		{0, []common.Hash{
			{},
			block1.Hash(),
			{},
			block10.Hash(),
			{},
			block100.Hash(),
			{},
		}, []bool{false, true, false, true, false, true, false}, 3},
	}
	// Run each of the tests and verify the results against the chain
	for i, tt := range tests {
		// Collect the hashes to request, and the response to expectva
		var (
			hashes []common.Hash
			bodies []*BlockBody
			seen   = make(map[int64]bool)
		)
		for j := 0; j < tt.random; j++ {
			for {
				num := rand.Int63n(int64(rawdb.ReadCurrentHeader(backend.db).Number.Uint64()))
				if !seen[num] {
					seen[num] = true

					block, _ := rawdb.ReadBlockByNumberDeprecated(backend.db, uint64(num))
					hashes = append(hashes, block.Hash())
					if len(bodies) < tt.expected {
						bodies = append(bodies, &BlockBody{Transactions: block.Transactions(), Uncles: block.Uncles()})
					}
					break
				}
			}
		}
		for j, hash := range tt.explicit {
			hashes = append(hashes, hash)
			if tt.available[j] && len(bodies) < tt.expected {
				block, _ := rawdb.ReadBlockByHashDeprecated(backend.db, hash)
				bodies = append(bodies, &BlockBody{Transactions: block.Transactions(), Uncles: block.Uncles()})
			}
		}
		// Send the hash request and verify the response
		if err := p2p.Send(peer.app, GetBlockBodiesMsg, hashes); err != nil {
			t.Fatal(err)
		}
		if err := p2p.ExpectMsg(peer.app, BlockBodiesMsg, bodies); err != nil {
			t.Errorf("test %d: bodies mismatch: %v", i, err)
		}
	}
}

// Tests that the transaction receipts can be retrieved based on hashes.
func TestGetBlockReceipts64(t *testing.T) { testGetBlockReceipts(t, 64) }
func TestGetBlockReceipts65(t *testing.T) { testGetBlockReceipts(t, 65) }

func testGetBlockReceipts(t *testing.T, protocol uint) {
	// Define three accounts to simulate transactions with
	acc1Key, _ := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	acc2Key, _ := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	acc1Addr := crypto.PubkeyToAddress(acc1Key.PublicKey)
	acc2Addr := crypto.PubkeyToAddress(acc2Key.PublicKey)

	signer := types.LatestSignerForChainID(nil)
	// Create a chain generator with some simple transactions (blatantly stolen from @fjl/chain_markets_test)
	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, the test bank sends account #1 some ether.
			tx, _ := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), acc1Addr, uint256.NewInt(10000), params.TxGas, nil, nil), *signer, testKey)
			block.AddTx(tx)
		case 1:
			// In block 2, the test bank sends some more ether to account #1.
			// acc1Addr passes it on to account #2.
			tx1, _ := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), acc1Addr, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, testKey)
			tx2, _ := types.SignTx(types.NewTransaction(block.TxNonce(acc1Addr), acc2Addr, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, acc1Key)
			block.AddTx(tx1)
			block.AddTx(tx2)
		case 2:
			// Block 3 is empty but was mined by account #2.
			block.SetCoinbase(acc2Addr)
			block.SetExtra([]byte("yeehaw"))
		case 3:
			// Block 4 includes blocks 2 and 3 as uncle headers (with modified extra data).
			b2 := block.PrevBlock(1).Header()
			b2.Extra = []byte("foo")
			block.AddUncle(b2)
			b3 := block.PrevBlock(2).Header()
			b3.Extra = []byte("foo")
			block.AddUncle(b3)
		}
	}
	// Assemble the test environment
	backend := newTestBackendWithGenerator(t, 4, generator)

	peer, _ := newTestPeer("peer", protocol, backend)
	defer peer.close()

	// Collect the hashes to request, and the response to expect
	var (
		hashes   []common.Hash
		receipts []types.Receipts
	)
	for i := uint64(0); i <= rawdb.ReadCurrentHeader(backend.db).Number.Uint64(); i++ {
		block := rawdb.ReadHeaderByNumber(backend.db, i)

		hashes = append(hashes, block.Hash())
		receipts = append(receipts, rawdb.ReadReceiptsByHashDeprecated(backend.db, block.Hash()))
	}
	// Send the hash request and verify the response
	if err := p2p.Send(peer.app, GetReceiptsMsg, hashes); err != nil {
		t.Fatal(err)
	}
	if err := p2p.ExpectMsg(peer.app, ReceiptsMsg, receipts); err != nil {
		t.Errorf("receipts mismatch: %v", err)
	}
}
