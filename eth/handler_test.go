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
	"fmt"
	"io/ioutil"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

// Tests that block headers can be retrieved from a remote chain based on user queries.
func TestGetBlockHeaders64(t *testing.T) { testGetBlockHeaders(t, 64) }
func TestGetBlockHeaders65(t *testing.T) { testGetBlockHeaders(t, 65) }

func testGetBlockHeaders(t *testing.T, protocol int) {
	pm, clear := newTestProtocolManagerMust(t, downloader.StagedSync, downloader.MaxHashFetch+15, nil, nil)
	defer clear()

	// Create a "random" unknown hash for testing
	var unknown common.Hash
	for i := range unknown {
		unknown[i] = byte(i)
	}
	// Create a batch of tests for various scenarios
	limit := uint64(downloader.MaxHeaderFetch)
	tests := []struct {
		query  *GetBlockHeadersData // The query to execute for header retrieval
		expect []common.Hash        // The hashes of the block whose headers are expected
	}{
		// A single random block should be retrievable by hash and number too
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Hash: pm.blockchain.GetBlockByNumber(limit / 2).Hash()}, Amount: 1},
			[]common.Hash{pm.blockchain.GetBlockByNumber(limit / 2).Hash()},
		}, {
			&GetBlockHeadersData{Origin: HashOrNumber{Number: limit / 2}, Amount: 1},
			[]common.Hash{pm.blockchain.GetBlockByNumber(limit / 2).Hash()},
		},
		// Multiple headers should be retrievable in both directions
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Number: limit / 2}, Amount: 3},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(limit / 2).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 + 1).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 + 2).Hash(),
			},
		}, {
			&GetBlockHeadersData{Origin: HashOrNumber{Number: limit / 2}, Amount: 3, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(limit / 2).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 - 1).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 - 2).Hash(),
			},
		},
		// Multiple headers with skip lists should be retrievable
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Number: limit / 2}, Skip: 3, Amount: 3},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(limit / 2).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 + 4).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 + 8).Hash(),
			},
		}, {
			&GetBlockHeadersData{Origin: HashOrNumber{Number: limit / 2}, Skip: 3, Amount: 3, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(limit / 2).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 - 4).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 - 8).Hash(),
			},
		},
		// The chain endpoints should be retrievable
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Number: 0}, Amount: 1},
			[]common.Hash{pm.blockchain.GetBlockByNumber(0).Hash()},
		}, {
			&GetBlockHeadersData{Origin: HashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64()}, Amount: 1},
			[]common.Hash{pm.blockchain.CurrentBlock().Hash()},
		},
		// Ensure protocol limits are honored
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64() - 1}, Amount: limit + 10, Reverse: true},
			pm.blockchain.GetBlockHashesFromHash(pm.blockchain.CurrentBlock().Hash(), limit),
		},
		// Check that requesting more than available is handled gracefully
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64() - 4}, Skip: 3, Amount: 3},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(pm.blockchain.CurrentBlock().NumberU64() - 4).Hash(),
				pm.blockchain.GetBlockByNumber(pm.blockchain.CurrentBlock().NumberU64()).Hash(),
			},
		}, {
			&GetBlockHeadersData{Origin: HashOrNumber{Number: 4}, Skip: 3, Amount: 3, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(4).Hash(),
				pm.blockchain.GetBlockByNumber(0).Hash(),
			},
		},
		// Check that requesting more than available is handled gracefully, even if mid skip
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64() - 4}, Skip: 2, Amount: 3},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(pm.blockchain.CurrentBlock().NumberU64() - 4).Hash(),
				pm.blockchain.GetBlockByNumber(pm.blockchain.CurrentBlock().NumberU64() - 1).Hash(),
			},
		}, {
			&GetBlockHeadersData{Origin: HashOrNumber{Number: 4}, Skip: 2, Amount: 3, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(4).Hash(),
				pm.blockchain.GetBlockByNumber(1).Hash(),
			},
		},
		// Check a corner case where requesting more can iterate past the endpoints
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Number: 2}, Amount: 5, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(2).Hash(),
				pm.blockchain.GetBlockByNumber(1).Hash(),
				pm.blockchain.GetBlockByNumber(0).Hash(),
			},
		},
		// Check a corner case where skipping overflow loops back into the chain start
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Hash: pm.blockchain.GetBlockByNumber(3).Hash()}, Amount: 2, Reverse: false, Skip: math.MaxUint64 - 1},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(3).Hash(),
			},
		},
		// Check a corner case where skipping overflow loops back to the same header
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Hash: pm.blockchain.GetBlockByNumber(1).Hash()}, Amount: 2, Reverse: false, Skip: math.MaxUint64},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(1).Hash(),
			},
		},
		// Check that non existing headers aren't returned
		{
			&GetBlockHeadersData{Origin: HashOrNumber{Hash: unknown}, Amount: 1},
			[]common.Hash{},
		}, {
			&GetBlockHeadersData{Origin: HashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64() + 1}, Amount: 1},
			[]common.Hash{},
		},
	}
	// Run each of the tests and verify the results against the chain
	for i, tt := range tests {
		i := i
		tt := tt
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			peer, _ := newTestPeer("peer", protocol, pm, true /* handshake */)
			defer peer.close()
			// Collect the headers to expect in the response
			headers := []*types.Header{}
			for _, hash := range tt.expect {
				headers = append(headers, pm.blockchain.GetBlockByHash(hash).Header())
			}
			// Send the hash request and verify the response
			if err := p2p.Send(peer.app, 0x03, tt.query); err != nil {
				t.Error(err)
			}
			if err := p2p.ExpectMsg(peer.app, 0x04, headers); err != nil {
				t.Errorf("test %d: headers mismatch: %v", i, err)
			}
			// If the test used number origins, repeat with hashes as the too
			if tt.query.Origin.Hash == (common.Hash{}) {
				if origin := pm.blockchain.GetBlockByNumber(tt.query.Origin.Number); origin != nil {
					tt.query.Origin.Hash, tt.query.Origin.Number = origin.Hash(), 0

					if err := p2p.Send(peer.app, 0x03, tt.query); err != nil {
						t.Error(err)
					}

					if err := p2p.ExpectMsg(peer.app, 0x04, headers); err != nil {
						t.Errorf("test %d: headers mismatch: %v", i, err)
					}
				}
			}

		})
	}
}

// Tests that block contents can be retrieved from a remote chain based on their hashes.
func TestGetBlockBodies64(t *testing.T) { testGetBlockBodies(t, 64) }
func TestGetBlockBodies65(t *testing.T) { testGetBlockBodies(t, 65) }

func testGetBlockBodies(t *testing.T, protocol int) {
	pm, clear := newTestProtocolManagerMust(t, downloader.StagedSync, downloader.MaxBlockFetch+15, nil, nil)
	defer clear()

	// Create a batch of tests for various scenarios
	limit := downloader.MaxBlockFetch
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
		{0, []common.Hash{pm.blockchain.Genesis().Hash()}, []bool{true}, 1},      // The genesis block should be retrievable
		{0, []common.Hash{pm.blockchain.CurrentBlock().Hash()}, []bool{true}, 1}, // The chains head block should be retrievable
		{0, []common.Hash{{}}, []bool{false}, 0},                                 // A non existent block should not be returned

		// Existing and non-existing blocks interleaved should not cause problems
		{0, []common.Hash{
			{},
			pm.blockchain.GetBlockByNumber(1).Hash(),
			{},
			pm.blockchain.GetBlockByNumber(10).Hash(),
			{},
			pm.blockchain.GetBlockByNumber(100).Hash(),
			{},
		}, []bool{false, true, false, true, false, true, false}, 3},
	}
	// Run each of the tests and verify the results against the chain
	for i, tt := range tests {
		peer, _ := newTestPeer("peer", protocol, pm, true)
		defer peer.close()
		// Collect the hashes to request, and the response to expect
		hashes, seen := []common.Hash{}, make(map[int64]bool)
		bodies := []*blockBody{}

		for j := 0; j < tt.random; j++ {
			for {
				num := rand.Int63n(int64(pm.blockchain.CurrentBlock().NumberU64()))
				if !seen[num] {
					seen[num] = true

					block := pm.blockchain.GetBlockByNumber(uint64(num))
					hashes = append(hashes, block.Hash())
					if len(bodies) < tt.expected {
						bodies = append(bodies, &blockBody{Transactions: block.Transactions(), Uncles: block.Uncles()})
					}
					break
				}
			}
		}
		for j, hash := range tt.explicit {
			hashes = append(hashes, hash)
			if tt.available[j] && len(bodies) < tt.expected {
				block := pm.blockchain.GetBlockByHash(hash)
				bodies = append(bodies, &blockBody{Transactions: block.Transactions(), Uncles: block.Uncles()})
			}
		}
		// Send the hash request and verify the response
		p2p.Send(peer.app, 0x05, hashes)
		if err := p2p.ExpectMsg(peer.app, 0x06, bodies); err != nil {
			t.Errorf("test %d: bodies mismatch: %v", i, err)
		}
	}
}

// Tests that the node state database can be retrieved based on hashes.
func TestGetNodeData64(t *testing.T) { testGetNodeData(t, 64) }
func TestGetNodeData65(t *testing.T) { testGetNodeData(t, 65) }

func testGetNodeData(t *testing.T, protocol int) {
	t.Skip("turbo-geth does not support GetNodeData")
	debug.OverrideGetNodeData(true)
	defer debug.OverrideGetNodeData(false)
	// Assemble the test environment
	pm, addr, clear := setUpStorageContractA(t)
	defer clear()
	peer, _ := newTestPeer("peer", protocol, pm, true)
	defer peer.close()

	state, err := pm.blockchain.GetTrieDbState()
	assert.NoError(t, err)
	account, err := state.ReadAccountData(addr)
	assert.NoError(t, err)

	node0Rlp, node1Rlp, branchRlp := storageNodesOfContractA(t, 2)
	assert.Equal(t, account.Root, crypto.Keccak256Hash(branchRlp))

	// Fetch some nodes
	hashes := []common.Hash{
		crypto.Keccak256Hash(node1Rlp),
		pm.blockchain.CurrentBlock().Root(),
		crypto.Keccak256Hash(branchRlp),
		account.CodeHash,
		crypto.Keccak256Hash(node0Rlp),
	}

	err = p2p.Send(peer.app, GetNodeDataMsg, hashes)
	assert.NoError(t, err)

	msg, err := peer.app.ReadMsg()
	if err != nil {
		t.Fatalf("failed to read node data response: %v", err)
	}
	if msg.Code != NodeDataMsg {
		t.Fatalf("response packet code mismatch: have %x, want %x", msg.Code, NodeDataMsg)
	}
	var data [][]byte
	if err := msg.Decode(&data); err != nil {
		t.Fatalf("failed to decode response node data: %v", err)
	}

	// Verify that we get the right nodes back
	if len(data) != len(hashes) {
		t.Fatalf("response size mismatch: have %x, want %x", len(data), len(hashes))
	}
	for i := 0; i < len(hashes); i++ {
		assert.NotEmpty(t, data[i])
		assert.Equal(t, hashes[i], crypto.Keccak256Hash(data[i]))
	}
}

// Tests that the transaction receipts can be retrieved based on hashes.
func TestGetReceipt64(t *testing.T) { testGetReceipt(t, 64) }
func TestGetReceipt65(t *testing.T) { testGetReceipt(t, 65) }

func testGetReceipt(t *testing.T, protocol int) {
	// Define two accounts to simulate transactions with
	acc1Key, _ := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	acc2Key, _ := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	acc1Addr := crypto.PubkeyToAddress(acc1Key.PublicKey)
	acc2Addr := crypto.PubkeyToAddress(acc2Key.PublicKey)

	signer := types.HomesteadSigner{}
	// Create a chain generator with some simple transactions (blatantly stolen from @fjl/chain_markets_test)
	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, the test bank sends account #1 some ether.
			tx, _ := types.SignTx(types.NewTransaction(block.TxNonce(testBank), acc1Addr, uint256.NewInt().SetUint64(10000), params.TxGas, nil, nil), signer, testBankKey)
			block.AddTx(tx)
		case 1:
			// In block 2, the test bank sends some more ether to account #1.
			// acc1Addr passes it on to account #2.
			tx1, _ := types.SignTx(types.NewTransaction(block.TxNonce(testBank), acc1Addr, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, testBankKey)
			tx2, _ := types.SignTx(types.NewTransaction(block.TxNonce(acc1Addr), acc2Addr, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, acc1Key)
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
	pm, clear := newTestProtocolManagerMust(t, downloader.StagedSync, 4, generator, nil)
	defer clear()
	peer, _ := newTestPeer("peer", protocol, pm, true)
	defer peer.close()

	// Collect the hashes to request, and the response to expect
	hashes, receipts := []common.Hash{}, []types.Receipts{}
	for i := uint64(0); i <= pm.blockchain.CurrentBlock().NumberU64(); i++ {
		block := pm.blockchain.GetBlockByNumber(i)

		hashes = append(hashes, block.Hash())
		receipts = append(receipts, pm.blockchain.GetReceiptsByHash(block.Hash()))
	}
	// Send the hash request and verify the response
	p2p.Send(peer.app, 0x0f, hashes)
	if err := p2p.ExpectMsg(peer.app, 0x10, receipts); err != nil {
		t.Errorf("receipts mismatch: %v", err)
	}
}

// Tests that post eth protocol handshake, clients perform a mutual checkpoint
// challenge to validate each other's chains. Hash mismatches, or missing ones
// during a fast sync should lead to the peer getting dropped.
func TestCheckpointChallenge(t *testing.T) {
	tests := []struct {
		syncmode   downloader.SyncMode
		checkpoint bool
		timeout    bool
		empty      bool
		match      bool
		drop       bool
	}{
		// If checkpointing is not enabled locally, don't challenge and don't drop
		{downloader.StagedSync, false, false, false, false, false},

		// If checkpointing is enabled locally and remote response is empty, only drop during fast sync
		{downloader.StagedSync, true, false, true, false, false},

		// If checkpointing is enabled locally and remote response mismatches, always drop
		{downloader.StagedSync, true, false, false, false, true},

		// If checkpointing is enabled locally and remote response matches, never drop
		{downloader.StagedSync, true, false, false, true, false},

		// If checkpointing is enabled locally and remote times out, always drop
		{downloader.StagedSync, true, true, false, true, true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("sync %v checkpoint %v timeout %v empty %v match %v", tt.syncmode, tt.checkpoint, tt.timeout, tt.empty, tt.match), func(t *testing.T) {
			testCheckpointChallenge(t, tt.syncmode, tt.checkpoint, tt.timeout, tt.empty, tt.match, tt.drop)
		})
	}
}

func testCheckpointChallenge(t *testing.T, syncmode downloader.SyncMode, checkpoint bool, timeout bool, empty bool, match bool, drop bool) {
	// Reduce the checkpoint handshake challenge timeout
	defer func(old time.Duration) { syncChallengeTimeout = old }(syncChallengeTimeout)
	syncChallengeTimeout = 250 * time.Millisecond

	// Initialize a chain and generate a fake CHT if checkpointing is enabled
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		config = new(params.ChainConfig)
	)
	(&core.Genesis{Config: config}).MustCommit(db) // Commit genesis block
	// If checkpointing is enabled, create and inject a fake CHT and the corresponding
	// chllenge response.
	var response *types.Header
	var cht *params.TrustedCheckpoint
	if checkpoint {
		index := uint64(rand.Intn(500))
		number := (index+1)*params.CHTFrequency - 1
		response = &types.Header{Number: big.NewInt(int64(number)), Extra: []byte("valid")}

		cht = &params.TrustedCheckpoint{
			SectionIndex: index,
			SectionHead:  response.Hash(),
		}
	}
	// Create a checkpoint aware protocol manager
	blockchain, err := core.NewBlockChain(db, nil, config, ethash.NewFaker(), vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create new blockchain: %v", err)
	}
	defer blockchain.Stop()
	pm, err := NewProtocolManager(config, cht, syncmode, DefaultConfig.NetworkID, new(event.TypeMux), &testTxPool{pool: make(map[common.Hash]*types.Transaction)}, ethash.NewFaker(), blockchain, db, nil, nil)
	if err != nil {
		t.Fatalf("failed to start test protocol manager: %v", err)
	}
	if err = pm.Start(1000, true); err != nil {
		t.Fatalf("error on protocol manager start: %v", err)
	}
	defer pm.Stop()

	// Connect a new peer and check that we receive the checkpoint challenge
	peer, _ := newTestPeer("peer", eth65, pm, true)
	defer peer.close()

	if checkpoint {
		challenge := &GetBlockHeadersData{
			Origin:  HashOrNumber{Number: response.Number.Uint64()},
			Amount:  1,
			Skip:    0,
			Reverse: false,
		}
		if err := p2p.ExpectMsg(peer.app, GetBlockHeadersMsg, challenge); err != nil {
			t.Fatalf("challenge mismatch: %v", err)
		}
		// Create a block to reply to the challenge if no timeout is simulated
		if !timeout {
			if empty {
				if err := p2p.Send(peer.app, BlockHeadersMsg, []*types.Header{}); err != nil {
					t.Fatalf("failed to answer challenge: %v", err)
				}
			} else if match {
				if err := p2p.Send(peer.app, BlockHeadersMsg, []*types.Header{response}); err != nil {
					t.Fatalf("failed to answer challenge: %v", err)
				}
			} else {
				if err := p2p.Send(peer.app, BlockHeadersMsg, []*types.Header{{Number: response.Number}}); err != nil {
					t.Fatalf("failed to answer challenge: %v", err)
				}
			}
		}
	}
	// Wait until the test timeout passes to ensure proper cleanup
	time.Sleep(syncChallengeTimeout + 300*time.Millisecond)

	// Verify that the remote peer is maintained or dropped
	if drop {
		if peers := pm.peers.Len(); peers != 0 {
			t.Fatalf("peer count mismatch: have %d, want %d", peers, 0)
		}
	} else {
		if peers := pm.peers.Len(); peers != 1 {
			t.Fatalf("peer count mismatch: have %d, want %d", peers, 1)
		}
	}
}

func TestBroadcastBlock(t *testing.T) {
	var tests = []struct {
		totalPeers        int
		broadcastExpected int
	}{
		{1, 1},
		{2, 1},
		{3, 1},
		{4, 2},
		{5, 2},
		{9, 3},
		{12, 3},
		{16, 4},
		{26, 5},
		{100, 10},
	}
	for _, test := range tests {
		testBroadcastBlock(t, test.totalPeers, test.broadcastExpected)
	}
}

func testBroadcastBlock(t *testing.T, totalPeers, broadcastExpected int) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		evmux   = new(event.TypeMux)
		pow     = ethash.NewFaker()
		config  = &params.ChainConfig{}
		gspec   = &core.Genesis{Config: config}
		genesis = gspec.MustCommit(db)
	)
	blockchain, err := core.NewBlockChain(db, nil, config, pow, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create new blockchain: %v", err)
	}
	defer blockchain.Stop()
	cht := &params.TrustedCheckpoint{}
	pm, err := NewProtocolManager(config, cht, downloader.StagedSync, DefaultConfig.NetworkID, evmux, &testTxPool{pool: make(map[common.Hash]*types.Transaction)}, pow, blockchain, db, nil, nil)
	if err != nil {
		t.Fatalf("failed to start test protocol manager: %v", err)
	}
	if err = pm.Start(1000, true); err != nil {
		t.Fatalf("error on protocol manager start: %v", err)
	}

	defer pm.Stop()
	var peers []*testPeer
	for i := 0; i < totalPeers; i++ {
		peer, _ := newTestPeer(fmt.Sprintf("peer %d", i), eth65, pm, true)
		defer peer.close()
		peers = append(peers, peer)
	}
	chain, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 1, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	pm.BroadcastBlock(chain[0], true /*propagate*/)

	errCh := make(chan error, totalPeers)
	doneCh := make(chan struct{}, totalPeers)
	for _, peer := range peers {
		go func(p *testPeer) {
			if err1 := p2p.ExpectMsg(p.app, NewBlockMsg, &NewBlockData{Block: chain[0], TD: big.NewInt(131136)}); err1 != nil {
				errCh <- err1
			} else {
				doneCh <- struct{}{}
			}
		}(peer)
	}
	var received int
	for {
		select {
		case <-doneCh:
			received++
			if received > broadcastExpected {
				// We can bail early here
				t.Errorf("broadcast count mismatch: have %d > want %d", received, broadcastExpected)
				return
			}
		case <-time.After(2 * time.Second):
			if received != broadcastExpected {
				t.Errorf("broadcast count mismatch: have %d, want %d", received, broadcastExpected)
			}
			return
		case err = <-errCh:
			t.Fatalf("broadcast failed: %v", err)
		}
	}
}

var frhsAmnt = uint256.NewInt().SetUint64(10000)
var addrHash = make([]common.Hash, 5)

func setUpDummyAccountsForFirehose(t *testing.T) (*ProtocolManager, *testFirehosePeer, func()) {
	addr1 := common.HexToAddress("0x3b4fc1530da632624fa1e223a91d99dbb07c2d42")
	addr2 := common.HexToAddress("0xb574d96f69c1324e3b49e63f4cc899736dd52789")
	addr3 := common.HexToAddress("0xb11e2c7c5b96dbf120ec8af539d028311366af00")
	addr4 := common.HexToAddress("0x7d9eb619ce1033cc710d9f9806a2330f85875f22")

	addrHash[0] = crypto.Keccak256Hash(testBank.Bytes())
	addrHash[1] = crypto.Keccak256Hash(addr1.Bytes())
	addrHash[2] = crypto.Keccak256Hash(addr2.Bytes())
	addrHash[3] = crypto.Keccak256Hash(addr3.Bytes())
	addrHash[4] = crypto.Keccak256Hash(addr4.Bytes())

	assert.Equal(t, addrHash[0], common.HexToHash("0x00bf49f440a1cd0527e4d06e2765654c0f56452257516d793a9b8d604dcfdf2a"))
	assert.Equal(t, addrHash[1], common.HexToHash("0x1155f85cf8c36b3bf84a89b2d453da3cc5c647ff815a8a809216c47f5ab507a9"))
	assert.Equal(t, addrHash[2], common.HexToHash("0xac8e03d3673a43257a69fcd3ff99a7a17b7d0e0a900c337d55dbd36567938776"))
	assert.Equal(t, addrHash[3], common.HexToHash("0x464b54760c96939ce60fb73b20987db21fce5a624d190f4e769c54a2ba8be49e"))
	assert.Equal(t, addrHash[4], common.HexToHash("0x44091c88eed629ecac3ad260ab22318b52148b7a4cc2ac7d8bdf746877b54c15"))

	signer := types.HomesteadSigner{}
	numBlocks := 5
	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr1, frhsAmnt, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		case 1:
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr2, frhsAmnt, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		case 2:
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr3, frhsAmnt, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		case 3:
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr4, frhsAmnt, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		case 4:
			// top up account #3
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr3, frhsAmnt, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		}
	}

	pm, pmClear := newTestProtocolManagerMust(t, downloader.StagedSync, numBlocks, generator, nil)
	peer, _ := newFirehoseTestPeer("peer", pm)

	clear := func() {
		peer.close()
		pmClear()
	}
	return pm, peer, clear
}

func TestFirehoseStateRanges(t *testing.T) {
	// TODO: remove or recover
	t.Skip()

	pm, peer, clear := setUpDummyAccountsForFirehose(t)
	defer clear()

	block4 := pm.blockchain.GetBlockByNumber(4)

	var request getStateRangesOrNodes
	request.ID = 1
	request.Block = block4.Hash()

	// All known account keys start with either 0, 1, 4, or a.
	// Warning: we assume that the key of miner's account doesn't start with 2 or 4.
	request.Prefixes = []trie.Keybytes{
		{Data: common.FromHex("40"), Odd: true, Terminating: false},
		{Data: common.FromHex("20"), Odd: true, Terminating: false},
	}

	assert.NoError(t, p2p.Send(peer.app, GetStateRangesCode, request))

	account := accounts.NewAccount()
	account.Balance.Set(frhsAmnt)

	var reply1 stateRangesMsg
	reply1.ID = 1
	reply1.Entries = []firehoseAccountRange{
		{Status: OK, Leaves: []accountLeaf{{addrHash[4], &account}, {addrHash[3], &account}}},
		{Status: OK, Leaves: []accountLeaf{}},
	}

	if err := p2p.ExpectMsg(peer.app, StateRangesCode, reply1); err != nil {
		t.Errorf("unexpected StateRanges response: %v", err)
	}

	nonexistentBlock := common.HexToHash("4444444444444444444444444444444444444444444444444444444444444444")
	request.ID = 2
	request.Block = nonexistentBlock

	assert.NoError(t, p2p.Send(peer.app, GetStateRangesCode, request))

	block0 := pm.blockchain.GetBlockByNumber(0)
	block1 := pm.blockchain.GetBlockByNumber(1)
	block2 := pm.blockchain.GetBlockByNumber(2)
	block3 := pm.blockchain.GetBlockByNumber(3)
	block5 := pm.blockchain.GetBlockByNumber(5)

	var reply2 stateRangesMsg
	reply2.ID = 2
	reply2.Entries = []firehoseAccountRange{
		{Status: NoData, Leaves: []accountLeaf{}},
		{Status: NoData, Leaves: []accountLeaf{}},
	}
	reply2.AvailableBlocks = []common.Hash{block5.Hash(), block4.Hash(), block3.Hash(), block2.Hash(), block1.Hash(), block0.Hash()}

	if err := p2p.ExpectMsg(peer.app, StateRangesCode, reply2); err != nil {
		t.Errorf("unexpected StateRanges response: %v", err)
	}
}

func TestFirehoseTooManyLeaves(t *testing.T) {
	// TODO: remove or recover
	t.Skip()

	signer := types.HomesteadSigner{}
	amount := uint256.NewInt().SetUint64(10)
	generator := func(i int, block *core.BlockGen) {
		var rndAddr common.Address
		// #nosec G404
		rand.Read(rndAddr[:])

		tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), rndAddr, amount, params.TxGas, nil, nil), signer, testBankKey)
		assert.NoError(t, err)
		block.AddTx(tx)
	}

	pm, clear := newTestProtocolManagerMust(t, downloader.StagedSync, MaxLeavesPerPrefix, generator, nil)
	defer clear()
	peer, _ := newFirehoseTestPeer("peer", pm)
	defer peer.close()

	// ----------------------------------------------------
	// BLOCK #1

	var request getStateRangesOrNodes
	request.ID = 0
	request.Block = pm.blockchain.GetBlockByNumber(1).Hash()

	request.Prefixes = []trie.Keybytes{
		{Data: []byte{}, Odd: false, Terminating: false}, // empty prefix
	}

	assert.NoError(t, p2p.Send(peer.app, GetStateRangesCode, request))

	msg, err := peer.app.ReadMsg()
	assert.NoError(t, err)
	content, err := ioutil.ReadAll(msg.Payload)
	assert.NoError(t, err)
	var reply0 stateRangesMsg
	assert.NoError(t, rlp.DecodeBytes(content, &reply0))

	assert.Equal(t, uint64(0), reply0.ID)
	assert.Equal(t, 1, len(reply0.Entries))
	assert.Equal(t, OK, reply0.Entries[0].Status)
	// test bank account + miner's account + the first random account
	assert.Equal(t, 3, len(reply0.Entries[0].Leaves))

	// ----------------------------------------------------
	// BLOCK #MaxLeavesPerPrefix

	request.ID = 1
	request.Block = pm.blockchain.CurrentBlock().Hash()

	assert.NoError(t, p2p.Send(peer.app, GetStateRangesCode, request))

	var reply1 stateRangesMsg
	reply1.ID = 1
	reply1.Entries = []firehoseAccountRange{
		{Status: TooManyLeaves, Leaves: []accountLeaf{}},
	}

	err = p2p.ExpectMsg(peer.app, StateRangesCode, reply1)
	if err != nil {
		t.Errorf("unexpected StateRanges response: %v", err)
	}

	// ----------------------------------------------------
	// BLOCK #(MaxLeavesPerPrefix-2)

	request.ID = 2
	request.Block = pm.blockchain.GetBlockByNumber(MaxLeavesPerPrefix - 2).Hash()

	assert.NoError(t, p2p.Send(peer.app, GetStateRangesCode, request))

	msg, err = peer.app.ReadMsg()
	assert.NoError(t, err)
	content, err = ioutil.ReadAll(msg.Payload)
	assert.NoError(t, err)
	var reply2 stateRangesMsg
	assert.NoError(t, rlp.DecodeBytes(content, &reply2))

	assert.Equal(t, uint64(2), reply2.ID)
	assert.Equal(t, 1, len(reply2.Entries))
	assert.Equal(t, OK, reply2.Entries[0].Status)
	// mind the test bank and the miner accounts
	assert.Equal(t, MaxLeavesPerPrefix, len(reply2.Entries[0].Leaves))

	// ----------------------------------------------------
	// BLOCK #(MaxLeavesPerPrefix-1)

	request.ID = 3
	request.Block = pm.blockchain.GetBlockByNumber(MaxLeavesPerPrefix - 1).Hash()

	assert.NoError(t, p2p.Send(peer.app, GetStateRangesCode, request))

	var reply3 stateRangesMsg
	reply3.ID = 3
	reply3.Entries = []firehoseAccountRange{
		{Status: TooManyLeaves, Leaves: []accountLeaf{}},
	}

	err = p2p.ExpectMsg(peer.app, StateRangesCode, reply3)
	if err != nil {
		t.Errorf("unexpected StateRanges response: %v", err)
	}
}

// 2 storage items starting from different nibbles
func setUpStorageContractA(t *testing.T) (*ProtocolManager, common.Address, func()) {
	// This contract initially sets its 0th storage to 0x2a
	// and its 1st storage to 0x01c9.
	// When called, it updates the 0th storage to the input provided.
	code := common.FromHex("602a6000556101c960015560068060166000396000f3600035600055")
	// https://github.com/CoinCulture/evm-tools
	// 0      PUSH1  => 2a
	// 2      PUSH1  => 00
	// 4      SSTORE         // storage[0] = 0x2a
	// 5      PUSH2  => 01c9
	// 8      PUSH1  => 01
	// 10     SSTORE         // storage[1] = 0x01c9
	// 11     PUSH1  => 06   // deploy begin
	// 13     DUP1
	// 14     PUSH1  => 16
	// 16     PUSH1  => 00
	// 18     CODECOPY
	// 19     PUSH1  => 00
	// 21     RETURN         // deploy end
	// 22     PUSH1  => 00   // contract code
	// 24     CALLDATALOAD
	// 25     PUSH1  => 00
	// 27     SSTORE         // storage[0] = input[0]

	input := common.HexToHash("15").Bytes()

	signer := types.HomesteadSigner{}
	var addr common.Address

	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			nonce := block.TxNonce(testBank)
			// storage[0] = 0x2a, storage[1] = 0x01c9
			tx, err := types.SignTx(types.NewContractCreation(nonce, new(uint256.Int), 2e5, nil, code), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
			addr = crypto.CreateAddress(testBank, nonce)
		case 1:
			// storage[0] = 0x15
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr, new(uint256.Int), 2e5, nil, input), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		}
	}

	pm, clear := newTestProtocolManagerMust(t, downloader.FullSync, 2, generator, nil)
	return pm, addr, clear
}

func storageNodesOfContractA(t *testing.T, blockNbr uint64) (node0Rlp, node1Rlp, branchRlp []byte) {
	hashOf0 := crypto.Keccak256(common.HexToHash("00").Bytes())
	hashOf1 := crypto.Keccak256(common.HexToHash("01").Bytes())

	// https://github.com/ethereum/wiki/wiki/Patricia-Tree

	path0Compact := common.CopyBytes(hashOf0)
	// override the 0st nibble with aux one for compact encoding
	path0Compact[0] &= 0x0f
	path0Compact[0] |= 0x30

	path1Compact := common.CopyBytes(hashOf1)
	path1Compact[0] &= 0x0f
	path1Compact[0] |= 0x30

	var val0 uint = 0x2a
	if blockNbr >= 2 {
		val0 = 0x15
	}

	leafNode := make([][]byte, 2)
	leafNode[0] = path0Compact
	val0Rlp, err := rlp.EncodeToBytes(val0)
	assert.NoError(t, err)
	leafNode[1] = val0Rlp
	node0Rlp, err = rlp.EncodeToBytes(leafNode)
	assert.NoError(t, err)

	leafNode[0] = path1Compact
	val1Rlp, err := rlp.EncodeToBytes(uint(0x01c9))
	assert.NoError(t, err)
	leafNode[1] = val1Rlp
	node1Rlp, err = rlp.EncodeToBytes(leafNode)
	assert.NoError(t, err)

	branchNode := make([][]byte, 17)
	assert.True(t, len(node0Rlp) >= 32)
	branchNode[0x2] = crypto.Keccak256(node0Rlp)
	assert.True(t, len(node1Rlp) >= 32)
	branchNode[0xb] = crypto.Keccak256(node1Rlp)
	branchRlp, err = rlp.EncodeToBytes(branchNode)
	assert.NoError(t, err)

	return node0Rlp, node1Rlp, branchRlp
}

// 2 storage items starting with the same nibble
func setUpStorageContractB(t *testing.T) (*ProtocolManager, common.Address, func()) {
	// This contract initially sets its 6th storage to 0x2a
	// and its 8st storage to 0x01c9.
	// When called, it updates the 8th storage to the input provided.
	code := common.FromHex("602a6006556101c960085560068060166000396000f3600035600855")
	// https://github.com/CoinCulture/evm-tools
	// 0      PUSH1  => 2a
	// 2      PUSH1  => 06
	// 4      SSTORE         // storage[6] = 0x2a
	// 5      PUSH2  => 01c9
	// 8      PUSH1  => 08
	// 10     SSTORE        // storage[8] = 0x01c9
	// 11     PUSH1  => 06  // deploy begin
	// 13     DUP1
	// 14     PUSH1  => 16
	// 16     PUSH1  => 00
	// 18     CODECOPY
	// 19     PUSH1  => 00
	// 21     RETURN        // deploy end
	// 22     PUSH1  => 00
	// 24     CALLDATALOAD
	// 25     PUSH1  => 08
	// 27     SSTORE        // storage[8] = input[0]

	input := common.HexToHash("15").Bytes()

	signer := types.HomesteadSigner{}
	var addr common.Address

	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			nonce := block.TxNonce(testBank)
			// storage[6] = 0x2a, storage[8] = 0x01c9
			tx, err := types.SignTx(types.NewContractCreation(nonce, new(uint256.Int), 2e5, nil, code), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
			addr = crypto.CreateAddress(testBank, nonce)
		case 1:
			// storage[8] = 0x15
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr, new(uint256.Int), 2e5, nil, input), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		}
	}

	pm, clear := newTestProtocolManagerMust(t, downloader.FullSync, 2, generator, nil)
	return pm, addr, clear
}

func TestFirehoseStorageRanges(t *testing.T) {
	// TODO: remove or recover
	t.Skip()

	pm, addr, clear := setUpStorageContractA(t)
	defer clear()
	peer, _ := newFirehoseTestPeer("peer", pm)
	defer peer.close()

	// Block 1

	var storageReq getStorageRangesOrNodes
	storageReq.ID = 1
	storageReq.Block = pm.blockchain.GetBlockByNumber(1).Hash()
	emptyPrefix := trie.Keybytes{Data: []byte{}, Odd: false, Terminating: false}
	storageReq.Requests = []storageReqForOneAccount{
		{Account: addr.Bytes(), Prefixes: []trie.Keybytes{emptyPrefix}},
	}

	assert.NoError(t, p2p.Send(peer.app, GetStorageRangesCode, storageReq))

	hashOf0 := crypto.Keccak256Hash(common.HexToHash("00").Bytes())
	hashOf1 := crypto.Keccak256Hash(common.HexToHash("01").Bytes())

	var storageReply storageRangesMsg
	storageReply.ID = 1
	storageReply.Entries = [][]storageRange{{
		{Status: OK, Leaves: []storageLeaf{
			{Key: hashOf0, Val: *(big.NewInt(0x2a))},
			{Key: hashOf1, Val: *(big.NewInt(0x01c9))},
		}},
	}}

	err := p2p.ExpectMsg(peer.app, StorageRangesCode, storageReply)
	if err != nil {
		t.Fatalf("unexpected StorageRanges response: %v", err)
	}

	// Block 2

	storageReq.ID = 2
	storageReq.Block = pm.blockchain.GetBlockByNumber(2).Hash()

	assert.NoError(t, p2p.Send(peer.app, GetStorageRangesCode, storageReq))
	storageReply.ID = 2
	storageReply.Entries[0][0].Leaves[0].Val.SetUint64(0x15)

	err = p2p.ExpectMsg(peer.app, StorageRangesCode, storageReply)
	if err != nil {
		t.Errorf("unexpected StorageRanges response: %v", err)
	}

	// TODO [Andrew] test contract w/o any storage
}

// TestFirehoseStorageNodesA tests a trie with a branch node at the root and 2 leaf nodes.
func TestFirehoseStorageNodesA(t *testing.T) {
	// TODO: remove or recover
	t.Skip()

	pm, addr, clear := setUpStorageContractA(t)
	defer clear()
	peer, _ := newFirehoseTestPeer("peer", pm)
	defer peer.close()

	hashOf0 := crypto.Keccak256(common.HexToHash("00").Bytes())
	hashOf1 := crypto.Keccak256(common.HexToHash("01").Bytes())
	assert.Equal(t, hashOf0[0], byte(0x29))
	assert.Equal(t, hashOf1[0], byte(0xb1))

	var blockNbr uint64 = 1

	var storageReq getStorageRangesOrNodes
	storageReq.ID = 1
	storageReq.Block = pm.blockchain.GetBlockByNumber(blockNbr).Hash()
	emptyPrefix := trie.Keybytes{Data: []byte{}, Odd: false, Terminating: false}
	storageReq.Requests = []storageReqForOneAccount{
		{Account: addr.Bytes(), Prefixes: []trie.Keybytes{emptyPrefix}},
	}

	assert.NoError(t, p2p.Send(peer.app, GetStorageNodesCode, storageReq))

	_, _, branchRlp := storageNodesOfContractA(t, blockNbr)

	var storageReply storageNodesMsg
	storageReply.ID = 1
	storageReply.Nodes = make([][][]byte, 1)
	storageReply.Nodes[0] = make([][]byte, 1)
	storageReply.Nodes[0][0] = branchRlp

	if err := p2p.ExpectMsg(peer.app, StorageNodesCode, storageReply); err != nil {
		t.Errorf("unexpected StorageNodes response: %v", err)
	}
}

// TestFirehoseStorageNodesB tests a trie with an extension node at the root,
// 1 intermediate branch node, and 2 leaf nodes.
func TestFirehoseStorageNodesB(t *testing.T) {
	// TODO: remove or recover
	t.Skip()

	pm, addr, clear := setUpStorageContractB(t)
	defer clear()
	peer, _ := newFirehoseTestPeer("peer", pm)
	defer peer.close()

	hashOf6 := crypto.Keccak256(common.HexToHash("06").Bytes())
	hashOf8 := crypto.Keccak256(common.HexToHash("08").Bytes())
	assert.Equal(t, hashOf6[0], byte(0xf6))
	assert.Equal(t, hashOf8[0], byte(0xf3))

	var storageReq getStorageRangesOrNodes
	storageReq.ID = 1
	storageReq.Block = pm.blockchain.GetBlockByNumber(1).Hash()
	emptyPrefix := trie.Keybytes{Data: []byte{}, Odd: false, Terminating: false}
	nibblePrefix := trie.Keybytes{Data: common.FromHex("f0"), Odd: true, Terminating: false}
	storageReq.Requests = []storageReqForOneAccount{
		{Account: addr.Bytes(), Prefixes: []trie.Keybytes{emptyPrefix, nibblePrefix}},
	}

	assert.NoError(t, p2p.Send(peer.app, GetStorageNodesCode, storageReq))

	// https://github.com/ethereum/wiki/wiki/Patricia-Tree

	path6Compact := common.CopyBytes(hashOf6)
	// replace the first 2 nibbles with compact encoding stuff
	path6Compact[0] = 0x20

	path8Compact := common.CopyBytes(hashOf8)
	path8Compact[0] = 0x20

	leafNode := make([][]byte, 2)
	leafNode[0] = path6Compact
	val6Rlp, err := rlp.EncodeToBytes(uint(0x2a))
	assert.NoError(t, err)
	leafNode[1] = val6Rlp
	node6Rlp, err := rlp.EncodeToBytes(leafNode)
	assert.NoError(t, err)

	leafNode[0] = path8Compact
	val8Rlp, err := rlp.EncodeToBytes(uint(0x01c9))
	assert.NoError(t, err)
	leafNode[1] = val8Rlp
	node8Rlp, err := rlp.EncodeToBytes(leafNode)
	assert.NoError(t, err)

	branchNode := make([][]byte, 17)
	assert.True(t, len(node6Rlp) >= 32)
	branchNode[6] = crypto.Keccak256(node6Rlp)
	assert.True(t, len(node8Rlp) >= 32)
	branchNode[3] = crypto.Keccak256(node8Rlp)
	branchRlp, err := rlp.EncodeToBytes(branchNode)
	assert.NoError(t, err)

	extensionNode := make([][]byte, 2)
	extensionNode[0] = common.FromHex("1f")
	assert.True(t, len(branchRlp) >= 32)
	extensionNode[1] = crypto.Keccak256(branchRlp)
	extensionRlp, err := rlp.EncodeToBytes(extensionNode)
	assert.NoError(t, err)

	var storageReply storageNodesMsg
	storageReply.ID = 1
	storageReply.Nodes = make([][][]byte, 1)
	storageReply.Nodes[0] = make([][]byte, 2)
	storageReply.Nodes[0][0] = extensionRlp
	storageReply.Nodes[0][1] = branchRlp

	err = p2p.ExpectMsg(peer.app, StorageNodesCode, storageReply)
	if err != nil {
		t.Errorf("unexpected StorageNodes response: %v", err)
	}
}

func TestFirehoseStateNodes(t *testing.T) {
	// TODO: remove or recover
	t.Skip()

	pm, peer, clear := setUpDummyAccountsForFirehose(t)
	defer clear()

	// ------------------------------------------------------------------
	// Firstly test the latest state where account3 has double the amount
	// ------------------------------------------------------------------

	var request getStateRangesOrNodes
	request.ID = 0
	request.Block = pm.blockchain.GetBlockByNumber(5).Hash()

	// All known account keys start with either 0, 1, 4, or a.
	// Warning: we assume that the key of miner's account doesn't start with 2 or 4.
	prefixA := trie.Keybytes{Data: common.FromHex("40"), Odd: true}
	prefixB := trie.Keybytes{Data: common.FromHex("20"), Odd: true}
	request.Prefixes = []trie.Keybytes{prefixA, prefixB}

	assert.NoError(t, p2p.Send(peer.app, GetStateNodesCode, request))

	account3 := accounts.NewAccount()
	account3.Balance.Add(frhsAmnt, frhsAmnt)
	account3rlp, err := rlp.EncodeToBytes(&account3)
	assert.NoError(t, err)

	account4 := accounts.NewAccount()
	account4.Balance.Set(frhsAmnt)
	account4rlp, err := rlp.EncodeToBytes(&account4)
	assert.NoError(t, err)

	assert.Equal(t, addrHash[3], common.HexToHash("0x464b54760c96939ce60fb73b20987db21fce5a624d190f4e769c54a2ba8be49e"))
	assert.Equal(t, addrHash[4], common.HexToHash("0x44091c88eed629ecac3ad260ab22318b52148b7a4cc2ac7d8bdf746877b54c15"))

	// https://github.com/ethereum/wiki/wiki/Patricia-Tree

	addr3Node := make([][]byte, 2)
	prefix3rlp := make([]byte, common.HashLength)
	copy(prefix3rlp, addrHash[3].Bytes())
	prefix3rlp[0] = 0x20 // we don't need the first 2 nibbles of the hash in the encoded path
	addr3Node[0] = prefix3rlp
	addr3Node[1] = account3rlp
	node3rlp, err := rlp.EncodeToBytes(addr3Node)
	assert.NoError(t, err)

	addr4Node := make([][]byte, 2)
	prefix4rlp := make([]byte, common.HashLength)
	copy(prefix4rlp, addrHash[4].Bytes())
	prefix4rlp[0] = 0x20 // we don't need the first 2 nibbles of the hash in the encoded path
	addr4Node[0] = prefix4rlp
	addr4Node[1] = account4rlp
	node4rlp, err := rlp.EncodeToBytes(addr4Node)
	assert.NoError(t, err)

	branchNode := make([][]byte, 17)
	branchNode[6] = crypto.Keccak256(node3rlp)
	branchNode[4] = crypto.Keccak256(node4rlp)
	rlpA, err := rlp.EncodeToBytes(branchNode)
	assert.NoError(t, err)

	var reply stateNodesMsg
	reply.ID = 0
	reply.Nodes = [][]byte{rlpA, nil}

	err = p2p.ExpectMsg(peer.app, StateNodesCode, reply)
	if err != nil {
		t.Errorf("unexpected StateNodes response: %v", err)
	}

	// -------------------------------------------------------------------
	// Secondly test the previous state where account3 has once the amount
	// -------------------------------------------------------------------
	request.ID = 1
	request.Block = pm.blockchain.GetBlockByNumber(4).Hash()

	assert.NoError(t, p2p.Send(peer.app, GetStateNodesCode, request))

	account3.Balance.Set(frhsAmnt)
	account3rlp, err = rlp.EncodeToBytes(&account3)
	assert.NoError(t, err)

	addr3Node[1] = account3rlp
	node3rlp, err = rlp.EncodeToBytes(addr3Node)
	assert.NoError(t, err)

	branchNode[6] = crypto.Keccak256(node3rlp)
	rlpA, err = rlp.EncodeToBytes(branchNode)
	assert.NoError(t, err)

	reply.ID = 1
	reply.Nodes = [][]byte{rlpA, nil}

	err = p2p.ExpectMsg(peer.app, StateNodesCode, reply)
	if err != nil {
		t.Errorf("unexpected StateNodes response: %v", err)
	}
}

func TestFirehoseBytecode(t *testing.T) {
	// TODO: remove or recover
	t.Skip()

	// Define two accounts to simulate transactions with
	acc1Key, _ := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	acc2Key, _ := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	acc1Addr := crypto.PubkeyToAddress(acc1Key.PublicKey)
	acc2Addr := crypto.PubkeyToAddress(acc2Key.PublicKey)

	// Two byte codes
	runtimeCode1 := common.FromHex("60606040525b600080fd00a165627a7a7230582012c9bd00152fa1c480f6827f81515bb19c3e63bf7ed9ffbb5fda0265983ac7980029")
	contractCode1 := append(common.FromHex("606060405260186000553415601357600080fd5b5b60368060216000396000f300"), runtimeCode1...)
	runtimeCode2 := common.FromHex("60606040525bfe00a165627a7a72305820c442e8fb2f1f8c3e73151a596376ff0f8da7f4de18ed79a6471c1ec584a14b080029")
	contractCode2 := append(common.FromHex("606060405260046000553415601057fe5b5b603380601e6000396000f300"), runtimeCode2...)

	signer := types.HomesteadSigner{}
	numBlocks := 2
	// Chain generator with a couple of dummy contracts
	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			tx1, err1 := types.SignTx(types.NewTransaction(block.TxNonce(testBank), acc1Addr, uint256.NewInt().SetUint64(2e5), params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err1)
			block.AddTx(tx1)
			tx2, err2 := types.SignTx(types.NewContractCreation(block.TxNonce(acc1Addr), new(uint256.Int), 1e5, nil, contractCode1), signer, acc1Key)
			assert.NoError(t, err2)
			block.AddTx(tx2)
		case 1:
			tx1, err1 := types.SignTx(types.NewTransaction(block.TxNonce(testBank), acc2Addr, uint256.NewInt().SetUint64(2e5), params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err1)
			block.AddTx(tx1)
			tx2, err2 := types.SignTx(types.NewContractCreation(block.TxNonce(acc2Addr), new(uint256.Int), 1e5, nil, contractCode2), signer, acc2Key)
			assert.NoError(t, err2)
			block.AddTx(tx2)
		}
	}

	pm, clear := newTestProtocolManagerMust(t, downloader.StagedSync, numBlocks, generator, nil)
	defer clear()
	peer, _ := newFirehoseTestPeer("peer", pm)
	defer peer.close()

	block1 := pm.blockchain.GetBlockByNumber(1)
	receipts1 := pm.blockchain.GetReceiptsByHash(block1.Hash())
	contract1Addr := receipts1[1].ContractAddress

	block2 := pm.blockchain.GetBlockByNumber(2)
	receipts2 := pm.blockchain.GetReceiptsByHash(block2.Hash())
	contract2Addr := receipts2[1].ContractAddress

	var reqID uint64 = 3758329
	var request getBytecodeMsg
	request.ID = reqID
	request.Ref = []bytecodeRef{
		{Account: contract1Addr.Bytes(), CodeHash: crypto.Keccak256Hash(runtimeCode1)},
		{Account: crypto.Keccak256(contract2Addr.Bytes()), CodeHash: crypto.Keccak256Hash(runtimeCode2)},
	}

	codes := bytecodeMsg{ID: reqID, Code: [][]byte{runtimeCode1, runtimeCode2}}

	assert.NoError(t, p2p.Send(peer.app, GetBytecodeCode, request))
	if err := p2p.ExpectMsg(peer.app, BytecodeCode, codes); err != nil {
		t.Errorf("unexpected Bytecode response: %v", err)
	}
}

// Tests that a propagated malformed block (uncles or transactions don't match
// with the hashes in the header) gets discarded and not broadcast forward.
func TestBroadcastMalformedBlock(t *testing.T) {
	// Create a live node to test propagation with
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		engine  = ethash.NewFaker()
		config  = &params.ChainConfig{}
		gspec   = &core.Genesis{Config: config}
		genesis = gspec.MustCommit(db)
	)
	blockchain, err := core.NewBlockChain(db, nil, config, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create new blockchain: %v", err)
	}
	defer blockchain.Stop()
	pm, err := NewProtocolManager(config, nil, downloader.StagedSync, DefaultConfig.NetworkID, new(event.TypeMux), new(testTxPool), engine, blockchain, db, nil, nil)
	if err != nil {
		t.Fatalf("failed to start test protocol manager: %v", err)
	}
	if err = pm.Start(2, true); err != nil {
		t.Fatalf("error on protocol manager start: %v", err)
	}
	defer pm.Stop()

	// Create two peers, one to send the malformed block with and one to check
	// propagation
	source, _ := newTestPeer("source", eth65, pm, true)
	defer source.close()

	sink, _ := newTestPeer("sink", eth65, pm, true)
	defer sink.close()

	// Create various combinations of malformed blocks
	chain, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 1, func(i int, gen *core.BlockGen) {}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	malformedUncles := chain[0].Header()
	malformedUncles.UncleHash[0]++
	malformedTransactions := chain[0].Header()
	malformedTransactions.TxHash[0]++
	malformedEverything := chain[0].Header()
	malformedEverything.UncleHash[0]++
	malformedEverything.TxHash[0]++

	// Keep listening to broadcasts and notify if any arrives
	notify := make(chan struct{}, 1)
	go func() {
		if _, err := sink.app.ReadMsg(); err == nil {
			notify <- struct{}{}
		}
	}()
	// Try to broadcast all malformations and ensure they all get discarded
	for _, header := range []*types.Header{malformedUncles, malformedTransactions, malformedEverything} {
		block := types.NewBlockWithHeader(header).WithBody(chain[0].Transactions(), chain[0].Uncles())
		if err := p2p.Send(source.app, NewBlockMsg, []interface{}{block, big.NewInt(131136)}); err != nil {
			t.Fatalf("failed to broadcast block: %v", err)
		}
		select {
		case <-notify:
			t.Fatalf("malformed block forwarded")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
