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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/common"
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
	"github.com/ledgerwatch/turbo-geth/trie"
)

// Tests that protocol versions and modes of operations are matched up properly.
func TestProtocolCompatibility(t *testing.T) {
	// Define the compatibility chart
	tests := []struct {
		version    uint
		mode       downloader.SyncMode
		compatible bool
	}{
		{61, downloader.FullSync, true}, {62, downloader.FullSync, true}, {63, downloader.FullSync, true},
		{61, downloader.FastSync, false}, {62, downloader.FastSync, false}, {63, downloader.FastSync, true},
	}
	// Make sure anything we screw up is restored
	backup := ProtocolVersions
	defer func() { ProtocolVersions = backup }()

	// Try all available compatibility configs and check for errors
	for i, tt := range tests {
		ProtocolVersions = []uint{tt.version}

		pm, _, err := newTestProtocolManager(tt.mode, 0, nil, nil)
		if pm != nil {
			defer pm.Stop()
		}
		if (err == nil && !tt.compatible) || (err != nil && tt.compatible) {
			t.Errorf("test %d: compatibility mismatch: have error %v, want compatibility %v", i, err, tt.compatible)
		}
	}
}

// Tests that block headers can be retrieved from a remote chain based on user queries.
func TestGetBlockHeaders62(t *testing.T) { testGetBlockHeaders(t, 62) }
func TestGetBlockHeaders63(t *testing.T) { testGetBlockHeaders(t, 63) }

func testGetBlockHeaders(t *testing.T, protocol int) {
	pm, _ := newTestProtocolManagerMust(t, downloader.FullSync, downloader.MaxHashFetch+15, nil, nil)

	// Create a "random" unknown hash for testing
	var unknown common.Hash
	for i := range unknown {
		unknown[i] = byte(i)
	}
	// Create a batch of tests for various scenarios
	limit := uint64(downloader.MaxHeaderFetch)
	tests := []struct {
		query  *getBlockHeadersData // The query to execute for header retrieval
		expect []common.Hash        // The hashes of the block whose headers are expected
	}{
		// A single random block should be retrievable by hash and number too
		{
			&getBlockHeadersData{Origin: hashOrNumber{Hash: pm.blockchain.GetBlockByNumber(limit / 2).Hash()}, Amount: 1},
			[]common.Hash{pm.blockchain.GetBlockByNumber(limit / 2).Hash()},
		}, {
			&getBlockHeadersData{Origin: hashOrNumber{Number: limit / 2}, Amount: 1},
			[]common.Hash{pm.blockchain.GetBlockByNumber(limit / 2).Hash()},
		},
		// Multiple headers should be retrievable in both directions
		{
			&getBlockHeadersData{Origin: hashOrNumber{Number: limit / 2}, Amount: 3},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(limit / 2).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 + 1).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 + 2).Hash(),
			},
		}, {
			&getBlockHeadersData{Origin: hashOrNumber{Number: limit / 2}, Amount: 3, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(limit / 2).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 - 1).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 - 2).Hash(),
			},
		},
		// Multiple headers with skip lists should be retrievable
		{
			&getBlockHeadersData{Origin: hashOrNumber{Number: limit / 2}, Skip: 3, Amount: 3},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(limit / 2).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 + 4).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 + 8).Hash(),
			},
		}, {
			&getBlockHeadersData{Origin: hashOrNumber{Number: limit / 2}, Skip: 3, Amount: 3, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(limit / 2).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 - 4).Hash(),
				pm.blockchain.GetBlockByNumber(limit/2 - 8).Hash(),
			},
		},
		// The chain endpoints should be retrievable
		{
			&getBlockHeadersData{Origin: hashOrNumber{Number: 0}, Amount: 1},
			[]common.Hash{pm.blockchain.GetBlockByNumber(0).Hash()},
		}, {
			&getBlockHeadersData{Origin: hashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64()}, Amount: 1},
			[]common.Hash{pm.blockchain.CurrentBlock().Hash()},
		},
		// Ensure protocol limits are honored
		{
			&getBlockHeadersData{Origin: hashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64() - 1}, Amount: limit + 10, Reverse: true},
			pm.blockchain.GetBlockHashesFromHash(pm.blockchain.CurrentBlock().Hash(), limit),
		},
		// Check that requesting more than available is handled gracefully
		{
			&getBlockHeadersData{Origin: hashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64() - 4}, Skip: 3, Amount: 3},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(pm.blockchain.CurrentBlock().NumberU64() - 4).Hash(),
				pm.blockchain.GetBlockByNumber(pm.blockchain.CurrentBlock().NumberU64()).Hash(),
			},
		}, {
			&getBlockHeadersData{Origin: hashOrNumber{Number: 4}, Skip: 3, Amount: 3, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(4).Hash(),
				pm.blockchain.GetBlockByNumber(0).Hash(),
			},
		},
		// Check that requesting more than available is handled gracefully, even if mid skip
		{
			&getBlockHeadersData{Origin: hashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64() - 4}, Skip: 2, Amount: 3},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(pm.blockchain.CurrentBlock().NumberU64() - 4).Hash(),
				pm.blockchain.GetBlockByNumber(pm.blockchain.CurrentBlock().NumberU64() - 1).Hash(),
			},
		}, {
			&getBlockHeadersData{Origin: hashOrNumber{Number: 4}, Skip: 2, Amount: 3, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(4).Hash(),
				pm.blockchain.GetBlockByNumber(1).Hash(),
			},
		},
		// Check a corner case where requesting more can iterate past the endpoints
		{
			&getBlockHeadersData{Origin: hashOrNumber{Number: 2}, Amount: 5, Reverse: true},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(2).Hash(),
				pm.blockchain.GetBlockByNumber(1).Hash(),
				pm.blockchain.GetBlockByNumber(0).Hash(),
			},
		},
		// Check a corner case where skipping overflow loops back into the chain start
		{
			&getBlockHeadersData{Origin: hashOrNumber{Hash: pm.blockchain.GetBlockByNumber(3).Hash()}, Amount: 2, Reverse: false, Skip: math.MaxUint64 - 1},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(3).Hash(),
			},
		},
		// Check a corner case where skipping overflow loops back to the same header
		{
			&getBlockHeadersData{Origin: hashOrNumber{Hash: pm.blockchain.GetBlockByNumber(1).Hash()}, Amount: 2, Reverse: false, Skip: math.MaxUint64},
			[]common.Hash{
				pm.blockchain.GetBlockByNumber(1).Hash(),
			},
		},
		// Check that non existing headers aren't returned
		{
			&getBlockHeadersData{Origin: hashOrNumber{Hash: unknown}, Amount: 1},
			[]common.Hash{},
		}, {
			&getBlockHeadersData{Origin: hashOrNumber{Number: pm.blockchain.CurrentBlock().NumberU64() + 1}, Amount: 1},
			[]common.Hash{},
		},
	}
	// Run each of the tests and verify the results against the chain
	for i, tt := range tests {
		peer, _ := newTestPeer("peer", protocol, pm, true)
		defer peer.close()
		// Collect the headers to expect in the response
		headers := []*types.Header{}
		for _, hash := range tt.expect {
			headers = append(headers, pm.blockchain.GetBlockByHash(hash).Header())
		}
		// Send the hash request and verify the response
		p2p.Send(peer.app, 0x03, tt.query)
		if err := p2p.ExpectMsg(peer.app, 0x04, headers); err != nil {
			t.Errorf("test %d: headers mismatch: %v", i, err)
		}
		// If the test used number origins, repeat with hashes as the too
		if tt.query.Origin.Hash == (common.Hash{}) {
			if origin := pm.blockchain.GetBlockByNumber(tt.query.Origin.Number); origin != nil {
				tt.query.Origin.Hash, tt.query.Origin.Number = origin.Hash(), 0

				p2p.Send(peer.app, 0x03, tt.query)
				if err := p2p.ExpectMsg(peer.app, 0x04, headers); err != nil {
					t.Errorf("test %d: headers mismatch: %v", i, err)
				}
			}
		}
	}
}

// Tests that block contents can be retrieved from a remote chain based on their hashes.
func TestGetBlockBodies62(t *testing.T) { testGetBlockBodies(t, 62) }
func TestGetBlockBodies63(t *testing.T) { testGetBlockBodies(t, 63) }

func testGetBlockBodies(t *testing.T, protocol int) {
	pm, _ := newTestProtocolManagerMust(t, downloader.FullSync, downloader.MaxBlockFetch+15, nil, nil)

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

// Tests that the transaction receipts can be retrieved based on hashes.
func TestGetReceipt63(t *testing.T) { testGetReceipt(t, 63) }

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
			tx, _ := types.SignTx(types.NewTransaction(block.TxNonce(testBank), acc1Addr, big.NewInt(10000), params.TxGas, nil, nil), signer, testBankKey)
			block.AddTx(tx)
		case 1:
			// In block 2, the test bank sends some more ether to account #1.
			// acc1Addr passes it on to account #2.
			tx1, _ := types.SignTx(types.NewTransaction(block.TxNonce(testBank), acc1Addr, big.NewInt(1000), params.TxGas, nil, nil), signer, testBankKey)
			tx2, _ := types.SignTx(types.NewTransaction(block.TxNonce(acc1Addr), acc2Addr, big.NewInt(1000), params.TxGas, nil, nil), signer, acc1Key)
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
	pm, _ := newTestProtocolManagerMust(t, downloader.FullSync, 4, generator, nil)
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

// Tests that post eth protocol handshake, DAO fork-enabled clients also execute
// a DAO "challenge" verifying each others' DAO fork headers to ensure they're on
// compatible chains.
func TestDAOChallengeNoVsNo(t *testing.T)       { testDAOChallenge(t, false, false, false) }
func TestDAOChallengeNoVsPro(t *testing.T)      { testDAOChallenge(t, false, true, false) }
func TestDAOChallengeProVsNo(t *testing.T)      { testDAOChallenge(t, true, false, false) }
func TestDAOChallengeProVsPro(t *testing.T)     { testDAOChallenge(t, true, true, false) }
func TestDAOChallengeNoVsTimeout(t *testing.T)  { testDAOChallenge(t, false, false, true) }
func TestDAOChallengeProVsTimeout(t *testing.T) { testDAOChallenge(t, true, true, true) }

func testDAOChallenge(t *testing.T, localForked, remoteForked bool, timeout bool) {
	// Reduce the DAO handshake challenge timeout
	if timeout {
		defer func(old time.Duration) { daoChallengeTimeout = old }(daoChallengeTimeout)
		daoChallengeTimeout = 500 * time.Millisecond
	}
	// Create a DAO aware protocol manager
	var (
		evmux   = new(event.TypeMux)
		pow     = ethash.NewFaker()
		db      = ethdb.NewMemDatabase()
		config  = &params.ChainConfig{DAOForkBlock: big.NewInt(1), DAOForkSupport: localForked}
		gspec   = &core.Genesis{Config: config}
		genesis = gspec.MustCommit(db)
	)
	blockchain, err := core.NewBlockChain(db, nil, config, pow, vm.Config{}, nil)
	if err != nil {
		t.Fatalf("failed to create new blockchain: %v", err)
	}
	pm, err := NewProtocolManager(config, downloader.FullSync, DefaultConfig.NetworkId, evmux, new(testTxPool), pow, blockchain, db, nil)
	if err != nil {
		t.Fatalf("failed to start test protocol manager: %v", err)
	}
	pm.Start(1000)
	defer pm.Stop()

	// Connect a new peer and check that we receive the DAO challenge
	peer, _ := newTestPeer("peer", eth63, pm, true)
	defer peer.close()

	challenge := &getBlockHeadersData{
		Origin:  hashOrNumber{Number: config.DAOForkBlock.Uint64()},
		Amount:  1,
		Skip:    0,
		Reverse: false,
	}
	if err := p2p.ExpectMsg(peer.app, GetBlockHeadersMsg, challenge); err != nil {
		t.Fatalf("challenge mismatch: %v", err)
	}
	// Create a block to reply to the challenge if no timeout is simulated
	if !timeout {
		blocks, _ := core.GenerateChain(&params.ChainConfig{}, genesis, ethash.NewFaker(), db, 1, func(i int, block *core.BlockGen) {
			if remoteForked {
				block.SetExtra(params.DAOForkBlockExtra)
			}
		})
		if err := p2p.Send(peer.app, BlockHeadersMsg, []*types.Header{blocks[0].Header()}); err != nil {
			t.Fatalf("failed to answer challenge: %v", err)
		}
		time.Sleep(100 * time.Millisecond) // Sleep to avoid the verification racing with the drops
	} else {
		// Otherwise wait until the test timeout passes
		time.Sleep(daoChallengeTimeout + 500*time.Millisecond)
	}
	// Verify that depending on fork side, the remote peer is maintained or dropped
	if localForked == remoteForked && !timeout {
		if peers := pm.peers.Len(); peers != 1 {
			t.Fatalf("peer count mismatch: have %d, want %d", peers, 1)
		}
	} else {
		if peers := pm.peers.Len(); peers != 0 {
			t.Fatalf("peer count mismatch: have %d, want %d", peers, 0)
		}
	}
}

func TestBroadcastBlock(t *testing.T) {
	var tests = []struct {
		totalPeers        int
		broadcastExpected int
	}{
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 4},
		{9, 4},
		{12, 4},
		{16, 4},
		{26, 5},
		{100, 10},
	}
	for _, test := range tests {
		testBroadcastBlock(t, test.totalPeers, test.broadcastExpected)
	}
}

func testBroadcastBlock(t *testing.T, totalPeers, broadcastExpected int) {
	var (
		evmux   = new(event.TypeMux)
		pow     = ethash.NewFaker()
		db      = ethdb.NewMemDatabase()
		config  = &params.ChainConfig{}
		gspec   = &core.Genesis{Config: config}
		genesis = gspec.MustCommit(db)
	)
	blockchain, err := core.NewBlockChain(db, nil, config, pow, vm.Config{}, nil)
	if err != nil {
		t.Fatalf("failed to create new blockchain: %v", err)
	}
	pm, err := NewProtocolManager(config, downloader.FullSync, DefaultConfig.NetworkId, evmux, new(testTxPool), pow, blockchain, db, nil)
	if err != nil {
		t.Fatalf("failed to start test protocol manager: %v", err)
	}
	pm.Start(1000)
	defer pm.Stop()
	var peers []*testPeer
	for i := 0; i < totalPeers; i++ {
		peer, _ := newTestPeer(fmt.Sprintf("peer %d", i), eth63, pm, true)
		defer peer.close()
		peers = append(peers, peer)
	}
	chain, _ := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 1, func(i int, gen *core.BlockGen) {})
	pm.BroadcastBlock(chain[0], true /*propagate*/)

	errCh := make(chan error, totalPeers)
	doneCh := make(chan struct{}, totalPeers)
	for _, peer := range peers {
		go func(p *testPeer) {
			if err := p2p.ExpectMsg(p.app, NewBlockMsg, &newBlockData{Block: chain[0], TD: big.NewInt(131136)}); err != nil {
				errCh <- err
			} else {
				doneCh <- struct{}{}
			}
		}(peer)
	}
	timeout := time.After(300 * time.Millisecond)
	var receivedCount int
outer:
	for {
		select {
		case err = <-errCh:
			break outer
		case <-doneCh:
			receivedCount++
			if receivedCount == totalPeers {
				break outer
			}
		case <-timeout:
			break outer
		}
	}
	for _, peer := range peers {
		peer.close()
	}
	if err != nil {
		t.Errorf("error matching block by peer: %v", err)
	}
	if receivedCount != broadcastExpected {
		t.Errorf("block broadcast to %d peers, expected %d", receivedCount, broadcastExpected)
	}
}

func TestFirehoseStateRanges(t *testing.T) {
	addr1 := common.HexToAddress("0x3b4fc1530da632624fa1e223a91d99dbb07c2d42")
	addr2 := common.HexToAddress("0xb574d96f69c1324e3b49e63f4cc899736dd52789")
	addr3 := common.HexToAddress("0xb11e2c7c5b96dbf120ec8af539d028311366af00")
	addr4 := common.HexToAddress("0x7d9eb619ce1033cc710d9f9806a2330f85875f22")

	addr0Hash := crypto.Keccak256Hash(testBank.Bytes())
	addr1Hash := crypto.Keccak256Hash(addr1.Bytes())
	addr2Hash := crypto.Keccak256Hash(addr2.Bytes())
	addr3Hash := crypto.Keccak256Hash(addr3.Bytes())
	addr4Hash := crypto.Keccak256Hash(addr4.Bytes())

	assert.Equal(t, addr0Hash, common.HexToHash("0x00bf49f440a1cd0527e4d06e2765654c0f56452257516d793a9b8d604dcfdf2a"))
	assert.Equal(t, addr1Hash, common.HexToHash("0x1155f85cf8c36b3bf84a89b2d453da3cc5c647ff815a8a809216c47f5ab507a9"))
	assert.Equal(t, addr2Hash, common.HexToHash("0xac8e03d3673a43257a69fcd3ff99a7a17b7d0e0a900c337d55dbd36567938776"))
	assert.Equal(t, addr3Hash, common.HexToHash("0x464b54760c96939ce60fb73b20987db21fce5a624d190f4e769c54a2ba8be49e"))
	assert.Equal(t, addr4Hash, common.HexToHash("0x44091c88eed629ecac3ad260ab22318b52148b7a4cc2ac7d8bdf746877b54c15"))

	signer := types.HomesteadSigner{}
	numBlocks := 5
	amount := big.NewInt(10000)
	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr1, amount, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		case 1:
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr2, amount, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		case 2:
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr3, amount, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		case 3:
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr4, amount, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		case 4:
			// top up account #3
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr3, amount, params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		}
	}

	pm, _ := newTestProtocolManagerMust(t, downloader.FullSync, numBlocks, generator, nil)
	peer, _ := newFirehoseTestPeer("peer", pm)
	defer peer.close()

	block4 := pm.blockchain.GetBlockByNumber(4)

	var request getStateRangesMsg
	request.ID = 1
	request.Block = block4.Hash()

	// All known account keys start with either 0, 1, 4, or a.
	// Warning: we assume that the key of miner's account doesn't start with 2 or 4.
	request.Prefixes = []trie.Keybytes{
		{Data: common.FromHex("40"), Odd: true, Terminating: false},
		{Data: common.FromHex("20"), Odd: true, Terminating: false},
	}

	assert.NoError(t, p2p.Send(peer.app, GetStateRangesCode, request))

	var account accounts.Account
	account.Balance.Set(amount)

	// TODO [yperbasis] remove this RLP hack
	copy(account.CodeHash[:], crypto.Keccak256(nil))
	account.Root = trie.EmptyRoot

	var reply1 stateRangesMsg
	reply1.ID = 1
	reply1.Entries = []accountRange{
		{Status: OK, Leaves: []accountLeaf{{addr4Hash, &account}, {addr3Hash, &account}}},
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
	reply2.Entries = []accountRange{
		{Status: NoData, Leaves: []accountLeaf{}},
		{Status: NoData, Leaves: []accountLeaf{}},
	}
	reply2.AvailableBlocks = []common.Hash{block5.Hash(), block4.Hash(), block3.Hash(), block2.Hash(), block1.Hash(), block0.Hash()}

	if err := p2p.ExpectMsg(peer.app, StateRangesCode, reply2); err != nil {
		t.Errorf("unexpected StateRanges response: %v", err)
	}
}

func TestFirehoseTooManyLeaves(t *testing.T) {
	signer := types.HomesteadSigner{}
	amount := big.NewInt(10)
	generator := func(i int, block *core.BlockGen) {
		var rndAddr common.Address
		// #nosec G404
		rand.Read(rndAddr[:])

		tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), rndAddr, amount, params.TxGas, nil, nil), signer, testBankKey)
		assert.NoError(t, err)
		block.AddTx(tx)
	}

	pm, _ := newTestProtocolManagerMust(t, downloader.FullSync, MaxLeavesPerPrefix, generator, nil)
	peer, _ := newFirehoseTestPeer("peer", pm)
	defer peer.close()

	// ----------------------------------------------------
	// BLOCK #1

	var request getStateRangesMsg
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
	reply1.Entries = []accountRange{
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
	reply3.Entries = []accountRange{
		{Status: TooManyLeaves, Leaves: []accountLeaf{}},
	}

	err = p2p.ExpectMsg(peer.app, StateRangesCode, reply3)
	if err != nil {
		t.Errorf("unexpected StateRanges response: %v", err)
	}
}

func TestFirehoseStorageRanges(t *testing.T) {
	// this smart contract sets its 0th storage to 42
	// see tests/contracts/storage.sol
	code := common.FromHex("0x6080604052602a600055348015601457600080fd5b506083806100236000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c806360fe47b114602d575b600080fd5b604760048036036020811015604157600080fd5b50356049565b005b60005556fea265627a7a72305820c6d2a85ef1ffd7a88a51c6087851886ab108ffe628b0b80e4d95d923559e8d9564736f6c634300050a0032")

	// call set(21)
	set21 := common.FromHex("0x60fe47b10000000000000000000000000000000000000000000000000000000000000015")

	signer := types.HomesteadSigner{}
	var addr common.Address

	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			nonce := block.TxNonce(testBank)
			// storage is initialized to 42
			tx, err := types.SignTx(types.NewContractCreation(nonce, new(big.Int), 2e5, nil, code), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
			addr = crypto.CreateAddress(testBank, nonce)
		case 1:
			// storage is set to 21
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testBank), addr, new(big.Int), 2e5, nil, set21), signer, testBankKey)
			assert.NoError(t, err)
			block.AddTx(tx)
		}
	}

	pm, _ := newTestProtocolManagerMust(t, downloader.FullSync, 2, generator, nil)
	peer, _ := newFirehoseTestPeer("peer", pm)
	defer peer.close()

	var accountReq getStateRangesMsg
	accountReq.ID = 0
	accountReq.Block = pm.blockchain.GetBlockByNumber(1).Hash()
	accountKey := crypto.Keccak256(addr.Bytes())
	accountReq.Prefixes = []trie.Keybytes{
		{Data: accountKey, Odd: false, Terminating: false},
	}

	assert.NoError(t, p2p.Send(peer.app, GetStateRangesCode, accountReq))

	msg, err := peer.app.ReadMsg()
	assert.NoError(t, err)
	content, err := ioutil.ReadAll(msg.Payload)
	assert.NoError(t, err)
	var accountReply stateRangesMsg
	assert.NoError(t, rlp.DecodeBytes(content, &accountReply))

	assert.Equal(t, uint64(0), accountReply.ID)
	assert.Equal(t, 1, len(accountReply.Entries))
	assert.Equal(t, OK, accountReply.Entries[0].Status)
	assert.Equal(t, 1, len(accountReply.Entries[0].Leaves))
	assert.Equal(t, common.BytesToHash(accountKey), accountReply.Entries[0].Leaves[0].Key)

	storageRoot := accountReply.Entries[0].Leaves[0].Val.Root
	assert.NotEqual(t, trie.EmptyRoot, storageRoot)

	var storageReq getStorageRangesMsg
	storageReq.ID = 1
	emptyPrefix := trie.Keybytes{Data: []byte{}, Odd: false, Terminating: false}
	storageReq.Requests = []storageRangeReq{
		{Account: addr.Bytes(), StorageRoot: storageRoot, Prefixes: []trie.Keybytes{emptyPrefix}},
	}

	assert.NoError(t, p2p.Send(peer.app, GetStorageRangesCode, storageReq))

	var storageReply storageRangesMsg
	storageReply.ID = 1
	zerothHash := crypto.Keccak256Hash(common.FromHex("0000000000000000000000000000000000000000000000000000000000000000"))
	storageReply.Entries = [][]storageRange{{
		{Status: OK, Leaves: []storageLeaf{{Key: zerothHash, Val: *(big.NewInt(42))}}},
	}}

	err = p2p.ExpectMsg(peer.app, StorageRangesCode, storageReply)
	if err != nil {
		t.Errorf("unexpected StorageRanges response: %v", err)
	}
}

func TestFirehoseBytecode(t *testing.T) {
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
			tx1, err1 := types.SignTx(types.NewTransaction(block.TxNonce(testBank), acc1Addr, big.NewInt(2e5), params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err1)
			block.AddTx(tx1)
			tx2, err2 := types.SignTx(types.NewContractCreation(block.TxNonce(acc1Addr), new(big.Int), 1e5, nil, contractCode1), signer, acc1Key)
			assert.NoError(t, err2)
			block.AddTx(tx2)
		case 1:
			tx1, err1 := types.SignTx(types.NewTransaction(block.TxNonce(testBank), acc2Addr, big.NewInt(2e5), params.TxGas, nil, nil), signer, testBankKey)
			assert.NoError(t, err1)
			block.AddTx(tx1)
			tx2, err2 := types.SignTx(types.NewContractCreation(block.TxNonce(acc2Addr), new(big.Int), 1e5, nil, contractCode2), signer, acc2Key)
			assert.NoError(t, err2)
			block.AddTx(tx2)
		}
	}

	pm, _ := newTestProtocolManagerMust(t, downloader.FullSync, numBlocks, generator, nil)
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
