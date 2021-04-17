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

package eth

import (
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/params"

	"github.com/holiman/uint256"
)

// testEthHandler is a mock event handler to listen for inbound network requests
// on the `eth` protocol and convert them into a more easily testable form.
type testEthHandler struct {
	blockBroadcasts event.Feed
	txAnnounces     event.Feed
	txBroadcasts    event.Feed
}

func (h *testEthHandler) Chain() *core.BlockChain              { panic("no backing chain") }
func (h *testEthHandler) TxPool() eth.TxPool                   { panic("no backing tx pool") }
func (h *testEthHandler) AcceptTxs() bool                      { return true }
func (h *testEthHandler) RunPeer(*eth.Peer, eth.Handler) error { panic("not used in tests") }
func (h *testEthHandler) PeerInfo(enode.ID) interface{}        { panic("not used in tests") }

func (h *testEthHandler) Handle(peer *eth.Peer, packet eth.Packet) error {
	switch packet := packet.(type) {
	case *eth.NewBlockPacket:
		h.blockBroadcasts.Send(packet.Block)
		return nil

	case *eth.NewPooledTransactionHashesPacket:
		h.txAnnounces.Send(([]common.Hash)(*packet))
		return nil

	case *eth.TransactionsPacket:
		h.txBroadcasts.Send(([]types.Transaction)(*packet))
		return nil

	case *eth.PooledTransactionsPacket:
		h.txBroadcasts.Send(([]types.Transaction)(*packet))
		return nil

	default:
		panic(fmt.Sprintf("unexpected eth packet type in tests: %T", packet))
	}
}

// Tests that peers are correctly accepted (or rejected) based on the advertised
// fork IDs in the protocol handshake.
func TestForkIDSplit65(t *testing.T) { testForkIDSplit(t, eth.ETH65) }

func testForkIDSplit(t *testing.T, protocol uint) {
	var (
		engine = ethash.NewFaker()

		configNoFork  = &params.ChainConfig{HomesteadBlock: big.NewInt(1)}
		configProFork = &params.ChainConfig{
			HomesteadBlock: big.NewInt(1),
			EIP150Block:    big.NewInt(2),
			EIP155Block:    big.NewInt(2),
			EIP158Block:    big.NewInt(2),
			ByzantiumBlock: big.NewInt(3),
		}
		dbNoFork  = ethdb.NewMemoryDatabase()
		dbProFork = ethdb.NewMemoryDatabase()

		gspecNoFork  = &core.Genesis{Config: configNoFork}
		gspecProFork = &core.Genesis{Config: configProFork}

		genesisNoFork  = gspecNoFork.MustCommit(dbNoFork)
		genesisProFork = gspecProFork.MustCommit(dbProFork)

		blocksNoFork, _, _  = core.GenerateChain(configNoFork, genesisNoFork, engine, dbNoFork, 2, nil, false)
		blocksProFork, _, _ = core.GenerateChain(configProFork, genesisProFork, engine, dbProFork, 2, nil, false)

		ethNoFork, _ = newHandler(&handlerConfig{
			Database:    dbNoFork,
			ChainConfig: configNoFork,
			genesis:     genesisNoFork,
			vmConfig:    &vm.Config{},
			engine:      engine,
			TxPool:      newTestTxPool(),
			Network:     1,
			BloomCache:  1,
		})
		ethProFork, _ = newHandler(&handlerConfig{
			Database:    dbProFork,
			ChainConfig: configProFork,
			genesis:     genesisNoFork,
			vmConfig:    &vm.Config{},
			engine:      engine,
			TxPool:      newTestTxPool(),
			Network:     1,
			BloomCache:  1,
		})
	)
	ethNoFork.Start(1000)
	ethProFork.Start(1000)

	// Clean up everything after ourselves
	defer ethNoFork.Stop()
	defer ethProFork.Stop()

	// Both nodes should allow the other to connect (same genesis, next fork is the same)
	p2pNoFork, p2pProFork := p2p.MsgPipe()
	defer p2pNoFork.Close()
	defer p2pProFork.Close()

	peerNoFork := eth.NewPeer(protocol, p2p.NewPeer(enode.ID{1}, "", nil), p2pNoFork, nil)
	peerProFork := eth.NewPeer(protocol, p2p.NewPeer(enode.ID{2}, "", nil), p2pProFork, nil)
	defer peerNoFork.Close()
	defer peerProFork.Close()

	errc := make(chan error, 2)
	go func(errc chan error) {
		errc <- ethNoFork.runEthPeer(peerProFork, func(peer *eth.Peer) error { return nil })
	}(errc)
	go func(errc chan error) {
		errc <- ethProFork.runEthPeer(peerNoFork, func(peer *eth.Peer) error { return nil })
	}(errc)

	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				t.Fatalf("frontier nofork <-> profork failed: %v", err)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("frontier nofork <-> profork handler timeout")
		}
	}
	// Progress into Homestead. Fork's match, so we don't care what the future holds
	if _, err := stagedsync.InsertBlocksInStages(dbNoFork, ethdb.DefaultStorageMode, configNoFork, &vm.Config{}, ethash.NewFaker(), blocksNoFork[:1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	atomic.StoreUint64(&ethNoFork.currentHeight, 1)
	if _, err := stagedsync.InsertBlocksInStages(dbProFork, ethdb.DefaultStorageMode, configProFork, &vm.Config{}, ethash.NewFaker(), blocksProFork[:1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	atomic.StoreUint64(&ethProFork.currentHeight, 1)

	p2pNoFork, p2pProFork = p2p.MsgPipe()
	defer p2pNoFork.Close()
	defer p2pProFork.Close()

	peerNoFork = eth.NewPeer(protocol, p2p.NewPeer(enode.ID{1}, "", nil), p2pNoFork, nil)
	peerProFork = eth.NewPeer(protocol, p2p.NewPeer(enode.ID{2}, "", nil), p2pProFork, nil)
	defer peerNoFork.Close()
	defer peerProFork.Close()

	errc = make(chan error, 2)
	go func(errc chan error) {
		errc <- ethNoFork.runEthPeer(peerProFork, func(peer *eth.Peer) error { return nil })
	}(errc)
	go func(errc chan error) {
		errc <- ethProFork.runEthPeer(peerNoFork, func(peer *eth.Peer) error { return nil })
	}(errc)

	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				t.Fatalf("homestead nofork <-> profork failed: %v", err)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("homestead nofork <-> profork handler timeout")
		}
	}
	// Progress into Spurious. Forks mismatch, signalling differing chains, reject
	if _, err := stagedsync.InsertBlocksInStages(dbNoFork, ethdb.DefaultStorageMode, configNoFork, &vm.Config{}, ethash.NewFaker(), blocksNoFork[1:2], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	atomic.StoreUint64(&ethNoFork.currentHeight, 2)
	if _, err := stagedsync.InsertBlocksInStages(dbProFork, ethdb.DefaultStorageMode, configProFork, &vm.Config{}, ethash.NewFaker(), blocksProFork[1:2], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	atomic.StoreUint64(&ethProFork.currentHeight, 2)

	p2pNoFork, p2pProFork = p2p.MsgPipe()
	defer p2pNoFork.Close()
	defer p2pProFork.Close()

	peerNoFork = eth.NewPeer(protocol, p2p.NewPeer(enode.ID{1}, "", nil), p2pNoFork, nil)
	peerProFork = eth.NewPeer(protocol, p2p.NewPeer(enode.ID{2}, "", nil), p2pProFork, nil)
	defer peerNoFork.Close()
	defer peerProFork.Close()

	errc = make(chan error, 2)
	go func(errc chan error) {
		errc <- ethNoFork.runEthPeer(peerProFork, func(peer *eth.Peer) error { return nil })
	}(errc)
	go func(errc chan error) {
		errc <- ethProFork.runEthPeer(peerNoFork, func(peer *eth.Peer) error { return nil })
	}(errc)

	var successes int
	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err == nil {
				successes++
				if successes == 2 { // Only one side disconnects
					t.Fatalf("fork ID rejection didn't happen")
				}
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("split peers not rejected")
		}
	}
}

// Tests that received transactions are added to the local pool.
func TestRecvTransactions66(t *testing.T) { testRecvTransactions(t, eth.ETH66) }

func testRecvTransactions(t *testing.T, protocol uint) {
	// Create a message handler, configure it to accept transactions and watch them
	handler := newTestHandler()
	defer handler.close()

	handler.handler.acceptTxs = 1 // mark synced to accept transactions

	txs := make(chan core.NewTxsEvent)
	sub := handler.txpool.SubscribeNewTxsEvent(txs)
	defer sub.Unsubscribe()

	// Create a source peer to send messages through and a sink handler to receive them
	p2pSrc, p2pSink := p2p.MsgPipe()
	defer p2pSrc.Close()
	defer p2pSink.Close()

	src := eth.NewPeer(protocol, p2p.NewPeer(enode.ID{1}, "", nil), p2pSrc, handler.txpool)
	sink := eth.NewPeer(protocol, p2p.NewPeer(enode.ID{2}, "", nil), p2pSink, handler.txpool)
	defer src.Close()
	defer sink.Close()

	//nolint:errcheck
	go handler.handler.runEthPeer(sink, func(peer *eth.Peer) error {
		return eth.Handle((*ethHandler)(handler.handler), peer)
	})
	// Run the handshake locally to avoid spinning up a source handler
	var (
		genesis = handler.genesis
		head    = handler.headBlock
	)
	td, err := rawdb.ReadTd(handler.db, head.Hash(), head.NumberU64())
	if err != nil {
		t.Fatal(err)
	}
	if err = src.Handshake(1, td, head.Hash(), genesis.Hash(), forkid.NewID(handler.ChainConfig, genesis.Hash(), head.NumberU64()), forkid.NewFilter(handler.ChainConfig, genesis.Hash(), func() uint64 { return head.NumberU64() })); err != nil {
		t.Fatalf("failed to run protocol handshake: %v", err)
	}
	// Send the transaction to the sink and verify that it's added to the tx pool
	var tx types.Transaction = types.NewTransaction(0, common.Address{}, uint256.NewInt(), 100000, uint256.NewInt(), nil)
	tx, _ = types.SignTx(tx, *types.LatestSignerForChainID(nil), testKey)

	if err := src.SendTransactions([]types.Transaction{tx}); err != nil {
		t.Fatalf("failed to send transaction: %v", err)
	}
	select {
	case event := <-txs:
		if len(event.Txs) != 1 {
			t.Errorf("wrong number of added transactions: got %d, want 1", len(event.Txs))
		} else if event.Txs[0].Hash() != tx.Hash() {
			t.Errorf("added wrong tx hash: got %v, want %v", event.Txs[0].Hash(), tx.Hash())
		}
	case <-time.After(2 * time.Second):
		t.Errorf("no NewTxsEvent received within 2 seconds")
	}
}

// This test checks that pending transactions are sent.
func TestSendTransactions66(t *testing.T) { testSendTransactions(t, eth.ETH66) }

func testSendTransactions(t *testing.T, protocol uint) {
	// Create a message handler and fill the pool with big transactions
	handler := newTestHandler()
	defer handler.close()

	insert := make([]types.Transaction, 100)
	for nonce := range insert {
		var tx types.Transaction = types.NewTransaction(uint64(nonce), common.Address{}, uint256.NewInt(), 100000, uint256.NewInt(), make([]byte, txsyncPackSize/10))
		tx, _ = types.SignTx(tx, *types.LatestSignerForChainID(nil), testKey)

		insert[nonce] = tx
	}
	go handler.txpool.AddRemotes(insert) // Need goroutine to not block on feed
	time.Sleep(250 * time.Millisecond)   // Wait until tx events get out of the system (can't use events, tx broadcaster races with peer join)

	// Create a source handler to send messages through and a sink peer to receive them
	p2pSrc, p2pSink := p2p.MsgPipe()
	defer p2pSrc.Close()
	defer p2pSink.Close()

	src := eth.NewPeer(protocol, p2p.NewPeer(enode.ID{1}, "", nil), p2pSrc, handler.txpool)
	sink := eth.NewPeer(protocol, p2p.NewPeer(enode.ID{2}, "", nil), p2pSink, handler.txpool)
	defer src.Close()
	defer sink.Close()

	//nolint:errcheck
	go handler.handler.runEthPeer(src, func(peer *eth.Peer) error {
		return eth.Handle((*ethHandler)(handler.handler), peer)
	})
	// Run the handshake locally to avoid spinning up a source handler
	var (
		genesis = handler.genesis
		head    = handler.headBlock
	)
	td, err := rawdb.ReadTd(handler.db, head.Hash(), head.NumberU64())
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Handshake(1, td, head.Hash(), genesis.Hash(), forkid.NewID(handler.ChainConfig, genesis.Hash(), head.NumberU64()), forkid.NewFilter(handler.ChainConfig, genesis.Hash(), func() uint64 { return head.NumberU64() })); err != nil {
		t.Fatalf("failed to run protocol handshake: %v", err)
	}
	// After the handshake completes, the source handler should stream the sink
	// the transactions, subscribe to all inbound network events
	backend := new(testEthHandler)

	anns := make(chan []common.Hash)
	annSub := backend.txAnnounces.Subscribe(anns)
	defer annSub.Unsubscribe()

	bcasts := make(chan []*types.Transaction)
	bcastSub := backend.txBroadcasts.Subscribe(bcasts)
	defer bcastSub.Unsubscribe()

	//nolint:errcheck
	go eth.Handle(backend, sink)

	// Make sure we get all the transactions on the correct channels
	seen := make(map[common.Hash]struct{})
	for len(seen) < len(insert) {
		switch protocol {
		case eth.ETH66:
			select {
			case hashes := <-anns:
				for _, hash := range hashes {
					if _, ok := seen[hash]; ok {
						t.Errorf("duplicate transaction announced: %x", hash)
					}
					seen[hash] = struct{}{}
				}
			case <-bcasts:
				t.Errorf("initial tx broadcast received on post eth/65")
			}

		default:
			panic("unsupported protocol, please extend test")
		}
	}
	for _, tx := range insert {
		if _, ok := seen[tx.Hash()]; !ok {
			t.Errorf("missing transaction: %x", tx.Hash())
		}
	}
}

// Tests that blocks are broadcast to a sqrt number of peers only.
func TestBroadcastBlock1Peer(t *testing.T)    { testBroadcastBlock(t, 1, 1) }
func TestBroadcastBlock2Peers(t *testing.T)   { testBroadcastBlock(t, 2, 1) }
func TestBroadcastBlock3Peers(t *testing.T)   { testBroadcastBlock(t, 3, 1) }
func TestBroadcastBlock4Peers(t *testing.T)   { testBroadcastBlock(t, 4, 2) }
func TestBroadcastBlock5Peers(t *testing.T)   { testBroadcastBlock(t, 5, 2) }
func TestBroadcastBlock8Peers(t *testing.T)   { testBroadcastBlock(t, 9, 3) }
func TestBroadcastBlock12Peers(t *testing.T)  { testBroadcastBlock(t, 12, 3) }
func TestBroadcastBlock16Peers(t *testing.T)  { testBroadcastBlock(t, 16, 4) }
func TestBroadcastBloc26Peers(t *testing.T)   { testBroadcastBlock(t, 26, 5) }
func TestBroadcastBlock100Peers(t *testing.T) { testBroadcastBlock(t, 100, 10) }

func testBroadcastBlock(t *testing.T, peers, bcasts int) {
	// Create a source handler to broadcast blocks from and a number of sinks
	// to receive them.
	source := newTestHandlerWithBlocks(1)
	defer source.close()

	sinks := make([]*testEthHandler, peers)
	for i := 0; i < len(sinks); i++ {
		sinks[i] = new(testEthHandler)
	}
	// Interconnect all the sink handlers with the source handler
	var (
		genesis = source.genesis
		head    = source.headBlock
	)
	td, err := rawdb.ReadTd(source.db, head.Hash(), head.NumberU64())
	if err != nil {
		t.Fatal(err)
	}
	for i, sink := range sinks {
		sink := sink // Closure for gorotuine below

		sourcePipe, sinkPipe := p2p.MsgPipe()
		defer sourcePipe.Close()
		defer sinkPipe.Close()

		sourcePeer := eth.NewPeer(eth.ETH66, p2p.NewPeer(enode.ID{byte(i)}, "", nil), sourcePipe, nil)
		sinkPeer := eth.NewPeer(eth.ETH66, p2p.NewPeer(enode.ID{0}, "", nil), sinkPipe, nil)
		defer sourcePeer.Close()
		defer sinkPeer.Close()

		//nolint:errcheck
		go source.handler.runEthPeer(sourcePeer, func(peer *eth.Peer) error {
			return eth.Handle((*ethHandler)(source.handler), peer)
		})
		if err := sinkPeer.Handshake(1, td, genesis.Hash(), genesis.Hash(), forkid.NewID(source.ChainConfig, genesis.Hash(), head.NumberU64()), forkid.NewFilter(source.ChainConfig, genesis.Hash(), func() uint64 { return head.NumberU64() })); err != nil {
			t.Fatalf("failed to run protocol handshake")
		}
		//nolint:errcheck
		go eth.Handle(sink, sinkPeer)
	}
	// Subscribe to all the transaction pools
	blockChs := make([]chan *types.Block, len(sinks))
	for i := 0; i < len(sinks); i++ {
		blockChs[i] = make(chan *types.Block, 1)
		defer close(blockChs[i])

		sub := sinks[i].blockBroadcasts.Subscribe(blockChs[i])
		defer sub.Unsubscribe()
	}
	// Initiate a block propagation across the peers
	time.Sleep(100 * time.Millisecond)

	source.handler.BroadcastBlock(rawdb.ReadCurrentBlock(source.db), true)

	// Iterate through all the sinks and ensure the correct number got the block
	done := make(chan struct{}, peers)
	for _, ch := range blockChs {
		ch := ch
		go func() {
			<-ch
			done <- struct{}{}
		}()
	}
	var received int
	for {
		select {
		case <-done:
			received++

		case <-time.After(100 * time.Millisecond):
			if received != bcasts {
				t.Errorf("broadcast count mismatch: have %d, want %d", received, bcasts)
			}
			return
		}
	}
}

func TestBroadcastMalformedBlock66(t *testing.T) { testBroadcastMalformedBlock(t, eth.ETH66) }

func testBroadcastMalformedBlock(t *testing.T, protocol uint) {
	// Create a source handler to broadcast blocks from and a number of sinks
	// to receive them.
	source := newTestHandlerWithBlocks(1)
	defer source.close()

	// Create a source handler to send messages through and a sink peer to receive them
	p2pSrc, p2pSink := p2p.MsgPipe()
	defer p2pSrc.Close()
	defer p2pSink.Close()

	src := eth.NewPeer(protocol, p2p.NewPeer(enode.ID{1}, "", nil), p2pSrc, source.txpool)
	sink := eth.NewPeer(protocol, p2p.NewPeer(enode.ID{2}, "", nil), p2pSink, source.txpool)
	defer src.Close()
	defer sink.Close()

	//nolint:errcheck
	go source.handler.runEthPeer(src, func(peer *eth.Peer) error {
		return eth.Handle((*ethHandler)(source.handler), peer)
	})
	// Run the handshake locally to avoid spinning up a sink handler
	var (
		genesis = source.genesis
		head    = source.headBlock
	)
	td, err := rawdb.ReadTd(source.db, head.Hash(), head.NumberU64())
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Handshake(1, td, genesis.Hash(), genesis.Hash(), forkid.NewID(source.ChainConfig, genesis.Hash(), head.NumberU64()), forkid.NewFilter(source.ChainConfig, genesis.Hash(), func() uint64 { return head.NumberU64() })); err != nil {
		t.Fatalf("failed to run protocol handshake")
	}
	// After the handshake completes, the source handler should stream the sink
	// the blocks, subscribe to inbound network events
	backend := new(testEthHandler)

	blocks := make(chan *types.Block, 1)
	sub := backend.blockBroadcasts.Subscribe(blocks)
	defer sub.Unsubscribe()

	//nolint:errcheck
	go eth.Handle(backend, sink)

	malformedUncles := head.Header()
	malformedUncles.UncleHash[0]++
	malformedTransactions := head.Header()
	malformedTransactions.TxHash[0]++
	malformedEverything := head.Header()
	malformedEverything.UncleHash[0]++
	malformedEverything.TxHash[0]++

	// Try to broadcast all malformations and ensure they all get discarded
	for _, header := range []*types.Header{malformedUncles, malformedTransactions, malformedEverything} {
		block := types.NewBlockWithHeader(header).WithBody(head.Transactions(), head.Uncles())
		if err := src.SendNewBlock(block, big.NewInt(131136)); err != nil {
			t.Fatalf("failed to broadcast block: %v", err)
		}
		select {
		case <-blocks:
			t.Fatalf("malformed block forwarded")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
