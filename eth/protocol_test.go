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

// nolint:errcheck
package eth

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func init() {
	// log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(false))))
}

var testAccount, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

func TestStatusMsgErrors64(t *testing.T) {
	pm, clear := newTestProtocolManagerMust(t, downloader.StagedSync, 0, nil, nil)
	defer clear()
	var (
		genesis = pm.blockchain.Genesis()
		head    = pm.blockchain.CurrentHeader()
		td      = pm.blockchain.GetTd(head.Hash(), head.Number.Uint64())
		forkID  = forkid.NewID(pm.blockchain.Config(), genesis.Hash(), head.Number.Uint64())
	)

	tests := []struct {
		code      uint64
		data      interface{}
		wantError error
	}{
		{
			code: TransactionMsg, data: []interface{}{},
			wantError: errResp(ErrNoStatusMsg, "first msg has code 2 (!= 0)"),
		},
		{
			code: StatusMsg, data: StatusData{10, DefaultConfig.NetworkID, td, head.Hash(), genesis.Hash(), forkID},
			wantError: errResp(ErrProtocolVersionMismatch, "10 (!= %d)", 64),
		},
		{
			code: StatusMsg, data: StatusData{64, 999, td, head.Hash(), genesis.Hash(), forkID},
			wantError: errResp(ErrNetworkIDMismatch, "999 (!= %d)", DefaultConfig.NetworkID),
		},
		{
			code: StatusMsg, data: StatusData{64, DefaultConfig.NetworkID, td, head.Hash(), common.Hash{3}, forkID},
			wantError: errResp(ErrGenesisMismatch, "0300000000000000000000000000000000000000000000000000000000000000 (!= %x)", genesis.Hash()),
		},
		{
			code: StatusMsg, data: StatusData{64, DefaultConfig.NetworkID, td, head.Hash(), genesis.Hash(), forkid.ID{Hash: [4]byte{0x00, 0x01, 0x02, 0x03}}},
			wantError: errResp(ErrForkIDRejected, forkid.ErrLocalIncompatibleOrStale.Error()),
		},
	}
	for i, test := range tests {
		p, errc := newTestPeer("peer", 64, pm, false)
		// The send call might hang until reset because
		// the protocol might not read the payload.
		go p2p.Send(p.app, test.code, test.data)

		select {
		case err := <-errc:
			if err == nil {
				t.Errorf("test %d: protocol returned nil error, want %q", i, test.wantError)
			} else if err.Error() != test.wantError.Error() {
				t.Errorf("test %d: wrong error: got %q, want %q", i, err, test.wantError)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("protocol did not shut down within 2 seconds")
		}
		p.close()
	}
}

func TestStatusMsgErrors65(t *testing.T) {
	pm, clear := newTestProtocolManagerMust(t, downloader.StagedSync, 0, nil, nil)
	defer clear()
	var (
		genesis = pm.blockchain.Genesis()
		head    = pm.blockchain.CurrentHeader()
		td      = pm.blockchain.GetTd(head.Hash(), head.Number.Uint64())
		forkID  = forkid.NewID(pm.blockchain.Config(), pm.blockchain.Genesis().Hash(), pm.blockchain.CurrentHeader().Number.Uint64())
	)

	tests := []struct {
		code      uint64
		data      interface{}
		wantError error
	}{
		{
			code: TransactionMsg, data: []interface{}{},
			wantError: errResp(ErrNoStatusMsg, "first msg has code 2 (!= 0)"),
		},
		{
			code: StatusMsg, data: StatusData{10, DefaultConfig.NetworkID, td, head.Hash(), genesis.Hash(), forkID},
			wantError: errResp(ErrProtocolVersionMismatch, "10 (!= %d)", 65),
		},
		{
			code: StatusMsg, data: StatusData{65, 999, td, head.Hash(), genesis.Hash(), forkID},
			wantError: errResp(ErrNetworkIDMismatch, "999 (!= %d)", DefaultConfig.NetworkID),
		},
		{
			code: StatusMsg, data: StatusData{65, DefaultConfig.NetworkID, td, head.Hash(), common.Hash{3}, forkID},
			wantError: errResp(ErrGenesisMismatch, "0300000000000000000000000000000000000000000000000000000000000000 (!= %x)", genesis.Hash()),
		},
		{
			code: StatusMsg, data: StatusData{65, DefaultConfig.NetworkID, td, head.Hash(), genesis.Hash(), forkid.ID{Hash: [4]byte{0x00, 0x01, 0x02, 0x03}}},
			wantError: errResp(ErrForkIDRejected, forkid.ErrLocalIncompatibleOrStale.Error()),
		},
	}
	for i, test := range tests {
		p, errc := newTestPeer("peer", 65, pm, false)
		// The send call might hang until reset because
		// the protocol might not read the payload.
		go p2p.Send(p.app, test.code, test.data)

		select {
		case err := <-errc:
			if err == nil {
				t.Errorf("test %d: protocol returned nil error, want %q", i, test.wantError)
			} else if err.Error() != test.wantError.Error() {
				t.Errorf("test %d: wrong error: got %q, want %q", i, err, test.wantError)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("protocol did not shut down within 2 seconds")
		}
		p.close()
	}
}

func TestForkIDSplit(t *testing.T) {
	dbNoFork := ethdb.NewMemDatabase()
	defer dbNoFork.Close()
	dbProFork := ethdb.NewMemDatabase()
	defer dbProFork.Close()

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

		gspecNoFork  = &core.Genesis{Config: configNoFork}
		gspecProFork = &core.Genesis{Config: configProFork}

		genesisNoFork  = gspecNoFork.MustCommit(dbNoFork)
		genesisProFork = gspecProFork.MustCommit(dbProFork)

		txCacherNoFork  = core.NewTxSenderCacher(runtime.NumCPU())
		chainNoFork, _  = core.NewBlockChain(dbNoFork, nil, configNoFork, engine, vm.Config{}, nil, txCacherNoFork)
		txCacherProFork = core.NewTxSenderCacher(runtime.NumCPU())
		chainProFork, _ = core.NewBlockChain(dbProFork, nil, configProFork, engine, vm.Config{}, nil, txCacherProFork)

		blocksNoFork, _, _  = core.GenerateChain(configNoFork, genesisNoFork, engine, dbNoFork, 2, nil, false /* intermediateHashes */)
		blocksProFork, _, _ = core.GenerateChain(configProFork, genesisProFork, engine, dbProFork, 2, nil, false /* intermediateHashes */)

		ethNoFork, _  = NewProtocolManager(configNoFork, nil, downloader.StagedSync, 1, new(event.TypeMux), new(testTxPool), engine, chainNoFork, dbNoFork, nil, nil)
		ethProFork, _ = NewProtocolManager(configProFork, nil, downloader.StagedSync, 1, new(event.TypeMux), new(testTxPool), engine, chainProFork, dbProFork, nil, nil)
	)

	defer func() {
		chainNoFork.Stop()
		chainProFork.Stop()
	}()

	if err := ethNoFork.Start(1000, true); err != nil {
		t.Fatalf("error on protocol manager start: %v", err)
	}
	defer ethNoFork.Stop()

	if err := ethProFork.Start(1000, true); err != nil {
		t.Fatalf("error on protocol manager start: %v", err)
	}
	defer ethProFork.Stop()

	// Both nodes should allow the other to connect (same genesis, next fork is the same)
	p2pNoFork, p2pProFork := p2p.MsgPipe()
	defer func() {
		p2pNoFork.Close()
		p2pProFork.Close()
	}()

	peerNoFork := newPeer(64, p2p.NewPeer(enode.ID{1}, "", nil), p2pNoFork, nil)
	peerProFork := newPeer(64, p2p.NewPeer(enode.ID{2}, "", nil), p2pProFork, nil)

	errc := make(chan error, 2)
	go func() { errc <- ethNoFork.handle(peerProFork) }()
	go func() { errc <- ethProFork.handle(peerNoFork) }()

	select {
	case err := <-errc:
		t.Fatalf("frontier nofork <-> profork failed: %v", err)
	case <-time.After(250 * time.Millisecond):
		p2pNoFork.Close()
		p2pProFork.Close()
	}
	// Progress into Homestead. Fork's match, so we don't care what the future holds
	_, _ = chainNoFork.InsertChain(context.Background(), blocksNoFork[:1])
	_, _ = chainProFork.InsertChain(context.Background(), blocksProFork[:1])

	p2pNoFork, p2pProFork = p2p.MsgPipe()
	peerNoFork = newPeer(64, p2p.NewPeer(enode.ID{1}, "", nil), p2pNoFork, nil)
	peerProFork = newPeer(64, p2p.NewPeer(enode.ID{2}, "", nil), p2pProFork, nil)

	errcHomesteadNoFork := make(chan error, 2)
	go func() { errcHomesteadNoFork <- ethNoFork.handle(peerProFork) }()
	go func() { errcHomesteadNoFork <- ethProFork.handle(peerNoFork) }()

	select {
	case err := <-errcHomesteadNoFork:
		t.Fatalf("homestead nofork <-> profork failed: %v", err)
	case <-time.After(250 * time.Millisecond):
		p2pNoFork.Close()
		p2pProFork.Close()
	}
	// Progress into Spurious. Forks mismatch, signalling differing chains, reject
	_, _ = chainNoFork.InsertChain(context.Background(), blocksNoFork[1:2])
	_, _ = chainProFork.InsertChain(context.Background(), blocksProFork[1:2])

	p2pNoFork, p2pProFork = p2p.MsgPipe()
	peerNoFork = newPeer(64, p2p.NewPeer(enode.ID{1}, "", nil), p2pNoFork, nil)
	peerProFork = newPeer(64, p2p.NewPeer(enode.ID{2}, "", nil), p2pProFork, nil)

	errcSpuriousNoFork := make(chan error, 2)
	go func() { errcSpuriousNoFork <- ethNoFork.handle(peerProFork) }()
	go func() { errcSpuriousNoFork <- ethProFork.handle(peerNoFork) }()

	select {
	case err := <-errcSpuriousNoFork:
		if want := errResp(ErrForkIDRejected, forkid.ErrLocalIncompatibleOrStale.Error()); err.Error() != want.Error() {
			t.Fatalf("fork ID rejection error mismatch: have %v, want %v", err, want)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("split peers not rejected")
	}
}

// This test checks that received transactions are added to the local pool.
func TestRecvTransactions64(t *testing.T) { testRecvTransactions(t, 64) }
func TestRecvTransactions65(t *testing.T) { testRecvTransactions(t, 65) }

func testRecvTransactions(t *testing.T, protocol int) {
	txAdded := make(chan []*types.Transaction)
	pm, clear := newTestProtocolManagerMust(t, downloader.StagedSync, 0, nil, txAdded)
	defer clear()
	pm.acceptTxs = 1 // mark synced to accept transactions
	p, _ := newTestPeer("peer", protocol, pm, true)
	defer p.close()

	tx := newTestTransaction(testAccount, 0, 0)
	if err := p2p.Send(p.app, TransactionMsg, []interface{}{tx}); err != nil {
		t.Fatalf("send error: %v", err)
	}
	select {
	case added := <-txAdded:
		if len(added) != 1 {
			t.Errorf("wrong number of added transactions: got %d, want 1", len(added))
		} else if added[0].Hash() != tx.Hash() {
			t.Errorf("added wrong tx hash: got %v, want %v", added[0].Hash(), tx.Hash())
		}
	case <-time.After(2 * time.Second):
		t.Errorf("no NewTxsEvent received within 2 seconds")
	}
}

// This test checks that pending transactions are sent.
func TestSendTransactions64(t *testing.T) { testSendTransactions(t, 64) }
func TestSendTransactions65(t *testing.T) { testSendTransactions(t, 65) }

func testSendTransactions(t *testing.T, protocol int) {
	pm, clear := newTestProtocolManagerMust(t, downloader.StagedSync, 0, nil, nil)
	defer clear()

	// Fill the pool with big transactions (use a subscription to wait until all
	// the transactions are announced to avoid spurious events causing extra
	// broadcasts).
	const txsize = txsyncPackSize / 10
	alltxs := make([]*types.Transaction, 100)
	for nonce := range alltxs {
		alltxs[nonce] = newTestTransaction(testAccount, uint64(nonce), txsize)
	}
	pm.txpool.AddRemotes(alltxs)
	time.Sleep(100 * time.Millisecond) // Wait until new tx even gets out of the system (lame)

	// Connect several peers. They should all receive the pending transactions.
	var wg sync.WaitGroup
	checktxs := func(p *testPeer) {
		defer wg.Done()
		defer p.close()
		seen := make(map[common.Hash]bool)
		for _, tx := range alltxs {
			seen[tx.Hash()] = false
		}
		for n := 0; n < len(alltxs) && !t.Failed(); {
			var forAllHashes func(callback func(hash common.Hash))
			switch protocol {
			case 64:
				msg, err := p.app.ReadMsg()
				if err != nil {
					t.Errorf("%v: read error: %v", p.Peer, err)
					continue
				} else if msg.Code != TransactionMsg {
					t.Errorf("%v: got code %d, want TxMsg", p.Peer, msg.Code)
					continue
				}
				var txs []*types.Transaction
				if err := msg.Decode(&txs); err != nil {
					t.Errorf("%v: %v", p.Peer, err)
					continue
				}
				forAllHashes = func(callback func(hash common.Hash)) {
					for _, tx := range txs {
						callback(tx.Hash())
					}
				}
			case 65:
				msg, err := p.app.ReadMsg()
				if err != nil {
					t.Errorf("%v: read error: %v", p.Peer, err)
					continue
				} else if msg.Code != NewPooledTransactionHashesMsg {
					t.Errorf("%v: got code %d, want NewPooledTransactionHashesMsg", p.Peer, msg.Code)
					continue
				}
				var hashes []common.Hash
				if err := msg.Decode(&hashes); err != nil {
					t.Errorf("%v: %v", p.Peer, err)
					continue
				}
				forAllHashes = func(callback func(hash common.Hash)) {
					for _, h := range hashes {
						callback(h)
					}
				}
			}
			forAllHashes(func(hash common.Hash) {
				seentx, want := seen[hash]
				if seentx {
					t.Errorf("%v: got tx more than once: %x", p.Peer, hash)
				}
				if !want {
					t.Errorf("%v: got unexpected tx: %x", p.Peer, hash)
				}
				seen[hash] = true
				n++
			})
		}
	}
	for i := 0; i < 3; i++ {
		p, _ := newTestPeer(fmt.Sprintf("peer #%d", i), protocol, pm, true)
		wg.Add(1)
		go checktxs(p)
	}
	wg.Wait()
}

func TestTransactionPropagation(t *testing.T)  { testSyncTransaction(t, true) }
func TestTransactionAnnouncement(t *testing.T) { testSyncTransaction(t, false) }

func testSyncTransaction(t *testing.T, propagtion bool) {
	t.Skip("deadlocks two peers but only in the test mode")
	// Create a protocol manager for transaction fetcher and sender
	pmFetcher, fetcherClear := newTestProtocolManagerMust(t, downloader.StagedSync, 0, nil, nil)
	defer fetcherClear()
	pmSender, senderClear := newTestProtocolManagerMust(t, downloader.StagedSync, 1024, nil, nil)
	defer senderClear()
	pmSender.broadcastTxAnnouncesOnly = !propagtion

	// Sync up the two peers
	io1, io2 := p2p.MsgPipe()

	senderPeer := pmSender.newPeer(65, p2p.NewPeer(enode.ID{}, "sender", nil), io2, pmSender.txpool.Get)
	go pmSender.handle(senderPeer)
	fetcherPeer := pmFetcher.newPeer(65, p2p.NewPeer(enode.ID{}, "fetcher", nil), io1, pmFetcher.txpool.Get)
	go pmFetcher.handle(fetcherPeer)

	time.Sleep(250 * time.Millisecond)
	pmFetcher.doSync(peerToSyncOp(downloader.StagedSync, pmFetcher.peers.BestPeer()))
	atomic.StoreUint32(&pmFetcher.acceptTxs, 1)

	newTxs := make(chan core.NewTxsEvent, 1024)
	sub := pmFetcher.txpool.SubscribeNewTxsEvent(newTxs)
	defer sub.Unsubscribe()

	// Fill the pool with new transactions
	alltxs := make([]*types.Transaction, 1024)
	for nonce := range alltxs {
		alltxs[nonce] = newTestTransaction(testAccount, uint64(nonce), 0)
	}
	pmSender.txpool.AddRemotes(alltxs)

	var got int
	const expected = 1024
loop:
	for {
		select {
		case ev := <-newTxs:
			got += len(ev.Txs)
			if got == expected {
				break loop
			}
		case <-time.NewTimer(time.Second).C:
			t.Fatalf("Failed to retrieve all transaction. got %d, expected %d", got, expected)
		}
	}
}

// Tests that the custom union field encoder and decoder works correctly.
func TestGetBlockHeadersDataEncodeDecode(t *testing.T) {
	// Create a "random" hash for testing
	var hash common.Hash
	for i := range hash {
		hash[i] = byte(i)
	}
	// Assemble some table driven tests
	tests := []struct {
		packet *GetBlockHeadersData
		fail   bool
	}{
		// Providing the origin as either a hash or a number should both work
		{fail: false, packet: &GetBlockHeadersData{Origin: HashOrNumber{Number: 314}}},
		{fail: false, packet: &GetBlockHeadersData{Origin: HashOrNumber{Hash: hash}}},

		// Providing arbitrary query field should also work
		{fail: false, packet: &GetBlockHeadersData{Origin: HashOrNumber{Number: 314}, Amount: 314, Skip: 1, Reverse: true}},
		{fail: false, packet: &GetBlockHeadersData{Origin: HashOrNumber{Hash: hash}, Amount: 314, Skip: 1, Reverse: true}},

		// Providing both the origin hash and origin number must fail
		{fail: true, packet: &GetBlockHeadersData{Origin: HashOrNumber{Hash: hash, Number: 314}}},
	}
	// Iterate over each of the tests and try to encode and then decode
	for i, tt := range tests {
		bytes, err := rlp.EncodeToBytes(tt.packet)
		if err != nil && !tt.fail {
			t.Fatalf("test %d: failed to encode packet: %v", i, err)
		} else if err == nil && tt.fail {
			t.Fatalf("test %d: encode should have failed", i)
		}
		if !tt.fail {
			packet := new(GetBlockHeadersData)
			if err := rlp.DecodeBytes(bytes, packet); err != nil {
				t.Fatalf("test %d: failed to decode packet: %v", i, err)
			}
			if packet.Origin.Hash != tt.packet.Origin.Hash || packet.Origin.Number != tt.packet.Origin.Number || packet.Amount != tt.packet.Amount ||
				packet.Skip != tt.packet.Skip || packet.Reverse != tt.packet.Reverse {
				t.Fatalf("test %d: encode decode mismatch: have %+v, want %+v", i, packet, tt.packet)
			}
		}
	}
}
