package download

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/params"
)

func testSentryServer(db ethdb.KVGetter, genesis *core.Genesis, genesisHash common.Hash) *SentryServerImpl {
	s := &SentryServerImpl{
		ctx:             context.Background(),
		receiveCh:       make(chan StreamMsg, 1024),
		receiveUploadCh: make(chan StreamMsg, 1024),
	}

	head := rawdb.ReadCurrentHeader(db)
	headTd, err := rawdb.ReadTd(db, head.Hash(), head.Number.Uint64())
	if err != nil {
		panic(err)
	}

	headTd256 := new(uint256.Int)
	headTd256.SetFromBig(headTd)
	s.statusData = &proto_sentry.StatusData{
		NetworkId:       1,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(headTd256),
		BestHash:        gointerfaces.ConvertHashToH256(head.Hash()),
		MaxBlock:        head.Number.Uint64(),
		ForkData: &proto_sentry.Forks{
			Genesis: gointerfaces.ConvertHashToH256(genesisHash),
			Forks:   forkid.GatherForks(genesis.Config),
		},
	}
	return s

}

// Tests that peers are correctly accepted (or rejected) based on the advertised
// fork IDs in the protocol handshake.
func TestForkIDSplit66(t *testing.T) { testForkIDSplit(t, eth.ETH66) }

func testForkIDSplit(t *testing.T, protocol uint) {
	var (
		//engine = ethash.NewFaker()

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

		//blocksNoFork, _, _  = core.GenerateChain(configNoFork, genesisNoFork, engine, dbNoFork, 2, nil, false)
		//blocksProFork, _, _ = core.GenerateChain(configProFork, genesisProFork, engine, dbProFork, 2, nil, false)
	)
	defer dbNoFork.Close()
	defer dbProFork.Close()

	s1, s2 := testSentryServer(dbNoFork, gspecNoFork, genesisNoFork.Hash()), testSentryServer(dbProFork, gspecProFork, genesisProFork.Hash())

	// Both nodes should allow the other to connect (same genesis, next fork is the same)
	p2pNoFork, p2pProFork := p2p.MsgPipe()
	defer p2pNoFork.Close()
	defer p2pProFork.Close()

	errc := make(chan error, 2)
	go func(errc chan error) {
		peerNoFork := p2p.NewPeer(enode.ID{1}, "", nil)
		errc <- runPeer(context.Background(), nil, nil, nil, peerNoFork, p2pNoFork, protocol, protocol, s1)
	}(errc)
	go func(errc chan error) {
		peerProFork := p2p.NewPeer(enode.ID{2}, "", nil)
		errc <- runPeer(context.Background(), nil, nil, nil, peerProFork, p2pProFork, protocol, protocol, s2)
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

	/*
			// Progress into Homestead. Fork's match, so we don't care what the future holds
			if _, err := stagedsync.InsertBlocksInStages(dbNoFork, ethdb.DefaultStorageMode, configNoFork, &vm.Config{}, ethash.NewFaker(), blocksNoFork[:1], true); err != nil {
				t.Fatal(err)
			}
			atomic.StoreUint64(&ethNoFork.currentHeight, 1)
			if _, err := stagedsync.InsertBlocksInStages(dbProFork, ethdb.DefaultStorageMode, configProFork, &vm.Config{}, ethash.NewFaker(), blocksProFork[:1], true); err != nil {
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
			if _, err := stagedsync.InsertBlocksInStages(dbNoFork, ethdb.DefaultStorageMode, configNoFork, &vm.Config{}, ethash.NewFaker(), blocksNoFork[1:2], true); err != nil {
				t.Fatal(err)
			}
			atomic.StoreUint64(&ethNoFork.currentHeight, 2)
			if _, err := stagedsync.InsertBlocksInStages(dbProFork, ethdb.DefaultStorageMode, configProFork, &vm.Config{}, ethash.NewFaker(), blocksProFork[1:2], true ); err != nil {
				t.Fatal(err)
			}
			atomic.StoreUint64(&ethProFork.currentHeight, 2)

			// Both nodes should allow the other to connect (same genesis, next fork is the same)
			p2pNoFork, p2pProFork := p2p.MsgPipe()
			defer p2pNoFork.Close()
			defer p2pProFork.Close()

			peerProFork = p2p.NewPeer(enode.ID{2}, "", nil)
		defer	peerProFork.Close()
			peerNoFork:=p2p.NewPeer(enode.ID{1}, "", nil)
		defer peerNoFork.Close()

			errc = make(chan error, 2)
			go func(errc chan error) {
				errc <- 		runPeer(context.Background(), nil,nil,nil, peerNoFork, p2pNoFork, protocol,protocol,nil)
			}(errc)
			go func(errc chan error) {
				errc <- 		runPeer(context.Background(), nil,nil,nil, peerProFork, p2pProFork, protocol,protocol,nil)
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
	*/
}
