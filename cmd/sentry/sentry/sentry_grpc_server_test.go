package sentry

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p"
)

func testSentryServer(db kv.Getter, genesis *types.Genesis, genesisHash libcommon.Hash) *GrpcServer {
	s := &GrpcServer{
		ctx: context.Background(),
	}

	head := rawdb.ReadCurrentHeader(db)
	headTd, err := rawdb.ReadTd(db, head.Hash(), head.Number.Uint64())
	if err != nil {
		panic(err)
	}

	headTd256 := new(uint256.Int)
	headTd256.SetFromBig(headTd)
	heightForks, timeForks := forkid.GatherForks(genesis.Config)
	s.statusData = &proto_sentry.StatusData{
		NetworkId:       1,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(headTd256),
		BestHash:        gointerfaces.ConvertHashToH256(head.Hash()),
		MaxBlockHeight:  head.Number.Uint64(),
		MaxBlockTime:    head.Time,
		ForkData: &proto_sentry.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(genesisHash),
			HeightForks: heightForks,
			TimeForks:   timeForks,
		},
	}
	return s

}

// Tests that peers are correctly accepted (or rejected) based on the advertised
// fork IDs in the protocol handshake.
func TestForkIDSplit66(t *testing.T) { testForkIDSplit(t, direct.ETH66) }

func testForkIDSplit(t *testing.T, protocol uint) {
	var (
		ctx           = context.Background()
		configNoFork  = &chain.Config{HomesteadBlock: big.NewInt(1), ChainID: big.NewInt(1)}
		configProFork = &chain.Config{
			ChainID:               big.NewInt(1),
			HomesteadBlock:        big.NewInt(1),
			TangerineWhistleBlock: big.NewInt(2),
			SpuriousDragonBlock:   big.NewInt(2),
			ByzantiumBlock:        big.NewInt(3),
		}
		_, dbNoFork, _  = temporal.NewTestDB(t, datadir.New(t.TempDir()), nil)
		_, dbProFork, _ = temporal.NewTestDB(t, datadir.New(t.TempDir()), nil)

		gspecNoFork  = &types.Genesis{Config: configNoFork}
		gspecProFork = &types.Genesis{Config: configProFork}

		genesisNoFork  = core.MustCommitGenesis(gspecNoFork, dbNoFork, "")
		genesisProFork = core.MustCommitGenesis(gspecProFork, dbProFork, "")
	)

	var s1, s2 *GrpcServer

	err := dbNoFork.Update(context.Background(), func(tx kv.RwTx) error {
		s1 = testSentryServer(tx, gspecNoFork, genesisNoFork.Hash())
		return nil
	})
	require.NoError(t, err)
	err = dbProFork.Update(context.Background(), func(tx kv.RwTx) error {
		s2 = testSentryServer(tx, gspecProFork, genesisProFork.Hash())
		return nil
	})
	require.NoError(t, err)

	// Both nodes should allow the other to connect (same genesis, next fork is the same)
	p2pNoFork, p2pProFork := p2p.MsgPipe()
	defer p2pNoFork.Close()
	defer p2pProFork.Close()

	errc := make(chan error, 2)
	go func() { errc <- handShake(ctx, s1.GetStatus(), [64]byte{1}, p2pNoFork, protocol, protocol, nil) }()
	go func() { errc <- handShake(ctx, s2.GetStatus(), [64]byte{2}, p2pProFork, protocol, protocol, nil) }()

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
	s1.statusData.MaxBlockHeight = 1
	s2.statusData.MaxBlockHeight = 1

	go func() { errc <- handShake(ctx, s1.GetStatus(), [64]byte{1}, p2pNoFork, protocol, protocol, nil) }()
	go func() { errc <- handShake(ctx, s2.GetStatus(), [64]byte{2}, p2pProFork, protocol, protocol, nil) }()

	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				t.Fatalf("homestead nofork <-> profork failed: %v", err)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("frontier nofork <-> profork handler timeout")
		}
	}

	// Progress into Spurious. Forks mismatch, signalling differing chains, reject
	s1.statusData.MaxBlockHeight = 2
	s2.statusData.MaxBlockHeight = 2

	// Both nodes should allow the other to connect (same genesis, next fork is the same)
	go func() { errc <- handShake(ctx, s1.GetStatus(), [64]byte{1}, p2pNoFork, protocol, protocol, nil) }()
	go func() { errc <- handShake(ctx, s2.GetStatus(), [64]byte{2}, p2pProFork, protocol, protocol, nil) }()

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

func TestSentryServerImpl_SetStatusInitPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic during server initialization")
		}
	}()

	configNoFork := &chain.Config{HomesteadBlock: big.NewInt(1), ChainID: big.NewInt(1)}
	_, dbNoFork, _ := temporal.NewTestDB(t, datadir.New(t.TempDir()), nil)
	gspecNoFork := &types.Genesis{Config: configNoFork}
	genesisNoFork := core.MustCommitGenesis(gspecNoFork, dbNoFork, "")
	ss := &GrpcServer{p2p: &p2p.Config{}}

	_, err := ss.SetStatus(context.Background(), &proto_sentry.StatusData{
		ForkData: &proto_sentry.Forks{Genesis: gointerfaces.ConvertHashToH256(genesisNoFork.Hash())},
	})
	if err == nil {
		t.Fatalf("error expected")
	}

	// Should not panic here.
	_, err = ss.SetStatus(context.Background(), &proto_sentry.StatusData{
		ForkData: &proto_sentry.Forks{Genesis: gointerfaces.ConvertHashToH256(genesisNoFork.Hash())},
	})
	if err == nil {
		t.Fatalf("error expected")
	}
}
