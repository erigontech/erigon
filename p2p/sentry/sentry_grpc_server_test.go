// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package sentry

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/genesiswrite"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/forkid"
)

func testSentryServer(db kv.Getter, genesis *types.Genesis, genesisHash common.Hash) *GrpcServer {
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
	heightForks, timeForks := forkid.GatherForks(genesis.Config, genesis.Timestamp)
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

func startHandshake(
	ctx context.Context,
	status *proto_sentry.StatusData,
	pipe *p2p.MsgPipeRW,
	protocolVersion uint,
	errChan chan *p2p.PeerError,
) {
	go func() {
		_, err := handShake(ctx, status, pipe, protocolVersion, protocolVersion)
		errChan <- err
	}()
}

// Tests that peers are correctly accepted (or rejected) based on the advertised
// fork IDs in the protocol handshake.
func TestForkIDSplit67(t *testing.T) { testForkIDSplit(t, direct.ETH67) }

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
		dbNoFork  = temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
		dbProFork = temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

		gspecNoFork  = &types.Genesis{Config: configNoFork}
		gspecProFork = &types.Genesis{Config: configProFork}

		genesisNoFork  = genesiswrite.MustCommitGenesis(gspecNoFork, dbNoFork, datadir.New(t.TempDir()), log.Root())
		genesisProFork = genesiswrite.MustCommitGenesis(gspecProFork, dbProFork, datadir.New(t.TempDir()), log.Root())
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

	errc := make(chan *p2p.PeerError, 2)
	startHandshake(ctx, s1.GetStatus(), p2pNoFork, protocol, errc)
	startHandshake(ctx, s2.GetStatus(), p2pProFork, protocol, errc)

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

	startHandshake(ctx, s1.GetStatus(), p2pNoFork, protocol, errc)
	startHandshake(ctx, s2.GetStatus(), p2pProFork, protocol, errc)

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
	startHandshake(ctx, s1.GetStatus(), p2pNoFork, protocol, errc)
	startHandshake(ctx, s2.GetStatus(), p2pProFork, protocol, errc)

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
	dbNoFork := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	gspecNoFork := &types.Genesis{Config: configNoFork}
	genesisNoFork := genesiswrite.MustCommitGenesis(gspecNoFork, dbNoFork, datadir.New(t.TempDir()), log.Root())
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
