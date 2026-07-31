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

package service

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/p2p/enode"
)

const AttestationSubnetSubscriptions = 2

type ServerConfig struct {
	Network       string
	Addr          string
	Creds         credentials.TransportCredentials
	InitialStatus *cltypes.Status
}

func createSentinel(
	ctx context.Context,
	cfg *sentinel.SentinelConfig,
	blockReader freezeblocks.BeaconSnapshotReader,
	blobStorage blob_storage.BlobStorage,
	indiciesDB kv.RwDB,
	forkChoiceReader forkchoice.ForkChoiceStorageReader,
	ethClock eth_clock.EthereumClock,
	dataColumnStorage blob_storage.DataColumnStorage,
	peerDasStateReader peerdasstate.PeerDasStateReader,
	p2p p2p.P2PManager,
	initialStatus *cltypes.Status,
	logger log.Logger,
) (*sentinel.Sentinel, *enode.LocalNode, error) {
	sent, err := sentinel.New(
		ctx,
		cfg,
		ethClock,
		blockReader,
		blobStorage,
		indiciesDB,
		logger,
		forkChoiceReader,
		dataColumnStorage,
		peerDasStateReader,
		p2p,
	)
	if err != nil {
		return nil, nil, err
	}
	// Set initial status BEFORE starting the listener so that peers connecting
	// immediately see a valid Status (fork digest, head, finalized checkpoint)
	// instead of all-zeros which causes them to penalize/ban us.
	if initialStatus != nil {
		sent.SetStatus(initialStatus)
	}
	localNode, err := sent.Start()
	if err != nil {
		return nil, nil, err
	}

	return sent, localNode, nil
}

// StartSentinelService starts the sentinel + gRPC serving stack. ctx
// drives shutdown: on cancel the sentinel's libp2p host is closed and
// the gRPC server is gracefully stopped so their listener ports are
// released. Required for CaplinService.Restart to bind them again on
// relaunch.
func StartSentinelService(
	ctx context.Context,
	cfg *sentinel.SentinelConfig,
	blockReader freezeblocks.BeaconSnapshotReader,
	blobStorage blob_storage.BlobStorage,
	indiciesDB kv.RwDB,
	srvCfg *ServerConfig,
	ethClock eth_clock.EthereumClock,
	forkChoiceReader forkchoice.ForkChoiceStorageReader,
	dataColumnStorage blob_storage.DataColumnStorage,
	peerDasStateReader peerdasstate.PeerDasStateReader,
	p2p p2p.P2PManager,
	logger log.Logger,
) (sentinelproto.SentinelClient, *enode.LocalNode, error) {
	sent, localNode, err := createSentinel(
		ctx,
		cfg,
		blockReader,
		blobStorage,
		indiciesDB,
		forkChoiceReader,
		ethClock,
		dataColumnStorage,
		peerDasStateReader,
		p2p,
		srvCfg.InitialStatus,
		logger,
	)
	if err != nil {
		return nil, nil, err
	}
	logger.Info("[Sentinel] Sentinel started", "enr", sent.String())
	server := NewSentinelServer(ctx, sent, logger)
	go StartServe(ctx, server, srvCfg, srvCfg.Creds, sent)

	return direct.NewSentinelClientDirect(server), localNode, nil
}

// StartServe runs the gRPC serving loop until ctx is cancelled. On
// cancel it calls Sentinel.Stop (releases the libp2p host + its TCP
// listener) and gRPCserver.GracefulStop (releases the gRPC port).
func StartServe(
	ctx context.Context,
	server *SentinelServer,
	srvCfg *ServerConfig,
	creds credentials.TransportCredentials,
	sent *sentinel.Sentinel,
) {
	lis, err := net.Listen(srvCfg.Network, srvCfg.Addr)
	if err != nil {
		log.Warn("[Sentinel] could not serve service", "reason", err)
		return
	}
	gRPCserver := grpc.NewServer(grpc.Creds(creds))
	sentinelproto.RegisterSentinelServer(gRPCserver, server)
	go func() {
		<-ctx.Done()
		sent.Stop()
		gRPCserver.GracefulStop()
	}()
	if err := gRPCserver.Serve(lis); err != nil {
		log.Warn("[Sentinel] could not serve service", "reason", err)
	}
}
