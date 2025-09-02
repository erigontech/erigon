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
	"fmt"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/erigontech/erigon-lib/common/math"
	sentinelrpc "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/p2p/enode"
)

const AttestationSubnetSubscriptions = 2

type ServerConfig struct {
	Network       string
	Addr          string
	Creds         credentials.TransportCredentials
	InitialStatus *cltypes.Status
}

func generateSubnetsTopics(template string, maxIds int) []sentinel.GossipTopic {
	topics := make([]sentinel.GossipTopic, 0, maxIds)
	for i := 0; i < maxIds; i++ {
		topics = append(topics, sentinel.GossipTopic{
			Name:     fmt.Sprintf(template, i),
			CodecStr: sentinel.SSZSnappyCodec,
		})
	}

	if template == gossip.TopicNamePrefixBeaconAttestation {
		rand.Shuffle(len(topics), func(i, j int) {
			topics[i], topics[j] = topics[j], topics[i]
		})
	}
	return topics
}

func getExpirationForTopic(topic string, subscribeAll bool) time.Time {
	if subscribeAll {
		return time.Unix(0, math.MaxInt64)
	}
	if strings.Contains(topic, "beacon_attestation") ||
		(strings.Contains(topic, "sync_committee_") && !strings.Contains(topic, gossip.TopicNameSyncCommitteeContributionAndProof)) {
		return time.Unix(0, 0)
	}

	return time.Unix(0, math.MaxInt64)
}

func createSentinel(
	cfg *sentinel.SentinelConfig,
	blockReader freezeblocks.BeaconSnapshotReader,
	blobStorage blob_storage.BlobStorage,
	indiciesDB kv.RwDB,
	forkChoiceReader forkchoice.ForkChoiceStorageReader,
	ethClock eth_clock.EthereumClock,
	dataColumnStorage blob_storage.DataColumnStorage,
	peerDasStateReader peerdasstate.PeerDasStateReader,
	logger log.Logger) (*sentinel.Sentinel, *enode.LocalNode, error) {
	sent, err := sentinel.New(
		context.Background(),
		cfg,
		ethClock,
		blockReader,
		blobStorage,
		indiciesDB,
		logger,
		forkChoiceReader,
		dataColumnStorage,
		peerDasStateReader,
	)
	if err != nil {
		return nil, nil, err
	}
	localNode, err := sent.Start()
	if err != nil {
		return nil, nil, err
	}
	gossipTopics := []sentinel.GossipTopic{
		sentinel.BeaconBlockSsz,
		//sentinel.VoluntaryExitSsz,
		sentinel.ProposerSlashingSsz,
		sentinel.AttesterSlashingSsz,
		sentinel.BlsToExecutionChangeSsz,
		////sentinel.LightClientFinalityUpdateSsz,
		////sentinel.LightClientOptimisticUpdateSsz,
		sentinel.SyncCommitteeContributionAndProofSsz,
		sentinel.BeaconAggregateAndProofSsz,
	}

	gossipTopics = append(
		gossipTopics,
		generateSubnetsTopics(
			gossip.TopicNamePrefixBlobSidecar,
			int(cfg.BeaconConfig.MaxBlobsPerBlockElectra),
		)...)

	attestationSubnetTopics := generateSubnetsTopics(
		gossip.TopicNamePrefixBeaconAttestation,
		int(cfg.NetworkConfig.AttestationSubnetCount),
	)

	gossipTopics = append(
		gossipTopics,
		attestationSubnetTopics[AttestationSubnetSubscriptions:]...)

	gossipTopics = append(
		gossipTopics,
		generateSubnetsTopics(
			gossip.TopicNamePrefixSyncCommittee,
			int(cfg.BeaconConfig.SyncCommitteeSubnetCount),
		)...)

	for subnet := range cfg.BeaconConfig.DataColumnSidecarSubnetCount {
		topic := sentinel.GossipTopic{
			Name:     gossip.TopicNameDataColumnSidecar(subnet),
			CodecStr: sentinel.SSZSnappyCodec,
		}
		// just subscribe but do not listen to the messages. This topic will be dynamically controlled in peerdas.
		if _, err := sent.SubscribeGossip(topic, time.Unix(0, 0)); err != nil {
			logger.Error("[Sentinel] failed to subscribe to data column sidecar", "err", err)
		}
	}

	for _, v := range gossipTopics {
		if err := sent.Unsubscribe(v); err != nil {
			logger.Error("[Sentinel] failed to start sentinel", "err", err)
			continue
		}

		// now lets separately connect to the gossip topics. this joins the room
		_, err := sent.SubscribeGossip(v, getExpirationForTopic(v.Name, cfg.SubscribeAllTopics)) // Listen forever.
		if err != nil {
			logger.Error("[Sentinel] failed to start sentinel", "err", err)
		}
	}

	for k := 0; k < AttestationSubnetSubscriptions; k++ {
		if err := sent.Unsubscribe(attestationSubnetTopics[k]); err != nil {
			logger.Error("[Sentinel] failed to start sentinel", "err", err)
			continue
		}
		_, err := sent.SubscribeGossip(attestationSubnetTopics[k], time.Unix(0, math.MaxInt64)) // Listen forever.
		if err != nil {
			logger.Error("[Sentinel] failed to start sentinel", "err", err)
		}
	}
	return sent, localNode, nil
}

func StartSentinelService(
	cfg *sentinel.SentinelConfig,
	blockReader freezeblocks.BeaconSnapshotReader,
	blobStorage blob_storage.BlobStorage,
	indiciesDB kv.RwDB,
	srvCfg *ServerConfig,
	ethClock eth_clock.EthereumClock,
	forkChoiceReader forkchoice.ForkChoiceStorageReader,
	dataColumnStorage blob_storage.DataColumnStorage,
	PeerDasStateReader peerdasstate.PeerDasStateReader,
	logger log.Logger) (sentinelrpc.SentinelClient, *enode.LocalNode, error) {
	ctx := context.Background()
	sent, localNode, err := createSentinel(
		cfg,
		blockReader,
		blobStorage,
		indiciesDB,
		forkChoiceReader,
		ethClock,
		dataColumnStorage,
		PeerDasStateReader,
		logger,
	)
	if err != nil {
		return nil, nil, err
	}
	// rcmgrObs.MustRegisterWith(prometheus.DefaultRegisterer)
	logger.Info("[Sentinel] Sentinel started", "enr", sent.String())
	if srvCfg.InitialStatus != nil {
		sent.SetStatus(srvCfg.InitialStatus)
	}
	server := NewSentinelServer(ctx, sent, logger)
	go StartServe(server, srvCfg, srvCfg.Creds)

	return direct.NewSentinelClientDirect(server), localNode, nil
}

func StartServe(
	server *SentinelServer,
	srvCfg *ServerConfig,
	creds credentials.TransportCredentials,
) {
	lis, err := net.Listen(srvCfg.Network, srvCfg.Addr)
	if err != nil {
		log.Warn("[Sentinel] could not serve service", "reason", err)
	}
	// Create a gRPC server
	gRPCserver := grpc.NewServer(grpc.Creds(creds))
	go server.ListenToGossip()
	// Regiser our server as a gRPC server
	sentinelrpc.RegisterSentinelServer(gRPCserver, server)
	if err := gRPCserver.Serve(lis); err != nil {
		log.Warn("[Sentinel] could not serve service", "reason", err)
	}
}
