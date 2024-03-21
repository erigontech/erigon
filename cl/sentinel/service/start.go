package service

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/persistence/blob_storage"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/sentinel"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	"github.com/ledgerwatch/erigon-lib/direct"
	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type ServerConfig struct {
	Network string
	Addr    string
}

func generateSubnetsTopics(template string, maxIds int) []sentinel.GossipTopic {
	topics := make([]sentinel.GossipTopic, 0, maxIds)
	for i := 0; i < maxIds; i++ {
		topics = append(topics, sentinel.GossipTopic{
			Name:     fmt.Sprintf(template, i),
			CodecStr: sentinel.SSZSnappyCodec,
		})
	}
	return topics
}

func getExpirationForTopic(topic string) time.Time {
	if strings.Contains(topic, "beacon_attestation") || strings.Contains(topic, "sync_committee") {
		return time.Unix(0, 0)
	}

	return time.Unix(math.MaxInt64, math.MaxInt64)
}

func createSentinel(cfg *sentinel.SentinelConfig, blockReader freezeblocks.BeaconSnapshotReader, blobStorage blob_storage.BlobStorage, indiciesDB kv.RwDB, forkChoiceReader forkchoice.ForkChoiceStorageReader, logger log.Logger) (*sentinel.Sentinel, error) {
	sent, err := sentinel.New(context.Background(), cfg, blockReader, blobStorage, indiciesDB, logger, forkChoiceReader)
	if err != nil {
		return nil, err
	}
	if err := sent.Start(); err != nil {
		return nil, err
	}
	gossipTopics := []sentinel.GossipTopic{
		sentinel.BeaconBlockSsz,
		sentinel.BeaconAggregateAndProofSsz,
		sentinel.VoluntaryExitSsz,
		sentinel.ProposerSlashingSsz,
		sentinel.AttesterSlashingSsz,
		sentinel.BlsToExecutionChangeSsz,
		sentinel.SyncCommitteeContributionAndProofSsz,
		////sentinel.LightClientFinalityUpdateSsz,
		////sentinel.LightClientOptimisticUpdateSsz,
	}
	gossipTopics = append(gossipTopics, generateSubnetsTopics(gossip.TopicNamePrefixBlobSidecar, int(cfg.BeaconConfig.MaxBlobsPerBlock))...)
	gossipTopics = append(gossipTopics, generateSubnetsTopics(gossip.TopicNamePrefixBeaconAttestation, int(cfg.NetworkConfig.AttestationSubnetCount))...)
	gossipTopics = append(gossipTopics, generateSubnetsTopics(gossip.TopicNamePrefixSyncCommittee, int(cfg.BeaconConfig.SyncCommitteeSubnetCount))...)

	for _, v := range gossipTopics {
		if err := sent.Unsubscribe(v); err != nil {
			logger.Error("[Sentinel] failed to start sentinel", "err", err)
			continue
		}

		// now lets separately connect to the gossip topics. this joins the room
		subscriber, err := sent.SubscribeGossip(v, getExpirationForTopic(v.Name)) // Listen forever.
		if err != nil {
			logger.Error("[Sentinel] failed to start sentinel", "err", err)
		}
		// actually start the subscription, aka listening and sending packets to the sentinel recv channel
		subscriber.Listen()
	}
	return sent, nil
}

func StartSentinelService(cfg *sentinel.SentinelConfig, blockReader freezeblocks.BeaconSnapshotReader, blobStorage blob_storage.BlobStorage, indiciesDB kv.RwDB, srvCfg *ServerConfig, creds credentials.TransportCredentials, initialStatus *cltypes.Status, forkChoiceReader forkchoice.ForkChoiceStorageReader, logger log.Logger) (sentinelrpc.SentinelClient, error) {
	ctx := context.Background()
	sent, err := createSentinel(cfg, blockReader, blobStorage, indiciesDB, forkChoiceReader, logger)
	if err != nil {
		return nil, err
	}
	// rcmgrObs.MustRegisterWith(prometheus.DefaultRegisterer)
	logger.Info("[Sentinel] Sentinel started", "enr", sent.String())
	if initialStatus != nil {
		sent.SetStatus(initialStatus)
	}
	server := NewSentinelServer(ctx, sent, logger)
	go StartServe(server, srvCfg, creds)

	return direct.NewSentinelClientDirect(server), nil
}

func StartServe(server *SentinelServer, srvCfg *ServerConfig, creds credentials.TransportCredentials) {
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
