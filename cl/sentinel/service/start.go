package service

import (
	"context"
	"net"

	"github.com/ledgerwatch/erigon/cl/sentinel"

	"github.com/ledgerwatch/erigon-lib/direct"
	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type ServerConfig struct {
	Network string
	Addr    string
}

func createSentinel(cfg *sentinel.SentinelConfig, db persistence.RawBeaconBlockChain, logger log.Logger) (*sentinel.Sentinel, error) {
	sent, err := sentinel.New(context.Background(), cfg, db, logger)
	if err != nil {
		return nil, err
	}
	if err := sent.Start(); err != nil {
		return nil, err
	}
	gossipTopics := []sentinel.GossipTopic{
		sentinel.BeaconBlockSsz,
		//sentinel.BeaconAggregateAndProofSsz,
		sentinel.VoluntaryExitSsz,
		sentinel.ProposerSlashingSsz,
		sentinel.AttesterSlashingSsz,
		sentinel.BlsToExecutionChangeSsz,
	}
	// gossipTopics = append(gossipTopics, sentinel.GossipSidecarTopics(chain.MaxBlobsPerBlock)...)

	for _, v := range gossipTopics {
		if err := sent.Unsubscribe(v); err != nil {
			logger.Error("[Sentinel] failed to start sentinel", "err", err)
			continue
		}
		// now lets separately connect to the gossip topics. this joins the room
		subscriber, err := sent.SubscribeGossip(v)
		if err != nil {
			logger.Error("[Sentinel] failed to start sentinel", "err", err)
		}
		// actually start the subscription, aka listening and sending packets to the sentinel recv channel
		err = subscriber.Listen()
		if err != nil {
			logger.Error("[Sentinel] failed to start sentinel", "err", err)
		}
	}
	return sent, nil
}

func StartSentinelService(cfg *sentinel.SentinelConfig, db persistence.RawBeaconBlockChain, srvCfg *ServerConfig, creds credentials.TransportCredentials, initialStatus *cltypes.Status, logger log.Logger) (sentinelrpc.SentinelClient, error) {
	ctx := context.Background()
	sent, err := createSentinel(cfg, db, logger)
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
