package service

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServerConfig struct {
	Network string
	Addr    string
}

func StartSentinelService(cfg *sentinel.SentinelConfig, srvCfg *ServerConfig) (consensusrpc.SentinelClient, error) {
	ctx := context.Background()
	sent, err := sentinel.New(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	if err := sent.Start(); err != nil {
		return nil, err
	}
	gossip_topics := []sentinel.GossipTopic{
		sentinel.BeaconBlockSsz,
		sentinel.BeaconAggregateAndProofSsz,
		sentinel.VoluntaryExitSsz,
		sentinel.ProposerSlashingSsz,
		sentinel.AttesterSlashingSsz,
		sentinel.LightClientFinalityUpdateSsz,
		sentinel.LightClientOptimisticUpdateSsz,
	}
	for _, v := range gossip_topics {
		// now lets separately connect to the gossip topics. this joins the room
		subscriber, err := sent.SubscribeGossip(v)
		if err != nil {
			log.Error("failed to start sentinel", "err", err)
		}
		// actually start the subscription, aka listening and sending packets to the sentinel recv channel
		err = subscriber.Listen()
		if err != nil {
			log.Error("failed to start sentinel", "err", err)
		}
	}
	log.Info("Sentinel started", "enr", sent.String())

	server := NewSentinelServer(ctx, sent)
	go StartServe(server, srvCfg)
	timeOutTimer := time.NewTimer(5 * time.Second)
WaitingLoop:
	for {
		select {
		case <-timeOutTimer.C:
			return nil, fmt.Errorf("[Server] timeout beginning server")
		default:
			if _, err := server.GetPeers(ctx, &consensusrpc.EmptyRequest{}); err == nil {
				break WaitingLoop
			}
		}
	}
	conn, err := grpc.DialContext(ctx, srvCfg.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return consensusrpc.NewSentinelClient(conn), nil
}

func StartServe(server *SentinelServer, srvCfg *ServerConfig) {
	lis, err := net.Listen(srvCfg.Network, srvCfg.Addr)
	if err != nil {
		log.Warn("[Sentinel] could not serve service", "reason", err)
	}
	// Create a gRPC server
	gRPCserver := grpc.NewServer()
	go server.ListenToGossip()
	// Regiser our server as a gRPC server
	consensusrpc.RegisterSentinelServer(gRPCserver, server)
	if err := gRPCserver.Serve(lis); err != nil {
		log.Warn("[Sentinel] could not serve service", "reason", err)
	}
}
