package service

import (
	"context"
	"net"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServerConfig struct {
	Network string
	Addr    string
}

func StartSentinelService(cfg *sentinel.SentinelConfig, srvCfg *ServerConfig) (lightrpc.SentinelClient, error) {
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
	// Wait a bit for the serving (TODO: make it better, this is ugly)
	time.Sleep(5 * time.Second)

	conn, err := grpc.DialContext(ctx, srvCfg.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return lightrpc.NewSentinelClient(conn), nil
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
	lightrpc.RegisterSentinelServer(gRPCserver, server)
	if err := gRPCserver.Serve(lis); err != nil {
		log.Warn("[Sentinel] could not serve service", "reason", err)
	}
}
