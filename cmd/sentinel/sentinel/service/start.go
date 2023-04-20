package service

import (
	"context"
	"fmt"
	"net"
	"time"

	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const maxMessageSize = 437323800

type ServerConfig struct {
	Network string
	Addr    string
}

func StartSentinelService(cfg *sentinel.SentinelConfig, db kv.RoDB, srvCfg *ServerConfig, creds credentials.TransportCredentials, initialStatus *cltypes.Status) (sentinelrpc.SentinelClient, error) {
	ctx := context.Background()
	sent, err := sentinel.New(context.Background(), cfg, db)
	if err != nil {
		return nil, err
	}
	if err := sent.Start(); err != nil {
		return nil, err
	}
	gossip_topics := []sentinel.GossipTopic{
		sentinel.BeaconBlockSsz,
		// Cause problem due to buggy msg id will uncomment in the future.
		//sentinel.BeaconAggregateAndProofSsz,
		//sentinel.VoluntaryExitSsz,
		//sentinel.ProposerSlashingSsz,
		//sentinel.AttesterSlashingSsz,
	}

	for _, v := range gossip_topics {
		if err := sent.Unsubscribe(v); err != nil {
			log.Error("[Sentinel] failed to start sentinel", "err", err)
			continue
		}
		// now lets separately connect to the gossip topics. this joins the room
		subscriber, err := sent.SubscribeGossip(v)
		if err != nil {
			log.Error("[Sentinel] failed to start sentinel", "err", err)
		}
		// actually start the subscription, aka listening and sending packets to the sentinel recv channel
		err = subscriber.Listen()
		if err != nil {
			log.Error("[Sentinel] failed to start sentinel", "err", err)
		}
	}

	log.Info("[Sentinel] Sentinel started", "enr", sent.String())
	if initialStatus != nil {
		sent.SetStatus(initialStatus)
	}
	server := NewSentinelServer(ctx, sent)
	if creds == nil {
		creds = insecure.NewCredentials()
	}

	go StartServe(server, srvCfg, creds)
	timeOutTimer := time.NewTimer(5 * time.Second)
WaitingLoop:
	for {
		select {
		case <-timeOutTimer.C:
			return nil, fmt.Errorf("[Server] timeout beginning server")
		default:
			if _, err := server.GetPeers(ctx, &sentinelrpc.EmptyMessage{}); err == nil {
				break WaitingLoop
			}
		}
	}

	conn, err := grpc.DialContext(ctx, srvCfg.Addr, grpc.WithTransportCredentials(creds), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize)))
	if err != nil {
		return nil, err
	}

	return sentinelrpc.NewSentinelClient(conn), nil
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
