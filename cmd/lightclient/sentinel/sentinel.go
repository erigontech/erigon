/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package sentinel

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/lightclient"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/handlers"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/peers"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/p2p"
	"github.com/ledgerwatch/erigon/p2p/discover"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/pkg/errors"
)

type Sentinel struct {
	started  bool
	listener *discover.UDPv5 // this is us in the network.
	ctx      context.Context
	host     host.Host
	cfg      *SentinelConfig
	peers    *peers.Peers

	state  *lightclient.LightState
	pubsub *pubsub.PubSub

	subManager subscriptionManager
}

func (s *Sentinel) createLocalNode(
	privKey *ecdsa.PrivateKey,
	ipAddr net.IP,
	udpPort, tcpPort int,
) (*enode.LocalNode, error) {
	db, err := enode.OpenDB("")
	if err != nil {
		return nil, errors.Wrap(err, "could not open node's peer database")
	}
	localNode := enode.NewLocalNode(db, privKey)

	ipEntry := enr.IP(ipAddr)
	udpEntry := enr.UDP(udpPort)
	tcpEntry := enr.TCP(tcpPort)

	localNode.Set(ipEntry)
	localNode.Set(udpEntry)
	localNode.Set(tcpEntry)

	localNode.SetFallbackIP(ipAddr)
	localNode.SetFallbackUDP(udpPort)
	s.setupENR(localNode)

	return localNode, nil
}

func (s *Sentinel) createListener() (*discover.UDPv5, error) {
	var (
		ipAddr  = s.cfg.IpAddr
		port    = s.cfg.Port
		discCfg = s.cfg.DiscoverConfig
	)

	ip := net.ParseIP(ipAddr)
	if ip.To4() == nil {
		return nil, fmt.Errorf("IPV4 address not provided instead %s was provided", ipAddr)
	}

	var bindIP net.IP
	var networkVersion string

	// check for our network version
	switch {
	// if we have 16 byte and 4 byte representation then we are in using udp6
	case ip.To16() != nil && ip.To4() == nil:
		bindIP = net.IPv6zero
		networkVersion = "udp6"
		// only 4 bytes then we are using udp4
	case ip.To4() != nil:
		bindIP = net.IPv4zero
		networkVersion = "udp4"
	default:
		return nil, fmt.Errorf("bad ip address provided, %s was provided", ipAddr)
	}

	udpAddr := &net.UDPAddr{
		IP:   bindIP,
		Port: port,
	}
	conn, err := net.ListenUDP(networkVersion, udpAddr)
	if err != nil {
		return nil, err
	}

	localNode, err := s.createLocalNode(discCfg.PrivateKey, ip, port, int(s.cfg.TCPPort))
	if err != nil {
		return nil, err
	}

	// Start stream handlers
	handlers.NewConsensusHandlers(s.host, s.peers).Start()

	net, err := discover.ListenV5(s.ctx, conn, localNode, discCfg)
	if err != nil {
		return nil, err
	}
	return net, err
}

func (s *Sentinel) pubsubOptions() []pubsub.Option {
	pubsubQueueSize := 600
	gsp := pubsub.DefaultGossipSubParams()
	psOpts := []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithMessageIdFn(func(pmsg *pubsub_pb.Message) string {
			// TODO: note that this is incorrect
			// i am simply doing this for now so we can get a unique id so that we can distinguish messages.
			hs := sha256.Sum256(pmsg.Data)
			return hex.EncodeToString(hs[:])
		}),
		pubsub.WithNoAuthor(),
		pubsub.WithSubscriptionFilter(nil),
		pubsub.WithPeerOutboundQueueSize(pubsubQueueSize),
		pubsub.WithMaxMessageSize(int(s.cfg.NetworkConfig.GossipMaxSize)),
		pubsub.WithValidateQueueSize(pubsubQueueSize),
		pubsub.WithGossipSubParams(gsp),
	}
	return psOpts
}

// This is just one of the examples from the libp2p repository.
func New(ctx context.Context, cfg *SentinelConfig) (*Sentinel, error) {
	s := &Sentinel{
		ctx:        ctx,
		cfg:        cfg,
		subManager: newSubscriptionManager(),
	}

	opts, err := buildOptions(cfg, s)
	if err != nil {
		return nil, err
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	host.RemoveStreamHandler(identify.IDDelta)
	s.host = host
	s.peers = peers.New(s.host)
	//TODO: populate with data from config
	s.state = lightclient.NewLightState(ctx, &p2p.LightClientBootstrap{}, [32]byte{})

	s.pubsub, err = pubsub.NewGossipSub(s.ctx, s.host, s.pubsubOptions()...)
	if err != nil {
		return nil, fmt.Errorf("[Sentinel] failed to subscribe to gossip err=%w", err)
	}

	return s, nil
}

func (s *Sentinel) Start() error {
	if s.started {
		log.Warn("Sentinel already running")
	}

	var err error
	s.listener, err = s.createListener()
	if err != nil {
		return fmt.Errorf("failed creating sentinel listener err=%w", err)
	}
	if err := s.connectToBootnodes(); err != nil {
		return fmt.Errorf("failed to connect to bootnodes err=%w", err)
	}
	go s.listenForPeers()

	//TODO: request and compute
	prefix := "/eth2/4a26c58b"

	if err := s.startGossip(prefix); err != nil {
		return fmt.Errorf("failed to start gossip err=%w", err)
	}

	go func() {
		tryEvery := time.NewTicker(5 * time.Second)
		defer tryEvery.Stop()
		for {
			select {
			case <-tryEvery.C:
				s.pingRequest()
			}
		}
	}()

	return nil
}

func (s *Sentinel) String() string {
	return s.listener.Self().String()
}

func (s *Sentinel) HasTooManyPeers() bool {
	return s.GetPeersCount() >= peers.DefaultMaxPeers
}

func (s *Sentinel) GetPeersCount() int {
	return len(s.host.Network().Peers())
}
