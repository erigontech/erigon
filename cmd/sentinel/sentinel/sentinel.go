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
	"fmt"
	"net"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/handlers"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/handshake"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/peers"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/p2p/discover"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/network"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/pkg/errors"
)

type Sentinel struct {
	started    bool
	listener   *discover.UDPv5 // this is us in the network.
	ctx        context.Context
	host       host.Host
	cfg        *SentinelConfig
	peers      *peers.Peers
	metadataV2 *cltypes.Metadata
	handshaker *handshake.HandShaker

	db kv.RoDB

	discoverConfig discover.Config
	pubsub         *pubsub.PubSub
	subManager     *GossipManager
	gossipTopics   []GossipTopic
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

func (s *Sentinel) SetStatus(status *cltypes.Status) {
	s.handshaker.SetStatus(status)
}

func (s *Sentinel) createListener() (*discover.UDPv5, error) {
	var (
		ipAddr  = s.cfg.IpAddr
		port    = s.cfg.Port
		discCfg = s.discoverConfig
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

	// TODO: Set up proper attestation number
	s.metadataV2 = &cltypes.Metadata{
		SeqNumber: localNode.Seq(),
		Attnets:   0,
		Syncnets:  new(uint64),
	}

	// Start stream handlers
	handlers.NewConsensusHandlers(s.ctx, s.db, s.host, s.peers, s.cfg.BeaconConfig, s.cfg.GenesisConfig, s.metadataV2).Start()

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
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithMessageIdFn(func(pmsg *pubsub_pb.Message) string {
			return fork.MsgID(pmsg, s.cfg.NetworkConfig, s.cfg.BeaconConfig, s.cfg.GenesisConfig)
		}), pubsub.WithNoAuthor(),
		pubsub.WithSubscriptionFilter(nil),
		pubsub.WithPeerOutboundQueueSize(pubsubQueueSize),
		pubsub.WithMaxMessageSize(int(s.cfg.NetworkConfig.GossipMaxSize)),
		pubsub.WithValidateQueueSize(pubsubQueueSize),
		pubsub.WithGossipSubParams(gsp),
	}
	return psOpts
}

// This is just one of the examples from the libp2p repository.
func New(
	ctx context.Context,
	cfg *SentinelConfig,
	db kv.RoDB,
	rule handshake.RuleFunc,
) (*Sentinel, error) {
	s := &Sentinel{
		ctx: ctx,
		cfg: cfg,
		db:  db,
	}

	// Setup discovery
	enodes := make([]*enode.Node, len(cfg.NetworkConfig.BootNodes))
	for i, bootnode := range cfg.NetworkConfig.BootNodes {
		newNode, err := enode.Parse(enode.ValidSchemes, bootnode)
		if err != nil {
			return nil, err
		}
		enodes[i] = newNode
	}
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	s.discoverConfig = discover.Config{
		PrivateKey: privateKey,
		Bootnodes:  enodes,
	}

	opts, err := buildOptions(cfg, s)
	if err != nil {
		return nil, err
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	s.handshaker = handshake.New(ctx, cfg.GenesisConfig, cfg.BeaconConfig, host, rule)

	host.RemoveStreamHandler(identify.IDDelta)
	s.host = host
	s.peers = peers.New(s.host)

	s.pubsub, err = pubsub.NewGossipSub(s.ctx, s.host, s.pubsubOptions()...)
	if err != nil {
		return nil, fmt.Errorf("[Sentinel] failed to subscribe to gossip err=%w", err)
	}

	return s, nil
}

func (s *Sentinel) ChainConfigs() (clparams.BeaconChainConfig, clparams.GenesisConfig) {
	return *s.cfg.BeaconConfig, *s.cfg.GenesisConfig
}

func (s *Sentinel) RecvGossip() <-chan *pubsub.Message {
	return s.subManager.Recv()
}

func (s *Sentinel) Start() error {
	if s.started {
		log.Warn("[Sentinel] already running")
	}
	var err error
	s.listener, err = s.createListener()
	if err != nil {
		return fmt.Errorf("failed creating sentinel listener err=%w", err)
	}

	if err := s.connectToBootnodes(); err != nil {
		return fmt.Errorf("failed to connect to bootnodes err=%w", err)
	}
	// Configuring handshake
	s.host.Network().Notify(&network.NotifyBundle{
		ConnectedF: s.onConnection,
	})
	if !s.cfg.NoDiscovery {
		go s.listenForPeers()
	}
	s.subManager = NewGossipManager(s.ctx)
	return nil
}

func (s *Sentinel) String() string {
	return s.listener.Self().String()
}

func (s *Sentinel) HasTooManyPeers() bool {
	return s.GetPeersCount() >= peers.DefaultMaxPeers
}

func (s *Sentinel) GetPeersCount() int {
	sub := s.subManager.GetMatchingSubscription(string(BeaconBlockTopic))

	if sub == nil {
		return len(s.host.Network().Peers())
	}
	return len(sub.topic.ListPeers())
}

func (s *Sentinel) Host() host.Host {
	return s.host
}

func (s *Sentinel) Peers() *peers.Peers {
	return s.peers
}
