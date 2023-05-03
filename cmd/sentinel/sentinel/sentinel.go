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
	"math"
	"net"

	"net/http"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/handlers"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/handshake"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/peers"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/p2p/discover"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	rcmgrObs "github.com/libp2p/go-libp2p/p2p/host/resource-manager/obs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// overlay parameters
	gossipSubD   = 8  // topic stable mesh target count
	gossipSubDlo = 6  // topic stable mesh low watermark
	gossipSubDhi = 12 // topic stable mesh high watermark

	// gossip parameters
	gossipSubMcacheLen    = 6   // number of windows to retain full messages in cache for `IWANT` responses
	gossipSubMcacheGossip = 3   // number of windows to gossip about
	gossipSubSeenTTL      = 550 // number of heartbeat intervals to retain message IDs
	// heartbeat interval
	gossipSubHeartbeatInterval = 700 * time.Millisecond // frequency of heartbeat, milliseconds

	// decayToZero specifies the terminal value that we will use when decaying
	// a value.
	decayToZero = 0.01
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

	discoverConfig       discover.Config
	pubsub               *pubsub.PubSub
	subManager           *GossipManager
	metrics              bool
	listenForPeersDoneCh chan struct{}
}

func (s *Sentinel) createLocalNode(
	privKey *ecdsa.PrivateKey,
	ipAddr net.IP,
	udpPort, tcpPort int,
	tmpDir string,
) (*enode.LocalNode, error) {
	db, err := enode.OpenDB("", tmpDir)
	if err != nil {
		return nil, fmt.Errorf("could not open node's peer database: %w", err)
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

	localNode, err := s.createLocalNode(discCfg.PrivateKey, ip, port, int(s.cfg.TCPPort), s.cfg.TmpDir)
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

// creates a custom gossipsub parameter set.
func pubsubGossipParam() pubsub.GossipSubParams {
	gParams := pubsub.DefaultGossipSubParams()
	gParams.Dlo = gossipSubDlo
	gParams.D = gossipSubD
	gParams.HeartbeatInterval = gossipSubHeartbeatInterval
	gParams.HistoryLength = gossipSubMcacheLen
	gParams.HistoryGossip = gossipSubMcacheGossip
	return gParams
}

// determines the decay rate from the provided time period till
// the decayToZero value. Ex: ( 1 -> 0.01)
func (s *Sentinel) scoreDecay(totalDurationDecay time.Duration) float64 {
	numOfTimes := totalDurationDecay / s.oneSlotDuration()
	return math.Pow(decayToZero, 1/float64(numOfTimes))
}

func (s *Sentinel) pubsubOptions() []pubsub.Option {
	thresholds := &pubsub.PeerScoreThresholds{
		GossipThreshold:             -4000,
		PublishThreshold:            -8000,
		GraylistThreshold:           -16000,
		AcceptPXThreshold:           100,
		OpportunisticGraftThreshold: 5,
	}
	scoreParams := &pubsub.PeerScoreParams{
		Topics:        make(map[string]*pubsub.TopicScoreParams),
		TopicScoreCap: 32.72,
		AppSpecificScore: func(p peer.ID) float64 {
			return 0
		},
		AppSpecificWeight:           1,
		IPColocationFactorWeight:    -35.11,
		IPColocationFactorThreshold: 10,
		IPColocationFactorWhitelist: nil,
		BehaviourPenaltyWeight:      -15.92,
		BehaviourPenaltyThreshold:   6,
		BehaviourPenaltyDecay:       s.scoreDecay(10 * s.oneEpochDuration()), // 10 epochs
		DecayInterval:               s.oneSlotDuration(),
		DecayToZero:                 decayToZero,
		RetainScore:                 100 * s.oneEpochDuration(), // Retain for 100 epochs
	}
	pubsubQueueSize := 600
	psOpts := []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithMessageIdFn(s.msgId),
		pubsub.WithNoAuthor(),
		pubsub.WithPeerOutboundQueueSize(pubsubQueueSize),
		pubsub.WithMaxMessageSize(int(s.cfg.NetworkConfig.GossipMaxSizeBellatrix)),
		pubsub.WithValidateQueueSize(pubsubQueueSize),
		pubsub.WithPeerScore(scoreParams, thresholds),
		pubsub.WithGossipSubParams(pubsubGossipParam()),
	}
	return psOpts
}

// This is just one of the examples from the libp2p repository.
func New(
	ctx context.Context,
	cfg *SentinelConfig,
	db kv.RoDB,
) (*Sentinel, error) {
	s := &Sentinel{
		ctx: ctx,
		cfg: cfg,
		db:  db,
		// metrics: true,
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
	if s.metrics {
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			server := &http.Server{
				Addr:              ":2112",
				ReadHeaderTimeout: time.Hour,
			}
			if err := server.ListenAndServe(); err != nil {
				panic(err)
			}
		}()

		rcmgrObs.MustRegisterWith(prometheus.DefaultRegisterer)

		str, err := rcmgrObs.NewStatsTraceReporter()
		if err != nil {
			return nil, err
		}

		rmgr, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale()), rcmgr.WithTraceReporter(str))
		if err != nil {
			return nil, err
		}
		opts = append(opts, libp2p.ResourceManager(rmgr))
	}
	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	s.handshaker = handshake.New(ctx, cfg.GenesisConfig, cfg.BeaconConfig, host)

	s.host = host
	s.peers = peers.New(s.host)

	pubsub.TimeCacheDuration = 550 * gossipSubHeartbeatInterval
	s.pubsub, err = pubsub.NewGossipSub(s.ctx, s.host, s.pubsubOptions()...)
	if err != nil {
		return nil, fmt.Errorf("[Sentinel] failed to subscribe to gossip err=%w", err)
	}

	return s, nil
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
	s.subManager = NewGossipManager(s.ctx)
	if !s.cfg.NoDiscovery {
		go s.listenForPeers()
	}
	return nil
}

func (s *Sentinel) Stop() {
	s.listenForPeersDoneCh <- struct{}{}
	s.listener.Close()
	s.subManager.Close()
	s.host.Close()
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

func (s *Sentinel) Host() host.Host {
	return s.host
}

func (s *Sentinel) Peers() *peers.Peers {
	return s.peers
}

func (s *Sentinel) GossipManager() *GossipManager {
	return s.subManager
}

func (s *Sentinel) Config() *SentinelConfig {
	return s.cfg
}

func (s *Sentinel) DB() kv.RoDB {
	return s.db
}

func (s *Sentinel) Status() *cltypes.Status {
	return s.handshaker.Status()
}

func (s *Sentinel) PeersList() []peer.AddrInfo {
	pids := s.host.Network().Peers()
	infos := []peer.AddrInfo{}
	for _, pid := range pids {
		infos = append(infos, s.host.Network().Peerstore().PeerInfo(pid))
	}
	return infos
}
