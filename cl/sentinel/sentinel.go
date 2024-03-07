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
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/persistence/blob_storage"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/sentinel/handlers"
	"github.com/ledgerwatch/erigon/cl/sentinel/handshake"
	"github.com/ledgerwatch/erigon/cl/sentinel/httpreqresp"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"

	sentinelrpc "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
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
	started  bool
	listener *discover.UDPv5 // this is us in the network.
	ctx      context.Context
	host     host.Host
	cfg      *SentinelConfig
	peers    *peers.Pool

	httpApi http.Handler

	handshaker *handshake.HandShaker

	blockReader freezeblocks.BeaconSnapshotReader
	blobStorage blob_storage.BlobStorage

	indiciesDB kv.RoDB

	discoverConfig       discover.Config
	pubsub               *pubsub.PubSub
	subManager           *GossipManager
	metrics              bool
	listenForPeersDoneCh chan struct{}
	logger               log.Logger
	forkChoiceReader     forkchoice.ForkChoiceStorageReader
	pidToEnr             sync.Map
}

func (s *Sentinel) createLocalNode(
	privKey *ecdsa.PrivateKey,
	ipAddr net.IP,
	udpPort, tcpPort int,
	tmpDir string,
) (*enode.LocalNode, error) {
	db, err := enode.OpenDB(s.ctx, "", tmpDir, s.logger)
	if err != nil {
		return nil, fmt.Errorf("could not open node's peer database: %w", err)
	}
	localNode := enode.NewLocalNode(db, privKey, s.logger)

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

	// Start stream handlers

	net, err := discover.ListenV5(s.ctx, "any", conn, localNode, discCfg)
	if err != nil {
		return nil, err
	}
	handlers.NewConsensusHandlers(s.ctx, s.blockReader, s.indiciesDB, s.host, s.peers, s.cfg.NetworkConfig, localNode, s.cfg.BeaconConfig, s.cfg.GenesisConfig, s.handshaker, s.forkChoiceReader, s.blobStorage, s.cfg.EnableBlocks).Start()

	return net, err
}

// This is just one of the examples from the libp2p repository.
func New(
	ctx context.Context,
	cfg *SentinelConfig,
	blockReader freezeblocks.BeaconSnapshotReader,
	blobStorage blob_storage.BlobStorage,
	indiciesDB kv.RoDB,
	logger log.Logger,
	forkChoiceReader forkchoice.ForkChoiceStorageReader,
) (*Sentinel, error) {
	s := &Sentinel{
		ctx:              ctx,
		cfg:              cfg,
		blockReader:      blockReader,
		indiciesDB:       indiciesDB,
		metrics:          true,
		logger:           logger,
		forkChoiceReader: forkChoiceReader,
		blobStorage:      blobStorage,
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
	str, err := rcmgrObs.NewStatsTraceReporter()
	if err != nil {
		return nil, err
	}

	rmgr, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale()), rcmgr.WithTraceReporter(str))
	if err != nil {
		return nil, err
	}
	opts = append(opts, libp2p.ResourceManager(rmgr))

	gater, err := NewGater(cfg)
	if err != nil {
		return nil, err
	}

	opts = append(opts, libp2p.ConnectionGater(gater))

	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	s.host = host

	s.peers = peers.NewPool()

	mux := chi.NewRouter()
	//	mux := httpreqresp.NewRequestHandler(host)
	mux.Get("/", httpreqresp.NewRequestHandler(host))
	s.httpApi = mux

	s.handshaker = handshake.New(ctx, cfg.GenesisConfig, cfg.BeaconConfig, s.httpApi)

	pubsub.TimeCacheDuration = 550 * gossipSubHeartbeatInterval
	s.pubsub, err = pubsub.NewGossipSub(s.ctx, s.host, s.pubsubOptions()...)
	if err != nil {
		return nil, fmt.Errorf("[Sentinel] failed to subscribe to gossip err=%w", err)
	}

	return s, nil
}

func (s *Sentinel) ReqRespHandler() http.Handler {
	return s.httpApi
}

func (s *Sentinel) RecvGossip() <-chan *GossipMessage {
	return s.subManager.Recv()
}

func (s *Sentinel) Start() error {
	if s.started {
		s.logger.Warn("[Sentinel] already running")
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
		DisconnectedF: func(n network.Network, c network.Conn) {
			peerId := c.RemotePeer()
			s.peers.RemovePeer(peerId)
		},
	})
	s.subManager = NewGossipManager(s.ctx)

	go s.listenForPeers()
	go s.forkWatcher()

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
	active, _, _ := s.GetPeersCount()
	return active >= peers.DefaultMaxPeers
}

func (s *Sentinel) GetPeersCount() (active int, connected int, disconnected int) {
	peers := s.host.Network().Peers()

	active = len(peers)
	for _, p := range peers {
		if s.host.Network().Connectedness(p) == network.Connected {
			connected++
		} else {
			disconnected++
		}
	}
	disconnected += s.peers.LenBannedPeers()
	return
}

func (s *Sentinel) GetPeersInfos() *sentinelrpc.PeersInfoResponse {
	peers := s.host.Network().Peers()

	out := &sentinelrpc.PeersInfoResponse{Peers: make([]*sentinelrpc.Peer, 0, len(peers))}

	for _, p := range peers {
		entry := &sentinelrpc.Peer{}
		peerInfo := s.host.Network().Peerstore().PeerInfo(p)
		if len(peerInfo.Addrs) == 0 {
			continue
		}
		entry.Address = peerInfo.Addrs[0].String()
		entry.Pid = peerInfo.ID.String()
		entry.State = "connected"
		if s.host.Network().Connectedness(p) != network.Connected {
			entry.State = "disconnected"
		}
		conns := s.host.Network().ConnsToPeer(p)
		if len(conns) == 0 {
			continue
		}
		if conns[0].Stat().Direction == network.DirOutbound {
			entry.Direction = "outbound"
		} else {
			entry.Direction = "inbound"
		}
		if enr, ok := s.pidToEnr.Load(p); ok {
			entry.Enr = enr.(string)
		} else {
			continue
		}
		agent, err := s.host.Peerstore().Get(p, "AgentVersion")
		if err == nil {
			entry.AgentVersion = agent.(string)
		}
		if entry.AgentVersion == "" {
			entry.AgentVersion = "unknown"
		}
		out.Peers = append(out.Peers, entry)
	}
	return out
}

func (s *Sentinel) Identity() (pid, enrStr string, p2pAddresses, discoveryAddresses []string, metadata *cltypes.Metadata) {
	pid = s.host.ID().String()
	enrStr = s.listener.LocalNode().Node().String()
	p2pAddresses = make([]string, 0, len(s.host.Addrs()))
	for _, addr := range s.host.Addrs() {
		p2pAddresses = append(p2pAddresses, fmt.Sprintf("%s/%s", addr.String(), pid))
	}
	discoveryAddresses = []string{}

	if s.listener.LocalNode().Node().TCP() != 0 {
		protocol := "ip4"
		if s.listener.LocalNode().Node().IP().To4() == nil {
			protocol = "ip6"
		}
		port := s.listener.LocalNode().Node().TCP()
		discoveryAddresses = append(discoveryAddresses, fmt.Sprintf("/%s/%s/tcp/%d/p2p/%s", protocol, s.listener.LocalNode().Node().IP(), port, pid))
	}
	if s.listener.LocalNode().Node().UDP() != 0 {
		protocol := "ip4"
		if s.listener.LocalNode().Node().IP().To4() == nil {
			protocol = "ip6"
		}
		port := s.listener.LocalNode().Node().UDP()
		discoveryAddresses = append(discoveryAddresses, fmt.Sprintf("/%s/%s/udp/%d/p2p/%s", protocol, s.listener.LocalNode().Node().IP(), port, pid))
	}
	subnetField := [8]byte{}
	syncnetField := [1]byte{}
	attSubEnr := enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, subnetField[:])
	syncNetEnr := enr.WithEntry(s.cfg.NetworkConfig.SyncCommsSubnetKey, syncnetField[:])
	if err := s.listener.LocalNode().Node().Load(attSubEnr); err != nil {
		return
	}
	if err := s.listener.LocalNode().Node().Load(syncNetEnr); err != nil {
		return
	}
	metadata = &cltypes.Metadata{
		SeqNumber: s.listener.LocalNode().Seq(),
		Attnets:   subnetField,
		Syncnets:  &syncnetField,
	}
	return
}

func (s *Sentinel) Host() host.Host {
	return s.host
}

func (s *Sentinel) Peers() *peers.Pool {
	return s.peers
}

func (s *Sentinel) GossipManager() *GossipManager {
	return s.subManager
}

func (s *Sentinel) Config() *SentinelConfig {
	return s.cfg
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
