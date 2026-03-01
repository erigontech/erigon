// Copyright 2022 The Erigon Authors
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

package sentinel

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"sync"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/go-bitfield"

	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/sentinel/handlers"
	"github.com/erigontech/erigon/cl/sentinel/handshake"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

type Sentinel struct {
	started  bool
	listener *discover.UDPv5 // this is us in the network.
	ctx      context.Context
	cfg      *SentinelConfig
	peers    *peers.Pool
	p2p      p2p.P2PManager

	httpApi http.Handler

	handshaker *handshake.HandShaker

	blockReader       freezeblocks.BeaconSnapshotReader
	blobStorage       blob_storage.BlobStorage
	dataColumnStorage blob_storage.DataColumnStorage

	indiciesDB kv.RoDB

	discoverConfig     discover.Config
	subManager         *GossipManager
	metrics            bool
	logger             log.Logger
	forkChoiceReader   forkchoice.ForkChoiceStorageReader
	pidToEnr           sync.Map
	pidToEnodeId       sync.Map
	ethClock           eth_clock.EthereumClock
	peerDasStateReader peerdasstate.PeerDasStateReader

	metadataLock sync.Mutex
}

func (s *Sentinel) SetStatus(status *cltypes.Status) {
	s.handshaker.SetStatus(status)
}

// This is just one of the examples from the libp2p repository.
func New(
	ctx context.Context,
	cfg *SentinelConfig,
	ethClock eth_clock.EthereumClock,
	blockReader freezeblocks.BeaconSnapshotReader,
	blobStorage blob_storage.BlobStorage,
	indiciesDB kv.RoDB,
	logger log.Logger,
	forkChoiceReader forkchoice.ForkChoiceStorageReader,
	dataColumnStorage blob_storage.DataColumnStorage,
	peerDasStateReader peerdasstate.PeerDasStateReader,
	p2p p2p.P2PManager,
) (*Sentinel, error) {
	s := &Sentinel{
		ctx:                ctx,
		cfg:                cfg,
		blockReader:        blockReader,
		indiciesDB:         indiciesDB,
		metrics:            true,
		logger:             logger,
		forkChoiceReader:   forkChoiceReader,
		blobStorage:        blobStorage,
		ethClock:           ethClock,
		dataColumnStorage:  dataColumnStorage,
		peerDasStateReader: peerDasStateReader,
		p2p:                p2p,
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

	signal.Reset(syscall.SIGINT)
	s.peers = peers.NewPool(s.p2p.Host())

	mux := chi.NewRouter()
	mux.Get("/", httpreqresp.NewRequestHandler(s.p2p.Host()))
	s.httpApi = mux

	s.handshaker = handshake.New(ctx, s.ethClock, cfg.BeaconConfig, s.httpApi, peerDasStateReader)

	return s, nil
}

func (s *Sentinel) ReqRespHandler() http.Handler {
	return s.httpApi
}

func (s *Sentinel) Start() (*enode.LocalNode, error) {
	if s.started {
		s.logger.Warn("[Sentinel] already running")
	}
	//var err error
	/*s.listener, err = s.createListener()
	if err != nil {
		return nil, fmt.Errorf("failed creating sentinel listener err=%w", err)
	}*/
	s.listener = s.p2p.UDPv5Listener()

	handlers.NewConsensusHandlers(
		s.ctx,
		s.blockReader,
		s.indiciesDB,
		s.p2p.Host(),
		s.peers,
		s.cfg.NetworkConfig,
		s.p2p.UDPv5Listener().LocalNode(),
		s.cfg.BeaconConfig, s.ethClock, s.handshaker, s.forkChoiceReader, s.blobStorage, s.dataColumnStorage, s.peerDasStateReader, s.cfg.EnableBlocks).Start()

	/*if err := s.connectToBootnodes(); err != nil {
		return nil, fmt.Errorf("failed to connect to bootnodes err=%w", err)
	}*/
	// Configuring handshake
	s.p2p.Host().Network().Notify(&network.NotifyBundle{
		ConnectedF: s.onConnection,
		DisconnectedF: func(n network.Network, c network.Conn) {
			peerId := c.RemotePeer()
			s.peers.RemovePeer(peerId)
		},
	})
	s.subManager = NewGossipManager(s.ctx)
	//s.subManager.Start(s.ctx)

	go s.listenForPeers()
	go s.proactiveSubnetPeerSearch() // Proactively search for peers when subnet coverage is low
	//go s.forkWatcher()
	//go s.observeBandwidth(s.ctx)

	return s.LocalNode(), nil
}

func (s *Sentinel) Stop() {
	//s.listener.Close()
	//s.subManager.Close()
	s.p2p.Host().Close()
}

func (s *Sentinel) String() string {
	return s.listener.Self().String()
}

func (s *Sentinel) HasTooManyPeers() bool {
	active, _, _ := s.GetPeersCount()
	return active >= int(s.cfg.MaxPeerCount)
}

func (s *Sentinel) GetPeersCount() (active int, connected int, disconnected int) {
	peers := s.p2p.Host().Network().Peers()

	active = len(peers)
	for _, p := range peers {
		if s.p2p.Host().Network().Connectedness(p) == network.Connected {
			connected++
		} else {
			disconnected++
		}
	}
	disconnected += s.peers.LenBannedPeers()
	return
}

func (s *Sentinel) GetPeersInfos() *sentinelproto.PeersInfoResponse {
	peers := s.p2p.Host().Network().Peers()

	out := &sentinelproto.PeersInfoResponse{Peers: make([]*sentinelproto.Peer, 0, len(peers))}

	for _, p := range peers {
		entry := &sentinelproto.Peer{}
		peerInfo := s.p2p.Host().Network().Peerstore().PeerInfo(p)
		if len(peerInfo.Addrs) != 0 {
			entry.Address = peerInfo.Addrs[0].String()
		}
		entry.Pid = peerInfo.ID.String()
		entry.State = "connected"
		if s.p2p.Host().Network().Connectedness(p) != network.Connected {
			entry.State = "disconnected"
		}
		conns := s.p2p.Host().Network().ConnsToPeer(p)
		if len(conns) == 0 || conns[0].Stat().Direction == network.DirOutbound {
			entry.Direction = "outbound"
		} else {
			entry.Direction = "inbound"
		}
		if node, ok := s.pidToEnr.Load(p); ok {
			entry.Enr = node.(*enode.Node).String()
		} else {
			entry.Enr = ""
		}
		if enodeId, ok := s.pidToEnodeId.Load(p); ok {
			entry.EnodeId = enodeId.(enode.ID).String()
		} else {
			entry.EnodeId = ""
		}
		agent, err := s.p2p.Host().Peerstore().Get(p, "AgentVersion")
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
	pid = s.p2p.Host().ID().String()
	enrStr = s.listener.LocalNode().Node().String()
	p2pAddresses = make([]string, 0, len(s.p2p.Host().Addrs()))
	for _, addr := range s.p2p.Host().Addrs() {
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
	subnetField := bitfield.NewBitvector64()
	syncnetField := bitfield.NewBitvector8()
	attSubEnr := enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &subnetField)
	syncNetEnr := enr.WithEntry(s.cfg.NetworkConfig.SyncCommsSubnetKey, &syncnetField)
	if err := s.listener.LocalNode().Node().Load(attSubEnr); err != nil {
		s.logger.Debug("[IDENTITY] Could not load att subnet", "err", err)
	}
	if err := s.listener.LocalNode().Node().Load(syncNetEnr); err != nil {
		s.logger.Debug("[IDENTITY] Could not load sync subnet", "err", err)
	}
	cgc := s.forkChoiceReader.GetPeerDas().StateReader().GetAdvertisedCgc()
	metadata = &cltypes.Metadata{
		SeqNumber:         s.listener.LocalNode().Seq(),
		Attnets:           [8]byte(subnetField),
		Syncnets:          (*[1]byte)(syncnetField),
		CustodyGroupCount: &cgc,
	}
	return
}

func (s *Sentinel) LocalNode() *enode.LocalNode {
	return s.listener.LocalNode()
}

func (s *Sentinel) Host() host.Host {
	return s.p2p.Host()
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
	pids := s.p2p.Host().Network().Peers()
	infos := make([]peer.AddrInfo, 0, len(pids))
	for _, pid := range pids {
		infos = append(infos, s.p2p.Host().Network().Peerstore().PeerInfo(pid))
	}
	return infos
}
