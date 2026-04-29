package p2p

import (
	"context"
	"net"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	p2pnat "github.com/erigontech/erigon/p2p/nat"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/go-bitfield"
)

type P2PConfig struct {
	NetworkConfig *clparams.NetworkConfig
	BeaconConfig  *clparams.BeaconChainConfig
	IpAddr        string
	Port          int
	TCPPort       uint

	// Optional
	LocalIP        string
	EnableUPnP     bool
	RelayNodeAddr  string
	HostAddress    string
	HostDNS        string
	NoDiscovery    bool
	TmpDir         string
	LocalDiscovery bool

	// NAT is the NAT interface used to resolve the external IP for discv5 ENR and
	// libp2p multiaddrs. When set, ExternalIP is populated at startup via ExternalIP().
	// Supports extip:<IP>, stun, upnp, pmp modes (same as devp2p --nat flag).
	NAT        p2pnat.Interface
	ExternalIP net.IP // resolved from NAT at startup; set by NewP2Pmanager

	MaxPeerCount       uint64
	SubscribeAllTopics bool // When true, advertise all attnets/syncnets in ENR
}

type p2pManager struct {
	cfg      *P2PConfig
	pubsub   *pubsub.PubSub
	bwc      *metrics.BandwidthCounter
	host     host.Host
	udpv5    *discover.UDPv5
	ethClock eth_clock.EthereumClock

	bannedPeers *lru.CacheWithTTL[peer.ID, struct{}]
}

func NewP2Pmanager(ctx context.Context, cfg *P2PConfig, logger log.Logger, ethClock eth_clock.EthereumClock) (P2PManager, error) {
	// Resolve external IP from NAT once so both discv5 ENR and libp2p multiaddrs use
	// the same public address. ExtIP resolves immediately; STUN/UPnP make network calls.
	if cfg.NAT != nil {
		if extIP, err := cfg.NAT.ExternalIP(); err == nil {
			cfg.ExternalIP = extIP
			logger.Info("[Caplin] NAT external IP resolved", "ip", extIP, "nat", cfg.NAT)
		} else {
			logger.Warn("[Caplin] NAT external IP resolution failed — incoming peers may not reach this node", "nat", cfg.NAT, "err", err)
		}
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

	gater, err := NewGater(cfg)
	if err != nil {
		return nil, err
	}
	opts, err := buildOptions(cfg, privateKey)
	if err != nil {
		return nil, err
	}
	bwc := metrics.NewBandwidthCounter()
	opts = append(opts, libp2p.ConnectionGater(gater), libp2p.BandwidthReporter(bwc))
	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	p := p2pManager{
		cfg:         cfg,
		host:        host,
		bwc:         bwc,
		ethClock:    ethClock,
		bannedPeers: lru.NewWithTTL[peer.ID, struct{}]("bannedPeers", 1_000, 30*time.Minute),
	}

	// pubsub
	pubsub.TimeCacheDuration = gossipSubSeenTTL * gossipSubHeartbeatInterval
	p.pubsub, err = pubsub.NewGossipSub(ctx, host, p.pubsubOptions(cfg.BeaconConfig)...)
	if err != nil {
		return nil, err
	}

	// udpv5
	discCfg := discover.Config{
		PrivateKey: privateKey,
		Bootnodes:  enodes,
	}
	p.udpv5, err = NewUDPv5Listener(ctx, cfg, discCfg, logger)
	if err != nil {
		return nil, err
	}

	// connect to bootnodes
	if err := p.connectToBootnodes(ctx, discCfg); err != nil {
		return nil, err
	}

	// setup ENR
	if err := p.setupENR(); err != nil {
		return nil, err
	}
	go p.updateENR()
	go p.peerMonitor(ctx)
	return &p, nil
}

func (p *p2pManager) Pubsub() *pubsub.PubSub {
	return p.pubsub
}

func (p *p2pManager) Host() host.Host {
	return p.host
}

func (p *p2pManager) BandwidthCounter() *metrics.BandwidthCounter {
	return p.bwc
}

func (p *p2pManager) UDPv5Listener() *discover.UDPv5 {
	return p.udpv5
}

func (p *p2pManager) setupENR() error {
	node := p.udpv5.LocalNode()
	if node == nil {
		panic("local node is nil")
	}
	forkId, err := p.ethClock.ForkId()
	if err != nil {
		return err
	}
	nfd, err := p.ethClock.NextForkDigest()
	if err != nil {
		return err
	}
	initialAttnets := bitfield.NewBitvector64()
	initialSyncnets := bitfield.Bitvector4{byte(0x00)}
	if p.cfg.SubscribeAllTopics {
		// Advertise all 64 attestation subnets and all 4 sync committee subnets
		// so that peers see us as a useful node and keep us connected.
		for i := 0; i < 64; i++ {
			initialAttnets.SetBitAt(uint64(i), true)
		}
		initialSyncnets = bitfield.Bitvector4{byte(0x0f)}
	} else {
		// Set a couple of subnets so peers don't see all-zeros and penalize us
		// as a "useless peer". The VC will update subnets later via committee
		// subscriptions; this just ensures early handshakes succeed.
		initialAttnets.SetBitAt(0, true) // subnet 0
		initialAttnets.SetBitAt(1, true) // subnet 1
	}
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.Eth2key, forkId))
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.AttSubnetKey, initialAttnets.Bytes()))
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.SyncCommsSubnetKey, initialSyncnets.Bytes()))
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.CgcKey, []byte{}))
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.NfdKey, nfd))
	return nil
}

func (s *p2pManager) updateENR() {
	node := s.udpv5.LocalNode()
	if node == nil {
		panic("local node is nil")
	}
	for {
		nextForkEpoch := s.ethClock.NextForkEpochIncludeBPO()
		if nextForkEpoch == s.cfg.BeaconConfig.FarFutureEpoch {
			break
		}
		// sleep until next fork epoch
		wakeupTime := s.ethClock.GetSlotTime(nextForkEpoch * s.cfg.BeaconConfig.SlotsPerEpoch).Add(time.Second)
		log.Info("[Sentinel] Sleeping until next fork epoch", "nextForkEpoch", nextForkEpoch, "wakeupTime", wakeupTime)
		time.Sleep(time.Until(wakeupTime)) // add 1 second for safety
		nfd, err := s.ethClock.NextForkDigest()
		if err != nil {
			log.Warn("[Sentinel] Could not get next fork digest", "err", err)
			break
		}
		node.Set(enr.WithEntry(s.cfg.NetworkConfig.NfdKey, nfd))
		forkId, err := s.ethClock.ForkId()
		if err != nil {
			log.Warn("[Sentinel] Could not get fork id", "err", err)
			break
		}
		node.Set(enr.WithEntry(s.cfg.NetworkConfig.Eth2key, forkId))
		log.Info("[Sentinel] Updated fork id and nfd")
	}
}

func (s *p2pManager) UpdateENRAttSubnets(subnetIndex int, on bool) {
	// Attestation subnets use Bitvector64 (8 bytes for 64 subnets).
	subnetField := bitfield.NewBitvector64()
	if err := s.udpv5.LocalNode().Node().Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &subnetField)); err != nil {
		log.Error("[Sentinel] Could not load AttSubnetKey", "err", err)
		return
	}
	subnetField = common.Copy(subnetField)
	if len(subnetField) <= subnetIndex/8 {
		log.Error("[Sentinel] Att subnet index out of range", "subnetIndex", subnetIndex, "len", len(subnetField))
		return
	}
	if on {
		subnetField[subnetIndex/8] |= 1 << (subnetIndex % 8)
	} else {
		subnetField[subnetIndex/8] &^= 1 << (subnetIndex % 8)
	}
	s.udpv5.LocalNode().Set(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &subnetField))
	log.Info("[Sentinel] Updated att subnet", "subnetIndex", subnetIndex, "on", on)
}

func (s *p2pManager) UpdateENRSyncNets(subnetIndex int, on bool) {
	// Sync committee subnets use Bitvector4 (1 byte for 4 subnets).
	subnetField := bitfield.NewBitvector4()
	if err := s.udpv5.LocalNode().Node().Load(enr.WithEntry(s.cfg.NetworkConfig.SyncCommsSubnetKey, &subnetField)); err != nil {
		log.Error("[Sentinel] Could not load SyncCommsSubnetKey", "err", err)
		return
	}
	subnetField = common.Copy(subnetField)
	if len(subnetField) <= subnetIndex/8 {
		log.Error("[Sentinel] Sync subnet index out of range", "subnetIndex", subnetIndex, "len", len(subnetField))
		return
	}
	if on {
		subnetField[subnetIndex/8] |= 1 << (subnetIndex % 8)
	} else {
		subnetField[subnetIndex/8] &^= 1 << (subnetIndex % 8)
	}
	s.udpv5.LocalNode().Set(enr.WithEntry(s.cfg.NetworkConfig.SyncCommsSubnetKey, &subnetField))
	log.Info("[Sentinel] Updated sync subnet", "subnetIndex", subnetIndex, "on", on)
}
