package p2p

import (
	"context"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
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

	MaxPeerCount uint64
}

type P2Pmanager struct {
	cfg      *P2PConfig
	pubsub   *pubsub.PubSub
	bwc      *metrics.BandwidthCounter
	host     host.Host
	udpv5    *discover.UDPv5
	ethClock eth_clock.EthereumClock
}

func NewP2Pmanager(ctx context.Context, cfg *P2PConfig, logger log.Logger, ethClock eth_clock.EthereumClock) (*P2Pmanager, error) {
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

	p := P2Pmanager{
		cfg:      cfg,
		host:     host,
		bwc:      bwc,
		ethClock: ethClock,
	}

	// pubsub
	pubsub.TimeCacheDuration = 550 * gossipSubHeartbeatInterval
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
	return &p, nil
}

func (p *P2Pmanager) Pubsub() *pubsub.PubSub {
	return p.pubsub
}

func (p *P2Pmanager) Host() host.Host {
	return p.host
}

func (p *P2Pmanager) BandwidthCounter() *metrics.BandwidthCounter {
	return p.bwc
}

func (p *P2Pmanager) UDPv5Listener() *discover.UDPv5 {
	return p.udpv5
}

func (p *P2Pmanager) setupENR() error {
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
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.Eth2key, forkId))
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.AttSubnetKey, bitfield.NewBitvector64().Bytes()))
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.SyncCommsSubnetKey, bitfield.Bitvector4{byte(0x00)}.Bytes()))
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.CgcKey, []byte{}))
	node.Set(enr.WithEntry(p.cfg.NetworkConfig.NfdKey, nfd))
	return nil
}

func (s *P2Pmanager) updateENR() {
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

func (s *P2Pmanager) UpdateENRAttSubnets(subnetIndex int, on bool) {
	s.updateSubnetENR(s.cfg.NetworkConfig.AttSubnetKey, subnetIndex, on)
}

func (s *P2Pmanager) UpdateENRSyncNets(subnetIndex int, on bool) {
	s.updateSubnetENR(s.cfg.NetworkConfig.SyncCommsSubnetKey, subnetIndex, on)
}

func (s *P2Pmanager) updateSubnetENR(subnetKey string, subnetIndex int, on bool) {
	subnetField := bitfield.NewBitvector4()
	if err := s.udpv5.LocalNode().Node().Load(enr.WithEntry(subnetKey, &subnetField)); err != nil {
		log.Error("[Sentinel] Could not load syncCommsSubnetKey", "err", err)
		return
	}
	subnetField = common.Copy(subnetField)
	if len(subnetField) <= subnetIndex/8 {
		log.Error("[Sentinel] Subnet index out of range", "subnetIndex", subnetIndex, "len", len(subnetField))
		return
	}
	if on {
		subnetField[subnetIndex/8] |= 1 << (subnetIndex % 8)
	} else {
		subnetField[subnetIndex/8] &^= 1 << (subnetIndex % 8)
	}
	s.udpv5.LocalNode().Set(enr.WithEntry(subnetKey, &subnetField))
	log.Info("[Sentinel] Updated subnet", "subnetKey", subnetKey, "subnetIndex", subnetIndex, "on", on)
}
