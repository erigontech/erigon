package p2p

import (
	"context"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
)

type SentinelConfig struct {
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
	cfg    *SentinelConfig
	pubsub *pubsub.PubSub
	bwc    *metrics.BandwidthCounter
	host   *host.Host
}

func NewP2Pmanager(ctx context.Context, cfg *SentinelConfig) (*P2Pmanager, error) {
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
		cfg:  cfg,
		host: &host,
		bwc:  bwc,
	}

	p.pubsub, err = pubsub.NewGossipSub(ctx, host, p.pubsubOptions(cfg.BeaconConfig)...)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *P2Pmanager) Pubsub() *pubsub.PubSub {
	return p.pubsub
}

func (p *P2Pmanager) Host() *host.Host {
	return p.host
}

func (p *P2Pmanager) BandwidthCounter() *metrics.BandwidthCounter {
	return p.bwc
}
