package sentinel

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"net"

	"github.com/ledgerwatch/erigon/p2p/discover"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/pkg/errors"
)

type Sentinel struct {
	started  bool
	listener *discover.UDPv5 // this is us in the network.
	ctx      context.Context
	cfg      SentinelConfig
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
	switch {
	case ip.To16() != nil && ip.To4() == nil:
		bindIP = net.IPv6zero
		networkVersion = "udp6"
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
	localNode, err := s.createLocalNode(discCfg.PrivateKey, ip, port, port+1)
	if err != nil {
		return nil, err
	}

	net, err := discover.ListenV5(s.ctx, conn, localNode, discCfg)
	if err != nil {
		return nil, err
	}
	return net, err
}

// This is just one of the examples from the libp2p repository.
func New(ctx context.Context, cfg SentinelConfig) *Sentinel {
	return &Sentinel{
		ctx: ctx,
		cfg: cfg,
	}
}

func (s *Sentinel) Start() error {
	if s.started {
		log.Error("Sentinel already running")
	}
	var err error
	s.listener, err = s.createListener()
	if err != nil {
		return err
	}
	return nil
}

// Debug methods

func (s *Sentinel) GetListener() *discover.UDPv5 {
	return s.listener
}
