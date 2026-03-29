package p2p

import (
	"crypto/ecdsa"
	"fmt"
	"net"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/crypto"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
)

func convertToCryptoPrivkey(privkey *ecdsa.PrivateKey) (crypto.PrivKey, error) {
	privBytes := privkey.D.Bytes()
	if len(privBytes) < 32 {
		privBytes = append(make([]byte, 32-len(privBytes)), privBytes...)
	}
	return crypto.UnmarshalSecp256k1PrivateKey(privBytes)
}

func privKeyOption(privkey *ecdsa.PrivateKey) libp2p.Option {
	return func(cfg *libp2p.Config) error {
		ifaceKey, err := convertToCryptoPrivkey(privkey)
		if err != nil {
			return err
		}
		log.Debug("[Sentinel] ECDSA private key generated")
		return cfg.Apply(libp2p.Identity(ifaceKey))
	}
}

// multiAddressBuilder takes in an ip address string and port to produce a go multiaddr format.
func multiAddressBuilder(ipAddr string, port uint) (multiaddr.Multiaddr, error) {
	parsedIP := net.ParseIP(ipAddr)
	if parsedIP.To4() == nil && parsedIP.To16() == nil {
		return nil, fmt.Errorf("invalid ip address provided: %s", ipAddr)
	}
	if parsedIP.To4() != nil {
		return multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ipAddr, port))
	}
	return multiaddr.NewMultiaddr(fmt.Sprintf("/ip6/%s/tcp/%d", ipAddr, port))
}

func buildOptions(cfg *P2PConfig, privateKey *ecdsa.PrivateKey) ([]libp2p.Option, error) {

	listen, err := multiAddressBuilder(cfg.IpAddr, cfg.TCPPort)
	if err != nil {
		return nil, err
	}
	if cfg.LocalIP != "" {
		if net.ParseIP(cfg.LocalIP) == nil {
			return nil, fmt.Errorf("invalid local ip provided: %s", cfg.LocalIP)
		}
		listen, err = multiAddressBuilder(cfg.LocalIP, cfg.TCPPort)
		if err != nil {
			return nil, err
		}
	}

	options := []libp2p.Option{
		privKeyOption(privateKey),
		libp2p.ListenAddrs(listen),
		libp2p.UserAgent("erigon/caplin"),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
		libp2p.DefaultMuxers,
		libp2p.Ping(false),
	}
	if cfg.EnableUPnP {
		options = append(options, libp2p.NATPortMap())
	}

	options = append(options, libp2p.Security(noise.ID, noise.New), libp2p.DisableRelay())

	// Prefer an explicit HostAddress; fall back to NAT-resolved ExternalIP.
	externalAddr := cfg.HostAddress
	if externalAddr == "" && cfg.ExternalIP != nil {
		externalAddr = cfg.ExternalIP.String()
	}
	if externalAddr != "" {
		options = append(options, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			external, err := multiAddressBuilder(externalAddr, cfg.TCPPort)
			if err != nil {
				return addrs
			}
			return append(addrs, external)
		}))
	}
	if cfg.HostDNS != "" {
		options = append(options, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			external, err := multiaddr.NewMultiaddr(fmt.Sprintf("/dns4/%s/tcp/%d", cfg.HostDNS, cfg.TCPPort))
			if err != nil {
				return nil
			} else {
				addrs = append(addrs, external)
			}
			return addrs
		}))
	}
	// Disable Ping Service.
	options = append(options, libp2p.Ping(false))

	// Enable libp2p resource manager with tightened per-peer inbound stream
	// limits. The default PeerBaseLimit.StreamsInbound (256) is far too
	// permissive; Lighthouse caps per-peer inbound substreams at 32.
	limits := rcmgr.DefaultLimits
	limits.PeerBaseLimit.StreamsInbound = 32
	limits.PeerLimitIncrease.StreamsInbound = 0 // do not scale with memory
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.AutoScale()))
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p resource manager: %w", err)
	}
	options = append(options, libp2p.ResourceManager(rm))

	return options, nil
}
