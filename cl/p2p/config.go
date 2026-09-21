package p2p

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"slices"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/version"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/crypto"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
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
	return addressBuilder(ipAddr, fmt.Sprintf("/tcp/%d", port))
}

func quicAddressBuilder(ipAddr string, port uint) (multiaddr.Multiaddr, error) {
	return addressBuilder(ipAddr, fmt.Sprintf("/udp/%d/quic-v1", port))
}

func addressBuilder(ipAddr, transport string) (multiaddr.Multiaddr, error) {
	parsedIP := net.ParseIP(ipAddr)
	if parsedIP == nil {
		return nil, fmt.Errorf("invalid ip address provided: %s", ipAddr)
	}
	host, err := manet.FromIP(parsedIP)
	if err != nil {
		return nil, fmt.Errorf("invalid ip address provided: %s", ipAddr)
	}
	transportAddr, err := multiaddr.NewMultiaddr(transport)
	if err != nil {
		return nil, err
	}
	return host.Encapsulate(transportAddr), nil
}

func appendAdvertisedAddresses(addrs []multiaddr.Multiaddr, host multiaddr.Multiaddr) []multiaddr.Multiaddr {
	advertised := slices.Clone(addrs)
	for _, addr := range addrs {
		_, transport := multiaddr.SplitFirst(addr)
		if transport == nil {
			continue
		}
		advertised = append(advertised, host.Encapsulate(transport))
	}
	return advertised
}

func buildOptions(cfg *P2PConfig, privateKey *ecdsa.PrivateKey) ([]libp2p.Option, error) {
	tcpListen, err := multiAddressBuilder(cfg.IpAddr, cfg.TCPPort)
	if err != nil {
		return nil, err
	}
	quicListen, err := quicAddressBuilder(cfg.IpAddr, cfg.QUICPort)
	if err != nil {
		return nil, err
	}
	if cfg.LocalIP != "" {
		if net.ParseIP(cfg.LocalIP) == nil {
			return nil, fmt.Errorf("invalid local ip provided: %s", cfg.LocalIP)
		}
		tcpListen, err = multiAddressBuilder(cfg.LocalIP, cfg.TCPPort)
		if err != nil {
			return nil, err
		}
		quicListen, err = quicAddressBuilder(cfg.LocalIP, cfg.QUICPort)
		if err != nil {
			return nil, err
		}
	}

	options := []libp2p.Option{
		privKeyOption(privateKey),
		libp2p.ListenAddrs(quicListen, tcpListen),
		libp2p.UserAgent("erigon/caplin/" + version.NodeVersion()),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Transport(tcp.NewTCPTransport),
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
		host, err := manet.FromIP(net.ParseIP(externalAddr))
		if err == nil {
			options = append(options, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				return appendAdvertisedAddresses(addrs, host)
			}))
		}
	}
	if cfg.HostDNS != "" {
		host, err := multiaddr.NewMultiaddr("/dns4/" + cfg.HostDNS)
		if err == nil {
			options = append(options, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				return appendAdvertisedAddresses(addrs, host)
			}))
		}
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
