package p2p

import (
	"crypto/ecdsa"
	"fmt"
	"math"
	"net"
	"net/netip"
	"slices"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/version"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
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
	listenIP := cfg.IpAddr
	if cfg.LocalIP != "" {
		if net.ParseIP(cfg.LocalIP) == nil {
			return nil, fmt.Errorf("invalid local ip provided: %s", cfg.LocalIP)
		}
		listenIP = cfg.LocalIP
	}
	tcpListen, err := multiAddressBuilder(listenIP, cfg.TCPPort)
	if err != nil {
		return nil, err
	}

	listenAddrs := []multiaddr.Multiaddr{tcpListen}
	transports := []libp2p.Option{libp2p.Transport(tcp.NewTCPTransport)}
	if !cfg.DisableQUIC {
		quicListen, err := quicAddressBuilder(listenIP, cfg.QUICPort)
		if err != nil {
			return nil, err
		}
		listenAddrs = append([]multiaddr.Multiaddr{quicListen}, listenAddrs...)
		transports = append([]libp2p.Option{libp2p.Transport(libp2pquic.NewTransport)}, transports...)
	}

	options := append([]libp2p.Option{
		privKeyOption(privateKey),
		libp2p.ListenAddrs(listenAddrs...),
		libp2p.UserAgent("erigon/caplin/" + version.NodeVersion()),
	}, transports...)
	options = append(options,
		libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
		libp2p.DefaultMuxers,
		libp2p.Ping(false),
	)
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

	rm, err := newResourceManager(cfg.MaxPeerCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p resource manager: %w", err)
	}
	options = append(options, libp2p.ResourceManager(rm))

	return options, nil
}

func newResourceManager(maxPeerCount uint64) (network.ResourceManager, error) {
	// Tighten per-peer inbound stream limits. The default PeerBaseLimit.StreamsInbound (256) is
	// far too permissive; Lighthouse caps per-peer inbound substreams at 32.
	limits := rcmgr.DefaultLimits
	limits.PeerBaseLimit.StreamsInbound = 32
	limits.PeerLimitIncrease.StreamsInbound = 0 // do not scale with memory
	perBlock24, perBlock16 := subnetConnLimits(maxPeerCount)
	// The per-subnet lists replace go-libp2p's defaults, so they repeat them (8 per IPv4 address or
	// IPv6 /56, 64 per IPv6 /48) before adding the larger blocks.
	return rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.AutoScale()),
		rcmgr.WithLimitPerSubnet(
			[]rcmgr.ConnLimitPerSubnet{
				{PrefixLength: 32, ConnCount: 8},
				{PrefixLength: 24, ConnCount: perBlock24},
				{PrefixLength: 16, ConnCount: perBlock16},
			},
			[]rcmgr.ConnLimitPerSubnet{
				{PrefixLength: 56, ConnCount: 8},
				{PrefixLength: 48, ConnCount: 64},
				{PrefixLength: 32, ConnCount: perBlock16},
			},
		),
		rcmgr.WithNetworkPrefixLimit(
			append(networkLimits(math.MaxInt, "127.0.0.0/8"), networkLimits(privateNetworkConnLimit, privateCIDRList...)...),
			append(networkLimits(math.MaxInt, "::1/128"), networkLimits(privateNetworkConnLimit, "fc00::/7", "fe80::/10")...),
		),
	)
}

// privateNetworkConnLimit caps the connections of all addresses in one private network together.
// It is far above any peer pool, and go-libp2p also uses half of it as the QUIC source-address
// verification burst for that network, which an unlimited count would remove.
const privateNetworkConnLimit = 1024

// networkLimits exempts each prefix from the per-subnet caps and counts all its connections
// against one connCount instead. Used for loopback and the networks the gater treats as private.
func networkLimits(connCount int, prefixes ...string) []rcmgr.NetworkPrefixLimit {
	out := make([]rcmgr.NetworkPrefixLimit, 0, len(prefixes))
	for _, p := range prefixes {
		out = append(out, rcmgr.NetworkPrefixLimit{Network: netip.MustParsePrefix(p), ConnCount: connCount})
	}
	return out
}

// subnetConnLimits returns the connection caps per IPv4 /24 and per IPv4 /16 (IPv6 /32). A /24
// may hold an eighth of the peer pool and a /16 half of it, so filling the pool takes several
// blocks. The floor of two default per-IP caps keeps small pools from refusing two busy hosts.
func subnetConnLimits(maxPeerCount uint64) (perBlock24, perBlock16 int) {
	const floor = 16
	perBlock24 = max(floor, int(maxPeerCount/8))
	perBlock16 = max(perBlock24, int(maxPeerCount/2))
	return perBlock24, perBlock16
}
