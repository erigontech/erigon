package p2p

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/version"
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

	rm, err := newResourceManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p resource manager: %w", err)
	}
	options = append(options, libp2p.ResourceManager(rm))

	return options, nil
}

func newResourceManager(cfg *P2PConfig) (network.ResourceManager, error) {
	// Tighten per-peer inbound stream limits. The default PeerBaseLimit.StreamsInbound (256) is
	// far too permissive; Lighthouse caps per-peer inbound substreams at 32.
	limits := rcmgr.DefaultLimits
	limits.PeerBaseLimit.StreamsInbound = 32
	limits.PeerLimitIncrease.StreamsInbound = 0 // do not scale with memory

	trusted, err := ParseTrustedCIDRs(cfg.TrustedCIDRs)
	if err != nil {
		return nil, err
	}
	// Loopback is already unlimited; a narrower IPv4 trusted entry there would match first and cap it.
	trusted = slices.DeleteFunc(trusted, func(prefix netip.Prefix) bool { return prefix.Addr().IsLoopback() })
	var opts []rcmgr.Option
	var staticPeers []netip.Prefix
	if !cfg.DisableIPBlockLimits {
		opts = append(opts, rcmgr.WithLimitPerSubnet(ipBlockLimits(cfg.MaxPeerCount)))
		if cfg.LocalDiscovery {
			trusted = append(trusted, mustParsePrefixes(append([]string{"fc00::/7", "fe80::/10"}, privateCIDRList...)...)...)
		}
		if cfg.NetworkConfig != nil {
			staticPeers = staticPeerPrefixes(cfg.NetworkConfig.StaticPeers, trusted)
		}
	}
	if len(trusted) > 0 || len(staticPeers) > 0 {
		// A network prefix limit replaces the per-subnet caps for its addresses. Exempt networks
		// share one connection count each; a static peer's address keeps the released cap of 8.
		// WithNetworkPrefixLimit sorts the lists in place, so the defaults are cloned.
		v4, v6 := slices.Clone(rcmgr.DefaultNetworkPrefixLimitV4), slices.Clone(rcmgr.DefaultNetworkPrefixLimitV6)
		add := func(prefix netip.Prefix, connCount int) {
			limit := rcmgr.NetworkPrefixLimit{Network: prefix, ConnCount: connCount}
			if prefix.Addr().Is4() {
				v4 = append(v4, limit)
			} else {
				v6 = append(v6, limit)
			}
		}
		for _, prefix := range trusted {
			add(prefix, exemptNetworkConnLimit(cfg.MaxPeerCount))
		}
		for _, prefix := range staticPeers {
			add(prefix, 8)
		}
		opts = append(opts, rcmgr.WithNetworkPrefixLimit(v4, v6))
	}
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.AutoScale()), opts...)
	if err != nil {
		return nil, err
	}
	counter := newRefusalCounter(rm)
	go counter.logSummaries()
	return counter, nil
}

// exemptNetworkConnLimit is the one connection count that all addresses of an exempt network
// share. It is eight times the peer pool, so it never limits the pool, and it stays finite because
// go-libp2p uses half of it as the network's QUIC source-address verification burst.
func exemptNetworkConnLimit(maxPeerCount uint64) int {
	return max(32, 8*int(maxPeerCount))
}

// staticPeerPrefixes returns the per-address prefixes (IPv4 /32, IPv6 /56, as go-libp2p counts
// them) of the static peers outside loopback and the exempt networks: the most specific prefix
// matches first, so an entry inside one would lower that network's allowance. Peers given
// by DNS name are skipped; invalid ones are reported when the sentinel dials them.
func staticPeerPrefixes(staticPeers []string, exempt []netip.Prefix) []netip.Prefix {
	var out []netip.Prefix
	for _, staticPeer := range staticPeers {
		addrs, err := ParseStaticPeerAddrs(staticPeer)
		if err != nil {
			continue
		}
		for _, maddr := range addrs {
			ip, err := manet.ToIP(maddr)
			if err != nil {
				continue
			}
			addr, ok := netip.AddrFromSlice(ip)
			if !ok {
				continue
			}
			addr = addr.Unmap()
			if addr.IsLoopback() {
				continue
			}
			bits := 32
			if addr.Is6() {
				bits = 56
			}
			prefix := netip.PrefixFrom(addr, bits).Masked()
			covered := slices.ContainsFunc(exempt, func(p netip.Prefix) bool { return p.Contains(addr) })
			if !covered && !slices.Contains(out, prefix) {
				out = append(out, prefix)
			}
		}
	}
	return out
}

// ParseTrustedCIDRs parses --caplin.ip-block-limits.trusted-cidrs. Empty entries are skipped.
// Networks with host bits set or broader than an IPv4 /8 or an IPv6 /16 are rejected: a typo there
// would drop the per-address caps for a large part of the internet.
func ParseTrustedCIDRs(cidrs []string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, cidr := range cidrs {
		if strings.TrimSpace(cidr) == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(strings.TrimSpace(cidr))
		if err != nil {
			return nil, fmt.Errorf("invalid trusted CIDR %q: %w", cidr, err)
		}
		if prefix != prefix.Masked() {
			return nil, fmt.Errorf("trusted CIDR %q has host bits set; did you mean %s?", cidr, prefix.Masked())
		}
		if addr := prefix.Addr(); addr.Is4In6() && prefix.Bits() >= 96 {
			prefix = netip.PrefixFrom(addr.Unmap(), prefix.Bits()-96)
		}
		if tooBroad := prefix.Addr().Is4() && prefix.Bits() < 8 || prefix.Addr().Is6() && prefix.Bits() < 16; tooBroad {
			return nil, fmt.Errorf("trusted CIDR %q is too broad: use at most an IPv4 /8 or an IPv6 /16", cidr)
		}
		out = append(out, prefix)
	}
	return out, nil
}

func mustParsePrefixes(cidrs ...string) []netip.Prefix {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		out = append(out, netip.MustParsePrefix(c))
	}
	return out
}

// ipBlockConnLimits returns the connection caps per IPv4 /24 and per IPv4 /16. A /24 may hold an
// eighth of the peer pool and a /16 half of it, so filling the pool takes several blocks. The
// floor of two per-address caps keeps small pools from refusing two busy hosts.
func ipBlockConnLimits(maxPeerCount uint64) (perBlock24, perBlock16 int) {
	const floor = 16
	perBlock24 = max(floor, int(maxPeerCount/8))
	perBlock16 = max(perBlock24, int(maxPeerCount/2))
	return perBlock24, perBlock16
}

// ipBlockLimits returns the per-subnet caps for IPv4 and IPv6. They replace go-libp2p's defaults,
// so they repeat its caps per IPv4 address and IPv6 /56 and keep its IPv6 /48 cap as an upper bound.
// Broadest first: go-libp2p stores a zero count for every prefix it checks before the one that
// refuses, and only a closed connection removes it again.
func ipBlockLimits(maxPeerCount uint64) (v4, v6 []rcmgr.ConnLimitPerSubnet) {
	const perAddress, libp2pPerSlash48 = 8, 64
	perBlock24, perBlock16 := ipBlockConnLimits(maxPeerCount)
	v4 = []rcmgr.ConnLimitPerSubnet{{PrefixLength: 16, ConnCount: perBlock16}, {PrefixLength: 24, ConnCount: perBlock24}, {PrefixLength: 32, ConnCount: perAddress}}
	v6 = []rcmgr.ConnLimitPerSubnet{{PrefixLength: 32, ConnCount: perBlock16}, {PrefixLength: 48, ConnCount: min(perBlock24, libp2pPerSlash48)}, {PrefixLength: 56, ConnCount: perAddress}}
	return v4, v6
}

// refusalCounter counts connections refused by the per-address and per-block caps. go-libp2p
// refuses them before its metrics count anything and logs them only at Debug.
type refusalCounter struct {
	network.ResourceManager
	mu       sync.Mutex
	counts   map[netip.Prefix]int
	stop     chan struct{}
	stopOnce sync.Once
}

func newRefusalCounter(rm network.ResourceManager) *refusalCounter {
	return &refusalCounter{ResourceManager: rm, counts: map[netip.Prefix]int{}, stop: make(chan struct{})}
}

func (c *refusalCounter) OpenConnection(dir network.Direction, usefd bool, endpoint multiaddr.Multiaddr) (network.ConnManagementScope, error) {
	scope, err := c.ResourceManager.OpenConnection(dir, usefd, endpoint)
	if err != nil && strings.Contains(err.Error(), "connections per ip limit exceeded") {
		if raw, ipErr := manet.ToIP(endpoint); ipErr == nil {
			if ip, ok := netip.AddrFromSlice(raw); ok {
				ip = ip.Unmap()
				bits := 24
				if ip.Is6() {
					bits = 48
				}
				ipBlock := netip.PrefixFrom(ip, bits).Masked()
				c.mu.Lock()
				c.counts[ipBlock]++
				c.mu.Unlock()
			}
		}
	}
	return scope, err
}

// flush returns the number of refusals since the last call and the most refused blocks.
func (c *refusalCounter) flush() (int, string) {
	c.mu.Lock()
	counts := c.counts
	c.counts = map[netip.Prefix]int{}
	c.mu.Unlock()
	total := 0
	blocks := make([]netip.Prefix, 0, len(counts))
	for ipBlock, n := range counts {
		total += n
		blocks = append(blocks, ipBlock)
	}
	slices.SortFunc(blocks, func(a, b netip.Prefix) int {
		if counts[a] != counts[b] {
			return counts[b] - counts[a]
		}
		return a.Addr().Compare(b.Addr())
	})
	mostRefusedBlocks := make([]string, 0, min(5, len(blocks)))
	for _, ipBlock := range blocks[:min(5, len(blocks))] {
		mostRefusedBlocks = append(mostRefusedBlocks, fmt.Sprintf("%s: %d", ipBlock, counts[ipBlock]))
	}
	return total, strings.Join(mostRefusedBlocks, ", ")
}

func (c *refusalCounter) logSummaries() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			if total, mostRefusedBlocks := c.flush(); total > 0 {
				log.Info("[p2p] Connections refused in the last minute by the per-address and per-IP-block limits; see --caplin.ip-block-limits.*", "refused", total, "mostRefusedBlocks", mostRefusedBlocks)
			}
		}
	}
}

func (c *refusalCounter) Close() error {
	c.stopOnce.Do(func() { close(c.stop) })
	return c.ResourceManager.Close()
}
