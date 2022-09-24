package sentinel

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"strings"

	"github.com/ledgerwatch/erigon/p2p/discover"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p/config"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

type SentinelConfig struct {
	DiscoverConfig discover.Config
	IpAddr         string
	Port           int
	TCPPort        uint
	// Optional
	LocalIP       string
	EnableUPnP    bool
	RelayNodeAddr string
	HostAddress   string
	HostDNS       string
}

func convertToInterfacePrivkey(privkey *ecdsa.PrivateKey) (crypto.PrivKey, error) {
	privBytes := privkey.D.Bytes()
	// In the event the number of bytes outputted by the big-int are less than 32,
	// we append bytes to the start of the sequence for the missing most significant
	// bytes.
	if len(privBytes) < 32 {
		privBytes = append(make([]byte, 32-len(privBytes)), privBytes...)
	}
	return crypto.UnmarshalSecp256k1PrivateKey(privBytes)
}

// Adds a private key to the libp2p option if the option was provided.
// If the private key file is missing or cannot be read, or if the
// private key contents cannot be marshaled, an exception is thrown.
func privKeyOption(privkey *ecdsa.PrivateKey) libp2p.Option {
	return func(cfg *libp2p.Config) error {
		ifaceKey, err := convertToInterfacePrivkey(privkey)
		if err != nil {
			return err
		}
		log.Debug("ECDSA private key generated")
		return cfg.Apply(libp2p.Identity(ifaceKey))
	}
}

func withRelayAddrs(relay string) config.AddrsFactory {
	return func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
		if relay == "" {
			return addrs
		}

		var relayAddrs []multiaddr.Multiaddr

		for _, a := range addrs {
			if strings.Contains(a.String(), "/p2p-circuit") {
				continue
			}
			relayAddr, err := multiaddr.NewMultiaddr(relay + "/p2p-circuit" + a.String())
			if err != nil {
				log.Debug("Failed to create multiaddress for relay node")
			} else {
				relayAddrs = append(relayAddrs, relayAddr)
			}
		}

		if len(relayAddrs) == 0 {
			log.Warn("Addresses via relay node are zero - using non-relay addresses")
			return addrs
		}
		return append(addrs, relayAddrs...)
	}
}

// multiAddressBuilder takes in an ip address string and port to produce a go multiaddr format.
func multiAddressBuilder(ipAddr string, port uint) (multiaddr.Multiaddr, error) {
	parsedIP := net.ParseIP(ipAddr)
	if parsedIP.To4() == nil && parsedIP.To16() == nil {
		return nil, errors.Errorf("invalid ip address provided: %s", ipAddr)
	}
	if parsedIP.To4() != nil {
		return multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ipAddr, port))
	}
	return multiaddr.NewMultiaddr(fmt.Sprintf("/ip6/%s/tcp/%d", ipAddr, port))
}

// buildOptions for the libp2p host.
func buildOptions(cfg SentinelConfig, s *Sentinel) ([]libp2p.Option, error) {
	var (
		//ip     = net.IP(cfg.IpAddr)
		priKey = cfg.DiscoverConfig.PrivateKey
	)

	listen, err := multiAddressBuilder(cfg.IpAddr, cfg.TCPPort)
	if err != nil {
		return nil, err
	}
	if cfg.LocalIP != "" {
		if net.ParseIP(cfg.LocalIP) == nil {
			return nil, fmt.Errorf("Invalid local ip provided: %s", cfg.LocalIP)
		}
		listen, err = multiAddressBuilder(cfg.LocalIP, cfg.TCPPort)
		if err != nil {
			return nil, err
		}
	}

	options := []libp2p.Option{
		privKeyOption(priKey),
		libp2p.ListenAddrs(listen),
		libp2p.UserAgent("erigon/lightclient"),
		libp2p.ConnectionGater(s),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
		libp2p.DefaultMuxers,
	}

	options = append(options, libp2p.Security(noise.ID, noise.New))

	if cfg.EnableUPnP {
		options = append(options, libp2p.NATPortMap()) // Allow to use UPnP
	}
	if cfg.RelayNodeAddr != "" {
		options = append(options, libp2p.AddrsFactory(withRelayAddrs(cfg.RelayNodeAddr)))
	} else {
		// Disable relay if it has not been set.
		options = append(options, libp2p.DisableRelay())
	}
	if cfg.HostAddress != "" {
		options = append(options, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			external, err := multiAddressBuilder(cfg.HostAddress, cfg.TCPPort)
			if err != nil {
				return nil
			} else {
				addrs = append(addrs, external)
			}
			return addrs
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
	return options, nil
}
