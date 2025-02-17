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
	"crypto/ecdsa"
	"fmt"
	"net"

	"github.com/c2h5oh/datasize"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"

	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
)

type SentinelConfig struct {
	NetworkConfig *clparams.NetworkConfig
	BeaconConfig  *clparams.BeaconChainConfig
	IpAddr        string
	Port          int
	TCPPort       uint

	MaxInboundTrafficPerPeer     datasize.ByteSize
	MaxOutboundTrafficPerPeer    datasize.ByteSize
	AdaptableTrafficRequirements bool
	// Optional
	LocalIP        string
	EnableUPnP     bool
	RelayNodeAddr  string
	HostAddress    string
	HostDNS        string
	NoDiscovery    bool
	TmpDir         string
	LocalDiscovery bool

	EnableBlocks       bool
	SubscribeAllTopics bool // Capture all topics
	ActiveIndicies     uint64
	MaxPeerCount       uint64
}

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

func buildOptions(cfg *SentinelConfig, s *Sentinel) ([]libp2p.Option, error) {
	var priKey = s.discoverConfig.PrivateKey

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
		privKeyOption(priKey),
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
