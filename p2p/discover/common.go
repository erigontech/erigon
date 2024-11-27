// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package discover

import (
	"context"
	"crypto/ecdsa"
	"net"
	"time"

	"github.com/erigontech/erigon-lib/common/mclock"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	"github.com/erigontech/erigon/p2p/netutil"
)

// UDPConn is a network connection on which discovery can operate.
type UDPConn interface {
	ReadFromUDP(b []byte) (n int, addr *net.UDPAddr, err error)
	WriteToUDP(b []byte, addr *net.UDPAddr) (n int, err error)
	Close() error
	LocalAddr() net.Addr
}

// Config holds settings for the discovery listener.
type Config struct {
	// These settings are required and configure the UDP listener:
	PrivateKey *ecdsa.PrivateKey

	// These settings are optional:
	NetRestrict  *netutil.Netlist   // network whitelist
	Bootnodes    []*enode.Node      // list of bootstrap nodes
	Unhandled    chan<- ReadPacket  // unhandled packets are sent on this channel
	Log          log.Logger         // if set, log messages go here
	ValidSchemes enr.IdentityScheme // allowed identity schemes
	Clock        mclock.Clock
	ReplyTimeout time.Duration

	PingBackDelay time.Duration

	PrivateKeyGenerator func() (*ecdsa.PrivateKey, error)

	TableRevalidateInterval time.Duration
}

func (cfg Config) withDefaults(defaultReplyTimeout time.Duration) Config {
	if cfg.Log == nil {
		cfg.Log = log.Root()
	}
	if cfg.ValidSchemes == nil {
		cfg.ValidSchemes = enode.ValidSchemes
	}
	if cfg.Clock == nil {
		cfg.Clock = mclock.System{}
	}
	if cfg.ReplyTimeout == 0 {
		cfg.ReplyTimeout = defaultReplyTimeout
	}
	if cfg.PingBackDelay == 0 {
		cfg.PingBackDelay = respTimeout
	}
	if cfg.PrivateKeyGenerator == nil {
		cfg.PrivateKeyGenerator = crypto.GenerateKey
	}
	if cfg.TableRevalidateInterval == 0 {
		cfg.TableRevalidateInterval = revalidateInterval
	}
	return cfg
}

// ListenUDP starts listening for discovery packets on the given UDP socket.
func ListenUDP(ctx context.Context, protocol string, c UDPConn, ln *enode.LocalNode, cfg Config) (*UDPv4, error) {
	return ListenV4(ctx, protocol, c, ln, cfg)
}

// ReadPacket is a packet that couldn't be handled. Those packets are sent to the unhandled
// channel if configured.
type ReadPacket struct {
	Data []byte
	Addr *net.UDPAddr
}

type UnhandledPacket struct {
	ReadPacket
	Reason error
}
