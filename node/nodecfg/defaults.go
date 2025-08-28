// Copyright 2016 The go-ethereum Authors
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

package nodecfg

import (
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/nat"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

const (
	DefaultHTTPHost    = "localhost" // Default host interface for the HTTP RPC server
	DefaultHTTPPort    = 8545        // Default TCP port for the HTTP RPC server
	DefaultAuthRpcPort = 8551        // Default TCP port for the Engine API HTTP RPC server
	DefaultWSHost      = "localhost" // Default host interface for the websocket RPC server
	DefaultWSPort      = 8546        // Default TCP port for the websocket RPC server
	DefaultGRPCHost    = "localhost" // Default host interface for the GRPC server
	DefaultGRPCPort    = 8547        // Default TCP port for the GRPC server
	DefaultTCPHost     = "localhost" // default host interface for TCP RPC server
	DefaultTCPPort     = 8548        // default TCP port for TCP RPC server
)

// DefaultConfig contains reasonable default settings.
var DefaultConfig = Config{
	HTTPPort:         DefaultHTTPPort,
	HTTPModules:      []string{"net", "web3"},
	HTTPVirtualHosts: []string{"localhost"},
	HTTPTimeouts:     rpccfg.DefaultHTTPTimeouts,
	WSPort:           DefaultWSPort,
	WSModules:        []string{"net", "web3"},
	P2P: p2p.Config{
		ListenAddr:      ":30303",
		ProtocolVersion: []uint{direct.ETH68, direct.ETH67},
		MaxPeers:        32,
		MaxPendingPeers: 1000,
		NAT:             nat.Any(),
	},
}
