// Copyright 2024 The Erigon Authors
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

package sentinelflags

import (
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/urfave/cli/v2"
)

var CliFlags = []cli.Flag{
	&utils.ChainFlag,

	&SentinelDiscoveryPort,
	&SentinelDiscoveryAddr,
	&SentinelServerPort,
	&SentinelServerAddr,
	&SentinelTcpPort,
	&NoDiscovery,
	&BootnodesFlag,
	&SentinelStaticPeersFlag,
}

var (
	SentinelDiscoveryPort = cli.IntFlag{
		Name:  "discovery.port",
		Usage: "sets the lightclient port",
		Value: 4000,
	}
	SentinelDiscoveryAddr = cli.StringFlag{
		Name:  "discovery.addr",
		Usage: "sets the lightclient discovery addr",
		Value: "127.0.0.1",
	}
	SentinelTcpPort = cli.UintFlag{
		Name:  "sentinel.tcp.port",
		Usage: "sets lightclient tcp port",
		Value: 4001,
	}
	SentinelServerPort = cli.IntFlag{
		Name:  "sentinel.port",
		Usage: "sets the lightclient server port",
		Value: 7777,
	}
	SentinelServerAddr = cli.StringFlag{
		Name:  "sentinel.addr",
		Usage: "sets the lightclient server host addr",
		Value: "localhost",
	}
	NoDiscovery = cli.BoolFlag{
		Name:  "no-discovery",
		Usage: "turn off or on the lightclient finding peers",
		Value: false,
	}
	LocalDiscovery = cli.BoolFlag{
		Name:  "local-discovery",
		Usage: "enable to also attempt to find peers over private ips. turning this on may cause issues with hosts such as hetzner",
		Value: false,
	}
	BootnodesFlag = cli.StringFlag{
		Name:  "sentinel.bootnodes",
		Usage: "Comma separated enode URLs for P2P discovery bootstrap",
		Value: "",
	}

	SentinelStaticPeersFlag = cli.StringFlag{
		Name:  "sentinel.staticpeers",
		Usage: "connect to comma-separated Consensus static peers",
		Value: "",
	}
)
