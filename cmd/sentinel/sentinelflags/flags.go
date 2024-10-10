package sentinelflags

import (
	"github.com/ledgerwatch/erigon/cmd/utils"
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
	&BeaconConfigFlag,
	&GenesisSSZFlag,
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
	BeaconConfigFlag = cli.StringFlag{
		Name:  "beacon-config",
		Usage: "Path to beacon config",
		Value: "",
	}
	GenesisSSZFlag = cli.StringFlag{
		Name:  "genesis-ssz",
		Usage: "Path to genesis ssz",
		Value: "",
	}
	SentinelStaticPeersFlag = cli.StringFlag{
		Name:  "sentinel.staticpeers",
		Usage: "connect to comma-separated Consensus static peers",
		Value: "",
	}
)
