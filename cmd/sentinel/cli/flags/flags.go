package flags

import "github.com/urfave/cli/v2"

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
	Verbosity = cli.UintFlag{
		Name:  "verbosity",
		Usage: "specify lightclient verbosity level 0=silent, 1=err, 2=warn, 3=info, 4=debug, 5=details",
		Value: 3,
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
	Chain = cli.StringFlag{
		Name:  "chain",
		Usage: "sets the chain specs for the lightclient",
		Value: "mainnet",
	}
	NoDiscovery = cli.BoolFlag{
		Name:  "no-discovery",
		Usage: "turn off or on the lightclient finding peers",
		Value: false,
	}
	ChaindataFlag = cli.StringFlag{
		Name:  "chaindata",
		Usage: "chaindata of database",
		Value: "",
	}
)
