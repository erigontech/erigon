package flags

import "github.com/urfave/cli/v2"

var (
	LightClientPort = cli.IntFlag{
		Name:  "lc.port",
		Usage: "sets the lightclient port",
		Value: 8080,
	}
	LightClientAddr = cli.StringFlag{
		Name:  "lc.addr",
		Usage: "sets the lightclient host addr",
		Value: "127.0.0.1",
	}
	LightClientTcpPort = cli.UintFlag{
		Name:  "lc.tcp.port",
		Usage: "sets lightclient tcp port",
		Value: 9000,
	}
	LightClientVerbosity = cli.UintFlag{
		Name:  "lc.verbosity",
		Usage: "specify lightclient verbosity level 0=silent, 1=err, 2=warn, 3=info, 4=debug, 5=details",
		Value: 3,
	}
	LightClientServerPort = cli.IntFlag{
		Name:  "lc.server.port",
		Usage: "sets the lightclient server port",
		Value: 7777,
	}
	LightClientServerAddr = cli.StringFlag{
		Name:  "lc.server.addr",
		Usage: "sets the lightclient server host addr",
		Value: "localhost",
	}
	LightClientServerProtocol = cli.UintFlag{
		Name:  "lc.server.protocol",
		Usage: "sets the lightclient server protocol 1=tcp 2=udp",
		Value: 1,
	}
	LightClientChain = cli.StringFlag{
		Name:  "lc.chain",
		Usage: "sets the chain specs for the lightclient",
		Value: "mainnet",
	}
	LightClientDiscovery = cli.BoolFlag{
		Name:  "lc.discover",
		Usage: "turn off or on the lightclient finding peers",
		Value: true,
	}
	ChaindataFlag = cli.StringFlag{
		Name:  "chaindata",
		Usage: "chaindata of database",
		Value: "",
	}
)
