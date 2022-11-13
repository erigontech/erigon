package flags

import "github.com/urfave/cli"

var (
	ConsensusLayerPort = cli.IntFlag{
		Name:  "cl.port",
		Usage: "sets the consensus layer port",
		Value: 8080,
	}
	ConsensusLayerAddr = cli.StringFlag{
		Name:  "cl.addr",
		Usage: "sets the consensus layer host addr",
		Value: "127.0.0.1",
	}
	ConsensusLayerTcpPort = cli.UintFlag{
		Name:  "cl.tcp.port",
		Usage: "sets consensus layer tcp port",
		Value: 9000,
	}
	ConsensusLayerVerbosity = cli.UintFlag{
		Name:  "cl.verbosity",
		Usage: "specify consensus layer verbosity level 0=silent, 1=err, 2=warn, 3=info, 4=debug, 5=details",
		Value: 3,
	}
	ConsensusLayerServerPort = cli.IntFlag{
		Name:  "cl.server.port",
		Usage: "sets the consensus layer server port",
		Value: 7777,
	}
	ConsensusLayerServerAddr = cli.StringFlag{
		Name:  "cl.server.addr",
		Usage: "sets the consensus layer server host addr",
		Value: "localhost",
	}
	ConsensusLayerServerProtocol = cli.UintFlag{
		Name:  "cl.server.protocol",
		Usage: "sets the consensus layer server protocol 1=tcp 2=udp",
		Value: 1,
	}
	ConsensusLayerChain = cli.StringFlag{
		Name:  "cl.chain",
		Usage: "sets the chain specs for the consensus layer",
		Value: "mainnet",
	}
	ConsensusLayerDiscovery = cli.BoolTFlag{
		Name:  "cl.discover",
		Usage: "turn off or on the consensus layer finding peers",
	}
)
