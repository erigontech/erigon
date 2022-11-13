package flags

import "github.com/urfave/cli"

var ConsensusLayerDefaultFlags = []cli.Flag{
	ConsensusLayerPort,
	ConsensusLayerAddr,
	ConsensusLayerTcpPort,
	ConsensusLayerVerbosity,
	ConsensusLayerChain,
	ConsensusLayerServerAddr,
	ConsensusLayerServerPort,
	ConsensusLayerServerProtocol,
	ConsensusLayerDiscovery,
}
