package flags

import "github.com/urfave/cli"

var LightClientDefaultFlags = []cli.Flag{
	LightClientPort,
	LightClientAddr,
	LightClientTcpPort,
	LightClientVerbosity,
	LightClientChain,
	LightClientServerAddr,
	LightClientServerPort,
	LightClientServerProtocol,
	LightClientDiscovery,
}
