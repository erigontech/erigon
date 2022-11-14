package flags

import "github.com/urfave/cli/v2"

var LightClientDefaultFlags = []cli.Flag{
	&LightClientPort,
	&LightClientAddr,
	&LightClientTcpPort,
	&LightClientVerbosity,
	&LightClientChain,
	&LightClientServerAddr,
	&LightClientServerPort,
	&LightClientServerProtocol,
	&LightClientDiscovery,
<<<<<<< HEAD
	&ChaindataFlag,
=======
>>>>>>> 878967894 (Upgrade urfave/cli to v2 (#6047))
}
