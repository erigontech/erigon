package flags

import "github.com/urfave/cli/v2"

var CLDefaultFlags = []cli.Flag{
	&SentinelDiscoveryPort,
	&SentinelDiscoveryAddr,
	&SentinelServerPort,
	&SentinelServerAddr,
	&Chain,
	&Verbosity,
	&SentinelTcpPort,
	&NoDiscovery,
	&ChaindataFlag,
	&BeaconDBModeFlag,
	&BootnodesFlag,
	&BeaconConfigFlag,
	&GenesisSSZFlag,
	&CheckpointSyncUrlFlag,
	&SentinelStaticPeersFlag,
	&TransitionChainFlag,
}

var LCDefaultFlags = []cli.Flag{
	&SentinelDiscoveryPort,
	&SentinelDiscoveryAddr,
	&SentinelServerPort,
	&SentinelServerAddr,
	&Chain,
	&Verbosity,
	&SentinelTcpPort,
	&NoDiscovery,
	&ChaindataFlag,
	&BeaconDBModeFlag,
	&BootnodesFlag,
	&BeaconConfigFlag,
	&GenesisSSZFlag,
	&CheckpointSyncUrlFlag,
	&SentinelStaticPeersFlag,
	&ErigonPrivateApiFlag,
}
