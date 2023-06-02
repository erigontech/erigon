package flags

import "github.com/urfave/cli/v2"

var CLDefaultFlags = []cli.Flag{
	&SentinelDiscoveryPort,
	&SentinelDiscoveryAddr,
	&SentinelServerPort,
	&SentinelServerAddr,
	&Chain,
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
	&InitSyncFlag,
}
