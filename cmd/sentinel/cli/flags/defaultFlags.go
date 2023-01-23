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
	&ELEnabledFlag,
	&BeaconDBModeFlag,
	&BootnodesFlag,
	&BeaconConfigFlag,
	&GenesisSSZFlag,
	&CheckpointSyncUrlFlag,
}
