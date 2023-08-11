package flags

import (
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/urfave/cli/v2"
)

var CLDefaultFlags = []cli.Flag{
	&SentinelDiscoveryPort,
	&SentinelDiscoveryAddr,
	&SentinelServerPort,
	&SentinelServerAddr,
	&NoBeaconApi,
	&BeaconApiReadTimeout,
	&BeaconApiWriteTimeout,
	&BeaconApiPort,
	&BeaconApiAddr,
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
	&RecordModeDir,
	&RecordModeFlag,
	&RunEngineAPI,
	&EngineApiHostFlag,
	&EngineApiPortFlag,
	&JwtSecret,
	&utils.DataDirFlag,
}
