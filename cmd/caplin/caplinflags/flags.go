package caplinflags

import (
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/urfave/cli/v2"
)

var CliFlags = []cli.Flag{
	&NoBeaconApi,
	&BeaconApiReadTimeout,
	&BeaconApiWriteTimeout,
	&BeaconApiPort,
	&BeaconApiAddr,
	&ChaindataFlag,
	&BeaconDBModeFlag,
	&CheckpointSyncUrlFlag,
	&TransitionChainFlag,
	&InitSyncFlag,
	&RecordModeDir,
	&RecordModeFlag,
	&RunEngineAPI,
	&EngineApiHostFlag,
	&EngineApiPortFlag,
	&JwtSecret,
	&utils.DataDirFlag,
	&utils.BeaconApiAllowCredentialsFlag,
	&utils.BeaconApiAllowMethodsFlag,
	&utils.BeaconApiAllowOriginsFlag,
}

var (
	ChaindataFlag = cli.StringFlag{
		Name:  "chaindata",
		Usage: "chaindata of database",
		Value: "",
	}
	NoBeaconApi = cli.BoolFlag{
		Name:  "no-beacon-api",
		Usage: "turn off the beacon api",
		Value: false,
	}
	BeaconApiReadTimeout = cli.Uint64Flag{
		Name:  "beacon.api.read.timeout",
		Usage: "Sets the seconds for a read time out in the beacon api",
		Value: 60,
	}
	BeaconApiWriteTimeout = cli.Uint64Flag{
		Name:  "beacon.api.write.timeout",
		Usage: "Sets the seconds for a write time out in the beacon api",
		Value: 60,
	}
	BeaconApiAddr = cli.StringFlag{
		Name:  "beacon.api.addr",
		Usage: "sets the host to listen for beacon api requests",
		Value: "localhost",
	}
	BeaconApiPort = cli.UintFlag{
		Name:  "beacon.api.port",
		Usage: "sets the port to listen for beacon api requests",
		Value: 5555,
	}
	BeaconDBModeFlag = cli.StringFlag{
		Name:  "beacon-db-mode",
		Usage: "level of storing on beacon chain, minimal(only 500k blocks stored), full (all blocks stored), light (no blocks stored)",
		Value: "full",
	}
	CheckpointSyncUrlFlag = cli.StringFlag{
		Name:  "checkpoint-sync-url",
		Usage: "checkpoint sync endpoint",
		Value: "",
	}
	TransitionChainFlag = cli.BoolFlag{
		Name:  "transition-chain",
		Usage: "enable chain transition",
	}
	InitSyncFlag = cli.BoolFlag{
		Value: false,
		Name:  "initial-sync",
		Usage: "use initial-sync",
	}
	RecordModeFlag = cli.BoolFlag{
		Value: false,
		Name:  "record-mode",
		Usage: "enable/disable record mode",
	}
	RecordModeDir = cli.StringFlag{
		Value: "caplin-recordings",
		Name:  "record-dir",
		Usage: "directory for states and block recordings",
	}
	ErigonPrivateApiFlag = cli.StringFlag{
		Name:  "private.api.addr",
		Usage: "connect to existing erigon instance",
		Value: "",
	}
	RunEngineAPI = cli.BoolFlag{
		Name:  "engine.api",
		Usage: "Turns on engine communication (Needed for none Erigon ELs)",
		Value: false,
	}
	EngineApiPortFlag = cli.UintFlag{
		Name:  "engine.api.port",
		Usage: "Sets engine API port",
		Value: 8551,
	}
	EngineApiHostFlag = cli.StringFlag{
		Name:  "engine.api.host",
		Usage: "Sets the engine API host",
		Value: "http://localhost",
	}
	JwtSecret = cli.StringFlag{
		Name:  "engine.api.jwtsecret",
		Usage: "Path to the token that ensures safe connection between CL and EL",
		Value: "",
	}
)
