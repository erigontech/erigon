package flags

import (
	"github.com/urfave/cli/v2"
)

var (
	SentinelDiscoveryPort = cli.IntFlag{
		Name:  "discovery.port",
		Usage: "sets the lightclient port",
		Value: 4000,
	}
	SentinelDiscoveryAddr = cli.StringFlag{
		Name:  "discovery.addr",
		Usage: "sets the lightclient discovery addr",
		Value: "127.0.0.1",
	}
	SentinelTcpPort = cli.UintFlag{
		Name:  "sentinel.tcp.port",
		Usage: "sets lightclient tcp port",
		Value: 4001,
	}
	SentinelServerPort = cli.IntFlag{
		Name:  "sentinel.port",
		Usage: "sets the lightclient server port",
		Value: 7777,
	}
	SentinelServerAddr = cli.StringFlag{
		Name:  "sentinel.addr",
		Usage: "sets the lightclient server host addr",
		Value: "localhost",
	}
	NoBeaconApi = cli.BoolFlag{
		Name:  "no-beacon-api",
		Usage: "turn off the beacon api",
		Value: false,
	}
	BeaconApiReadTimeout = cli.Uint64Flag{
		Name:  "beacon.api.read.timeout",
		Usage: "Sets the seconds for a read time out in the beacon api",
		Value: 5,
	}
	BeaconApiWriteTimeout = cli.Uint64Flag{
		Name:  "beacon.api.write.timeout",
		Usage: "Sets the seconds for a write time out in the beacon api",
		Value: 5,
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
	BootnodesFlag = cli.StringFlag{
		Name:  "sentinel.bootnodes",
		Usage: "Comma separated enode URLs for P2P discovery bootstrap",
		Value: "",
	}
	BeaconConfigFlag = cli.StringFlag{
		Name:  "beacon-config",
		Usage: "Path to beacon config",
		Value: "",
	}
	GenesisSSZFlag = cli.StringFlag{
		Name:  "genesis-ssz",
		Usage: "Path to genesis ssz",
		Value: "",
	}
	Chain = cli.StringFlag{
		Name:  "chain",
		Usage: "sets the chain specs for the lightclient",
		Value: "mainnet",
	}
	NoDiscovery = cli.BoolFlag{
		Name:  "no-discovery",
		Usage: "turn off or on the lightclient finding peers",
		Value: false,
	}
	LocalDiscovery = cli.BoolFlag{
		Name:  "local-discovery",
		Usage: "enable to also attempt to find peers over private ips. turning this on may cause issues with hosts such as hetzner",
		Value: false,
	}
	ChaindataFlag = cli.StringFlag{
		Name:  "chaindata",
		Usage: "chaindata of database",
		Value: "",
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
	SentinelStaticPeersFlag = cli.StringFlag{
		Name:  "sentinel.staticpeers",
		Usage: "connect to comma-separated Consensus static peers",
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
)
