package flags

import "github.com/urfave/cli/v2"

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
	Verbosity = cli.UintFlag{
		Name:  "verbosity",
		Usage: "specify lightclient verbosity level 0=silent, 1=err, 2=warn, 3=info, 4=debug, 5=details",
		Value: 3,
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
	SentinelStaticPeersFlag = cli.StringFlag{
		Name:  "sentinel.staticpeers",
		Usage: "connect to comma-separated Consensus static peers",
		Value: "",
	}
	TransitionChainFlag = cli.BoolFlag{
		Name:  "transition-chain",
		Usage: "enable chain transition",
	}
)
