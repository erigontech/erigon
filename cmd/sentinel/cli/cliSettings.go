package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon/cl/phase1/core/rawdb"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/common"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
	"github.com/ledgerwatch/erigon/turbo/logging"

	"github.com/ledgerwatch/log/v3"
)

type ConsensusClientCliCfg struct {
	GenesisCfg            *clparams.GenesisConfig
	BeaconCfg             *clparams.BeaconChainConfig
	NetworkCfg            *clparams.NetworkConfig
	BeaconDataCfg         *rawdb.BeaconDataConfig
	Port                  uint   `json:"port"`
	Addr                  string `json:"address"`
	ServerAddr            string `json:"server_addr"`
	ServerProtocol        string `json:"server_protocol"`
	ServerTcpPort         uint   `json:"server_tcp_port"`
	LogLvl                uint   `json:"log_level"`
	NoDiscovery           bool   `json:"no_discovery"`
	LocalDiscovery        bool   `json:"local_discovery"`
	CheckpointUri         string `json:"checkpoint_uri"`
	Chaindata             string `json:"chaindata"`
	ErigonPrivateApi      string `json:"erigon_private_api"`
	TransitionChain       bool   `json:"transition_chain"`
	NetworkType           clparams.NetworkType
	InitialSync           bool          `json:"initial_sync"`
	NoBeaconApi           bool          `json:"no_beacon_api"`
	BeaconApiReadTimeout  time.Duration `json:"beacon_api_read_timeout"`
	BeaconApiWriteTimeout time.Duration `json:"beacon_api_write_timeout"`
	BeaconAddr            string        `json:"beacon_addr"`
	BeaconProtocol        string        `json:"beacon_protocol"`
	RecordMode            bool          `json:"record_mode"`
	RecordDir             string        `json:"record_dir"`
	DataDir               string        `json:"data_dir"`
	RunEngineAPI          bool          `json:"run_engine_api"`
	EngineAPIAddr         string        `json:"engine_api_addr"`
	EngineAPIPort         int           `json:"engine_api_port"`
	JwtSecret             []byte

	InitalState *state.CachingBeaconState
	Dirs        datadir.Dirs
}

func SetupConsensusClientCfg(ctx *cli.Context) (*ConsensusClientCliCfg, error) {
	cfg := &ConsensusClientCliCfg{}
	chainName := ctx.String(flags.Chain.Name)
	var err error
	cfg.GenesisCfg, cfg.NetworkCfg, cfg.BeaconCfg, cfg.NetworkType, err = clparams.GetConfigsByNetworkName(chainName)
	if err != nil {
		return nil, err
	}
	cfg.ErigonPrivateApi = ctx.String(flags.ErigonPrivateApiFlag.Name)
	if ctx.String(flags.BeaconConfigFlag.Name) != "" {
		cfg.BeaconCfg = new(clparams.BeaconChainConfig)
		if *cfg.BeaconCfg, err = clparams.CustomConfig(ctx.String(flags.BeaconConfigFlag.Name)); err != nil {
			return nil, err
		}
		if ctx.String(flags.GenesisSSZFlag.Name) == "" {
			return nil, fmt.Errorf("no genesis file provided")
		}
		cfg.GenesisCfg = new(clparams.GenesisConfig)
		var stateByte []byte
		// Now parse genesis time and genesis fork
		if *cfg.GenesisCfg, stateByte, err = clparams.ParseGenesisSSZToGenesisConfig(
			ctx.String(flags.GenesisSSZFlag.Name),
			cfg.BeaconCfg.GetCurrentStateVersion(0)); err != nil {
			return nil, err
		}
		cfg.InitalState = state.New(cfg.BeaconCfg)
		if cfg.InitalState.DecodeSSZ(stateByte, int(cfg.BeaconCfg.GetCurrentStateVersion(0))); err != nil {
			return nil, err
		}
	}
	cfg.ServerAddr = fmt.Sprintf("%s:%d", ctx.String(flags.SentinelServerAddr.Name), ctx.Int(flags.SentinelServerPort.Name))
	cfg.ServerProtocol = "tcp"

	cfg.NoBeaconApi = ctx.Bool(flags.NoBeaconApi.Name)
	cfg.BeaconApiReadTimeout = time.Duration(ctx.Uint64(flags.BeaconApiReadTimeout.Name)) * time.Second
	cfg.BeaconApiWriteTimeout = time.Duration(ctx.Uint(flags.BeaconApiWriteTimeout.Name)) * time.Second
	cfg.BeaconAddr = fmt.Sprintf("%s:%d", ctx.String(flags.BeaconApiAddr.Name), ctx.Int(flags.BeaconApiPort.Name))
	cfg.BeaconProtocol = "tcp"
	cfg.RecordMode = ctx.Bool(flags.RecordModeFlag.Name)
	cfg.RecordDir = ctx.String(flags.RecordModeDir.Name)
	cfg.DataDir = ctx.String(utils.DataDirFlag.Name)
	cfg.Dirs = datadir.New(cfg.DataDir)

	cfg.RunEngineAPI = ctx.Bool(flags.RunEngineAPI.Name)
	cfg.EngineAPIAddr = ctx.String(flags.EngineApiHostFlag.Name)
	cfg.EngineAPIPort = ctx.Int(flags.EngineApiPortFlag.Name)
	if cfg.RunEngineAPI {
		secret, err := ObtainJwtSecret(ctx)
		if err != nil {
			log.Error("Failed to obtain jwt secret", "err", err)
			cfg.RunEngineAPI = false
		} else {
			cfg.JwtSecret = secret
		}
	}

	cfg.Port = uint(ctx.Int(flags.SentinelDiscoveryPort.Name))
	cfg.Addr = ctx.String(flags.SentinelDiscoveryAddr.Name)

	cfg.LogLvl = ctx.Uint(logging.LogVerbosityFlag.Name)
	if cfg.LogLvl == uint(log.LvlInfo) || cfg.LogLvl == 0 {
		cfg.LogLvl = uint(log.LvlDebug)
	}
	cfg.NoDiscovery = ctx.Bool(flags.NoDiscovery.Name)
	cfg.LocalDiscovery = ctx.Bool(flags.LocalDiscovery.Name)
	if ctx.String(flags.CheckpointSyncUrlFlag.Name) != "" {
		cfg.CheckpointUri = ctx.String(flags.CheckpointSyncUrlFlag.Name)
	} else {
		cfg.CheckpointUri = clparams.GetCheckpointSyncEndpoint(cfg.NetworkType)
	}
	cfg.Chaindata = ctx.String(flags.ChaindataFlag.Name)
	cfg.BeaconDataCfg = rawdb.BeaconDataConfigurations[ctx.String(flags.BeaconDBModeFlag.Name)]
	// Process bootnodes
	if ctx.String(flags.BootnodesFlag.Name) != "" {
		cfg.NetworkCfg.BootNodes = utils.SplitAndTrim(ctx.String(flags.BootnodesFlag.Name))
	}
	if ctx.String(flags.SentinelStaticPeersFlag.Name) != "" {
		cfg.NetworkCfg.StaticPeers = utils.SplitAndTrim(ctx.String(flags.SentinelStaticPeersFlag.Name))
		fmt.Println(cfg.NetworkCfg.StaticPeers)
	}
	cfg.TransitionChain = ctx.Bool(flags.TransitionChainFlag.Name)
	cfg.InitialSync = ctx.Bool(flags.InitSyncFlag.Name)
	return cfg, nil
}

func ObtainJwtSecret(ctx *cli.Context) ([]byte, error) {
	path := ctx.String(flags.JwtSecret.Name)
	if len(strings.TrimSpace(path)) == 0 {
		return nil, errors.New("Missing jwt secret path")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	jwtSecret := common.FromHex(strings.TrimSpace(string(data)))
	if len(jwtSecret) == 32 {
		return jwtSecret, nil
	}

	return nil, fmt.Errorf("Invalid JWT secret at %s, invalid size", path)
}
