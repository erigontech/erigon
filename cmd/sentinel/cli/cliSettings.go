package cli

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
	"github.com/ledgerwatch/erigon/turbo/logging"

	"github.com/ledgerwatch/log/v3"
)

type ConsensusClientCliCfg struct {
	GenesisCfg       *clparams.GenesisConfig     `json:"genesisCfg"`
	BeaconCfg        *clparams.BeaconChainConfig `json:"beaconCfg"`
	NetworkCfg       *clparams.NetworkConfig     `json:"networkCfg"`
	BeaconDataCfg    *rawdb.BeaconDataConfig     `json:"beaconDataConfig"`
	Port             uint                        `json:"port"`
	Addr             string                      `json:"address"`
	ServerAddr       string                      `json:"serverAddr"`
	ServerProtocol   string                      `json:"serverProtocol"`
	ServerTcpPort    uint                        `json:"serverTcpPort"`
	LogLvl           uint                        `json:"logLevel"`
	NoDiscovery      bool                        `json:"noDiscovery"`
	CheckpointUri    string                      `json:"checkpointUri"`
	Chaindata        string                      `json:"chaindata"`
	ErigonPrivateApi string                      `json:"erigonPrivateApi"`
	TransitionChain  bool                        `json:"transitionChain"`
	NetworkType      clparams.NetworkType
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
		// Now parse genesis time and genesis fork
		if *cfg.GenesisCfg, err = clparams.ParseGenesisSSZToGenesisConfig(ctx.String(flags.GenesisSSZFlag.Name)); err != nil {
			return nil, err
		}
	}
	cfg.ServerAddr = fmt.Sprintf("%s:%d", ctx.String(flags.SentinelServerAddr.Name), ctx.Int(flags.SentinelServerPort.Name))
	cfg.ServerProtocol = "tcp"

	cfg.Port = uint(ctx.Int(flags.SentinelDiscoveryPort.Name))
	cfg.Addr = ctx.String(flags.SentinelDiscoveryAddr.Name)

	cfg.LogLvl = ctx.Uint(logging.LogVerbosityFlag.Name)
	fmt.Println(cfg.LogLvl)
	if cfg.LogLvl == uint(log.LvlInfo) || cfg.LogLvl == 0 {
		cfg.LogLvl = uint(log.LvlDebug)
	}
	cfg.NoDiscovery = ctx.Bool(flags.NoDiscovery.Name)
	if ctx.String(flags.CheckpointSyncUrlFlag.Name) != "" {
		cfg.CheckpointUri = ctx.String(flags.CheckpointSyncUrlFlag.Name)
	} else {
		cfg.CheckpointUri = clparams.GetCheckpointSyncEndpoint(cfg.NetworkType)
		fmt.Println(cfg.CheckpointUri)
	}
	cfg.Chaindata = ctx.String(flags.ChaindataFlag.Name)
	cfg.BeaconDataCfg = rawdb.BeaconDataConfigurations[ctx.String(flags.BeaconDBModeFlag.Name)]
	// Process bootnodes
	if ctx.String(flags.BootnodesFlag.Name) != "" {
		cfg.NetworkCfg.BootNodes = utils.SplitAndTrim(ctx.String(flags.BootnodesFlag.Name))
	}
	if ctx.String(flags.SentinelStaticPeersFlag.Name) != "" {
		cfg.NetworkCfg.StaticPeers = utils.SplitAndTrim(ctx.String(flags.SentinelStaticPeersFlag.Name))
	}
	cfg.TransitionChain = ctx.Bool(flags.TransitionChainFlag.Name)
	return cfg, nil
}
