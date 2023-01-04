package cli

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/rawdb"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
)

type ConsensusClientCliCfg struct {
	GenesisCfg     *clparams.GenesisConfig     `json:"genesisCfg"`
	BeaconCfg      *clparams.BeaconChainConfig `json:"beaconCfg"`
	NetworkCfg     *clparams.NetworkConfig     `json:"networkCfg"`
	BeaconDataCfg  *rawdb.BeaconDataConfig     `json:"beaconDataConfig"`
	Port           uint                        `json:"port"`
	Addr           string                      `json:"address"`
	ServerAddr     string                      `json:"serverAddr"`
	ServerProtocol string                      `json:"serverProtocol"`
	ServerTcpPort  uint                        `json:"serverTcpPort"`
	LogLvl         uint                        `json:"logLevel"`
	NoDiscovery    bool                        `json:"noDiscovery"`
	CheckpointUri  string                      `json:"checkpointUri"`
	Chaindata      string                      `json:"chaindata"`
	ELEnabled      bool                        `json:"elEnabled"`
}

func SetupConsensusClientCfg(ctx *cli.Context) (*ConsensusClientCliCfg, error) {
	cfg := &ConsensusClientCliCfg{}
	chainName := ctx.String(flags.Chain.Name)
	var err error
	var network clparams.NetworkType
	cfg.GenesisCfg, cfg.NetworkCfg, cfg.BeaconCfg, network, err = clparams.GetConfigsByNetworkName(chainName)
	if err != nil {
		return nil, err
	}
	cfg.ServerAddr = fmt.Sprintf("%s:%d", ctx.String(flags.SentinelServerAddr.Name), ctx.Int(flags.SentinelServerPort.Name))
	cfg.ServerProtocol = "tcp"

	cfg.Port = uint(ctx.Int(flags.SentinelDiscoveryPort.Name))
	cfg.Addr = ctx.String(flags.SentinelDiscoveryAddr.Name)

	cfg.LogLvl = ctx.Uint(flags.Verbosity.Name)
	cfg.NoDiscovery = ctx.Bool(flags.NoDiscovery.Name)
	cfg.CheckpointUri = clparams.GetCheckpointSyncEndpoint(network)
	cfg.Chaindata = ctx.String(flags.ChaindataFlag.Name)
	cfg.ELEnabled = ctx.Bool(flags.ELEnabledFlag.Name)
	cfg.BeaconDataCfg = rawdb.BeaconDataConfigurations[ctx.String(flags.BeaconDBModeFlag.Name)]
	return cfg, nil
}
