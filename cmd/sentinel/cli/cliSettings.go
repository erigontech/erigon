package cli

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/sentinel/cli/flags"
)

type LightClientCliCfg struct {
	GenesisCfg     *clparams.GenesisConfig     `json:"genesisCfg"`
	BeaconCfg      *clparams.BeaconChainConfig `json:"beaconCfg"`
	NetworkCfg     *clparams.NetworkConfig     `json:"networkCfg"`
	Port           uint                        `json:"port"`
	Addr           string                      `json:"address"`
	ServerAddr     string                      `json:"serverAddr"`
	ServerProtocol string                      `json:"serverProtocol"`
	ServerTcpPort  uint                        `json:"serverTcpPort"`
	LogLvl         uint                        `json:"logLevel"`
	NoDiscovery    bool                        `json:"noDiscovery"`
	CheckpointUri  string                      `json:"checkpointUri"`
	Chaindata      string                      `json:"chaindata"`
}

func SetUpLightClientCfg(ctx *cli.Context) (*LightClientCliCfg, error) {
	cfg := &LightClientCliCfg{}
	chainName := ctx.String(flags.LightClientChain.Name)
	var err error
	var network clparams.NetworkType
	cfg.GenesisCfg, cfg.NetworkCfg, cfg.BeaconCfg, network, err = clparams.GetConfigsByNetworkName(chainName)
	if err != nil {
		return nil, err
	}
	cfg.ServerAddr = fmt.Sprintf("%s:%d", ctx.String(flags.LightClientServerAddr.Name), ctx.Int(flags.LightClientServerPort.Name))
	cfg.ServerProtocol = ServerProtocolFromInt(ctx.Uint(flags.LightClientServerProtocol.Name))

	cfg.Port = uint(ctx.Int(flags.LightClientPort.Name))
	cfg.Addr = ctx.String(flags.LightClientAddr.Name)

	cfg.LogLvl = ctx.Uint(flags.LightClientVerbosity.Name)
	cfg.NoDiscovery = !ctx.Bool(flags.LightClientDiscovery.Name)
	cfg.CheckpointUri = clparams.GetCheckpointSyncEndpoint(network)
	cfg.Chaindata = ctx.String(flags.ChaindataFlag.Name)
	return cfg, nil
}

func ServerProtocolFromInt(n uint) string {
	switch n {
	case 1:
		return "tcp"
	case 2:
		return "udp"
	default:
		return "tcp"
	}
}
