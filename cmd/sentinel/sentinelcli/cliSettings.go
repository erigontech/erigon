package sentinelcli

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinelflags"
	"github.com/ledgerwatch/erigon/cmd/utils"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/turbo/logging"

	"github.com/ledgerwatch/log/v3"
)

type SentinelCliCfg struct {
	GenesisCfg     *clparams.GenesisConfig
	BeaconCfg      *clparams.BeaconChainConfig
	NetworkCfg     *clparams.NetworkConfig
	NetworkType    clparams.NetworkType
	Port           uint   `json:"port"`
	Addr           string `json:"address"`
	ServerAddr     string `json:"server_addr"`
	ServerProtocol string `json:"server_protocol"`
	ServerTcpPort  uint   `json:"server_tcp_port"`
	LogLvl         uint   `json:"log_level"`
	NoDiscovery    bool   `json:"no_discovery"`
	LocalDiscovery bool   `json:"local_discovery"`
}

func SetupSentinelCli(ctx *cli.Context) (*SentinelCliCfg, error) {
	cfg := &SentinelCliCfg{}
	chainName := ctx.String(utils.ChainFlag.Name)
	var err error
	cfg.GenesisCfg, cfg.NetworkCfg, cfg.BeaconCfg, cfg.NetworkType, err = clparams.GetConfigsByNetworkName(chainName)
	if err != nil {
		return nil, err
	}
	if ctx.String(sentinelflags.BeaconConfigFlag.Name) != "" {
		cfg.BeaconCfg = new(clparams.BeaconChainConfig)
		if *cfg.BeaconCfg, err = clparams.CustomConfig(ctx.String(sentinelflags.BeaconConfigFlag.Name)); err != nil {
			return nil, err
		}
		if ctx.String(sentinelflags.GenesisSSZFlag.Name) == "" {
			return nil, fmt.Errorf("no genesis file provided")
		}
		cfg.GenesisCfg = new(clparams.GenesisConfig)

	}
	cfg.ServerAddr = fmt.Sprintf("%s:%d", ctx.String(sentinelflags.SentinelServerAddr.Name), ctx.Int(sentinelflags.SentinelServerPort.Name))
	cfg.ServerProtocol = "tcp"

	cfg.Port = uint(ctx.Int(sentinelflags.SentinelDiscoveryPort.Name))
	cfg.Addr = ctx.String(sentinelflags.SentinelDiscoveryAddr.Name)

	cfg.LogLvl = ctx.Uint(logging.LogVerbosityFlag.Name)
	if cfg.LogLvl == uint(log.LvlInfo) || cfg.LogLvl == 0 {
		cfg.LogLvl = uint(log.LvlDebug)
	}
	cfg.NoDiscovery = ctx.Bool(sentinelflags.NoDiscovery.Name)
	cfg.LocalDiscovery = ctx.Bool(sentinelflags.LocalDiscovery.Name)

	// Process bootnodes
	if ctx.String(sentinelflags.BootnodesFlag.Name) != "" {
		cfg.NetworkCfg.BootNodes = common.CliString2Array(ctx.String(sentinelflags.BootnodesFlag.Name))
	}
	if ctx.String(sentinelflags.SentinelStaticPeersFlag.Name) != "" {
		cfg.NetworkCfg.StaticPeers = common.CliString2Array(ctx.String(sentinelflags.SentinelStaticPeersFlag.Name))
	}
	return cfg, nil
}
