package cli

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/sentinel_node/cli/flags"
	"github.com/urfave/cli"
)

type LightClientCliCfg struct {
	GenesisCfg     *clparams.GenesisConfig
	BeaconCfg      *clparams.BeaconChainConfig
	NetworkCfg     *clparams.NetworkConfig
	Port           uint
	Addr           string
	ServerAddr     string
	ServerProtocol string
	ServerTcpPort  uint
	LogLvl         uint
}

func SetUpLightClientCfg(ctx *cli.Context) *LightClientCliCfg {
	cfg := &LightClientCliCfg{}
	chainName := ctx.GlobalString(flags.LightClientChain.Name)
	fmt.Println("here")
	cfg.GenesisCfg, cfg.NetworkCfg, cfg.BeaconCfg = clparams.GetConfigsByNetworkName(chainName)

	cfg.ServerAddr = fmt.Sprintf("%s:%d", ctx.GlobalString(flags.LightClientServerAddr.Name), ctx.GlobalInt(flags.LightClientServerPort.Name))
	cfg.ServerProtocol = ServerProtocolFromInt(ctx.GlobalUint(flags.LightClientServerProtocol.Name))

	cfg.Port = uint(ctx.GlobalInt(flags.LightClientPort.Name))
	cfg.Addr = ctx.GlobalString(flags.LightClientAddr.Name)

	cfg.LogLvl = ctx.GlobalUint(flags.LightClientVerbosity.Name)

	return cfg
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
