package lcCli

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
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
}

func SetUpLightClientCfg(ctx cli.Context) *LightClientCliCfg {
	var cfg *LightClientCliCfg
	chainName := ctx.GlobalString(utils.LightClientChain)
	cfg.GenesisCfg, cfg.NetworkCfg, cfg.BeaconCfg = clparams.GetConfigsByNetworkName(chainName)

	cfg.ServerAddr = fmt.Sprintf("%s:%d", ctx.GlobalString(utils.LightClientServerAddr), ctx.GlobalInt(utils.LightClientServerPort))
	cfg.ServerProtocol = ServerProtocolFromInt(ctx.GlobalUint(utils.LightClientServerProtocol))

	cfg.Port = uint(ctx.GlobalInt(utils.LightClientPort))
	cfg.Addr = ctx.GlobalString(utils.LightClientAddr)

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
