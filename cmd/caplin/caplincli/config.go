package caplincli

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cmd/caplin/caplinflags"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinelcli"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

type CaplinCliCfg struct {
	*sentinelcli.SentinelCliCfg

	CheckpointUri         string        `json:"checkpoint_uri"`
	Chaindata             string        `json:"chaindata"`
	ErigonPrivateApi      string        `json:"erigon_private_api"`
	TransitionChain       bool          `json:"transition_chain"`
	InitialSync           bool          `json:"initial_sync"`
	AllowedEndpoints      []string      `json:"endpoints"`
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

	AllowedMethods   []string `json:"allowed_methods"`
	AllowedOrigins   []string `json:"allowed_origins"`
	AllowCredentials bool     `json:"allow_credentials"`

	InitalState *state.CachingBeaconState
	Dirs        datadir.Dirs
}

func SetupCaplinCli(ctx *cli.Context) (cfg *CaplinCliCfg, err error) {
	cfg = &CaplinCliCfg{}
	cfg.SentinelCliCfg, err = sentinelcli.SetupSentinelCli(ctx)
	if err != nil {
		return nil, err
	}

	cfg.ErigonPrivateApi = ctx.String(caplinflags.ErigonPrivateApiFlag.Name)

	//T TODO(Giulio2002): Refactor later
	// if ctx.String(sentinelflags.BeaconConfigFlag.Name) != "" {
	// 	var stateByte []byte
	// 	// Now parse genesis time and genesis fork
	// 	if *cfg.GenesisCfg, stateByte, err = clparams.ParseGenesisSSZToGenesisConfig(
	// 		ctx.String(sentinelflags.GenesisSSZFlag.Name),
	// 		cfg.BeaconCfg.GetCurrentStateVersion(0)); err != nil {
	// 		return nil, err
	// 	}

	// 	cfg.InitalState = state.New(cfg.BeaconCfg)
	// 	if cfg.InitalState.DecodeSSZ(stateByte, int(cfg.BeaconCfg.GetCurrentStateVersion(0))); err != nil {
	// 		return nil, err
	// 	}
	// }

	cfg.AllowedEndpoints = ctx.StringSlice(utils.BeaconAPIFlag.Name)

	cfg.BeaconApiReadTimeout = time.Duration(ctx.Uint64(caplinflags.BeaconApiReadTimeout.Name)) * time.Second
	cfg.BeaconApiWriteTimeout = time.Duration(ctx.Uint(caplinflags.BeaconApiWriteTimeout.Name)) * time.Second
	cfg.BeaconAddr = fmt.Sprintf("%s:%d", ctx.String(caplinflags.BeaconApiAddr.Name), ctx.Int(caplinflags.BeaconApiPort.Name))
	cfg.AllowCredentials = ctx.Bool(utils.BeaconApiAllowCredentialsFlag.Name)
	cfg.AllowedMethods = ctx.StringSlice(utils.BeaconApiAllowMethodsFlag.Name)
	cfg.AllowedOrigins = ctx.StringSlice(utils.BeaconApiAllowOriginsFlag.Name)
	cfg.BeaconProtocol = "tcp"
	cfg.RecordMode = ctx.Bool(caplinflags.RecordModeFlag.Name)
	cfg.RecordDir = ctx.String(caplinflags.RecordModeDir.Name)
	cfg.DataDir = ctx.String(utils.DataDirFlag.Name)
	cfg.Dirs = datadir.New(cfg.DataDir)

	cfg.RunEngineAPI = ctx.Bool(caplinflags.RunEngineAPI.Name)
	cfg.EngineAPIAddr = ctx.String(caplinflags.EngineApiHostFlag.Name)
	cfg.EngineAPIPort = ctx.Int(caplinflags.EngineApiPortFlag.Name)
	if cfg.RunEngineAPI {
		secret, err := ObtainJwtSecret(ctx)
		if err != nil {
			log.Error("Failed to obtain jwt secret", "err", err)
			cfg.RunEngineAPI = false
		} else {
			cfg.JwtSecret = secret
		}
	}

	if ctx.String(caplinflags.CheckpointSyncUrlFlag.Name) != "" {
		cfg.CheckpointUri = ctx.String(caplinflags.CheckpointSyncUrlFlag.Name)
	} else {
		cfg.CheckpointUri = clparams.GetCheckpointSyncEndpoint(cfg.NetworkType)
	}

	cfg.Chaindata = ctx.String(caplinflags.ChaindataFlag.Name)

	cfg.TransitionChain = ctx.Bool(caplinflags.TransitionChainFlag.Name)
	cfg.InitialSync = ctx.Bool(caplinflags.InitSyncFlag.Name)

	return cfg, err
}

func ObtainJwtSecret(ctx *cli.Context) ([]byte, error) {
	path := ctx.String(caplinflags.JwtSecret.Name)
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
