// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package caplincli

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/caplin/caplinflags"
	"github.com/erigontech/erigon/cmd/sentinel/sentinelcli"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
)

type CaplinCliCfg struct {
	*sentinelcli.SentinelCliCfg

	Chaindata             string        `json:"chaindata"`
	ErigonPrivateApi      string        `json:"erigon_private_api"`
	AllowedEndpoints      []string      `json:"endpoints"`
	BeaconApiReadTimeout  time.Duration `json:"beacon_api_read_timeout"`
	BeaconApiWriteTimeout time.Duration `json:"beacon_api_write_timeout"`
	BeaconAddr            string        `json:"beacon_addr"`
	BeaconProtocol        string        `json:"beacon_protocol"`
	DataDir               string        `json:"data_dir"`
	RunEngineAPI          bool          `json:"run_engine_api"`
	EngineAPIAddr         string        `json:"engine_api_addr"`
	EngineAPIPort         int           `json:"engine_api_port"`
	MevRelayUrl           string        `json:"mev_relay_url"`
	CustomConfig          string        `json:"custom_config"`
	CustomGenesisState    string        `json:"custom_genesis_state"`
	MaxPeerCount          uint64        `json:"max_peer_count"`
	JwtSecret             []byte

	AllowedMethods   []string `json:"allowed_methods"`
	AllowedOrigins   []string `json:"allowed_origins"`
	AllowCredentials bool     `json:"allow_credentials"`

	Dirs datadir.Dirs
}

func SetupCaplinCli(ctx *cli.Context) (cfg *CaplinCliCfg, err error) {
	cfg = &CaplinCliCfg{}
	cfg.SentinelCliCfg, err = sentinelcli.SetupSentinelCli(ctx)
	if err != nil {
		return nil, err
	}

	cfg.ErigonPrivateApi = ctx.String(caplinflags.ErigonPrivateApiFlag.Name)

	cfg.AllowedEndpoints = ctx.StringSlice(utils.BeaconAPIFlag.Name)

	cfg.BeaconApiReadTimeout = time.Duration(ctx.Uint64(caplinflags.BeaconApiReadTimeout.Name)) * time.Second
	cfg.BeaconApiWriteTimeout = time.Duration(ctx.Uint(caplinflags.BeaconApiWriteTimeout.Name)) * time.Second
	cfg.MaxPeerCount = ctx.Uint64(utils.CaplinMaxPeerCount.Name)
	cfg.BeaconAddr = fmt.Sprintf("%s:%d", ctx.String(caplinflags.BeaconApiAddr.Name), ctx.Int(caplinflags.BeaconApiPort.Name))
	cfg.AllowCredentials = ctx.Bool(utils.BeaconApiAllowCredentialsFlag.Name)
	cfg.AllowedMethods = ctx.StringSlice(utils.BeaconApiAllowMethodsFlag.Name)
	cfg.AllowedOrigins = ctx.StringSlice(utils.BeaconApiAllowOriginsFlag.Name)
	cfg.BeaconProtocol = "tcp"

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

	if checkpointUrls := ctx.StringSlice(utils.CaplinCheckpointSyncUrlFlag.Name); len(checkpointUrls) > 0 {
		clparams.ConfigurableCheckpointsURLs = checkpointUrls
	}

	cfg.Chaindata = ctx.String(caplinflags.ChaindataFlag.Name)

	cfg.MevRelayUrl = ctx.String(caplinflags.MevRelayUrl.Name)

	// Custom Chain
	cfg.CustomConfig = ctx.String(caplinflags.CustomConfig.Name)
	cfg.CustomGenesisState = ctx.String(caplinflags.CustomGenesisState.Name)

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
