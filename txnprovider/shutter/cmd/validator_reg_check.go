// Copyright 2025 The Erigon Authors
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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

func registerValidatorRegCheckCmd(app *cli.App) {
	app.Commands = append(app.Commands, &cli.Command{
		Name:  "shutter-validator-reg-check",
		Usage: "check if the provided validators are registered with shutter",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "chain",
				Usage:    "chain name: gnosis, chiado, etc.",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "el-url",
				Usage:    "execution layer url",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "validator-info-file",
				Usage:    "path to validator info json file",
				Required: true,
			},
		},
		Action: func(cliCtx *cli.Context) error {
			ctx := cliCtx.Context
			chain := cliCtx.String("chain")
			elUrl := cliCtx.String("el-url")
			validatorInfoFile := cliCtx.String("validator-info-file")
			return validatorRegCheck(ctx, chain, elUrl, validatorInfoFile)
		},
	})
}

func validatorRegCheck(ctx context.Context, chain, elUrl, validatorInfoFile string) error {
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	config := shuttercfg.ConfigByChainName(chain)
	cb := contracts.NewJsonRpcBackend(elUrl, logger)
	registryAddr := common.HexToAddress(config.ValidatorRegistryContractAddress)
	checker := shutter.NewValidatorRegistryChecker(logger, cb, registryAddr, config.ChainId)
	validatorInfo, err := parseValidatorInfo(validatorInfoFile)
	if err != nil {
		return fmt.Errorf("failed to parse validator info file: %w", err)
	}
	registered, err := checker.FilterRegistered(ctx, validatorInfo)
	if err != nil {
		return fmt.Errorf("failed to filter registered validators: %w", err)
	}
	missing := shutter.ValidatorInfo{}
	for index, pubKey := range validatorInfo {
		if _, ok := registered[index]; !ok {
			missing[index] = pubKey
		}
	}
	if len(missing) == 0 {
		logger.Info("all validators are registered", "count", len(validatorInfo))
		return nil
	}
	for index, pubKey := range missing {
		logger.Error("validator is not registered", "index", index, "pubkey", pubKey)
	}
	logger.Error("validators are not registered", "count", len(missing))
	return nil
}

func parseValidatorInfo(validatorInfoFile string) (shutter.ValidatorInfo, error) {
	b, err := os.ReadFile(validatorInfoFile)
	if err != nil {
		return nil, err
	}
	var validatorInfo shutter.ValidatorInfo
	err = json.Unmarshal(b, &validatorInfo)
	return validatorInfo, err
}
