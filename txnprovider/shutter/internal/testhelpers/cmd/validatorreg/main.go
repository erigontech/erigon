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

package main

import (
	"flag"
	"fmt"
	"math"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

func main() {
	elUrlFlag := flag.String("el-url", "", "execution layer url")
	valRegAddrFlag := flag.String("validator-registry-address", "", "validator registry smart contract address")
	fromIndexFlag := flag.Int64("from-index", 0, "validator from index filter")
	toIndexFlag := flag.Int64("to-index", math.MaxInt64, "validator to index filter (exclusive)")
	flag.Parse()
	if elUrlFlag == nil || *elUrlFlag == "" {
		panic("el-url flag is required")
	}
	if valRegAddrFlag == nil || *valRegAddrFlag == "" {
		panic("validator-registry-address flag is required")
	}

	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StderrHandler))
	cb := contracts.NewJsonRpcBackend(*elUrlFlag, logger)
	valRegAddr := common.HexToAddress(*valRegAddrFlag)
	valReg, err := shuttercontracts.NewValidatorRegistry(valRegAddr, cb)
	if err != nil {
		panic(err)
	}

	callOpts := bind.CallOpts{}
	n, err := valReg.GetNumUpdates(&callOpts)
	if err != nil {
		panic(err)
	}

	logger.Info("num updates", "num", n.Uint64())
	chainId := params.ChiadoChainConfig.ChainID
	for i := uint64(0); i < n.Uint64(); i++ {
		u, err := valReg.GetUpdate(&callOpts, big.NewInt(int64(i)))
		if err != nil {
			panic(err)
		}

		msg := new(shutter.AggregateRegistrationMessage)
		err = msg.Unmarshal(u.Message)
		if err != nil {
			panic(err)
		}

		if !checkStaticRegistrationMessageFields(logger, msg, chainId.Uint64(), valRegAddr) {
			continue
		}

		for _, i := range msg.ValidatorIndices() {
			if fromIndexFlag != nil && i < *fromIndexFlag {
				continue
			}
			if toIndexFlag != nil && i >= *toIndexFlag {
				break
			}

			logger.Info(fmt.Sprintf("validator index found: %d, %+v", i, msg))
		}
	}
}

func checkStaticRegistrationMessageFields(
	logger log.Logger,
	msg *shutter.AggregateRegistrationMessage,
	chainID uint64,
	validatorRegistryAddress common.Address,
) bool {
	if msg.Version != shutter.AggregateValidatorRegistrationMessageVersion &&
		msg.Version != shutter.LegacyValidatorRegistrationMessageVersion {
		logger.Info("ignoring registration message with invalid version", "version", msg.Version)
		return false
	}

	if msg.ChainId != chainID {
		logger.Info("ignoring registration message with invalid chain id", "chainId", msg.ChainId)
		return false
	}

	if msg.ValidatorRegistryAddress != validatorRegistryAddress {
		logger.Info("ignoring registration message with invalid validator registry address", "addr", msg.ValidatorRegistryAddress)
		return false
	}

	if msg.ValidatorIndex > math.MaxInt64 {
		logger.Info("ignoring registration message with invalid validator index")
		return false
	}

	return true
}
