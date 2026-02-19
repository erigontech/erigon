// Copyright 2021 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package misc

import (
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

// VerifyEip1559Header verifies some header attributes which were changed in EIP-1559,
// - gas limit check
// - basefee check
func VerifyEip1559Header(config *chain.Config, parent, header *types.Header, skipGasLimit bool) error {
	if !skipGasLimit {
		// Verify that the gas limit remains within allowed bounds
		parentGasLimit := parent.GasLimit
		if !config.IsLondon(parent.Number.Uint64()) {
			parentGasLimit = parent.GasLimit * params.ElasticityMultiplier
		}
		if err := VerifyGaslimit(parentGasLimit, header.GasLimit); err != nil {
			return err
		}
	}
	// Verify the header is not malformed
	if header.BaseFee == nil {
		return errors.New("header is missing baseFee")
	}
	// Verify the baseFee is correct based on the parent header.
	expectedBaseFee := CalcBaseFee(config, parent)
	if header.BaseFee.Cmp(expectedBaseFee) != 0 {
		return fmt.Errorf("invalid baseFee: have %s, want %s, parentBaseFee %s, parentGasUsed %d",
			header.BaseFee, expectedBaseFee, parent.BaseFee, parent.GasUsed)
	}
	return nil
}

var Eip1559FeeCalculator eip1559Calculator

type eip1559Calculator struct{}

func (f eip1559Calculator) CurrentFees(chainConfig *chain.Config, db kv.Getter) (baseFee, blobFee, minBlobGasPrice, blockGasLimit uint64, err error) {
	hash := rawdb.ReadHeadHeaderHash(db)

	if hash == (common.Hash{}) {
		return 0, 0, 0, 0, errors.New("can't get head header hash")
	}

	currentHeader, err := rawdb.ReadHeaderByHash(db, hash)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if currentHeader == nil {
		return 0, 0, 0, 0, nil
	}

	if chainConfig != nil {
		if currentHeader.BaseFee != nil {
			baseFee = CalcBaseFee(chainConfig, currentHeader).Uint64()
		}

		if currentHeader.ExcessBlobGas != nil {
			nextBlockTime := currentHeader.Time + chainConfig.SecondsPerSlot()
			excessBlobGas := CalcExcessBlobGas(chainConfig, currentHeader, nextBlockTime)
			b, err := GetBlobGasPrice(chainConfig, excessBlobGas, nextBlockTime)
			if err != nil {
				return 0, 0, 0, 0, err
			}
			blobFee = b.Uint64()
		}
	}

	minBlobGasPrice = chainConfig.GetMinBlobGasPrice()

	return baseFee, blobFee, minBlobGasPrice, currentHeader.GasLimit, nil
}

// CalcBaseFee calculates the basefee of the header.
func CalcBaseFee(config *chain.Config, parent *types.Header) *uint256.Int {
	// If the current block is the first EIP-1559 block, return the InitialBaseFee.
	if !config.IsLondon(parent.Number.Uint64()) {
		return uint256.NewInt(params.InitialBaseFee)
	}

	var (
		parentGasTarget          = parent.GasLimit / params.ElasticityMultiplier
		parentGasTargetU256      = uint256.NewInt(parentGasTarget)
		baseFeeChangeDenominator = uint256.NewInt(getBaseFeeChangeDenominator(config.Bor, parent.Number.Uint64()))
	)
	// If the parent gasUsed is the same as the target, the baseFee remains unchanged.
	if parent.GasUsed == parentGasTarget {
		return new(uint256.Int).Set(parent.BaseFee)
	}
	if parent.GasUsed > parentGasTarget {
		// If the parent block used more gas than its target, the baseFee should increase.
		gasUsedDelta := uint256.NewInt(parent.GasUsed - parentGasTarget)
		x := new(uint256.Int).Mul(parent.BaseFee, gasUsedDelta)
		y := x.Div(x, parentGasTargetU256)
		baseFeeDelta := x.Div(y, baseFeeChangeDenominator)
		if baseFeeDelta.IsZero() {
			baseFeeDelta.SetUint64(1)
		}
		return x.Add(parent.BaseFee, baseFeeDelta)
	} else {
		// Otherwise if the parent block used less gas than its target, the baseFee should decrease.
		gasUsedDelta := uint256.NewInt(parentGasTarget - parent.GasUsed)
		x := new(uint256.Int).Mul(parent.BaseFee, gasUsedDelta)
		y := x.Div(x, parentGasTargetU256)
		baseFeeDelta := x.Div(y, baseFeeChangeDenominator)

		x, overflow := x.SubOverflow(parent.BaseFee, baseFeeDelta)
		if overflow {
			return common.Num0
		}
		return x
	}
}

func getBaseFeeChangeDenominator(borConfig chain.BorConfig, number uint64) uint64 {
	// If we're running bor based chain post delhi hardfork, return the new value
	if borConfig, ok := borConfig.(*borcfg.BorConfig); ok {
		switch {
		case borConfig.IsBhilai(number):
			return params.BaseFeeChangeDenominatorPostBhilai
		case borConfig.IsDelhi(number):
			return params.BaseFeeChangeDenominatorPostDelhi
		}
	}

	// Return the original once for other chains and pre-fork cases
	return params.BaseFeeChangeDenominator
}
