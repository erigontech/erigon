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

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

// VerifyEip1559Header checks base fee only; gas limit is in VerifyParentGasLimit.
func VerifyEip1559Header(config *chain.Config, parent, header *types.Header) error {
	if header.BaseFee == nil {
		return errors.New("header is missing baseFee")
	}
	expectedBaseFee := CalcBaseFee(config, parent)
	if header.BaseFee.Cmp(expectedBaseFee) != 0 {
		return fmt.Errorf("invalid baseFee: have %s, want %s, parentBaseFee %s, parentGasUsed %d",
			header.BaseFee, expectedBaseFee, parent.BaseFee, parent.GasUsed)
	}
	return nil
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
		baseFeeChangeDenominator = uint256.NewInt(params.BaseFeeChangeDenominator)
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
			return new(uint256.Int)
		}
		return x
	}
}
