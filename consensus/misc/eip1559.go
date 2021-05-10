package misc

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/params"
)

func VerifyEip1559Header(config *params.ChainConfig, parent, header *types.Header) error {
	// Verify the header is not malformed
	if header.BaseFee == nil {
		return fmt.Errorf("invalid baseFee: have <nil>")
	}
	// Verify the baseFee is correct based on the parent header.
	expectedBaseFee := CalcBaseFee(config, parent)
	if header.BaseFee.Cmp(expectedBaseFee) != 0 {
		return fmt.Errorf("invalid baseFee: have %s, want %s, parentBaseFee %s, parentGasUsed %d",
			expectedBaseFee, header.BaseFee, parent.BaseFee, parent.GasUsed)
	}
	// Verify that the gas target remains within allowed bounds
	var (
		parentGasTarget = parent.GasLimit / params.ElasticityMultiplier
		thisGasTarget   = header.GasLimit / params.ElasticityMultiplier
	)
	if !config.IsLondon(parent.Number.Uint64()) {
		parentGasTarget = parent.GasLimit
	}
	limit := parentGasTarget / params.GasLimitBoundDivisor
	if thisGasTarget > parentGasTarget+limit {
		return fmt.Errorf("gas target too high: have %d (limit %d), want max %d",
			thisGasTarget, header.GasLimit, parentGasTarget+limit)
	}
	if thisGasTarget < parentGasTarget-limit {
		return fmt.Errorf("gas target too low: have %d (limit %d), want min %d",
			thisGasTarget, header.GasLimit, parentGasTarget+limit)
	}
	if header.GasLimit < params.MinGasLimit {
		return errors.New("gas limit below minimum 5000")
	}
	return nil
}

func CalcBaseFee(config *params.ChainConfig, parent *types.Header) *big.Int {
	// If the current block is the first EIP-1559 block, return the InitialBaseFee.
	if !config.IsLondon(parent.Number.Uint64()) {
		return new(big.Int).SetUint64(params.InitialBaseFee)
	}

	var (
		parentGasTarget          = parent.GasLimit / params.ElasticityMultiplier
		parentGasTargetBig       = new(big.Int).SetUint64(parentGasTarget)
		baseFeeChangeDenominator = new(big.Int).SetUint64(params.BaseFeeChangeDenominator)
	)
	// If the parent gasUsed is the same as the target, the baseFee remains unchanged.
	if parent.GasUsed == parentGasTarget {
		return new(big.Int).Set(parent.BaseFee)
	}
	if parent.GasUsed > parentGasTarget {
		// If the parent block used more gas than its target, the baseFee should increase.
		gasUsedDelta := new(big.Int).SetUint64(parent.GasUsed - parentGasTarget)
		x := new(big.Int).Mul(parent.BaseFee, gasUsedDelta)
		y := x.Div(x, parentGasTargetBig)
		baseFeeDelta := math.BigMax(
			x.Div(y, baseFeeChangeDenominator),
			common.Big1,
		)

		return x.Add(parent.BaseFee, baseFeeDelta)
	} else {
		// Otherwise if the parent block used less gas than its target, the baseFee should decrease.
		gasUsedDelta := new(big.Int).SetUint64(parentGasTarget - parent.GasUsed)
		x := new(big.Int).Mul(parent.BaseFee, gasUsedDelta)
		y := x.Div(x, parentGasTargetBig)
		baseFeeDelta := x.Div(y, baseFeeChangeDenominator)

		return math.BigMax(
			x.Sub(parent.BaseFee, baseFeeDelta),
			common.Big0,
		)
	}
}
