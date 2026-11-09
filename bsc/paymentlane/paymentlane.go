// Copyright 2026 The Erigon Authors
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

// Package paymentlane implements the BEP-703 payment lane rule, active on BSC from the block
// after Jenner. One inequality per block:
//
//	header.GasUsed + max(0, paymentLaneQuota - paymentGasUsed) <= GasLimit
//
// The quota is a pure function of the parent post-state's ratio and this block's gas limit,
// so every node derives it independently. Parlia's system transactions count as general gas.
package paymentlane

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"

	"github.com/erigontech/erigon/common"
)

// ContractAddress is the PaymentLane system contract, installed by the Jenner fork.
var ContractAddress = common.HexToAddress("0x0000000000000000000000000000000000002007")

const (
	RatioDenom                = 10_000
	MaxLaneRatio              = 1_000
	MaxListedContracts uint64 = 100_000
)

var (
	ErrViolated      = errors.New("payment lane inequality violated")
	ErrCorruptConfig = errors.New("payment lane config mismatch")
)

// CheckRatio checks the ratio at the getter's full uint256 width: a value narrowed to 64 bits
// first can land inside the guard when the returned value did not.
func CheckRatio(ratio *big.Int) (uint64, error) {
	if ratio == nil || !ratio.IsUint64() || ratio.Uint64() == 0 || ratio.Uint64() > MaxLaneRatio {
		return 0, fmt.Errorf("%w: ratio 0 < %v <= %d", ErrCorruptConfig, ratio, MaxLaneRatio)
	}
	return ratio.Uint64(), nil
}

// Quota is ratio × gasLimit / RatioDenom with a 128-bit product.
func Quota(ratio, gasLimit uint64) uint64 {
	hi, lo := bits.Mul64(ratio, gasLimit)
	if hi >= RatioDenom {
		return math.MaxUint64
	}
	q, _ := bits.Div64(hi, lo, RatioDenom)
	return q
}

// CheckInequality is the block validity rule.
func CheckInequality(gasLimit, gasUsed, paymentGasUsed, paymentLaneQuota uint64) error {
	sum, carry := bits.Add64(gasUsed, satSub(paymentLaneQuota, paymentGasUsed), 0)
	if carry != 0 || sum > gasLimit {
		return fmt.Errorf("%w: gas used %d payment %d quota %d limit %d",
			ErrViolated, gasUsed, paymentGasUsed, paymentLaneQuota, gasLimit)
	}
	return nil
}

func satSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}
