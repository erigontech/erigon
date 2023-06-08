// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package misc

import (
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

// CalcExcessDataGas implements calc_excess_data_gas from EIP-4844
func CalcExcessDataGas(parent *types.Header) uint64 {
	var excessDataGas, dataGasUsed uint64
	if parent.ExcessDataGas != nil {
		excessDataGas = *parent.ExcessDataGas
	}
	if parent.DataGasUsed != nil {
		dataGasUsed = *parent.DataGasUsed
	}

	if excessDataGas+dataGasUsed < params.TargetDataGasPerBlock {
		return 0
	}
	return excessDataGas + dataGasUsed - params.TargetDataGasPerBlock
}

// FakeExponential approximates factor * e ** (num / denom) using a taylor expansion
// as described in the EIP-4844 spec.
func FakeExponential(factor, denom *uint256.Int, excessDataGas uint64) (*uint256.Int, error) {
	numerator := uint256.NewInt(excessDataGas)
	output := uint256.NewInt(0)
	numeratorAccum := new(uint256.Int)
	_, overflow := numeratorAccum.MulOverflow(factor, denom)
	if overflow {
		return nil, fmt.Errorf("FakeExponential: overflow in MulOverflow(factor=%v, denom=%v)", factor, denom)
	}
	divisor := new(uint256.Int)
	for i := 1; numeratorAccum.Sign() > 0; i++ {
		_, overflow = output.AddOverflow(output, numeratorAccum)
		if overflow {
			return nil, fmt.Errorf("FakeExponential: overflow in AddOverflow(output=%v, numeratorAccum=%v)", output, numeratorAccum)
		}
		_, overflow = divisor.MulOverflow(denom, uint256.NewInt(uint64(i)))
		if overflow {
			return nil, fmt.Errorf("FakeExponential: overflow in MulOverflow(denom=%v, i=%v)", denom, i)
		}
		_, overflow = numeratorAccum.MulDivOverflow(numeratorAccum, numerator, divisor)
		if overflow {
			return nil, fmt.Errorf("FakeExponential: overflow in MulDivOverflow(numeratorAccum=%v, numerator=%v, divisor=%v)", numeratorAccum, numerator, divisor)
		}
	}
	return output.Div(output, denom), nil
}

// CountBlobs returns the number of blob transactions in txs
func CountBlobs(txs []types.Transaction) int {
	var count int
	for _, tx := range txs {
		count += len(tx.GetDataHashes())
	}
	return count
}

// VerifyEip4844Header verifies that the header is not malformed
func VerifyEip4844Header(config *chain.Config, parent, header *types.Header) error {
	if header.DataGasUsed == nil {
		return fmt.Errorf("header is missing dataGasUsed")
	}
	if header.ExcessDataGas == nil {
		return fmt.Errorf("header is missing excessDataGas")
	}
	return nil
}

// GetDataGasPrice implements get_data_gas_price from EIP-4844
func GetDataGasPrice(excessDataGas uint64) (*uint256.Int, error) {
	return FakeExponential(uint256.NewInt(params.MinDataGasPrice), uint256.NewInt(params.DataGasPriceUpdateFraction), excessDataGas)
}

func GetDataGasUsed(numBlobs int) uint64 {
	return uint64(numBlobs) * params.DataGasPerBlob
}
