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

	"github.com/ledgerwatch/erigon-lib/common/fixedgas"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

// CalcExcessBlobGas implements calc_excess_blob_gas from EIP-4844
func CalcExcessBlobGas(parent *types.Header) uint64 {
	var excessBlobGas, blobGasUsed uint64
	if parent.ExcessBlobGas != nil {
		excessBlobGas = *parent.ExcessBlobGas
	}
	if parent.BlobGasUsed != nil {
		blobGasUsed = *parent.BlobGasUsed
	}

	if excessBlobGas+blobGasUsed < fixedgas.TargetBlobGasPerBlock {
		return 0
	}
	return excessBlobGas + blobGasUsed - fixedgas.TargetBlobGasPerBlock
}

// FakeExponential approximates factor * e ** (num / denom) using a taylor expansion
// as described in the EIP-4844 spec.
func FakeExponential(factor, denom *uint256.Int, excessBlobGas uint64) (*uint256.Int, error) {
	numerator := uint256.NewInt(excessBlobGas)
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

// VerifyPresenceOfCancunHeaderFields checks that the fields introduced in Cancun (EIP-4844, EIP-4788) are present.
func VerifyPresenceOfCancunHeaderFields(header *types.Header) error {
	if header.BlobGasUsed == nil {
		return fmt.Errorf("header is missing blobGasUsed")
	}
	if header.ExcessBlobGas == nil {
		return fmt.Errorf("header is missing excessBlobGas")
	}
	if header.ParentBeaconBlockRoot == nil {
		return fmt.Errorf("header is missing parentBeaconBlockRoot")
	}
	return nil
}

// VerifyAbsenceOfCancunHeaderFields checks that the header doesn't have any fields added in Cancun (EIP-4844, EIP-4788).
func VerifyAbsenceOfCancunHeaderFields(header *types.Header) error {
	if header.BlobGasUsed != nil {
		return fmt.Errorf("invalid blobGasUsed before fork: have %v, expected 'nil'", header.BlobGasUsed)
	}
	if header.ExcessBlobGas != nil {
		return fmt.Errorf("invalid excessBlobGas before fork: have %v, expected 'nil'", header.ExcessBlobGas)
	}
	if header.ParentBeaconBlockRoot != nil {
		return fmt.Errorf("invalid parentBeaconBlockRoot before fork: have %v, expected 'nil'", header.ParentBeaconBlockRoot)
	}
	return nil
}

func GetBlobGasPrice(excessBlobGas uint64) (*uint256.Int, error) {
	return FakeExponential(uint256.NewInt(params.MinBlobGasPrice), uint256.NewInt(params.BlobGasPriceUpdateFraction), excessBlobGas)
}

func GetBlobGasUsed(numBlobs int) uint64 {
	return uint64(numBlobs) * fixedgas.BlobGasPerBlob
}
