// Copyright 2014 The go-ethereum Authors
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

package fixedgas

import (
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol/params"
)

type IntrinsicGasCalcArgs struct {
	Data               []byte
	DataNonZeroLen     uint64
	AuthorizationsLen  uint64
	AccessListLen      uint64
	StorageKeysLen     uint64
	IsContractCreation bool
	IsEIP2             bool
	IsEIP2028          bool
	IsEIP3860          bool
	IsEIP7623          bool
	IsAATxn            bool
}

type IntrinsicGasCalcResult struct {
	RegularGas   uint64
	FloorGasCost uint64
}

// IntrinsicGas computes the 'intrinsic gas' for a message with the given data.
// It counts the non-zero bytes in args.Data and then calls CalcIntrinsicGas.
func IntrinsicGas(args IntrinsicGasCalcArgs) (IntrinsicGasCalcResult, bool) {
	// Zero and non-zero bytes are priced differently
	args.DataNonZeroLen = 0
	for _, byt := range args.Data {
		if byt != 0 {
			args.DataNonZeroLen++
		}
	}

	return CalcIntrinsicGas(args)
}

// CalcIntrinsicGas computes the 'intrinsic gas' for a message with the given data.
// Unlike IntrinsicGas, it expects args.DataNonZeroLen to be pre-computed by the caller.
func CalcIntrinsicGas(args IntrinsicGasCalcArgs) (IntrinsicGasCalcResult, bool) {
	var result IntrinsicGasCalcResult
	dataLen := uint64(len(args.Data))
	// Set the starting gas for the raw transaction
	if args.IsContractCreation && args.IsEIP2 {
		result.RegularGas = params.TxGasContractCreation
	} else if args.IsAATxn {
		result.RegularGas = params.TxAAGas
	} else {
		result.RegularGas = params.TxGas
	}
	result.FloorGasCost = params.TxGas
	// Bump the required gas by the amount of transactional data
	if dataLen > 0 {
		// Zero and non-zero bytes are priced differently
		nz := args.DataNonZeroLen
		// Make sure we don't exceed uint64 for all data combinations
		nonZeroGas := params.TxDataNonZeroGasFrontier
		if args.IsEIP2028 {
			nonZeroGas = params.TxDataNonZeroGasEIP2028
		}

		product, overflow := math.SafeMul(nz, nonZeroGas)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		result.RegularGas, overflow = math.SafeAdd(result.RegularGas, product)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}

		z := dataLen - nz

		product, overflow = math.SafeMul(z, params.TxDataZeroGas)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		result.RegularGas, overflow = math.SafeAdd(result.RegularGas, product)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}

		if args.IsContractCreation && args.IsEIP3860 {
			numWords := toWordSize(dataLen)
			product, overflow = math.SafeMul(numWords, params.InitCodeWordGas)
			if overflow {
				return IntrinsicGasCalcResult{}, true
			}
			result.RegularGas, overflow = math.SafeAdd(result.RegularGas, product)
			if overflow {
				return IntrinsicGasCalcResult{}, true
			}
		}

		if args.IsEIP7623 {
			tokenLen := dataLen + 3*nz
			dataGas, overflow := math.SafeMul(tokenLen, params.TxTotalCostFloorPerToken)
			if overflow {
				return IntrinsicGasCalcResult{}, true
			}
			result.FloorGasCost, overflow = math.SafeAdd(result.FloorGasCost, dataGas)
			if overflow {
				return IntrinsicGasCalcResult{}, true
			}
		}
	}
	if args.AccessListLen > 0 {
		product, overflow := math.SafeMul(args.AccessListLen, params.TxAccessListAddressGas)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		result.RegularGas, overflow = math.SafeAdd(result.RegularGas, product)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}

		product, overflow = math.SafeMul(args.StorageKeysLen, params.TxAccessListStorageKeyGas)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		result.RegularGas, overflow = math.SafeAdd(result.RegularGas, product)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
	}

	// Add the cost of authorizations
	product, overflow := math.SafeMul(args.AuthorizationsLen, params.PerEmptyAccountCost)
	if overflow {
		return IntrinsicGasCalcResult{}, true
	}

	result.RegularGas, overflow = math.SafeAdd(result.RegularGas, product)
	if overflow {
		return IntrinsicGasCalcResult{}, true
	}

	return result, false
}

// toWordSize returns the ceiled word size required for memory expansion.
func toWordSize(size uint64) uint64 {
	if size > math.MaxUint64-31 {
		return math.MaxUint64/32 + 1
	}
	return (size + 31) / 32
}
