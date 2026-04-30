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

package mdgas

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
	CostPerStateByte   uint64
	IsContractCreation bool
	IsEIP2             bool
	IsEIP2028          bool
	IsEIP3860          bool
	IsEIP7623          bool
	IsEIP7976          bool
	IsEIP7981          bool
	IsEIP8037          bool
	IsAATxn            bool
}

type IntrinsicGasCalcResult struct {
	RegularGas   uint64
	FloorGasCost uint64
	StateGas     uint64
}

// CountNonZeroBytes returns the number of non-zero bytes in data.
func CountNonZeroBytes(data []byte) int {
	count := 0
	for _, b := range data {
		if b != 0 {
			count++
		}
	}
	return count
}

// IntrinsicGas computes the 'intrinsic gas' for a message with the given data.
// It counts the non-zero bytes in args.Data and then calls CalcIntrinsicGas.
func IntrinsicGas(args IntrinsicGasCalcArgs) (IntrinsicGasCalcResult, bool) {
	args.DataNonZeroLen = uint64(CountNonZeroBytes(args.Data))
	return CalcIntrinsicGas(args)
}

// CalcIntrinsicGas computes the 'intrinsic gas' for a message with the given data.
// Unlike IntrinsicGas, it expects args.DataNonZeroLen to be pre-computed by the caller.
func CalcIntrinsicGas(args IntrinsicGasCalcArgs) (IntrinsicGasCalcResult, bool) {
	var result IntrinsicGasCalcResult
	dataLen := uint64(len(args.Data))
	// Set the starting gas for the raw transaction
	if args.IsEIP8037 && args.IsContractCreation {
		// EIP-8037: GAS_CREATE = 112*cpsb (state) + 9000 (regular), plus TxGas (21000)
		result.RegularGas = params.TxGas + params.CreateGasEIP8037
		result.StateGas = params.StateBytesNewAccount * args.CostPerStateByte
	} else if args.IsContractCreation && args.IsEIP2 {
		result.RegularGas = params.TxGasContractCreation
	} else if args.IsAATxn {
		result.RegularGas = params.TxAAGas
	} else {
		result.RegularGas = params.TxGas
	}
	result.FloorGasCost = params.TxGas
	nz := args.DataNonZeroLen
	// Bump the required gas by the amount of transactional data
	if dataLen > 0 {
		// Zero and non-zero bytes are priced differently
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

	// Floor data gas cost.
	//
	// Three EIPs layer here — each covers an independent contribution, and they
	// can be combined (EIP-7976 + EIP-7981 both activate at Glamsterdam):
	//   - EIP-7623 (Prague): legacy calldata floor using zero/non-zero byte
	//     tokens (zero_bytes + 4*nonzero_bytes) at 10 gas/token.
	//   - EIP-7976 (Glamsterdam): supersedes the EIP-7623 calldata formula —
	//     every byte of calldata counts as 4 tokens regardless of value, priced
	//     at 16 gas/token.
	//   - EIP-7981 (Glamsterdam): extends floor coverage to access-list data
	//     (addresses + storage keys) on top of the calldata floor, using the
	//     same 4 tokens-per-byte / 16 gas-per-token rate. Also charges the
	//     access-list data cost at floor rate in the regular intrinsic gas path
	//     so access lists cannot be used to bypass calldata floor pricing.
	//
	// Compute calldata floor tokens (EIP-7623 or EIP-7976) independently from
	// access-list floor tokens (EIP-7981), then combine at the appropriate cost
	// per token so both EIPs are exercised when both are active.
	var (
		calldataFloorTokens   uint64
		accessListFloorTokens uint64
		costPerToken          uint64
	)
	// EIP-7976 and EIP-7981 share the same per-token rate (16 gas/token), and
	// EIP-7981 spec requires EIP-7976 as a precondition. Selecting the rate on
	// either flag keeps the access-list floor surcharge (charged at floor rate
	// in RegularGas) consistent with the FloorGasCost rate.
	if args.IsEIP7976 || args.IsEIP7981 {
		costPerToken = params.TxTotalCostFloorPerTokenEIP7976
	} else {
		costPerToken = params.TxTotalCostFloorPerToken
	}
	if args.IsEIP7623 && dataLen > 0 {
		if args.IsEIP7976 {
			var overflow bool
			calldataFloorTokens, overflow = math.SafeMul(dataLen, params.TxStandardTokensPerByte)
			if overflow {
				return IntrinsicGasCalcResult{}, true
			}
		} else {
			nzTokens, overflow := math.SafeMul(3, nz)
			if overflow {
				return IntrinsicGasCalcResult{}, true
			}
			calldataFloorTokens, overflow = math.SafeAdd(dataLen, nzTokens)
			if overflow {
				return IntrinsicGasCalcResult{}, true
			}
		}
	}
	if args.IsEIP7981 {
		accessListBytes, overflow := math.SafeMul(args.AccessListLen, params.TxAccessListAddressBytes)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		storageKeyBytes, overflow := math.SafeMul(args.StorageKeysLen, params.TxAccessListStorageKeyBytes)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		accessListBytes, overflow = math.SafeAdd(accessListBytes, storageKeyBytes)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		accessListFloorTokens, overflow = math.SafeMul(accessListBytes, params.TxStandardTokensPerByte)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}

		// Always charge the access list data cost in the standard intrinsic gas
		// path so access list data is charged at floor rate regardless of
		// execution level.
		if accessListFloorTokens > 0 {
			accessListDataGas, overflow := math.SafeMul(accessListFloorTokens, costPerToken)
			if overflow {
				return IntrinsicGasCalcResult{}, true
			}
			result.RegularGas, overflow = math.SafeAdd(result.RegularGas, accessListDataGas)
			if overflow {
				return IntrinsicGasCalcResult{}, true
			}
		}
	}
	totalFloorTokens, overflow := math.SafeAdd(calldataFloorTokens, accessListFloorTokens)
	if overflow {
		return IntrinsicGasCalcResult{}, true
	}
	if totalFloorTokens > 0 {
		dataGas, overflow := math.SafeMul(totalFloorTokens, costPerToken)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		result.FloorGasCost, overflow = math.SafeAdd(result.FloorGasCost, dataGas)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
	}

	// Add the cost of authorizations
	if args.IsEIP8037 {
		regularCost, overflow := math.SafeMul(args.AuthorizationsLen, params.PerAuthBaseCostEIP8037)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		result.RegularGas, overflow = math.SafeAdd(result.RegularGas, regularCost)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		authCost, overflow := math.SafeMul(params.StateBytesNewAccount+params.StateBytesAuthBase, args.CostPerStateByte)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		authCost, overflow = math.SafeMul(args.AuthorizationsLen, authCost)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		result.StateGas, overflow = math.SafeAdd(result.StateGas, authCost)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
	} else {
		authCost, overflow := math.SafeMul(args.AuthorizationsLen, params.PerEmptyAccountCost)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
		result.RegularGas, overflow = math.SafeAdd(result.RegularGas, authCost)
		if overflow {
			return IntrinsicGasCalcResult{}, true
		}
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
