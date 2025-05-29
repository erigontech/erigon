package fixedgas

import (
	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common/math"
)

// IntrinsicGas computes the 'intrinsic gas' for a message with the given data.
// TODO: convert the input to a struct
func IntrinsicGas(data []byte, accessListLen, storageKeysLen uint64, isContractCreation bool, isHomestead, isEIP2028, isEIP3860, isPrague, isAATxn bool, authorizationsLen uint64) (uint64, uint64, bool) {
	// Zero and non-zero bytes are priced differently
	dataLen := uint64(len(data))
	dataNonZeroLen := uint64(0)
	for _, byt := range data {
		if byt != 0 {
			dataNonZeroLen++
		}
	}

	return CalcIntrinsicGas(dataLen, dataNonZeroLen, authorizationsLen, accessListLen, storageKeysLen, isContractCreation, isHomestead, isEIP2028, isEIP3860, isPrague, isAATxn)
}

// CalcIntrinsicGas computes the 'intrinsic gas' for a message with the given data.
func CalcIntrinsicGas(dataLen, dataNonZeroLen, authorizationsLen, accessListLen, storageKeysLen uint64, isContractCreation, isHomestead, isEIP2028, isShanghai, isPrague, isAATxn bool) (gas uint64, floorGas7623 uint64, overflow bool) {
	// Set the starting gas for the raw transaction
	if isContractCreation && isHomestead {
		gas = params.TxGasContractCreation
	} else if isAATxn {
		gas = params.TxAAGas
	} else {
		gas = params.TxGas
	}
	floorGas7623 = params.TxGas
	// Bump the required gas by the amount of transactional data
	if dataLen > 0 {
		// Zero and non-zero bytes are priced differently
		nz := dataNonZeroLen
		// Make sure we don't exceed uint64 for all data combinations
		nonZeroGas := params.TxDataNonZeroGasFrontier
		if isEIP2028 {
			nonZeroGas = params.TxDataNonZeroGasEIP2028
		}

		product, overflow := math.SafeMul(nz, nonZeroGas)
		if overflow {
			return 0, 0, true
		}
		gas, overflow = math.SafeAdd(gas, product)
		if overflow {
			return 0, 0, true
		}

		z := dataLen - nz

		product, overflow = math.SafeMul(z, params.TxDataZeroGas)
		if overflow {
			return 0, 0, true
		}
		gas, overflow = math.SafeAdd(gas, product)
		if overflow {
			return 0, 0, true
		}

		if isContractCreation && isShanghai {
			numWords := toWordSize(dataLen)
			product, overflow = math.SafeMul(numWords, params.InitCodeWordGas)
			if overflow {
				return 0, 0, true
			}
			gas, overflow = math.SafeAdd(gas, product)
			if overflow {
				return 0, 0, true
			}
		}

		// EIP-7623
		if isPrague {
			tokenLen := dataLen + 3*nz
			dataGas, overflow := math.SafeMul(tokenLen, params.TxTotalCostFloorPerToken)
			if overflow {
				return 0, 0, true
			}
			floorGas7623, overflow = math.SafeAdd(floorGas7623, dataGas)
			if overflow {
				return 0, 0, true
			}
		}
	}
	if accessListLen > 0 {
		product, overflow := math.SafeMul(accessListLen, params.TxAccessListAddressGas)
		if overflow {
			return 0, 0, true
		}
		gas, overflow = math.SafeAdd(gas, product)
		if overflow {
			return 0, 0, true
		}

		product, overflow = math.SafeMul(storageKeysLen, params.TxAccessListStorageKeyGas)
		if overflow {
			return 0, 0, true
		}
		gas, overflow = math.SafeAdd(gas, product)
		if overflow {
			return 0, 0, true
		}
	}

	// Add the cost of authorizations
	product, overflow := math.SafeMul(authorizationsLen, params.PerEmptyAccountCost)
	if overflow {
		return 0, 0, true
	}

	gas, overflow = math.SafeAdd(gas, product)
	if overflow {
		return 0, 0, true
	}

	return gas, floorGas7623, false
}

// toWordSize returns the ceiled word size required for memory expansion.
func toWordSize(size uint64) uint64 {
	if size > math.MaxUint64-31 {
		return math.MaxUint64/32 + 1
	}
	return (size + 31) / 32
}
