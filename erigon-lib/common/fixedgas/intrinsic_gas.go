package fixedgas

import (
	"github.com/erigontech/erigon-lib/common/math"
)

// CalcIntrinsicGas computes the 'intrinsic gas' for a message with the given data.
func CalcIntrinsicGas(dataLen, dataNonZeroLen, authorizationsLen, accessListLen, storageKeysLen uint64, isContractCreation, isHomestead, isEIP2028, isShanghai, isPrague bool) (gas uint64, floorGas7623 uint64, overflow bool) {
	// Set the starting gas for the raw transaction
	if isContractCreation && isHomestead {
		gas = TxGasContractCreation
	} else {
		gas = TxGas
		floorGas7623 = TxGas
	}
	// Bump the required gas by the amount of transactional data
	if dataLen > 0 {
		// Zero and non-zero bytes are priced differently
		nz := dataNonZeroLen
		// Make sure we don't exceed uint64 for all data combinations
		nonZeroGas := TxDataNonZeroGasFrontier
		if isEIP2028 {
			nonZeroGas = TxDataNonZeroGasEIP2028
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

		product, overflow = math.SafeMul(z, TxDataZeroGas)
		if overflow {
			return 0, 0, true
		}
		gas, overflow = math.SafeAdd(gas, product)
		if overflow {
			return 0, 0, true
		}

		if isContractCreation && isShanghai {
			numWords := toWordSize(dataLen)
			product, overflow = math.SafeMul(numWords, InitCodeWordGas)
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
			dataGas, overflow := math.SafeMul(tokenLen, TxTotalCostFloorPerToken)
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
		product, overflow := math.SafeMul(accessListLen, TxAccessListAddressGas)
		if overflow {
			return 0, 0, true
		}
		gas, overflow = math.SafeAdd(gas, product)
		if overflow {
			return 0, 0, true
		}

		product, overflow = math.SafeMul(storageKeysLen, TxAccessListStorageKeyGas)
		if overflow {
			return 0, 0, true
		}
		gas, overflow = math.SafeAdd(gas, product)
		if overflow {
			return 0, 0, true
		}
	}

	// Add the cost of authorizations
	product, overflow := math.SafeMul(authorizationsLen, PerEmptyAccountCost)
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
