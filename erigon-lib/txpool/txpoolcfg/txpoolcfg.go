// Copyright 2022 The Erigon Authors
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

package txpoolcfg

import (
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/fixedgas"
	emath "github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/types"
)

// BorDefaultTxPoolPriceLimit defines the minimum gas price limit for bor to enforce txs acceptance into the pool.
const BorDefaultTxPoolPriceLimit = 25 * common.GWei

type Config struct {
	DBDir               string
	TracedSenders       []string // List of senders for which txn pool should print out debugging info
	PendingSubPoolLimit int
	BaseFeeSubPoolLimit int
	QueuedSubPoolLimit  int
	MinFeeCap           uint64
	AccountSlots        uint64 // Number of executable transaction slots guaranteed per account
	BlobSlots           uint64 // Total number of blobs (not txs) allowed per account
	TotalBlobPoolLimit  uint64 // Total number of blobs (not txs) allowed within the txpool
	PriceBump           uint64 // Price bump percentage to replace an already existing transaction
	BlobPriceBump       uint64 //Price bump percentage to replace an existing 4844 blob txn (type-3)
	OverridePragueTime  *big.Int

	// regular batch tasks processing
	SyncToNewPeersEvery   time.Duration
	ProcessRemoteTxsEvery time.Duration
	CommitEvery           time.Duration
	LogEvery              time.Duration

	//txpool db
	MdbxPageSize    datasize.ByteSize
	MdbxDBSizeLimit datasize.ByteSize
	MdbxGrowthStep  datasize.ByteSize
	MdbxWriteMap    bool

	NoGossip bool // this mode doesn't broadcast any txs, and if receive remote-txn - skip it
}

var DefaultConfig = Config{
	SyncToNewPeersEvery:   5 * time.Second,
	ProcessRemoteTxsEvery: 100 * time.Millisecond,
	CommitEvery:           15 * time.Second,
	LogEvery:              30 * time.Second,

	PendingSubPoolLimit: 10_000,
	BaseFeeSubPoolLimit: 10_000,
	QueuedSubPoolLimit:  10_000,

	MinFeeCap:          1,
	AccountSlots:       16,  //TODO: to choose right value (16 to be compatible with Geth)
	BlobSlots:          48,  // Default for a total of 8 txs for 6 blobs each - for hive tests
	TotalBlobPoolLimit: 480, // Default for a total of 10 different accounts hitting the above limit
	PriceBump:          10,  // Price bump percentage to replace an already existing transaction
	BlobPriceBump:      100,

	NoGossip:     false,
	MdbxWriteMap: false,
}

type DiscardReason uint8

const (
	NotSet              DiscardReason = 0 // analog of "nil-value", means it will be set in future
	Success             DiscardReason = 1
	AlreadyKnown        DiscardReason = 2
	Mined               DiscardReason = 3
	ReplacedByHigherTip DiscardReason = 4
	UnderPriced         DiscardReason = 5
	ReplaceUnderpriced  DiscardReason = 6 // if a transaction is attempted to be replaced with a different one without the required price bump.
	FeeTooLow           DiscardReason = 7
	OversizedData       DiscardReason = 8
	InvalidSender       DiscardReason = 9
	NegativeValue       DiscardReason = 10 // ensure no one is able to specify a transaction with a negative value.
	Spammer             DiscardReason = 11
	PendingPoolOverflow DiscardReason = 12
	BaseFeePoolOverflow DiscardReason = 13
	QueuedPoolOverflow  DiscardReason = 14
	GasUintOverflow     DiscardReason = 15
	IntrinsicGas        DiscardReason = 16
	RLPTooLong          DiscardReason = 17
	NonceTooLow         DiscardReason = 18
	InsufficientFunds   DiscardReason = 19
	NotReplaced         DiscardReason = 20 // There was an existing transaction with the same sender and nonce, not enough price bump to replace
	DuplicateHash       DiscardReason = 21 // There was an existing transaction with the same hash
	InitCodeTooLarge    DiscardReason = 22 // EIP-3860 - transaction init code is too large
	TypeNotActivated    DiscardReason = 23 // For example, an EIP-4844 transaction is submitted before Cancun activation
	InvalidCreateTxn    DiscardReason = 24 // EIP-4844 & 7702 transactions cannot have the form of a create transaction
	NoBlobs             DiscardReason = 25 // Blob transactions must have at least one blob
	TooManyBlobs        DiscardReason = 26 // There's a limit on how many blobs a block (and thus any transaction) may have
	UnequalBlobTxExt    DiscardReason = 27 // blob_versioned_hashes, blobs, commitments and proofs must have equal number
	BlobHashCheckFail   DiscardReason = 28 // KZGcommitment's versioned hash has to be equal to blob_versioned_hash at the same index
	UnmatchedBlobTxExt  DiscardReason = 29 // KZGcommitments must match the corresponding blobs and proofs
	BlobTxReplace       DiscardReason = 30 // Cannot replace type-3 blob txn with another type of txn
	BlobPoolOverflow    DiscardReason = 31 // The total number of blobs (through blob txs) in the pool has reached its limit
	NoAuthorizations    DiscardReason = 32 // EIP-7702 transactions with an empty authorization list are invalid
)

func (r DiscardReason) String() string {
	switch r {
	case NotSet:
		return "not set"
	case Success:
		return "success"
	case AlreadyKnown:
		return "already known"
	case Mined:
		return "mined"
	case ReplacedByHigherTip:
		return "replaced by transaction with higher tip"
	case UnderPriced:
		return "underpriced"
	case ReplaceUnderpriced:
		return "replacement transaction underpriced"
	case FeeTooLow:
		return "fee too low"
	case OversizedData:
		return "oversized data"
	case InvalidSender:
		return "invalid sender"
	case NegativeValue:
		return "negative value"
	case Spammer:
		return "spammer"
	case PendingPoolOverflow:
		return "pending sub-pool is full"
	case BaseFeePoolOverflow:
		return "baseFee sub-pool is full"
	case QueuedPoolOverflow:
		return "queued sub-pool is full"
	case GasUintOverflow:
		return "GasUintOverflow"
	case IntrinsicGas:
		return "IntrinsicGas"
	case RLPTooLong:
		return "RLPTooLong"
	case NonceTooLow:
		return "nonce too low"
	case InsufficientFunds:
		return "insufficient funds"
	case NotReplaced:
		return "could not replace existing tx"
	case DuplicateHash:
		return "existing txn with same hash"
	case InitCodeTooLarge:
		return "initcode too large"
	case TypeNotActivated:
		return "fork supporting this transaction type is not activated yet"
	case InvalidCreateTxn:
		return "EIP-4844 & 7702 transactions cannot have the form of a create transaction"
	case NoBlobs:
		return "blob transactions must have at least one blob"
	case TooManyBlobs:
		return "max number of blobs exceeded"
	case BlobTxReplace:
		return "can't replace blob-txn with a non-blob-txn"
	case BlobPoolOverflow:
		return "blobs limit in txpool is full"
	case NoAuthorizations:
		return "EIP-7702 transactions with an empty authorization list are invalid"
	default:
		panic(fmt.Sprintf("discard reason: %d", r))
	}
}

// CalcIntrinsicGas computes the 'intrinsic gas' for a message with the given data.
// TODO: move input data to a struct
func CalcIntrinsicGas(dataLen, dataNonZeroLen, authorizationsLen uint64, accessList types.AccessList, isContractCreation, isHomestead, isEIP2028, isShanghai bool) (uint64, DiscardReason) {
	// Set the starting gas for the raw transaction
	var gas uint64
	if isContractCreation && isHomestead {
		gas = fixedgas.TxGasContractCreation
	} else {
		gas = fixedgas.TxGas
	}
	// Bump the required gas by the amount of transactional data
	if dataLen > 0 {
		// Zero and non-zero bytes are priced differently
		nz := dataNonZeroLen
		// Make sure we don't exceed uint64 for all data combinations
		nonZeroGas := fixedgas.TxDataNonZeroGasFrontier
		if isEIP2028 {
			nonZeroGas = fixedgas.TxDataNonZeroGasEIP2028
		}

		product, overflow := emath.SafeMul(nz, nonZeroGas)
		if overflow {
			return 0, GasUintOverflow
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0, GasUintOverflow
		}

		z := dataLen - nz

		product, overflow = emath.SafeMul(z, fixedgas.TxDataZeroGas)
		if overflow {
			return 0, GasUintOverflow
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0, GasUintOverflow
		}

		if isContractCreation && isShanghai {
			numWords := toWordSize(dataLen)
			product, overflow = emath.SafeMul(numWords, fixedgas.InitCodeWordGas)
			if overflow {
				return 0, GasUintOverflow
			}
			gas, overflow = emath.SafeAdd(gas, product)
			if overflow {
				return 0, GasUintOverflow
			}
		}
	}
	if accessList != nil {
		product, overflow := emath.SafeMul(uint64(len(accessList)), fixedgas.TxAccessListAddressGas)
		if overflow {
			return 0, GasUintOverflow
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0, GasUintOverflow
		}

		product, overflow = emath.SafeMul(uint64(accessList.StorageKeys()), fixedgas.TxAccessListStorageKeyGas)
		if overflow {
			return 0, GasUintOverflow
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0, GasUintOverflow
		}
	}

	// Add the cost of authorizations
	product, overflow := emath.SafeMul(authorizationsLen, fixedgas.PerEmptyAccountCost)
	if overflow {
		return 0, GasUintOverflow
	}

	gas, overflow = emath.SafeAdd(gas, product)
	if overflow {
		return 0, GasUintOverflow
	}

	return gas, Success
}

// toWordSize returns the ceiled word size required for memory expansion.
func toWordSize(size uint64) uint64 {
	if size > math.MaxUint64-31 {
		return math.MaxUint64/32 + 1
	}
	return (size + 31) / 32
}
