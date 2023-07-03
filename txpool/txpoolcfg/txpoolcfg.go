package txpoolcfg

import (
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	emath "github.com/ledgerwatch/erigon-lib/common/math"
	"github.com/ledgerwatch/erigon-lib/types"
)

type Config struct {
	DBDir                 string
	TracedSenders         []string // List of senders for which tx pool should print out debugging info
	SyncToNewPeersEvery   time.Duration
	ProcessRemoteTxsEvery time.Duration
	CommitEvery           time.Duration
	LogEvery              time.Duration
	PendingSubPoolLimit   int
	BaseFeeSubPoolLimit   int
	QueuedSubPoolLimit    int
	MinFeeCap             uint64
	AccountSlots          uint64 // Number of executable transaction slots guaranteed per account
	PriceBump             uint64 // Price bump percentage to replace an already existing transaction
	OverrideShanghaiTime  *big.Int
}

var DefaultConfig = Config{
	SyncToNewPeersEvery:   2 * time.Minute,
	ProcessRemoteTxsEvery: 100 * time.Millisecond,
	CommitEvery:           15 * time.Second,
	LogEvery:              30 * time.Second,

	PendingSubPoolLimit: 10_000,
	BaseFeeSubPoolLimit: 10_000,
	QueuedSubPoolLimit:  10_000,

	MinFeeCap:            1,
	AccountSlots:         16, //TODO: to choose right value (16 to be compatible with Geth)
	PriceBump:            10, // Price bump percentage to replace an already existing transaction
	OverrideShanghaiTime: nil,
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
	CreateBlobTxn       DiscardReason = 24 // Blob transactions cannot have the form of a create transaction
	NoBlobs             DiscardReason = 25 // Blob transactions must have at least one blob
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
		return "existing tx with same hash"
	case InitCodeTooLarge:
		return "initcode too large"
	case TypeNotActivated:
		return "fork supporting this transaction type is not activated yet"
	case CreateBlobTxn:
		return "blob transactions cannot have the form of a create transaction"
	case NoBlobs:
		return "blob transactions must have at least one blob"
	default:
		panic(fmt.Sprintf("discard reason: %d", r))
	}
}

// CalcIntrinsicGas computes the 'intrinsic gas' for a message with the given data.
func CalcIntrinsicGas(dataLen, dataNonZeroLen uint64, accessList types.AccessList, isContractCreation, isHomestead, isEIP2028, isShanghai bool) (uint64, DiscardReason) {
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
	return gas, Success
}

// toWordSize returns the ceiled word size required for memory expansion.
func toWordSize(size uint64) uint64 {
	if size > math.MaxUint64-31 {
		return math.MaxUint64/32 + 1
	}
	return (size + 31) / 32
}
