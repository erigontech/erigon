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
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
)

// BorDefaultTxPoolPriceLimit defines the minimum gas price limit for bor to enforce txns acceptance into the pool.
const BorDefaultTxPoolPriceLimit = 25 * common.GWei

type Config struct {
	Disable             bool
	DBDir               string
	TracedSenders       []string // List of senders for which txn pool should print out debugging info
	PendingSubPoolLimit int
	BaseFeeSubPoolLimit int
	QueuedSubPoolLimit  int
	MinFeeCap           uint64
	AccountSlots        uint64 // Number of executable transaction slots guaranteed per account
	BlobSlots           uint64 // Total number of blobs (not txns) allowed per account
	TotalBlobPoolLimit  uint64 // Total number of blobs (not txns) allowed within the txpool
	PriceBump           uint64 // Price bump percentage to replace an already existing transaction
	BlobPriceBump       uint64 //Price bump percentage to replace an existing 4844 blob txn (type-3)

	// regular batch tasks processing
	SyncToNewPeersEvery    time.Duration
	ProcessRemoteTxnsEvery time.Duration
	CommitEvery            time.Duration
	LogEvery               time.Duration

	//txpool db
	MdbxPageSize    datasize.ByteSize
	MdbxDBSizeLimit datasize.ByteSize
	MdbxGrowthStep  datasize.ByteSize
	MdbxWriteMap    bool

	NoGossip bool // this mode doesn't broadcast any txns, and if receive remote-txn - skip it

	// Account Abstraction
	AllowAA bool
}

var DefaultConfig = Config{
	SyncToNewPeersEvery:    5 * time.Second,
	ProcessRemoteTxnsEvery: 100 * time.Millisecond,
	CommitEvery:            15 * time.Second,
	LogEvery:               30 * time.Second,

	PendingSubPoolLimit: 10_000,
	BaseFeeSubPoolLimit: 30_000,
	QueuedSubPoolLimit:  30_000,

	MinFeeCap:          1,
	AccountSlots:       16,  // TODO: to choose right value (16 to be compatible with Geth)
	BlobSlots:          48,  // Default for a total of 8 txns for 6 blobs each - for hive tests
	TotalBlobPoolLimit: 480, // Default for a total of 10 different accounts hitting the above limit
	PriceBump:          10,  // Price bump percentage to replace an already existing transaction
	BlobPriceBump:      100,

	NoGossip:     false,
	MdbxWriteMap: false,
}

type DiscardReason uint8

const (
	NotSet               DiscardReason = 0 // analog of "nil-value", means it will be set in future
	Success              DiscardReason = 1
	AlreadyKnown         DiscardReason = 2
	Mined                DiscardReason = 3
	ReplacedByHigherTip  DiscardReason = 4
	UnderPriced          DiscardReason = 5
	ReplaceUnderpriced   DiscardReason = 6 // if a transaction is attempted to be replaced with a different one without the required price bump.
	FeeTooLow            DiscardReason = 7
	OversizedData        DiscardReason = 8
	InvalidSender        DiscardReason = 9
	NegativeValue        DiscardReason = 10 // ensure no one is able to specify a transaction with a negative value.
	Spammer              DiscardReason = 11
	PendingPoolOverflow  DiscardReason = 12
	BaseFeePoolOverflow  DiscardReason = 13
	QueuedPoolOverflow   DiscardReason = 14
	GasUintOverflow      DiscardReason = 15
	IntrinsicGas         DiscardReason = 16
	RLPTooLong           DiscardReason = 17
	NonceTooLow          DiscardReason = 18
	InsufficientFunds    DiscardReason = 19
	NotReplaced          DiscardReason = 20 // There was an existing transaction with the same sender and nonce, not enough price bump to replace
	DuplicateHash        DiscardReason = 21 // There was an existing transaction with the same hash
	InitCodeTooLarge     DiscardReason = 22 // EIP-3860 - transaction init code is too large
	TypeNotActivated     DiscardReason = 23 // For example, an EIP-4844 transaction is submitted before Cancun activation
	InvalidCreateTxn     DiscardReason = 24 // EIP-4844 & 7702 transactions cannot have the form of a create transaction
	NoBlobs              DiscardReason = 25 // Blob transactions must have at least one blob
	TooManyBlobs         DiscardReason = 26 // There's a limit on how many blobs a block (and thus any transaction) may have
	UnequalBlobTxExt     DiscardReason = 27 // blob_versioned_hashes, blobs, commitments and proofs must have equal number
	BlobHashCheckFail    DiscardReason = 28 // KZGcommitment's versioned hash has to be equal to blob_versioned_hash at the same index
	UnmatchedBlobTxExt   DiscardReason = 29 // KZGcommitments must match the corresponding blobs and proofs
	BlobTxReplace        DiscardReason = 30 // Cannot replace type-3 blob txn with another type of txn
	BlobPoolOverflow     DiscardReason = 31 // The total number of blobs (through blob txns) in the pool has reached its limit
	NoAuthorizations     DiscardReason = 32 // EIP-7702 transactions with an empty authorization list are invalid
	GasLimitTooHigh      DiscardReason = 33 // Gas limit is too high
	ErrAuthorityReserved DiscardReason = 34 // EIP-7702 transaction with authority already reserved
	InvalidAA            DiscardReason = 35 // Invalid RIP-7560 transaction
	ErrGetCode           DiscardReason = 36 // Error getting code during AA validation
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
	case GasLimitTooHigh:
		return "gas limit is too high"
	case BlobHashCheckFail:
		return "KZGcommitment's versioned hash has to be equal to blob_versioned_hash at the same index"
	case UnmatchedBlobTxExt:
		return "KZGcommitments must match the corresponding blobs and proofs"
	case UnequalBlobTxExt:
		return "blob_versioned_hashes, blobs, commitments and proofs must have equal number"
	case ErrAuthorityReserved:
		return "EIP-7702 transaction with authority already reserved"
	case InvalidAA:
		return "RIP-7560 transaction failed validation"
	case ErrGetCode:
		return "error getting account code during RIP-7560 validation"
	default:
		panic(fmt.Sprintf("discard reason: %d", r))
	}
}
