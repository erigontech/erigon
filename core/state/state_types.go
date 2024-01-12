package state

import (
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
)

type (
	// CanTransferFunc is the signature of a transfer guard function
	CanTransferFunc func(*IntraBlockState, common.Address, *uint256.Int) bool

	// TransferFunc is the signature of a transfer function
	TransferFunc func(*IntraBlockState, common.Address, common.Address, *uint256.Int, bool)

	// GetHashFunc returns the nth block hash in the blockchain
	// and is used by the BLOCKHASH EVM op code.
	GetHashFunc func(uint64) common.Hash
)

// BlockContext provides the EVM with auxiliary information. Once provided
// it shouldn't be modified.
type BlockContext struct {
	// CanTransfer returns whether the account contains
	// sufficient ether to transfer the value
	CanTransfer CanTransferFunc
	// Transfer transfers ether from one account to the other
	Transfer TransferFunc
	// GetHash returns the hash corresponding to n
	GetHash GetHashFunc

	// Block information
	Coinbase      common.Address // Provides information for COINBASE
	GasLimit      uint64         // Provides information for GASLIMIT
	MaxGasLimit   bool           // Use GasLimit override for 2^256-1 (to be compatible with OpenEthereum's trace_call)
	BlockNumber   uint64         // Provides information for NUMBER
	Time          uint64         // Provides information for TIME
	Difficulty    *big.Int       // Provides information for DIFFICULTY
	BaseFee       *uint256.Int   // Provides information for BASEFEE
	PrevRanDao    *common.Hash   // Provides information for PREVRANDAO
	ExcessBlobGas *uint64        // Provides information for handling data blobs
}

// TxContext provides the EVM with information about a transaction.
// All fields can change between transactions.
type TxContext struct {
	// Message information
	TxHash     common.Hash
	Origin     common.Address // Provides information for ORIGIN
	GasPrice   *uint256.Int   // Provides information for GASPRICE
	BlobHashes []common.Hash  // Provides versioned blob hashes for BLOBHASH
}
