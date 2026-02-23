// Copyright 2024 The Erigon Authors
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

package evmtypes

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
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
	GetHash          GetHashFunc
	PostApplyMessage PostApplyMessageFunc

	// Block information
	Coinbase         accounts.Address // Provides information for COINBASE
	GasLimit         uint64           // Provides information for GASLIMIT
	MaxGasLimit      bool             // Use GasLimit override for 2^256-1 (to be compatible with OpenEthereum's trace_call)
	BlockNumber      uint64           // Provides information for NUMBER
	Time             uint64           // Provides information for TIME
	Difficulty       uint256.Int      // Provides information for DIFFICULTY
	BaseFee          uint256.Int      // Provides information for BASEFEE
	PrevRanDao       *common.Hash     // Provides information for PREVRANDAO
	BlobBaseFee      uint256.Int      // Provides information for BLOBBASEFEE
	SlotNumber       uint64           // Provides information for SLOTNUM
	CostPerStateByte uint64           // Holds the calculated cost per state byte for the given block
}

// TxContext provides the EVM with information about a transaction.
// All fields can change between transactions.
type TxContext struct {
	// Message information
	TxHash     common.Hash
	Origin     accounts.Address // Provides information for ORIGIN
	GasPrice   uint256.Int      // Provides information for GASPRICE
	BlobFee    uint256.Int      // The fee for blobs(blobGas * blobGasPrice) incurred in the txn
	BlobHashes []common.Hash    // Provides versioned blob hashes for BLOBHASH
}

// ExecutionResult includes all output after executing given evm
// message no matter the execution itself is successful or not.
type ExecutionResult struct {
	ReceiptGasUsed       uint64 // Gas used by the transaction with refunds (what the user pays) - see EIP-7778
	BlockGasUsed         uint64 // Gas used for block limit accounting - see EIP-7778
	Err                  error  // Any error encountered during the execution(listed in core/vm/errors.go)
	Reverted             bool   // Whether the execution was aborted by `REVERT`
	ReturnData           []byte // Returned data from evm(function result or data supplied with revert opcode)
	SenderInitBalance    uint256.Int
	CoinbaseInitBalance  uint256.Int
	FeeTipped            uint256.Int
	FeeBurnt             uint256.Int
	BurntContractAddress accounts.Address

	// SelfDestructedWithBalance holds accounts that were selfdestructed during
	// execution but received ETH after the SELFDESTRUCT opcode ran (EIP-7708).
	// Captured before SoftFinalise clears the journal.
	SelfDestructedWithBalance []AddressAndBalance
}

// Unwrap returns the internal evm error which allows us for further
// analysis outside.
func (result *ExecutionResult) Unwrap() error {
	return result.Err
}

// Failed returns the indicator whether the execution is successful or not
func (result *ExecutionResult) Failed() bool { return result.Err != nil }

// Return is a helper function to help caller distinguish between revert reason
// and function return. Return returns the data after execution if no error occurs.
func (result *ExecutionResult) Return() []byte {
	if result.Err != nil {
		return nil
	}
	return common.Copy(result.ReturnData)
}

// Revert returns the concrete revert reason if the execution is aborted by `REVERT`
// opcode. Note the reason can be nil if no data supplied with revert opcode.
func (result *ExecutionResult) Revert() []byte {
	if !result.Reverted {
		return nil
	}
	return common.Copy(result.ReturnData)
}

type (
	// CanTransferFunc is the signature of a transfer guard function
	CanTransferFunc func(IntraBlockState, accounts.Address, uint256.Int) (bool, error)

	// TransferFunc is the signature of a transfer function
	TransferFunc func(IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error

	// GetHashFunc returns the nth block hash in the blockchain
	// and is used by the BLOCKHASH EVM op code.
	GetHashFunc func(uint64) (common.Hash, error)

	// PostApplyMessageFunc is an extension point to execute custom logic at the end of core.ApplyMessage.
	// It's used in Bor for AddFeeTransferLog or in ethereum to clear out the authority code at end of tx.
	PostApplyMessageFunc func(ibs IntraBlockState, sender accounts.Address, coinbase accounts.Address, result *ExecutionResult, chainRules *chain.Rules)
)

type AddressAndBalance struct {
	Address common.Address
	Balance uint256.Int
}

// IntraBlockState is an EVM database for full state querying.
type IntraBlockState interface {
	SubBalance(accounts.Address, uint256.Int, tracing.BalanceChangeReason) error
	AddBalance(accounts.Address, uint256.Int, tracing.BalanceChangeReason) error
	GetBalance(accounts.Address) (uint256.Int, error)

	GetRemovedAccountsWithBalance() []AddressAndBalance

	AddLog(*types.Log)

	SetHooks(hooks *tracing.Hooks)
	Trace() bool
	BlockNumber() uint64
	TxIndex() int
	Incarnation() int
}
