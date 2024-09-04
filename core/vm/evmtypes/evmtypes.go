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
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	types2 "github.com/erigontech/erigon-lib/types"

	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
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
	Coinbase    common.Address // Provides information for COINBASE
	GasLimit    uint64         // Provides information for GASLIMIT
	MaxGasLimit bool           // Use GasLimit override for 2^256-1 (to be compatible with OpenEthereum's trace_call)
	BlockNumber uint64         // Provides information for NUMBER
	Time        uint64         // Provides information for TIME
	Difficulty  *big.Int       // Provides information for DIFFICULTY
	BaseFee     *uint256.Int   // Provides information for BASEFEE
	PrevRanDao  *common.Hash   // Provides information for PREVRANDAO
	BlobBaseFee *uint256.Int   // Provides information for BLOBBASEFEE
}

// TxContext provides the EVM with information about a transaction.
// All fields can change between transactions.
type TxContext struct {
	// Message information
	TxHash     common.Hash
	Origin     common.Address // Provides information for ORIGIN
	GasPrice   *uint256.Int   // Provides information for GASPRICE
	BlobFee    *uint256.Int   // The fee for blobs(blobGas * blobGasPrice) incurred in the txn
	BlobHashes []common.Hash  // Provides versioned blob hashes for BLOBHASH
}

// ExecutionResult includes all output after executing given evm
// message no matter the execution itself is successful or not.
type ExecutionResult struct {
	UsedGas             uint64 // Total used gas but include the refunded gas
	Err                 error  // Any error encountered during the execution(listed in core/vm/errors.go)
	Reverted            bool   // Whether the execution was aborted by `REVERT`
	ReturnData          []byte // Returned data from evm(function result or data supplied with revert opcode)
	SenderInitBalance   *uint256.Int
	CoinbaseInitBalance *uint256.Int
	FeeTipped           *uint256.Int
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
	return common.CopyBytes(result.ReturnData)
}

// Revert returns the concrete revert reason if the execution is aborted by `REVERT`
// opcode. Note the reason can be nil if no data supplied with revert opcode.
func (result *ExecutionResult) Revert() []byte {
	if !result.Reverted {
		return nil
	}
	return common.CopyBytes(result.ReturnData)
}

type (
	// CanTransferFunc is the signature of a transfer guard function
	CanTransferFunc func(IntraBlockState, common.Address, *uint256.Int) bool

	// TransferFunc is the signature of a transfer function
	TransferFunc func(IntraBlockState, common.Address, common.Address, *uint256.Int, bool)

	// GetHashFunc returns the nth block hash in the blockchain
	// and is used by the BLOCKHASH EVM op code.
	GetHashFunc func(uint64) common.Hash

	// PostApplyMessageFunc is an extension point to execute custom logic at the end of core.ApplyMessage.
	// It's used in Bor for AddFeeTransferLog or in ethereum to clear out the authority code at end of tx.
	PostApplyMessageFunc func(ibs IntraBlockState, sender common.Address, coinbase common.Address, result *ExecutionResult)
)

// IntraBlockState is an EVM database for full state querying.
type IntraBlockState interface {
	CreateAccount(common.Address, bool)

	SubBalance(common.Address, *uint256.Int, tracing.BalanceChangeReason)
	AddBalance(common.Address, *uint256.Int, tracing.BalanceChangeReason)
	GetBalance(common.Address) *uint256.Int

	GetNonce(common.Address) uint64
	SetNonce(common.Address, uint64)

	GetCodeHash(common.Address) common.Hash
	GetCode(common.Address) []byte
	SetCode(common.Address, []byte)
	GetCodeSize(common.Address) int

	// eip-7702; delegated designations
	ResolveCodeHash(common.Address) common.Hash
	ResolveCode(common.Address) []byte
	ResolveCodeSize(common.Address) int
	GetDelegatedDesignation(common.Address) (common.Address, bool)

	AddRefund(uint64)
	SubRefund(uint64)
	GetRefund() uint64

	GetCommittedState(common.Address, *common.Hash, *uint256.Int)
	GetState(address common.Address, slot *common.Hash, outValue *uint256.Int)
	SetState(common.Address, *common.Hash, uint256.Int)

	GetTransientState(addr common.Address, key common.Hash) uint256.Int
	SetTransientState(addr common.Address, key common.Hash, value uint256.Int)

	Selfdestruct(common.Address) bool
	HasSelfdestructed(common.Address) bool
	Selfdestruct6780(common.Address)

	// Exist reports whether the given account exists in state.
	// Notably this should also return true for suicided accounts.
	Exist(common.Address) bool
	// Empty returns whether the given account is empty. Empty
	// is defined according to EIP161 (balance = nonce = code = 0).
	Empty(common.Address) bool

	Prepare(rules *chain.Rules, sender, coinbase common.Address, dest *common.Address,
		precompiles []common.Address, txAccesses types2.AccessList, authorities []common.Address)

	AddressInAccessList(addr common.Address) bool
	// AddAddressToAccessList adds the given address to the access list. This operation is safe to perform
	// even if the feature/fork is not active yet
	AddAddressToAccessList(addr common.Address) (addrMod bool)
	// AddSlotToAccessList adds the given (address,slot) to the access list. This operation is safe to perform
	// even if the feature/fork is not active yet
	AddSlotToAccessList(addr common.Address, slot common.Hash) (addrMod, slotMod bool)

	RevertToSnapshot(int)
	Snapshot() int

	AddLog(*types.Log)
}
