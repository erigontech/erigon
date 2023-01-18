package evmtypes

import (
	"math/big"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
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
	Coinbase    libcommon.Address // Provides information for COINBASE
	GasLimit    uint64            // Provides information for GASLIMIT
	MaxGasLimit bool              // Use GasLimit override for 2^256-1 (to be compatible with OpenEthereum's trace_call)
	BlockNumber uint64            // Provides information for NUMBER
	Time        uint64            // Provides information for TIME
	Difficulty  *big.Int          // Provides information for DIFFICULTY
	BaseFee     *uint256.Int      // Provides information for BASEFEE
	PrevRanDao  *libcommon.Hash   // Provides information for PREVRANDAO
}

// TxContext provides the EVM with information about a transaction.
// All fields can change between transactions.
type TxContext struct {
	// Message information
	TxHash   libcommon.Hash
	Origin   libcommon.Address // Provides information for ORIGIN
	GasPrice *uint256.Int      // Provides information for GASPRICE
}

type (
	// CanTransferFunc is the signature of a transfer guard function
	CanTransferFunc func(IntraBlockState, libcommon.Address, *uint256.Int) bool
	// TransferFunc is the signature of a transfer function
	TransferFunc func(IntraBlockState, libcommon.Address, libcommon.Address, *uint256.Int, bool)
	// GetHashFunc returns the nth block hash in the blockchain
	// and is used by the BLOCKHASH EVM op code.
	GetHashFunc func(uint64) libcommon.Hash
)

// IntraBlockState is an EVM database for full state querying.
type IntraBlockState interface {
	CreateAccount(libcommon.Address, bool)

	SubBalance(libcommon.Address, *uint256.Int)
	AddBalance(libcommon.Address, *uint256.Int)
	GetBalance(libcommon.Address) *uint256.Int

	GetNonce(libcommon.Address) uint64
	SetNonce(libcommon.Address, uint64)

	GetCodeHash(libcommon.Address) libcommon.Hash
	GetCode(libcommon.Address) []byte
	SetCode(libcommon.Address, []byte)
	GetCodeSize(libcommon.Address) int

	AddRefund(uint64)
	SubRefund(uint64)
	GetRefund() uint64

	GetCommittedState(libcommon.Address, *libcommon.Hash, *uint256.Int)
	GetState(address libcommon.Address, slot *libcommon.Hash, outValue *uint256.Int)
	SetState(libcommon.Address, *libcommon.Hash, uint256.Int)

	Selfdestruct(libcommon.Address) bool
	HasSelfdestructed(libcommon.Address) bool

	// Exist reports whether the given account exists in state.
	// Notably this should also return true for suicided accounts.
	Exist(libcommon.Address) bool
	// Empty returns whether the given account is empty. Empty
	// is defined according to EIP161 (balance = nonce = code = 0).
	Empty(libcommon.Address) bool

	PrepareAccessList(sender libcommon.Address, dest *libcommon.Address, precompiles []libcommon.Address, txAccesses types.AccessList)
	AddressInAccessList(addr libcommon.Address) bool
	SlotInAccessList(addr libcommon.Address, slot libcommon.Hash) (addressOk bool, slotOk bool)
	// AddAddressToAccessList adds the given address to the access list. This operation is safe to perform
	// even if the feature/fork is not active yet
	AddAddressToAccessList(addr libcommon.Address)
	// AddSlotToAccessList adds the given (address,slot) to the access list. This operation is safe to perform
	// even if the feature/fork is not active yet
	AddSlotToAccessList(addr libcommon.Address, slot libcommon.Hash)

	RevertToSnapshot(int)
	Snapshot() int

	AddLog(*types.Log)
}
