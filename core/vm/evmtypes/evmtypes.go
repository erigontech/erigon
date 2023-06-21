package evmtypes

import (
	"math/big"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"

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
	Coinbase      common.Address // Provides information for COINBASE
	GasLimit      uint64         // Provides information for GASLIMIT
	MaxGasLimit   bool           // Use GasLimit override for 2^256-1 (to be compatible with OpenEthereum's trace_call)
	BlockNumber   uint64         // Provides information for NUMBER
	Time          uint64         // Provides information for TIME
	Difficulty    *big.Int       // Provides information for DIFFICULTY
	BaseFee       *uint256.Int   // Provides information for BASEFEE
	PrevRanDao    *common.Hash   // Provides information for PREVRANDAO
	ExcessDataGas *uint64        // Provides information for handling data blobs
}

// TxContext provides the EVM with information about a transaction.
// All fields can change between transactions.
type TxContext struct {
	// Message information
	TxHash     common.Hash
	Origin     common.Address // Provides information for ORIGIN
	GasPrice   *uint256.Int   // Provides information for GASPRICE
	DataHashes []common.Hash  // Provides versioned data hashes for DATAHASH
}

type (
	// CanTransferFunc is the signature of a transfer guard function
	CanTransferFunc func(IntraBlockState, common.Address, *uint256.Int) bool
	// TransferFunc is the signature of a transfer function
	TransferFunc func(IntraBlockState, common.Address, common.Address, *uint256.Int, bool)
	// GetHashFunc returns the nth block hash in the blockchain
	// and is used by the BLOCKHASH EVM op code.
	GetHashFunc func(uint64) common.Hash
)

// IntraBlockState is an EVM database for full state querying.
type IntraBlockState interface {
	CreateAccount(common.Address, bool)

	SubBalance(common.Address, *uint256.Int)
	AddBalance(common.Address, *uint256.Int)
	GetBalance(common.Address) *uint256.Int

	GetNonce(common.Address) uint64
	SetNonce(common.Address, uint64)

	GetCodeHash(common.Address) common.Hash
	GetCode(common.Address) []byte
	SetCode(common.Address, []byte)
	GetCodeSize(common.Address) int

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

	// Exist reports whether the given account exists in state.
	// Notably this should also return true for suicided accounts.
	Exist(common.Address) bool
	// Empty returns whether the given account is empty. Empty
	// is defined according to EIP161 (balance = nonce = code = 0).
	Empty(common.Address) bool

	Prepare(rules *chain.Rules, sender, coinbase common.Address, dest *common.Address,
		precompiles []common.Address, txAccesses types2.AccessList)

	AddressInAccessList(addr common.Address) bool
	SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool)
	// AddAddressToAccessList adds the given address to the access list. This operation is safe to perform
	// even if the feature/fork is not active yet
	AddAddressToAccessList(addr common.Address)
	// AddSlotToAccessList adds the given (address,slot) to the access list. This operation is safe to perform
	// even if the feature/fork is not active yet
	AddSlotToAccessList(addr common.Address, slot common.Hash)

	RevertToSnapshot(int)
	Snapshot() int

	AddLog(*types.Log)
}
