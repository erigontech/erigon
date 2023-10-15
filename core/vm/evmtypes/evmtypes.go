package evmtypes

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/core/types"
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
	Selfdestruct6780(common.Address)

	// Exist reports whether the given account exists in state.
	// Notably this should also return true for suicided accounts.
	Exist(common.Address) bool
	// Empty returns whether the given account is empty. Empty
	// is defined according to EIP161 (balance = nonce = code = 0).
	Empty(common.Address) bool

	Prepare(rules *chain.Rules, sender, coinbase common.Address, dest *common.Address,
		precompiles []common.Address, txAccesses types2.AccessList)

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
