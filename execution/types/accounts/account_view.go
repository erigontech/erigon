package accounts

import (
	"github.com/holiman/uint256"
)

// AccountView is the read-only account abstraction for in-block readers. It
// lets a reader compose account field values on demand from whatever source
// backs it — a materialized Account, or a versionMap-backed view that composes
// each field from the tx's cell / floor / committed state — without the reader
// caring which. The concrete Account stays the materialization at the
// serialize / commitment / RPC boundary; this interface is for the read path.
//
// Accessors are Get-prefixed so the concrete Account satisfies the interface
// without clashing with its public fields.
type AccountView interface {
	GetBalance() uint256.Int
	GetNonce() uint64
	GetCodeHash() CodeHash
	GetIncarnation() uint64
	IsEmptyCodeHash() bool
	Empty() bool
}

// Account satisfies AccountView (GetIncarnation, IsEmptyCodeHash, Empty already
// exist on the concrete type).

func (a *Account) GetBalance() uint256.Int { return a.Balance }
func (a *Account) GetNonce() uint64        { return a.Nonce }
func (a *Account) GetCodeHash() CodeHash   { return a.CodeHash }

var _ AccountView = (*Account)(nil)
