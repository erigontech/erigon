package accounts

import (
	"github.com/holiman/uint256"
)

// AccountView is the read-only account abstraction for in-block readers, letting
// a reader compose account fields on demand from whatever backs it — a
// materialized Account or a versionMap-backed view — without caring which.
// Accessors are Get-prefixed so the concrete Account satisfies it without
// clashing with its public fields.
type AccountView interface {
	GetBalance() uint256.Int
	GetNonce() uint64
	GetCodeHash() CodeHash
	GetIncarnation() uint64
	IsEmptyCodeHash() bool
	Empty() bool
}

func (a *Account) GetBalance() uint256.Int { return a.Balance }
func (a *Account) GetNonce() uint64        { return a.Nonce }
func (a *Account) GetCodeHash() CodeHash   { return a.CodeHash }

var _ AccountView = (*Account)(nil)
