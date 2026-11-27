package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// VersionedAccountView is an accounts.AccountView over the versionMap at a fixed
// txIndex, falling back to base. It composes the account once through the common
// read core (GetAccount) and serves every field accessor from that result.
type VersionedAccountView struct {
	addr  accounts.Address
	txIdx int
	vm    *VersionMap
	base  StateReader

	composed bool
	acc      *accounts.Account
	err      error
}

func NewVersionedAccountView(addr accounts.Address, txIdx int, vm *VersionMap, base StateReader) *VersionedAccountView {
	return &VersionedAccountView{addr: addr, txIdx: txIdx, vm: vm, base: base}
}

// compose resolves the account once through the common read core (GetAccount
// over the version map + base), memoizing the result for the per-field accessors.
func (v *VersionedAccountView) compose() {
	if v.composed {
		return
	}
	v.composed = true
	if v.vm == nil {
		if v.base != nil {
			v.acc, v.err = v.base.ReadAccountData(v.addr)
		}
		return
	}
	v.acc, v.err = v.vm.ViewAtTxIndex(v.txIdx, v.base).ReadAccountData(v.addr)
}

// Err reports a base read error from the composed read; a failed read reports
// absent, so callers must check Err before trusting the zeroed fields.
func (v *VersionedAccountView) Err() error {
	v.compose()
	return v.err
}

func (v *VersionedAccountView) GetBalance() uint256.Int {
	v.compose()
	if v.acc != nil {
		return v.acc.Balance
	}
	return uint256.Int{}
}

func (v *VersionedAccountView) GetNonce() uint64 {
	v.compose()
	if v.acc != nil {
		return v.acc.Nonce
	}
	return 0
}

func (v *VersionedAccountView) GetCodeHash() accounts.CodeHash {
	v.compose()
	if v.acc != nil {
		return v.acc.CodeHash
	}
	return accounts.EmptyCodeHash
}

func (v *VersionedAccountView) GetIncarnation() uint64 {
	v.compose()
	if v.acc != nil {
		return v.acc.Incarnation
	}
	return 0
}

// GetCode reads the bytecode — a read distinct from the account record, but one
// that must agree with GetCodeHash on the EOA/contract verdict.
func (v *VersionedAccountView) GetCode() ([]byte, error) {
	if v.vm == nil {
		if v.base != nil {
			return v.base.ReadAccountCode(v.addr)
		}
		return nil, nil
	}
	return v.vm.ViewAtTxIndex(v.txIdx, v.base).ReadAccountCode(v.addr)
}

func (v *VersionedAccountView) exists() bool {
	v.compose()
	return v.acc != nil
}

func (v *VersionedAccountView) IsEmptyCodeHash() bool {
	if !v.exists() {
		return true
	}
	return v.GetCodeHash() == accounts.EmptyCodeHash
}

func (v *VersionedAccountView) Empty() bool {
	bal := v.GetBalance()
	return v.GetNonce() == 0 && bal.IsZero() && v.IsEmptyCodeHash()
}

// Account returns the composed account at txIdx, or nil if it does not exist there.
func (v *VersionedAccountView) Account() *accounts.Account {
	v.compose()
	if v.acc == nil {
		return nil
	}
	cp := *v.acc
	return &cp
}

var _ accounts.AccountView = (*VersionedAccountView)(nil)
