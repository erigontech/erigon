package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// VersionedAccountView is an accounts.AccountView that composes each account
// field on demand from the versionMap at a fixed txIndex, falling back to a
// lazily-loaded base (versionMap AddressPath record, then the committed
// stateReader). It is the read-path dual of the per-tx versionMap slice: a
// view over the cells rather than a materialized copy.
//
// It mirrors versionedStateReader.ReadAccountData's composition
// (applyVersionedUpdates) but resolves per field lazily, so a consumer reading
// only one field pays for only that cell. The base is loaded at most once.
type VersionedAccountView struct {
	addr  accounts.Address
	txIdx int
	vm    *VersionMap
	base  StateReader

	baseLoaded bool
	baseAcc    *accounts.Account
	baseErr    error
}

func NewVersionedAccountView(addr accounts.Address, txIdx int, vm *VersionMap, base StateReader) *VersionedAccountView {
	return &VersionedAccountView{addr: addr, txIdx: txIdx, vm: vm, base: base}
}

// destroyed reports whether a prior tx self-destructed this account with no
// re-creation at a higher txIndex — in which case the account does not exist
// and every field composes to zero.
func (v *VersionedAccountView) destroyed() bool {
	if v.vm == nil {
		return false
	}
	d, _, revived := v.vm.AccountLifecycle(v.addr, v.txIdx)
	return d && !revived
}

func (v *VersionedAccountView) ensureBase() {
	if v.baseLoaded {
		return
	}
	v.baseLoaded = true
	if v.vm != nil {
		if acc, ok := versionedUpdateAddress(v.vm, v.addr, v.txIdx); ok && acc != nil {
			v.baseAcc = acc
			return
		}
	}
	if v.base != nil {
		v.baseAcc, v.baseErr = v.base.ReadAccountData(v.addr)
	}
}

func (v *VersionedAccountView) GetBalance() uint256.Int {
	if v.destroyed() {
		return uint256.Int{}
	}
	if v.vm != nil {
		if u, ok := versionedUpdateBalance(v.vm, v.addr, v.txIdx); ok {
			return u
		}
	}
	v.ensureBase()
	if v.baseAcc != nil {
		return v.baseAcc.Balance
	}
	return uint256.Int{}
}

func (v *VersionedAccountView) GetNonce() uint64 {
	if v.destroyed() {
		return 0
	}
	if v.vm != nil {
		if u, ok := versionedUpdateNonce(v.vm, v.addr, v.txIdx); ok {
			return u
		}
	}
	v.ensureBase()
	if v.baseAcc != nil {
		return v.baseAcc.Nonce
	}
	return 0
}

func (v *VersionedAccountView) GetCodeHash() accounts.CodeHash {
	if v.destroyed() {
		return accounts.EmptyCodeHash
	}
	if v.vm != nil {
		if u, ok := versionedUpdateCodeHash(v.vm, v.addr, v.txIdx); ok {
			return u
		}
	}
	v.ensureBase()
	if v.baseAcc != nil {
		return v.baseAcc.CodeHash
	}
	// No base account: mirror the synth path (accounts.Account{}), whose
	// CodeHash is the zero value — not EmptyCodeHash.
	return accounts.CodeHash{}
}

func (v *VersionedAccountView) GetIncarnation() uint64 {
	if v.destroyed() {
		return 0
	}
	if v.vm != nil {
		if u, ok := versionedUpdateIncarnation(v.vm, v.addr, v.txIdx); ok {
			return u
		}
	}
	v.ensureBase()
	if v.baseAcc != nil {
		return v.baseAcc.Incarnation
	}
	return 0
}

// exists reports whether the account exists at txIdx — i.e. the materialized
// reader would return non-nil: not destroyed, and either some field cell was
// written at ≤ txIdx or a base account is present.
func (v *VersionedAccountView) exists() bool {
	if v.destroyed() {
		return false
	}
	if v.vm != nil {
		if _, ok := versionedUpdateBalance(v.vm, v.addr, v.txIdx); ok {
			return true
		}
		if _, ok := versionedUpdateNonce(v.vm, v.addr, v.txIdx); ok {
			return true
		}
		if _, ok := versionedUpdateIncarnation(v.vm, v.addr, v.txIdx); ok {
			return true
		}
		if _, ok := versionedUpdateCodeHash(v.vm, v.addr, v.txIdx); ok {
			return true
		}
	}
	v.ensureBase()
	return v.baseAcc != nil
}

func (v *VersionedAccountView) IsEmptyCodeHash() bool {
	// A non-existent account has no code. An existing account with an
	// uninitialized zero CodeHash reports non-empty, matching *Account.
	if !v.exists() {
		return true
	}
	return v.GetCodeHash() == accounts.EmptyCodeHash
}

func (v *VersionedAccountView) Empty() bool {
	bal := v.GetBalance()
	return v.GetNonce() == 0 && bal.IsZero() && v.IsEmptyCodeHash()
}

var _ accounts.AccountView = (*VersionedAccountView)(nil)
