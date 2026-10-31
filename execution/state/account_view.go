package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// AccountBaseReader supplies the committed base account for fields the versionMap
// has no cell for. It is the only capability the view needs from its base, so any
// StateReader satisfies it — as does a lighter domain-only reader the commitment
// calculator holds, which avoids forcing a full StateReader at the call site.
type AccountBaseReader interface {
	ReadAccountData(accounts.Address) (*accounts.Account, error)
}

// StorageBaseReader supplies a committed slot value for slots the versionMap has no
// cell for — the storage counterpart of AccountBaseReader, used by VersionMapView's
// cross-block fallback where the touched union spans earlier blocks' writes.
type StorageBaseReader interface {
	ReadStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error)
}

// VersionedAccountView is an accounts.AccountView composing each account field on
// demand from the versionMap at a fixed txIndex, falling back to a lazily-loaded
// base. A consumer reading only one field pays for only that cell; the base is
// loaded at most once.
type VersionedAccountView struct {
	addr  accounts.Address
	txIdx int
	vm    *VersionMap
	base  AccountBaseReader

	baseLoaded bool
	baseAcc    *accounts.Account
	baseErr    error
}

func NewVersionedAccountView(addr accounts.Address, txIdx int, vm *VersionMap, base AccountBaseReader) *VersionedAccountView {
	return &VersionedAccountView{addr: addr, txIdx: txIdx, vm: vm, base: base}
}

// destroyed reports whether a prior tx self-destructed this account with no
// re-creation at a higher txIndex — in which case the account does not exist
// and every field composes to zero.
func (v *VersionedAccountView) destroyed() bool {
	if v.vm == nil {
		return false
	}
	return v.vm.IsNetAbsent(v.addr, v.txIdx)
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

// Err reports an error from the lazily-loaded base read. It forces the base
// load so callers can bail before trusting the composed (zeroed-on-error)
// fields.
func (v *VersionedAccountView) Err() error {
	v.ensureBase()
	return v.baseErr
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
	// No base account: mirror accounts.Account{}, whose CodeHash zero value is
	// not EmptyCodeHash.
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

func (v *VersionedAccountView) GetCode() ([]byte, error) {
	if v.destroyed() {
		return nil, nil
	}
	if v.vm != nil {
		if code, ok := versionedUpdateCode(v.vm, v.addr, v.txIdx); ok {
			return code.Bytes, nil
		}
	}
	// Code bytes live beyond the minimal AccountBaseReader (GetAccount never needs
	// them); a base that can serve them implements ReadAccountCode.
	if cr, ok := v.base.(interface {
		ReadAccountCode(accounts.Address) ([]byte, error)
	}); ok {
		return cr.ReadAccountCode(v.addr)
	}
	return nil, nil
}

// exists reports whether the account exists at txIdx — i.e. the materialized
// reader would return non-nil: not destroyed, and either some field cell was
// written at ≤ txIdx or a base account is present.
func (v *VersionedAccountView) exists() bool {
	if v.destroyed() {
		return false
	}
	return v.recordExists()
}

// recordExists reports whether any record of the account is present in the
// cascade — a version-map field cell, the version-map address cell, or the base —
// ignoring lifecycle. It is the data-only existence the commitment read uses;
// exists() layers the lifecycle destroyed() verdict on top for execution reads.
func (v *VersionedAccountView) recordExists() bool {
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

// Account composes the whole account at txIdx from the versionMap (seeded origin
// base plus field cells below txIdx), or nil if it does not exist there. Correct
// only when every account written this block has its origin seeded.
func (v *VersionedAccountView) Account() *accounts.Account {
	if !v.exists() {
		return nil
	}
	return v.compose()
}

// AccountForCommitment composes the account with data-only existence: present
// unless its record is gone from the whole cascade, ignoring the lifecycle
// destroyed() verdict exec reads. The caller applies the ValueDeleted removal.
func (v *VersionedAccountView) AccountForCommitment() *accounts.Account {
	if !v.recordExists() {
		return nil
	}
	return v.compose()
}

// compose overlays the version-map field cells on the base record, reading raw
// cells (no destroyed() gate) — the caller has already decided existence, so a
// destroyed-but-preserved account (EIP-8246 no-burn) keeps its surviving fields.
// For a non-destroyed account this equals the lifecycle getters. A field with no
// cell keeps the base value, or NewAccount's default (EmptyCodeHash for codeless).
func (v *VersionedAccountView) compose() *accounts.Account {
	v.ensureBase()
	acc := accounts.NewAccount()
	if v.baseAcc != nil {
		acc = *v.baseAcc
	}
	if v.vm != nil {
		if u, _, ok := v.vm.ReadBalance(v.addr, v.txIdx); ok {
			acc.Balance = u
		}
		if u, _, ok := v.vm.ReadNonce(v.addr, v.txIdx); ok {
			acc.Nonce = u
		}
		if u, _, ok := v.vm.ReadIncarnation(v.addr, v.txIdx); ok {
			acc.Incarnation = u
		}
		if ch, _, ok := v.vm.ReadCodeHash(v.addr, v.txIdx); ok {
			acc.CodeHash = ch
		}
	}
	return &acc
}

// GetAccount returns the account composed at txIdx and whether it exists there, in
// comma-ok form. A false ok means the account is deleted/absent (self-destructed with
// no revival, or removed as an EIP-161 empty via the self-destruct-encoded write), so
// the caller removes rather than writes its leaf — no separate emptiness check needed.
func (v *VersionedAccountView) GetAccount() (accounts.Account, bool) {
	acc := v.Account()
	if acc == nil {
		return accounts.Account{}, false
	}
	return *acc, true
}

var _ accounts.AccountView = (*VersionedAccountView)(nil)
