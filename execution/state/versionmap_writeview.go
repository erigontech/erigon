package state

import (
	"iter"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// versionMapWriteView is a read-only WriteSetView over a tx's versionMap slice.
// The key-set (which cells the tx wrote) comes from keys; the values are read
// from the versionMap floor at the tx's txIndex — the validated single source
// of truth — rather than from a copied WriteSet. Yielded VersionedWrite values
// are fresh, never pointers into the map, so a consumer cannot mutate the map
// through the view, and it exposes no Set*/Write*/Flush*.
type versionMapWriteView struct {
	keys  WriteSetView
	vm    *VersionMap
	txIdx int
}

// NewVersionMapWriteView wraps the tx's key-set + versionMap as a read-only
// WriteSetView whose values come from the map. Reads use floor at txIdx+1 so
// they include the tx's OWN write at txIdx (readFloor descends from txIdx-1, so
// the reader convention at txIdx yields the pre-tx state; the writer/publication
// convention here wants the tx's produced values — matching normalize's
// SetAccountFieldFromMap(..., txIndex+1)).
func NewVersionMapWriteView(keys WriteSetView, vm *VersionMap, txIdx int) WriteSetView {
	return &versionMapWriteView{keys: keys, vm: vm, txIdx: txIdx}
}

func (v *versionMapWriteView) Balances() iter.Seq2[accounts.Address, *VersionedWrite[uint256.Int]] {
	return func(yield func(accounts.Address, *VersionedWrite[uint256.Int]) bool) {
		for addr := range v.keys.Balances() {
			val, _ := versionedUpdateBalance(v.vm, addr, v.txIdx+1)
			if !yield(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Nonces() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return func(yield func(accounts.Address, *VersionedWrite[uint64]) bool) {
		for addr := range v.keys.Nonces() {
			val, _ := versionedUpdateNonce(v.vm, addr, v.txIdx+1)
			if !yield(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Incarnations() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return func(yield func(accounts.Address, *VersionedWrite[uint64]) bool) {
		for addr := range v.keys.Incarnations() {
			val, _ := versionedUpdateIncarnation(v.vm, addr, v.txIdx+1)
			if !yield(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) CodeHashes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.CodeHash]] {
	return func(yield func(accounts.Address, *VersionedWrite[accounts.CodeHash]) bool) {
		for addr := range v.keys.CodeHashes() {
			val, _ := versionedUpdateCodeHash(v.vm, addr, v.txIdx+1)
			if !yield(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Codes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.Code]] {
	return func(yield func(accounts.Address, *VersionedWrite[accounts.Code]) bool) {
		for addr := range v.keys.Codes() {
			b, _ := versionedUpdateCode(v.vm, addr, v.txIdx+1)
			if !yield(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath}, Val: accounts.NewCode(b)}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) SelfDestructs() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	return func(yield func(accounts.Address, *VersionedWrite[bool]) bool) {
		for addr := range v.keys.SelfDestructs() {
			val := false
			if sd, res, ok := v.vm.ReadSelfDestruct(addr, v.txIdx+1); ok && res.Status() == MVReadResultDone {
				val = sd
			}
			if !yield(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Storages() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return func(yield func(accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]) bool) {
		for addr, inner := range v.keys.Storages() {
			out := make(map[accounts.StorageKey]*VersionedWrite[uint256.Int], len(inner))
			for key := range inner {
				val, _ := versionedUpdateStorage(v.vm, addr, key, v.txIdx+1)
				out[key] = &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key}, Val: val}
			}
			if !yield(addr, out) {
				return
			}
		}
	}
}

var _ WriteSetView = (*versionMapWriteView)(nil)
