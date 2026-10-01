package state

import (
	"iter"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// versionMapWriteView is a read-only WriteSetView over a tx's versionMap slice.
// The key-set (which cells the tx wrote) comes from keys; the values are read
// from the versionMap floor at the tx's txIndex rather than from a copied
// WriteSet. Yielded VersionedWrite values are fresh, never pointers into the
// map, so a consumer cannot mutate the map through the view.
type versionMapWriteView struct {
	keys  WriteSetView
	vm    *VersionMap
	txIdx int
}

// NewVersionMapWriteView wraps the tx's key-set + versionMap as a read-only
// WriteSetView whose values come from the map. Reads use floor at txIdx+1 so they
// include the tx's OWN write at txIdx (the reader convention at txIdx would yield
// the pre-tx state; this publication view wants the tx's produced values).
func NewVersionMapWriteView(keys WriteSetView, vm *VersionMap, txIdx int) WriteSetView {
	return &versionMapWriteView{keys: keys, vm: vm, txIdx: txIdx}
}

func (v *versionMapWriteView) Balances() iter.Seq2[accounts.Address, *VersionedWrite[uint256.Int]] {
	return func(yield func(accounts.Address, *VersionedWrite[uint256.Int]) bool) {
		for addr, kw := range v.keys.Balances() {
			val, ok := versionedUpdateBalance(v.vm, addr, v.txIdx+1)
			if !ok {
				val = kw.Val
			}
			if !yield(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Nonces() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return func(yield func(accounts.Address, *VersionedWrite[uint64]) bool) {
		for addr, kw := range v.keys.Nonces() {
			val, ok := versionedUpdateNonce(v.vm, addr, v.txIdx+1)
			if !ok {
				val = kw.Val
			}
			if !yield(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Incarnations() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return func(yield func(accounts.Address, *VersionedWrite[uint64]) bool) {
		for addr, kw := range v.keys.Incarnations() {
			val, ok := versionedUpdateIncarnation(v.vm, addr, v.txIdx+1)
			if !ok {
				val = kw.Val
			}
			if !yield(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) CodeHashes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.CodeHash]] {
	return func(yield func(accounts.Address, *VersionedWrite[accounts.CodeHash]) bool) {
		for addr, kw := range v.keys.CodeHashes() {
			val, ok := versionedUpdateCodeHash(v.vm, addr, v.txIdx+1)
			if !ok {
				val = kw.Val
			}
			if !yield(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Codes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.Code]] {
	return func(yield func(accounts.Address, *VersionedWrite[accounts.Code]) bool) {
		for addr, kw := range v.keys.Codes() {
			code := kw.Val
			if b, ok := versionedUpdateCode(v.vm, addr, v.txIdx+1); ok {
				code = accounts.NewCode(b)
			}
			if !yield(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath}, Val: code}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) SelfDestructs() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	return func(yield func(accounts.Address, *VersionedWrite[bool]) bool) {
		for addr, kw := range v.keys.SelfDestructs() {
			val := kw.Val
			if sd, res, ok := v.vm.ReadSelfDestruct(addr, v.txIdx+1); ok && res.resolved() {
				val = sd
			}
			if !yield(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath}, Val: val}) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) CreateContracts() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	return v.keys.CreateContracts()
}

func (v *versionMapWriteView) IsEmpty() bool {
	return v.keys.IsEmpty()
}

func (v *versionMapWriteView) Count() int {
	return v.keys.Count()
}

func (v *versionMapWriteView) Storages() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return func(yield func(accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]) bool) {
		// The map and the writes it points at are scratch, reused for every address:
		// a consumer must read what it needs inside the loop body and retain neither.
		// Allocating per address made this the hot path of storage-heavy blocks.
		out := map[accounts.StorageKey]*VersionedWrite[uint256.Int]{}
		var scratch []VersionedWrite[uint256.Int]
		for addr, inner := range v.keys.Storages() {
			clear(out)
			if cap(scratch) < len(inner) {
				scratch = make([]VersionedWrite[uint256.Int], len(inner))
			}
			scratch = scratch[:len(inner)]
			i := 0
			for key, kw := range inner {
				val, ok := versionedUpdateStorage(v.vm, addr, key, v.txIdx+1)
				if !ok {
					val = kw.Val
				}
				scratch[i] = VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key}, Val: val}
				out[key] = &scratch[i]
				i++
			}
			if !yield(addr, out) {
				return
			}
		}
	}
}

// StoragesChanged drops the writes whose final value equals what this tx would
// have read before it ran: they leave the domain value untouched, so passing
// them on makes commitment refold a leaf that did not change. Only the domain
// and commitment paths may use it — the access list and the notification
// accumulator must still see every write.
func (v *versionMapWriteView) StoragesChanged() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return func(yield func(accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]) bool) {
		// An account this tx created or reincarnated has its storage wiped, so the
		// origin below it is the pre-creation snapshot and every write is a real
		// change however it compares. SetCode states the same rule for code via
		// newlyCreated.
		reset := map[accounts.Address]struct{}{}
		for addr := range v.keys.CreateContracts() {
			reset[addr] = struct{}{}
		}
		for addr := range v.keys.Incarnations() {
			reset[addr] = struct{}{}
		}

		for addr, inner := range v.Storages() {
			if _, keepAll := reset[addr]; !keepAll {
				// A destruct wiped the account's storage, so a slot's baseline is zero
				// rather than the cell predating the destruct, and a write-back of the
				// pre-destruct value is a real change.
				lifecycle, _, destroyedAt := v.vm.AccountLifecycleAt(addr, v.txIdx)
				destructed := lifecycle != LifecycleLive
				for key, w := range inner {
					originVal, origin, originOK := v.vm.ReadStorage(addr, key, v.txIdx)
					if originOK && origin.Status() == MVReadResultDone &&
						!(destructed && destroyedAt > origin.Version().TxIndex) &&
						w.Val.Eq(&originVal) {
						delete(inner, key)
					}
				}
			}
			if len(inner) == 0 {
				continue
			}
			if !yield(addr, inner) {
				return
			}
		}
	}
}

var _ WriteSetView = (*versionMapWriteView)(nil)
