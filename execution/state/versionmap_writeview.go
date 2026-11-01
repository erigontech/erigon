package state

import (
	"iter"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// versionMapWriteView is a read-only WriteSetView over a tx's versionMap slice.
// The key-set (which cells the tx wrote) comes from keys; the values are read
// from the versionMap floor at the tx's txIndex rather than from a copied
// WriteSet. Storage values come from keys: a storage cell at txIndex holds the
// key-set's value for every write the flush materializes, and a net-zero write on
// a BAL block has no cell. Yielded VersionedWrite values are reused per element, never pointers
// into the map, so a consumer cannot mutate the map through the view.
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
		var vw VersionedWrite[uint256.Int]
		for addr, kw := range v.keys.Balances() {
			vw = VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, valStatus: kw.valStatus}, Val: kw.Val}
			if val, res, ok := v.vm.ReadBalance(addr, v.txIdx+1); ok && res.Status() != MVReadResultNone {
				vw.Val, vw.valStatus = val, res.ValStatus()
			}
			if !yield(addr, &vw) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Nonces() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return func(yield func(accounts.Address, *VersionedWrite[uint64]) bool) {
		var vw VersionedWrite[uint64]
		for addr, kw := range v.keys.Nonces() {
			val, ok := versionedUpdateNonce(v.vm, addr, v.txIdx+1)
			if !ok {
				val = kw.Val
			}
			vw = VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: val}
			if !yield(addr, &vw) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Incarnations() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return func(yield func(accounts.Address, *VersionedWrite[uint64]) bool) {
		var vw VersionedWrite[uint64]
		for addr, kw := range v.keys.Incarnations() {
			val, ok := versionedUpdateIncarnation(v.vm, addr, v.txIdx+1)
			if !ok {
				val = kw.Val
			}
			vw = VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath}, Val: val}
			if !yield(addr, &vw) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) CodeHashes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.CodeHash]] {
	return func(yield func(accounts.Address, *VersionedWrite[accounts.CodeHash]) bool) {
		var vw VersionedWrite[accounts.CodeHash]
		for addr, kw := range v.keys.CodeHashes() {
			val, ok := versionedUpdateCodeHash(v.vm, addr, v.txIdx+1)
			if !ok {
				val = kw.Val
			}
			vw = VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath}, Val: val}
			if !yield(addr, &vw) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) Codes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.Code]] {
	return func(yield func(accounts.Address, *VersionedWrite[accounts.Code]) bool) {
		var vw VersionedWrite[accounts.Code]
		for addr, kw := range v.keys.Codes() {
			code := kw.Val
			if val, ok := versionedUpdateCode(v.vm, addr, v.txIdx+1); ok {
				code = val
			}
			vw = VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath}, Val: code}
			if !yield(addr, &vw) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) SelfDestructs() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	return func(yield func(accounts.Address, *VersionedWrite[bool]) bool) {
		var vw VersionedWrite[bool]
		for addr, kw := range v.keys.SelfDestructs() {
			val := kw.Val
			if sd, res, ok := v.vm.ReadSelfDestruct(addr, v.txIdx+1); ok && res.resolved() {
				val = sd
				if sd {
					// Take the lifecycle verdict (WasDestroyed), not the raw cell — a retracted
					// destruct stays live, so apply must not wipe it. On a map miss the key-set
					// value stands, so an uncelled delete is not dropped.
					st, _, _, _ := v.vm.AccountLifecycleAt(addr, v.txIdx+1)
					val = st != LifecycleLive
				}
			}
			vw = VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath}, Val: val}
			if !yield(addr, &vw) {
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
			for key := range inner {
				val, _ := versionedUpdateStorage(v.vm, addr, key, v.txIdx+1)
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

// StorageChanges drops the no-op writes — those the writing tx stamped
// ValueUnchanged because their value equalled what the tx read, so they leave the
// domain/commitment leaf untouched. The status is decided at write time from the
// tx's own (lifecycle-aware) prior read, so there is no baseline to re-derive here
// and a destruct needs no special case. Only the domain and commitment paths may
// use it — the access list and notification accumulator must see every write.
func (v *versionMapWriteView) StorageChanges() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return func(yield func(accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]) bool) {
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
				if kw.valStatus == ValueUnchanged {
					continue
				}
				val, _ := versionedUpdateStorage(v.vm, addr, key, v.txIdx+1)
				scratch[i] = VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key}, Val: val}
				out[key] = &scratch[i]
				i++
			}
			if len(out) == 0 {
				continue
			}
			if !yield(addr, out) {
				return
			}
		}
	}
}

func (v *versionMapWriteView) AccountChanges() iter.Seq[accounts.Address] {
	return accountChangeAddrs(v)
}

var _ WriteSetView = (*versionMapWriteView)(nil)

// WithoutVanished returns ws without the accounts tx txIdx created and self-destructed
// that did not exist before it: they leave no state, so publishing their deletes would
// only cost domain lookups and commitment work. preBlock reads the state before the block.
func WithoutVanished(ws *WriteSet, vm *VersionMap, txIdx int, eip8246 bool, preBlock StateReader) (*WriteSet, error) {
	var vanished map[accounts.Address]bool
	for addr := range ws.SelfDestructs() {
		gone, err := vanishedInTx(ws, vm, addr, txIdx, eip8246, preBlock)
		if err != nil {
			return nil, err
		}
		if gone {
			if vanished == nil {
				vanished = map[accounts.Address]bool{}
			}
			vanished[addr] = true
		}
	}
	if vanished == nil {
		return ws, nil
	}
	return ws.Filter(func(h WriteHeader) bool { return !vanished[h.Address] }), nil
}

func vanishedInTx(ws *WriteSet, vm *VersionMap, addr accounts.Address, txIdx int, eip8246 bool, preBlock StateReader) (bool, error) {
	if sd, res, ok := vm.ReadSelfDestruct(addr, txIdx+1); !ok || !res.resolved() || !sd {
		return false, nil
	}
	if cc, ok := ws.GetCreateContract(addr); !ok || !cc.Val {
		return false, nil
	}
	if bal, res, ok := vm.ReadBalance(addr, txIdx+1); eip8246 && ok && (!res.resolved() || !bal.IsZero()) {
		return false, nil
	}
	// An earlier tx of the block may have funded addr, or before EIP-161 left it empty,
	// where preBlock cannot see it.
	if vm.accountLiveAt(addr, txIdx) {
		return false, nil
	}
	if acc, res, ok := vm.ReadAddress(addr, txIdx); ok && (!res.resolved() || acc != nil) {
		return false, nil
	}
	acc, err := preBlock.ReadAccountData(addr)
	return acc == nil, err
}
