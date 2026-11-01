package state

import (
	"iter"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// WriteSetView is the read-only view of a tx's writes handed to publication
// consumers (the commitment calculator, apply, indexing), constraining access to
// reads so a consumer cannot perturb the backing map. The mutable *WriteSet
// satisfies it today.
type WriteSetView interface {
	Balances() iter.Seq2[accounts.Address, *VersionedWrite[uint256.Int]]
	Nonces() iter.Seq2[accounts.Address, *VersionedWrite[uint64]]
	Incarnations() iter.Seq2[accounts.Address, *VersionedWrite[uint64]]
	CodeHashes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.CodeHash]]
	Codes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.Code]]
	SelfDestructs() iter.Seq2[accounts.Address, *VersionedWrite[bool]]
	CreateContracts() iter.Seq2[accounts.Address, *VersionedWrite[bool]]
	Storages() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]]
	// StorageChanges is Storages without the writes that leave the stored value
	// as it was. Only the domain and commitment paths may use it.
	StorageChanges() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]]
	// AccountChanges yields each address whose account leaf this write set changes: a
	// balance/nonce/codeHash write whose value equals the committed one (ValueUnchanged)
	// is a no-op and is skipped, mirroring StorageChanges; a create/incarnation/code/
	// self-destruct write is always structural. The commitment folds exactly these, so a
	// bare touch that changes nothing produces no leaf.
	AccountChanges() iter.Seq[accounts.Address]
	IsEmpty() bool
	Count() int
}

var _ WriteSetView = (*WriteSet)(nil)

// accountChangeAddrs is the shared AccountChanges implementation over any view: it reads
// each field's value status, so a net-zero (ValueUnchanged) balance/nonce/codeHash write
// never marks the account changed, while the put-only fields always do.
func accountChangeAddrs(v, structural WriteSetView) iter.Seq[accounts.Address] {
	return func(yield func(accounts.Address) bool) {
		for a, vw := range v.Balances() {
			if vw.valStatus != ValueUnchanged && !yield(a) {
				return
			}
		}
		for a, vw := range v.Nonces() {
			if vw.valStatus != ValueUnchanged && !yield(a) {
				return
			}
		}
		for a, vw := range v.CodeHashes() {
			if vw.valStatus != ValueUnchanged && !yield(a) {
				return
			}
		}
		for a := range structural.Incarnations() {
			if !yield(a) {
				return
			}
		}
		for a := range structural.Codes() {
			if !yield(a) {
				return
			}
		}
		for a := range structural.CreateContracts() {
			if !yield(a) {
				return
			}
		}
		for a := range structural.SelfDestructs() {
			if !yield(a) {
				return
			}
		}
	}
}
