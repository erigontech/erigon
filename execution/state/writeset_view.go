package state

import (
	"iter"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// WriteSetView is the read-only view of a tx's writes handed to publication
// consumers (the commitment calculator, apply, indexing). It constrains the
// access space to reads — no Set*/Write*/Flush* — so a consumer cannot perturb
// the backing map's write integrity. The mutable *WriteSet satisfies it today
// (transition); the eventual backing is a thin read-only wrapper over the
// tx's versionMap slice.
//
// The iterators still hand out *VersionedWrite pointers to match *WriteSet
// verbatim for the transition; tightening them to value returns (so the
// backing cells can't be mutated through the view) lands with the versionMap
// wrapper backing.
type WriteSetView interface {
	Balances() iter.Seq2[accounts.Address, *VersionedWrite[uint256.Int]]
	Nonces() iter.Seq2[accounts.Address, *VersionedWrite[uint64]]
	Incarnations() iter.Seq2[accounts.Address, *VersionedWrite[uint64]]
	CodeHashes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.CodeHash]]
	Codes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.Code]]
	SelfDestructs() iter.Seq2[accounts.Address, *VersionedWrite[bool]]
	CreateContracts() iter.Seq2[accounts.Address, *VersionedWrite[bool]]
	Storages() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]]
	IsEmpty() bool
	Count() int
}

var _ WriteSetView = (*WriteSet)(nil)
