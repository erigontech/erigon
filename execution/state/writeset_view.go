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
	IsEmpty() bool
	Count() int
}

var _ WriteSetView = (*WriteSet)(nil)
