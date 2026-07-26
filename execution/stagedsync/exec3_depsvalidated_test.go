package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func addr(b byte) accounts.Address { return accounts.InternAddress(common.Address{b}) }

// mapReadOn builds a read-set with a single versionMap (MapRead) balance read
// naming predecessor tx `p` at `addr`.
func mapReadOn(a accounts.Address, p int) state.ReadSet {
	var rs state.ReadSet
	rs.SetBalance(a, state.VersionedRead[uint256.Int]{
		ReadHeader: state.ReadHeader{Source: state.MapRead, Version: state.Version{TxIndex: p}},
		Val:        *uint256.NewInt(0),
	})
	return rs
}

func storageReadOn(a accounts.Address, p int) state.ReadSet {
	var rs state.ReadSet
	key := accounts.InternKey(common.Hash{0x01})
	rs.SetStorage(a, key, state.VersionedRead[uint256.Int]{
		ReadHeader: state.ReadHeader{Source: state.StorageRead, Version: state.Version{TxIndex: p}},
		Val:        *uint256.NewInt(0),
	})
	return rs
}

// beWithReads builds a blockExecutor whose only populated fields are blockIO
// (the per-tx read-sets) and validateTasks. `validated` lists txs already
// validated; `reads` maps tx → its recorded read-set.
func beWithReads(nTx int, validated []int, reads map[int]state.ReadSet) *blockExecutor {
	io := &state.VersionedIO{}
	for tx, rs := range reads {
		io.RecordReads(state.Version{TxIndex: tx}, rs)
	}
	be := &blockExecutor{blockIO: io}
	for _, tx := range validated {
		be.validateTasks.setComplete(tx)
	}
	return be
}

func TestDepsValidated_NoReads_Ready(t *testing.T) {
	be := beWithReads(3, nil, nil)
	require.True(t, be.depsValidated(2), "a tx with no recorded reads has no dependencies")
}

func TestDepsValidated_MapReadPredecessorValidated(t *testing.T) {
	be := beWithReads(4, []int{1}, map[int]state.ReadSet{
		3: mapReadOn(addr(0xaa), 1),
	})
	require.True(t, be.depsValidated(3), "predecessor 1 is validated → ready")
}

func TestDepsValidated_MapReadPredecessorNotValidated(t *testing.T) {
	be := beWithReads(4, nil, map[int]state.ReadSet{
		3: mapReadOn(addr(0xaa), 1),
	})
	require.False(t, be.depsValidated(3), "predecessor 1 not validated → blocked")
}

func TestDepsValidated_NonMapReadImposesNoConstraint(t *testing.T) {
	be := beWithReads(4, nil, map[int]state.ReadSet{
		// StorageRead is a base/db read, not a cross-tx versionMap dep.
		3: storageReadOn(addr(0xaa), 1),
	})
	require.True(t, be.depsValidated(3), "non-MapRead source is not a validation dependency")
}

func TestDepsValidated_SelfOrFutureIgnored(t *testing.T) {
	be := beWithReads(4, nil, map[int]state.ReadSet{
		2: mapReadOn(addr(0xaa), 2), // p == tx
		3: mapReadOn(addr(0xaa), 5), // p > tx (out of range)
	})
	require.True(t, be.depsValidated(2), "self-reference imposes no constraint")
	require.True(t, be.depsValidated(3), "future/out-of-range predecessor imposes no constraint")
}

func TestDepsValidated_MultipleDeps_OneMissing(t *testing.T) {
	a1, a2 := addr(0x01), addr(0x02)
	var rs state.ReadSet
	rs.SetBalance(a1, state.VersionedRead[uint256.Int]{
		ReadHeader: state.ReadHeader{Source: state.MapRead, Version: state.Version{TxIndex: 1}},
		Val:        *uint256.NewInt(0),
	})
	rs.SetNonce(a2, state.VersionedRead[uint64]{
		ReadHeader: state.ReadHeader{Source: state.MapRead, Version: state.Version{TxIndex: 2}},
		Val:        0,
	})
	be := beWithReads(4, []int{1}, map[int]state.ReadSet{3: rs}) // 2 not validated
	require.False(t, be.depsValidated(3), "all MapRead predecessors must be validated")

	be2 := beWithReads(4, []int{1, 2}, map[int]state.ReadSet{3: rs})
	require.True(t, be2.depsValidated(3), "both predecessors validated → ready")
}

// The coinbase gate is built on ReadSet.ReadsAccount: a tx that reads the
// coinbase account is implicitly dependent on all prior txs (every tx credits
// fees to the coinbase), so it must fall back to the all-prior gate rather than
// dependency-ordered validation.
func TestReadsAccount_DetectsCoinbaseRead(t *testing.T) {
	coinbase := addr(0xc0)
	other := addr(0x0a)

	cbRead := mapReadOn(coinbase, 1)
	require.True(t, cbRead.ReadsAccount(coinbase), "balance read of coinbase is detected")

	otherRead := mapReadOn(other, 1)
	require.False(t, otherRead.ReadsAccount(coinbase), "read of a different account is not")

	// A storage read of the coinbase's slots is not an account-field read.
	cbStorage := storageReadOn(coinbase, 1)
	require.False(t, cbStorage.ReadsAccount(coinbase), "storage-only read is not an account read")
}
