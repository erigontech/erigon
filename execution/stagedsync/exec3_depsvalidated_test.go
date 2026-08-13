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

// ReadSet.ReadsAccount detects a coinbase account-field read: a tx that reads
// the coinbase is implicitly dependent on every prior tx (each credits fees to
// the coinbase). The finalize path uses this to fold the coinbase in order.
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
