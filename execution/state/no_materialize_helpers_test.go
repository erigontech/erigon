// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// staticAccountReader returns a fixed account without allocating, so an
// AllocsPerRun loop measures only the IntraBlockState path.
type staticAccountReader struct {
	emptyReader
	addr accounts.Address
	acc  *accounts.Account
}

func (r *staticAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.addr {
		return r.acc, nil
	}
	return nil, nil
}

func newNoMaterializeIBS(reader StateReader) (*IntraBlockState, *VersionMap) {
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	return ibs, vm
}

func startNoMaterializeTx(ibs *IntraBlockState, vm *VersionMap, txIndex int) {
	ibs.Reset()
	ibs.SetVersionMap(vm)
	ibs.SetTxContext(1, txIndex)
}

type countingAccountReader struct {
	staticAccountReader
	reads int
}

func (r *countingAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	r.reads++
	return r.staticAccountReader.ReadAccountData(addr)
}

// seedOrigin and versionedAccountBase share one committed read per tx, also for
// an account that does not exist.
func TestNoMaterializeReadsCommittedAccountOncePerTx(t *testing.T) {
	absent := accounts.InternAddress([20]byte{0xCD})
	reader := &countingAccountReader{staticAccountReader: staticAccountReader{addr: accounts.InternAddress([20]byte{0xAB})}}
	ibs, vm := newNoMaterializeIBS(reader)
	defer ibs.Close()
	startNoMaterializeTx(ibs, vm, 0)

	_, _, _, seeded, err := seedOrigin(ibs, absent)
	require.NoError(t, err)
	require.False(t, seeded)
	acc, _, _, err := ibs.versionedAccountBase(absent, true)
	require.NoError(t, err)
	require.Nil(t, acc)
	require.Equal(t, 1, reader.reads)
}
