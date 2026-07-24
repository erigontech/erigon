// Copyright 2024 The Erigon Authors
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

// Code, CodeHash and CodeSize always co-write (one code change bumps all
// three) and validate in the SAME value class: each carries a tiebreaker, so a
// cold StorageRead that collides with a concurrent worker's Done flush stays
// valid when the flushed value matches what was read, and invalidates when it
// differs. The unification is deliberate (the tiebreaker was extended from
// CodeHash to Code and CodeSize, per this test's original guidance): a
// value-matching collision is not a real conflict, and re-executing on it costs
// determinism on the BAL-fed path.
func TestCodeTrio_ValidationClassAsymmetry(t *testing.T) {
	t.Parallel()
	addr := getAddress(80)
	code := accounts.NewCode([]byte{0x60, 0x00, 0x60, 0x00, 0xf3})

	vm := NewVersionMap(nil)
	// A concurrent worker's Done flush of the trio lands at tx1.
	vm.WriteCode(addr, Version{TxIndex: 1}, code, true)
	vm.WriteCodeHash(addr, Version{TxIndex: 1}, code.Hash, true)
	vm.WriteCodeSize(addr, Version{TxIndex: 1}, code.Len(), true)

	// tx3's reads were served from storage (cold) and now collide with the flush.
	readStorage := ReadHeader{Source: StorageRead, Version: Version{TxIndex: 3}}

	ioHash := NewVersionedIO(4)
	rsHash := ReadSet{}
	rsHash.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{ReadHeader: readStorage, Val: code.Hash})
	ioHash.RecordReads(Version{TxIndex: 3}, rsHash)
	require.Equal(t, VersionValid, vm.ValidateVersion(3, ioHash, validateEqualVersion, true, false, ""),
		"CodeHash value-path tiebreaker: the flushed value matches, so the cold read stays valid")

	ioCode := NewVersionedIO(4)
	rsCode := ReadSet{}
	rsCode.SetCode(addr, VersionedRead[[]byte]{ReadHeader: readStorage, Val: code.Bytes})
	ioCode.RecordReads(Version{TxIndex: 3}, rsCode)
	require.Equal(t, VersionValid, vm.ValidateVersion(3, ioCode, validateEqualVersion, true, false, ""),
		"CodePath tiebreaker: the flushed bytes match, so the cold read stays valid")

	ioSize := NewVersionedIO(4)
	rsSize := ReadSet{}
	rsSize.SetCodeSize(addr, VersionedRead[int]{ReadHeader: readStorage, Val: code.Len()})
	ioSize.RecordReads(Version{TxIndex: 3}, rsSize)
	require.Equal(t, VersionValid, vm.ValidateVersion(3, ioSize, validateEqualVersion, true, false, ""),
		"CodeSizePath tiebreaker: the flushed size matches, so the cold read stays valid")

	ioCodeStale := NewVersionedIO(4)
	rsCodeStale := ReadSet{}
	rsCodeStale.SetCode(addr, VersionedRead[[]byte]{ReadHeader: readStorage, Val: []byte{0xde, 0xad}})
	ioCodeStale.RecordReads(Version{TxIndex: 3}, rsCodeStale)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(3, ioCodeStale, validateEqualVersion, true, false, ""),
		"CodePath: a genuinely different flushed value still invalidates")

	ioSizeStale := NewVersionedIO(4)
	rsSizeStale := ReadSet{}
	rsSizeStale.SetCodeSize(addr, VersionedRead[int]{ReadHeader: readStorage, Val: 2})
	ioSizeStale.RecordReads(Version{TxIndex: 3}, rsSizeStale)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(3, ioSizeStale, validateEqualVersion, true, false, ""),
		"CodeSizePath: a genuinely different flushed size still invalidates")
}
