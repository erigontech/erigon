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

// Code, CodeHash and CodeSize always co-write (one code change bumps all three)
// but validate in different classes: CodeHash is a value path (its cell carries a
// valStatus) while Code and CodeSize are noValueRead (version/status only).
//
// The split shows up on a StorageRead-sourced read — a cold read that saw no
// VersionMap entry at execution time but now collides with a concurrent worker's
// Done flush. When the shadowing CodeHash cell is a net-zero (ValueUnchanged)
// write, the live hash still equals the committed origin the cold read saw, so the
// read stays valid on version+valStatus; the co-written Code/CodeSize cells are
// ValueChanged (the default for those structural paths) and invalidate. The
// divergence is benign — the hash is unchanged so the read is accurate; the
// version/status checks are conservative, never wrong — and it is what lets an
// EXTCODEHASH-only tx avoid a re-execution on such a collision.
func TestCodeTrio_ValidationClassAsymmetry(t *testing.T) {
	t.Parallel()
	addr := getAddress(80)
	code := accounts.NewCode([]byte{0x60, 0x00, 0x60, 0x00, 0xf3})

	vm := NewVersionMap(nil)
	// A concurrent worker's Done flush of the trio lands at tx1. The code-hash is a
	// net-zero re-publication of the committed origin (ValueUnchanged); code and
	// code-size are stamped as real changes (ValueChanged).
	ws := &WriteSet{}
	ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: Version{TxIndex: 1}, valStatus: ValueChanged}, Val: code})
	ws.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: Version{TxIndex: 1}, valStatus: ValueUnchanged}, Val: code.Hash})
	ws.SetCodeSize(addr, &VersionedWrite[int]{WriteHeader: WriteHeader{Address: addr, Path: CodeSizePath, Version: Version{TxIndex: 1}, valStatus: ValueChanged}, Val: code.Len()})
	vm.FlushVersionedWrites(ws, true, "")

	// tx3's reads were served from storage (cold) and now collide with the flush.
	readStorage := ReadHeader{Source: StorageRead, Version: Version{TxIndex: 3}}

	ioHash := NewVersionedIO(4)
	rsHash := ReadSet{}
	rsHash.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{ReadHeader: readStorage, Val: code.Hash})
	ioHash.RecordReads(Version{TxIndex: 3}, rsHash)
	require.Equal(t, VersionValid, vm.ValidateVersion(3, ioHash, validateEqualVersion, false, ""),
		"CodeHash value path: a net-zero (ValueUnchanged) shadow leaves the cold read's value live")

	ioCode := NewVersionedIO(4)
	rsCode := ReadSet{}
	rsCode.SetCode(addr, VersionedRead[accounts.Code]{ReadHeader: readStorage, Val: code})
	ioCode.RecordReads(Version{TxIndex: 3}, rsCode)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(3, ioCode, validateEqualVersion, false, ""),
		"CodePath noValue: the storage/flush collision invalidates (conservative)")

	ioSize := NewVersionedIO(4)
	rsSize := ReadSet{}
	rsSize.SetCodeSize(addr, VersionedRead[int]{ReadHeader: readStorage, Val: code.Len()})
	ioSize.RecordReads(Version{TxIndex: 3}, rsSize)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(3, ioSize, validateEqualVersion, false, ""),
		"CodeSizePath noValue: same collision verdict as Code")
}
