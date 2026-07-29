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
// but validate in different classes: CodeHash is a value path (carries a
// tiebreaker) while Code and CodeSize are noValueRead (version/status only).
//
// The split is only observable for a StorageRead-sourced read — a cold read
// that saw no VersionMap entry at execution time but now collides with a
// concurrent worker's Done flush. For a MapRead the tiebreaker is bypassed
// (validateReadImpl uses checkVersion for every path), so there is no
// asymmetry among map reads. In the StorageRead collision (no BAL): a CodeHash
// read whose value still matches survives via the tiebreaker, while the
// co-written Code/CodeSize reads invalidate. The divergence is benign — the hash
// is unchanged so the read is accurate; the version/status checks are
// conservative, never wrong — and the tiebreaker is what lets an EXTCODEHASH-only
// tx avoid a re-execution on such a collision. A rationalization that unifies the
// trio's validation class must do so deliberately (extend the tiebreaker to
// Code/CodeSize to save re-execs, not drop it from CodeHash).
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
	require.Equal(t, VersionValid, vm.ValidateVersion(3, ioHash, validateEqualVersion, false, ""),
		"CodeHash value-path tiebreaker: the flushed value matches, so the cold read stays valid")

	ioCode := NewVersionedIO(4)
	rsCode := ReadSet{}
	rsCode.SetCode(addr, VersionedRead[[]byte]{ReadHeader: readStorage, Val: code.Bytes})
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
