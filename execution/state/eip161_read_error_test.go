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
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// failingBaseReader errors on ReadAccountData, standing in for a transient
// storage/domain read failure while composing an account's pre-tx state.
type failingBaseReader struct {
	minimalStateReader
	err error
}

func (r *failingBaseReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	return nil, r.err
}

// A failed base read must not let encodeExistingEmptyRemovals mistake a
// live account for an EIP-161 empty and delete it. The read error zeroes the
// composed fields; without propagation the account reads as empty and is
// rewritten as a self-destruct — silently dropping a live account whose read
// merely errored. The removal must bail and the error must surface.
func TestEncodeExistingEmptyRemovals_BailsOnBaseReadError(t *testing.T) {
	t.Parallel()

	addr := getAddress(1)
	readErr := errors.New("domain read failed")
	reader := &failingBaseReader{err: readErr}
	vm := NewVersionMap(nil)

	ibs := New(NewVersionedStateReader(0, ReadSet{}, vm, reader, false))
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)

	// A storage write puts the account in the write set without carrying any
	// balance/nonce/codeHash cell, so emptiness is decided from the base read.
	writes := newWriteSet(&VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: accounts.InternKey(uint256.NewInt(7).Bytes32()), Version: Version{BlockNum: 1, TxIndex: 0}},
		Val:         *uint256.NewInt(9),
	})

	rules := &chain.Rules{IsSpuriousDragon: true}
	ibs.encodeExistingEmptyRemovals(rules, writes)

	sd, ok := writes.GetSelfDestruct(addr)
	require.False(t, ok && sd.Val, "a failed base read must not be rewritten as a self-destruct")
	require.ErrorIs(t, ibs.StateReadError(), readErr, "the base read error must surface, not be swallowed as an empty account")
}
