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

package composer

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// reads is a state reader keeping the errors its reader returns: the EVM
// reports a failed read as the transaction's failure.
type reads struct {
	state.StateReader
	err error
}

func (r *reads) keep(err error) error {
	if err != nil {
		r.err = err
	}
	return err
}

// failed returns a failed read, nil when none failed.
func (r *reads) failed() error {
	if r.err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrStateRead, r.err)
}

// blockHash returns lookup, keeping its errors as failed reads.
func (r *reads) blockHash(lookup func(uint64) (common.Hash, error)) func(uint64) (common.Hash, error) {
	return func(n uint64) (common.Hash, error) {
		h, err := lookup(n)
		return h, r.keep(err)
	}
}

func (r *reads) ReadAccountData(a accounts.Address) (*accounts.Account, error) {
	acc, err := r.StateReader.ReadAccountData(a)
	return acc, r.keep(err)
}

func (r *reads) ReadAccountByPlainAddr(a common.Address) (*accounts.Account, error) {
	acc, err := r.StateReader.ReadAccountByPlainAddr(a)
	return acc, r.keep(err)
}

func (r *reads) HasAccount(a common.Address) (bool, error) {
	ok, err := r.StateReader.HasAccount(a)
	return ok, r.keep(err)
}

func (r *reads) ReadAccountDataForDebug(a accounts.Address) (*accounts.Account, error) {
	acc, err := r.StateReader.ReadAccountDataForDebug(a)
	return acc, r.keep(err)
}

func (r *reads) ReadAccountStorage(a accounts.Address, k accounts.StorageKey) (uint256.Int, bool, error) {
	v, ok, err := r.StateReader.ReadAccountStorage(a, k)
	return v, ok, r.keep(err)
}

func (r *reads) ReadAccountCode(a accounts.Address) ([]byte, error) {
	code, err := r.StateReader.ReadAccountCode(a)
	return code, r.keep(err)
}

func (r *reads) ReadAccountCodeSize(a accounts.Address) (int, error) {
	n, err := r.StateReader.ReadAccountCodeSize(a)
	return n, r.keep(err)
}

func (r *reads) ReadAccountIncarnation(a accounts.Address) (uint64, error) {
	n, err := r.StateReader.ReadAccountIncarnation(a)
	return n, r.keep(err)
}
