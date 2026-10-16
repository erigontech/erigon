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

package runner

import (
	"bytes"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

func Update(op commitmenttest.Op) *commitment.Update {
	if op.Read {
		return nil
	}
	u := &commitment.Update{}
	if op.Delete {
		u.Flags = commitment.DeleteUpdate
		return u
	}
	if op.Account != nil {
		value := op.Account
		u.Nonce, u.Balance, u.CodeHash = value.Nonce, value.Balance, value.CodeHash
		if value.Fields&commitmenttest.NonceField != 0 {
			u.Flags |= commitment.NonceUpdate
		}
		if value.Fields&commitmenttest.BalanceField != 0 {
			u.Flags |= commitment.BalanceUpdate
		}
		if value.Fields&commitmenttest.CodeField != 0 {
			u.Flags |= commitment.CodeUpdate
		}
	} else {
		if len(op.Storage) > len(u.Storage) {
			panic("storage value exceeds 32 bytes")
		}
		u.Flags = commitment.StorageUpdate
		u.StorageLen = int8(len(op.Storage))
		copy(u.Storage[:], op.Storage)
	}
	return u
}

func CloneDeltas(in []commitment.BranchDelta) []commitment.BranchDelta {
	out := make([]commitment.BranchDelta, len(in))
	for i, d := range in {
		out[i] = commitment.BranchDelta{Key: bytes.Clone(d.Key), Data: bytes.Clone(d.Data), Prev: bytes.Clone(d.Prev)}
	}
	return out
}
