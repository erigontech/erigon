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

package exec

import (
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// recipientHasCode reports whether tx moves value to an account that holds code on the state
// the transaction starts from. It is the state input of a rules.LaneEngine's classification.
func recipientHasCode(tx types.Transaction, ibs *state.IntraBlockState) (bool, error) {
	to := tx.GetTo()
	if to == nil || tx.GetValue().IsZero() {
		return false, nil
	}
	hash, err := ibs.GetCodeHash(accounts.InternAddress(*to))
	if err != nil {
		return false, err
	}
	return !hash.IsEmpty(), nil
}

func isLaneEngine(engine rules.Engine) bool {
	_, ok := engine.(rules.LaneEngine)
	return ok
}
