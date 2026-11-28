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
package v3_test

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPartitionWriteSet(t *testing.T) {
	t.Run("E35/SelfDestructOnlyWriteSetCreatesStorageWipe", func(t *testing.T) {
		v3.PartitionWipeCase(t, true, func(updates *commitment.Updates) {
			ws := &state.WriteSet{}
			ws.SetSelfDestruct(accounts.InternAddress(common.Address{0xd4}), &state.VersionedWrite[bool]{Val: true})
			ws.TouchUpdates(updates)
		})
	})
}
