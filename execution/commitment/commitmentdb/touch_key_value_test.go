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

package commitmentdb

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestTouchKeyValueKeepsTheValueInModeUpdate(t *testing.T) {
	sdc := NewSharedDomainsCommitmentContext(nil, commitment.ModeUpdate, t.TempDir(), commitment.TrieConfig{})
	addr := make([]byte, length.Addr)
	addr[19] = 1
	acc := accounts.Account{Nonce: 7, Balance: *uint256.NewInt(5), CodeHash: accounts.EmptyCodeHash}
	require.NoError(t, sdc.TouchKeyValue(addr, accounts.SerialiseV3(&acc)))

	var got *commitment.Update
	require.NoError(t, sdc.updates.HashSort(context.Background(), nil, func(_, _ []byte, u *commitment.Update) error {
		got = u
		return nil
	}))
	require.NotNil(t, got)
	require.Zero(t, got.Flags&commitment.DeleteUpdate, "a touched account with a value is not a delete")
	require.Equal(t, uint64(7), got.Nonce)
}
