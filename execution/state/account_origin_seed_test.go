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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// An account's committed origin (AddressPath at originIndex) is the immutable
// pre-block base for the life of the block. calcFees obtains the coinbase via a
// floor-composed reader, so its balance climbs as tips accumulate; if SeedOrigin
// re-published that value it would overwrite the origin every finalized tx,
// corrupting every fall-through read with a mid-block, tip-inflated balance. The
// seed must therefore stick to the first (pre-block) value, mirroring the
// seed-once discipline seedStorageOrigin already applies to storage slots.
func TestAccountOriginSeedsOnce(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x39, 0x63, 0x43})
	vm := NewVersionMap(nil)

	preBlock := accounts.NewAccount()
	preBlock.Nonce = 7
	preBlock.Balance = *uint256.NewInt(100)

	base := &oneAccountReader{acc: &preBlock}
	require.NoError(t, SeedOrigin(vm, base, addr))

	// A later finalized tx re-seeds while the reader returns a tip-inflated balance:
	// the origin must NOT move.
	midBlock := preBlock
	midBlock.Balance = *uint256.NewInt(150)
	base.acc = &midBlock
	require.NoError(t, SeedOrigin(vm, base, addr))

	got, res, ok := vm.ReadAddress(addr, 0)
	require.True(t, ok, "origin must be seeded")
	require.Equal(t, MVReadResultDone, res.Status())
	require.Equal(t, originIndex, res.DepIdx(), "account origin must sit at originIndex (-2)")
	require.Equal(t, *uint256.NewInt(100), got.Balance,
		"origin must retain the pre-block balance, not the mid-block re-seed")
	require.Equal(t, uint64(7), got.Nonce)
}

type oneAccountReader struct {
	NoopReader
	acc *accounts.Account
}

func (r *oneAccountReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	return r.acc, nil
}
