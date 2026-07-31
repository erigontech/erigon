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

package vm

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestInternAddressMatchesInternAddress(t *testing.T) {
	evm := &EVM{}
	words := []uint256.Int{
		{},
		*uint256.NewInt(1),
		{0, 0, 1, 0},
		{1, 1, 1, 1},
		*uint256.NewInt(1),
	}
	for round := range addressCacheMinOps + 10 {
		for _, word := range words {
			require.Equal(t, accounts.InternAddress(word.Bytes20()), evm.internAddress(&word), "round %d word %s", round, &word)
		}
	}
}

func TestInternAddressIgnoresBitsAboveTheAddress(t *testing.T) {
	evm := &EVM{}
	clean := uint256.Int{0x1122334455667788, 0x99aabbccddeeff00, 0x12345678, 0}
	dirty := clean
	dirty[2] |= 0xffffffff00000000
	dirty[3] = 0xdeadbeefdeadbeef

	want := accounts.InternAddress(clean.Bytes20())
	for range addressCacheMinOps + 10 {
		require.Equal(t, want, evm.internAddress(&clean))
		require.Equal(t, want, evm.internAddress(&dirty))
	}
}

func TestAddressCacheNotAllocatedForShortLivedEVM(t *testing.T) {
	evm := &EVM{}
	word := uint256.NewInt(7)
	for range addressCacheMinOps {
		evm.internAddress(word)
	}
	assert.Nil(t, evm.addrCache)

	evm.internAddress(word)
	assert.NotNil(t, evm.addrCache)
}
