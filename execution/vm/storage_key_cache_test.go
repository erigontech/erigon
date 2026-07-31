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

func TestInternStorageKeyMatchesInternKey(t *testing.T) {
	evm := &EVM{}
	words := []uint256.Int{
		{},
		*uint256.NewInt(1),
		{0, 0, 0, 1},
		{1, 1, 1, 1},
		*uint256.NewInt(1),
	}
	for round := range storageKeyCacheMinOps + 10 {
		for _, word := range words {
			require.Equal(t, accounts.InternKey(word.Bytes32()), evm.internStorageKey(&word), "round %d word %s", round, &word)
		}
	}
}

func TestStorageKeyCacheNotAllocatedForShortLivedEVM(t *testing.T) {
	evm := &EVM{}
	word := uint256.NewInt(7)
	for range storageKeyCacheMinOps {
		evm.internStorageKey(word)
	}
	assert.Nil(t, evm.internCache)

	evm.internStorageKey(word)
	assert.NotNil(t, evm.internCache)
}
