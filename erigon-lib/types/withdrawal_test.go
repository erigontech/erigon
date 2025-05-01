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

package types

import (
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/stretchr/testify/assert"
)

func TestWithdrawalsHash(t *testing.T) {
	t.Parallel()
	w := &Withdrawal{
		Index:     0,
		Validator: 0,
		Address:   common.HexToAddress("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"),
		Amount:    1,
	}
	withdrawals := Withdrawals([]*Withdrawal{w})
	hash := DeriveSha(withdrawals)
	// The only trie node is short (its RLP < 32 bytes).
	// Its Keccak should be returned, not the node itself.
	assert.Equal(t, common.HexToHash("82cc6fbe74c41496b382fcdf25216c5af7bdbb5a3929e8f2e61bd6445ab66436"), hash)
}
