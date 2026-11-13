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

package protocol

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestNewEVMBlockContextMilliseconds(t *testing.T) {
	header := &types.Header{
		Number:     *uint256.NewInt(1),
		Time:       1_800_000_000,
		Difficulty: *uint256.NewInt(2),
		MixDigest:  common.Hash(uint256.NewInt(750).Bytes32()),
	}
	author := accounts.InternAddress(common.Address{1})

	parlia := &chain.Config{Parlia: &chain.ParliaConfig{}}
	require.Equal(t, uint16(750), NewEVMBlockContext(header, nil, nil, author, parlia).Milliseconds)

	require.Zero(t, NewEVMBlockContext(header, nil, nil, author, &chain.Config{}).Milliseconds)
}
