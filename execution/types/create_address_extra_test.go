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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCreateAddress(t *testing.T) {
	t.Parallel()
	a := common.HexToAddress("0x6ac7ea33f8831ea9dcc53393aaa88b25a785dbf0")

	addr0 := CreateAddress(a, 0)
	require.Equal(t, addr0, CreateAddress(a, 0)) // deterministic
	require.NotEqual(t, addr0, CreateAddress(a, 1))
	require.NotEqual(t, common.Address{}, addr0)
}

func TestCreateAddress2(t *testing.T) {
	t.Parallel()
	b := common.HexToAddress("0x00000000000000000000000000000000deadbeef")
	inithash := accounts.InternCodeHash(common.HexToHash("0xcafe"))
	var salt [32]byte
	salt[0] = 0x01

	addr := CreateAddress2(b, salt, inithash)
	require.Equal(t, addr, CreateAddress2(b, salt, inithash)) // deterministic

	var salt2 [32]byte
	salt2[0] = 0x02
	require.NotEqual(t, addr, CreateAddress2(b, salt2, inithash))
	require.NotEqual(t, common.Address{}, addr)
}
