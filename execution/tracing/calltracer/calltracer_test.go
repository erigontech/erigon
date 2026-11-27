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

package calltracer

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestEnterV2PreservesCallIndexes(t *testing.T) {
	from := accounts.ZeroAddress
	to := accounts.InternAddress(common.HexToAddress("0x1000"))
	gas := mdgas.MdGas{Execution: 100, State: 50}
	var calls int
	ct := NewCallTracer(&tracing.Hooks{OnEnterV2: func(_ int, _ byte, caller, target accounts.Address, _ bool, _ []byte, got mdgas.MdGas, _ uint256.Int, _ []byte) {
		calls++
		require.Equal(t, from, caller)
		require.Equal(t, to, target)
		require.Equal(t, gas, got)
	}})
	ct.Tracer().EmitEnter(0, 0xf1, from, to, false, nil, gas, uint256.Int{}, nil)
	require.Equal(t, 1, calls)
	froms, tos := ct.Addresses()
	require.Equal(t, []common.Address{from.Value()}, froms)
	require.Equal(t, []common.Address{to.Value()}, tos)
}

func TestAddressesAreDistinct(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0x1000"))
	b := accounts.InternAddress(common.HexToAddress("0x2000"))
	ct := NewCallTracer(nil)
	froms, tos := ct.Addresses()
	require.Empty(t, froms)
	require.Empty(t, tos)

	ct.OnEnterV2(a, b)
	ct.OnEnterV2(b, a)
	ct.OnEnterV2(a, b)
	froms, tos = ct.Addresses()
	require.ElementsMatch(t, []common.Address{a.Value(), b.Value()}, froms)
	require.ElementsMatch(t, []common.Address{a.Value(), b.Value()}, tos)

	froms = append(froms, common.Address{})
	require.ElementsMatch(t, []common.Address{a.Value(), b.Value()}, tos)
}
