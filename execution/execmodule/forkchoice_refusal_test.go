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

package execmodule

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForkchoiceRefusalNamesItsCause(t *testing.T) {
	t.Parallel()
	num := func(n uint64) *uint64 { return &n }

	require.Nil(t, checkForkchoiceRef("finalized", num(3797432), num(3797366), true),
		"head above a canonical finalized block is usable")

	offChain := checkForkchoiceRef("finalized", num(3797432), num(3797366), false)
	require.NotNil(t, offChain)
	require.Contains(t, offChain.String(), "not on the head's chain")
	require.Contains(t, offChain.String(), "3797366")

	notBehind := checkForkchoiceRef("safe", num(3797430), num(3797430), true)
	require.NotNil(t, notBehind)
	require.Contains(t, notBehind.String(), "not above")
	require.Contains(t, notBehind.String(), "safe")
}

func TestForkchoiceRefusalOfAnUnknownBlockNamesIt(t *testing.T) {
	t.Parallel()
	head := uint64(3797432)

	unknown := checkForkchoiceRef("finalized", &head, nil, false)
	require.NotNil(t, unknown)
	require.Contains(t, unknown.String(), "finalized block is not in our chain")
}
