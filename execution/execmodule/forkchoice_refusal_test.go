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

// A refused forkchoice is retried unchanged every slot, so when one repeats the
// reason is the only thing separating a reorg that will settle from a node that
// can no longer move. The two causes need different fixes and the refusal must
// say which it is.
func TestForkchoiceRefusalNamesItsCause(t *testing.T) {
	t.Parallel()

	require.Nil(t, checkForkchoiceRef("finalized", 3797432, 3797366, true),
		"head above a canonical finalized block is usable")

	offChain := checkForkchoiceRef("finalized", 3797432, 3797366, false)
	require.NotNil(t, offChain)
	require.Contains(t, offChain.String(), "not on the head's chain")
	require.Contains(t, offChain.String(), "3797366")

	notBehind := checkForkchoiceRef("safe", 3797430, 3797430, true)
	require.NotNil(t, notBehind)
	require.Contains(t, notBehind.String(), "not above")
	require.Contains(t, notBehind.String(), "safe")
}
