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

package validator_params

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestAttachedValidatorsAreIndependentFromFeeRecipients(t *testing.T) {
	params := NewValidatorParams()

	params.AddAttachedValidator(7)

	require.ElementsMatch(t, []uint64{7}, params.GetValidators())
	_, found := params.GetFeeRecipient(7)
	require.False(t, found)
	require.Zero(t, params.Generation())

	params.SetFeeRecipient(7, common.Address{0x11})
	params.SetFeeRecipient(8, common.Address{0x12})
	params.AddAttachedValidator(9)

	require.ElementsMatch(t, []uint64{7, 8, 9}, params.GetValidators())
	require.Equal(t, uint64(2), params.Generation())
}
