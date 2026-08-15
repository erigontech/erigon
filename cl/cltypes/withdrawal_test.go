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

package cltypes

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestConvertConsensusWithdrawalsToExecutionWithdrawals(t *testing.T) {
	t.Parallel()

	source := []*Withdrawal{
		{Index: 1, Validator: 10, Address: common.Address{0xaa}, Amount: 100},
		{Index: 2, Validator: 20, Address: common.Address{0xbb}, Amount: 200},
	}

	converted := ConvertConsensusWithdrawalsToExecutionWithdrawals(source)

	require.Equal(t, []*types.Withdrawal{
		{Index: 1, Validator: 10, Address: common.Address{0xaa}, Amount: 100},
		{Index: 2, Validator: 20, Address: common.Address{0xbb}, Amount: 200},
	}, converted)

	// The execution layer owns its copy: mutating the source afterwards must not reach it.
	source[0].Amount = 999
	require.Equal(t, uint64(100), converted[0].Amount)
}

func TestConvertConsensusWithdrawalsToExecutionWithdrawalsNeverReturnsNil(t *testing.T) {
	t.Parallel()

	// A nil list and an empty one are rejected by the execution layer under opposite conditions,
	// so an absent input must not become an absent list.
	require.NotNil(t, ConvertConsensusWithdrawalsToExecutionWithdrawals(nil))
	require.Empty(t, ConvertConsensusWithdrawalsToExecutionWithdrawals(nil))
	require.NotNil(t, ConvertConsensusWithdrawalsToExecutionWithdrawals([]*Withdrawal{}))
}
