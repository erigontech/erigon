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

package builder

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestParametersCopyKeepsNothingShared(t *testing.T) {
	t.Parallel()

	require.Nil(t, (*Parameters)(nil).Copy())

	// An empty slice is not the same request as an absent one, so the distinction has to survive.
	empty := (&Parameters{Withdrawals: []*types.Withdrawal{}, ExtraData: []byte{}}).Copy()
	require.NotNil(t, empty.Withdrawals)
	require.NotNil(t, empty.ExtraData)

	root := common.Hash{0x01}
	slot := uint64(2)
	gasLimit := uint64(3)
	params := &Parameters{
		Withdrawals:           []*types.Withdrawal{nil, {Index: 4}},
		ParentBeaconBlockRoot: &root,
		SlotNumber:            &slot,
		TargetGasLimit:        &gasLimit,
		ExtraData:             []byte{5},
	}
	copied := params.Copy()

	params.Withdrawals[1].Index = 40
	root[0] = 10
	slot = 20
	gasLimit = 30
	params.ExtraData[0] = 50

	require.Nil(t, copied.Withdrawals[0])
	require.Equal(t, uint64(4), copied.Withdrawals[1].Index)
	require.Equal(t, common.Hash{0x01}, *copied.ParentBeaconBlockRoot)
	require.Equal(t, uint64(2), *copied.SlotNumber)
	require.Equal(t, uint64(3), *copied.TargetGasLimit)
	require.Equal(t, byte(5), copied.ExtraData[0])
}

func TestParametersCopyCoversEveryField(t *testing.T) {
	t.Parallel()

	// Copy has to be revisited whenever a reference-typed field is added, and nothing else will say
	// so: a shallow copy of a new slice or pointer compiles and silently shares it.
	require.Equal(t, 11, reflect.TypeFor[Parameters]().NumField(),
		"Parameters gained or lost a field; check whether Copy has to copy it")
}
