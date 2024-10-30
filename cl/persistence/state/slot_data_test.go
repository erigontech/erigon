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

package state_accessors

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/v3/cl/clparams"
	"github.com/erigontech/erigon/v3/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func TestSlotData(t *testing.T) {
	m := &SlotData{
		Version:                      clparams.CapellaVersion,
		Eth1Data:                     &cltypes.Eth1Data{},
		Eth1DepositIndex:             0,
		NextWithdrawalIndex:          0,
		NextWithdrawalValidatorIndex: 0,
		Fork:                         &cltypes.Fork{Epoch: 12},
	}
	var b bytes.Buffer
	if err := m.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	m2 := &SlotData{}
	if err := m2.ReadFrom(&b); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, m, m2)
}
