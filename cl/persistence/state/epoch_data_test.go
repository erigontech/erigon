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

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestEpochData(t *testing.T) {
	e := &EpochData{
		TotalActiveBalance:          123,
		JustificationBits:           &cltypes.JustificationBits{true},
		CurrentJustifiedCheckpoint:  solid.Checkpoint{Epoch: 123},
		PreviousJustifiedCheckpoint: solid.Checkpoint{Epoch: 123},
		FinalizedCheckpoint:         solid.Checkpoint{Epoch: 123},
		HistoricalSummariesLength:   235,
		HistoricalRootsLength:       345,
	}
	var b bytes.Buffer
	if err := e.WriteTo(&b); err != nil {
		t.Fatal(err)
	}

	e2 := &EpochData{}
	if err := e2.ReadFrom(&b); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, e, e2)
}
