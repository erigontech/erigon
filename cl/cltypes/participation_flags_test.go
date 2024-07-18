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

package cltypes_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func TestParticipationFlags(t *testing.T) {
	flagsList := cltypes.ParticipationFlagsListFromBytes([]byte{0, 0, 0, 0})
	flagsList[0] = flagsList[0].Add(4) // Turn on fourth bit
	require.True(t, flagsList[0].HasFlag(4))
}
