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

package pbt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func TestValidateEngineIdentity(t *testing.T) {
	state := []byte{commitment.PBinStateMarker, commitment.PBinRowStateFormat, 0, 0}
	require.NoError(t, ValidateEngineIdentity(state, nil))
	require.Error(t, ValidateEngineIdentity([]byte{commitment.PBinStateMarker, 0x10}, nil))
	err := ValidateEngineIdentity(state, []StoredRecord{{Key: []byte{0, 0}, Value: []byte{0x10}}})
	require.Error(t, err)
	require.ErrorContains(t, err, "record format requires rebuild")
}
