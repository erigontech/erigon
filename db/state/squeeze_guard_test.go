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

package state_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestExpandShortenedKeysInBranchRejectsEdgeRecord(t *testing.T) {
	record := make([]byte, 1+2+32)
	record[0] = 0x10

	_, err := state.ExpandShortenedKeysInBranchForFormat(commitment.BranchData(record), nil, nil, nil, nil, 0, 0, true)
	require.ErrorIs(t, err, commitment.ErrEdgeRecord)
}
