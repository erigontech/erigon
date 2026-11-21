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

package state

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
)

func TestRebuildStorageWorkersMax(t *testing.T) {
	require.Equal(t, 1, rebuildStorageWorkersMax(0))
	require.Equal(t, 1, rebuildStorageWorkersMax(uint64(3*datasize.GB)))
	require.Equal(t, 15, rebuildStorageWorkersMax(uint64(62*datasize.GB)))
	require.Equal(t, 47, rebuildStorageWorkersMax(uint64(188*datasize.GB)))
}
