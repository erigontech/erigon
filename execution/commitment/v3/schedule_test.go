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

package v3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageWorkerCountHonoursTheCap(t *testing.T) {
	require.Equal(t, 16*storageOversubscribe, storageWorkerCount(0, 16, 0), "no cap: procs oversubscribed")
	require.Equal(t, 15, storageWorkerCount(0, 16, 15), "the cap bounds the oversubscribed count")
	require.Equal(t, 4, storageWorkerCount(4, 16, 15), "explicit workers below the cap")
	require.Equal(t, 2, storageWorkerCount(4, 16, 2), "the cap bounds explicit workers")
}
