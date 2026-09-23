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

package v4

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanesRejectNonNibblePaths(t *testing.T) {
	bad := append([]byte{0x10}, bytes.Repeat([]byte{3}, 63)...)
	good := append([]byte{0x02}, bytes.Repeat([]byte{3}, 63)...)

	t.Run("storage", func(t *testing.T) {
		ctx := newDeltaContext()
		_, err := runStorageTask(ctx, storageTask{addrHash: [32]byte{1}, entries: []storageEntry{
			{path: bad, update: phaseAStorageUpdate([]byte{1})},
		}})
		require.ErrorIs(t, err, errPhaseAUpdate)

		_, err = runStorageTask(ctx, storageTask{addrHash: [32]byte{1}, entries: []storageEntry{
			{path: good, update: phaseAStorageUpdate([]byte{1})},
		}})
		require.NoError(t, err)
	})

	t.Run("account", func(t *testing.T) {
		ctx := newDeltaContext()
		before := new(keySet)
		g := accountGraph(before)
		root := fork(nil)
		root.plane = planeAccount
		_, err := makeAccountPlans(ctx, g, root, []accountEntry{{hashedKey: bad}})
		require.ErrorIs(t, err, errPhaseBKey)

		_, err = makeAccountPlans(ctx, g, root, []accountEntry{{hashedKey: good}})
		require.NoError(t, err)
	})
}
