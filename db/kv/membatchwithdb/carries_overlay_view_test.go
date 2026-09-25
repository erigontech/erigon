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

package membatchwithdb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
)

// TestCarriesOverlayView pins the predicate overlay wrap points rely on to
// avoid re-wrapping: read views over an overlay are recognized, while raw txs
// and writable batches are not.
func TestCarriesOverlayView(t *testing.T) {
	_, rwTx := newTestTx(t)

	overlay, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
	require.NoError(t, err)
	defer overlay.Close()

	require.False(t, membatchwithdb.CarriesOverlayView(rwTx), "a raw tx is not an overlay view")
	require.False(t, membatchwithdb.CarriesOverlayView(overlay), "a writable batch is not an overlay view")
	require.True(t, membatchwithdb.CarriesOverlayView(overlay.NewReadView(rwTx)))
	require.True(t, membatchwithdb.CarriesOverlayView(overlay.NewTemporalReadView(rwTx)))
}
