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

package execctx_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
)

// TestClose_KeepsDomainRamForReaders pins the lifetime guarantee readers rely
// on: a read view keeps its DomainReader pointing at the SD's in-memory domain
// maps, so Close must release writer resources without clearing those maps —
// otherwise in-flight domain reads (e.g. the head block's receipts) silently
// miss and fall back to a tx that does not have the data yet.
func TestClose_KeepsDomainRamForReaders(t *testing.T) {
	t.Parallel()
	db := newTestDb(t, 16)
	ctx := context.Background()

	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	require.NoError(t, sd.InitBlockOverlay(tx, t.TempDir()))

	const txNum = 7
	const cumGas = 21000
	require.NoError(t, rawtemporaldb.AppendReceiptMetadata(sd.AsPutDel(tx), 0, cumGas, 0, txNum))

	view := sd.BlockOverlayTemporalTx(tx)
	require.NotNil(t, view)

	assertReceiptVisible := func(msg string) {
		got, _, _, err := rawtemporaldb.ReceiptAsOf(view, txNum+1)
		require.NoError(t, err)
		require.Equal(t, uint64(cumGas), got, msg)
	}
	assertReceiptVisible("the in-flight receipt must be visible through the view before Close")

	sd.Close()

	assertReceiptVisible("Close must not clear the domain RAM readers still hold")
}
