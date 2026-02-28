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

package freezeblocks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snaptype"
)

// TestDumpBeaconBlocksNoPanic is a higher-level regression test: the production
// panic occurred inside DumpBeaconBlocks when chooseSegmentEnd was called with
// nil snCfg. This test uses toSlot=CaplinMergeLimit so the loop body executes
// (doesn't break early), exercising the exact code path that panicked.
func TestDumpBeaconBlocksNoPanic(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())

	// toSlot == CaplinMergeLimit: toSlot-fromSlot is not < blocksPerFile,
	// so the loop reaches chooseSegmentEnd and dumpBeaconBlocksRange.
	// Before the fix this panicked; now it returns an error (no snap dir / empty db).
	require.NotPanics(t, func() {
		_ = DumpBeaconBlocks(context.Background(), db, 0, snaptype.CaplinMergeLimit, 0, dirs, 1, log.LvlDebug, log.New())
	})
}
