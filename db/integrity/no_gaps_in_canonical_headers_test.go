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

package integrity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// everyMarkerMissing answers as a datadir whose canonical markers are all absent, which is the
// shape this check exists to find. Only the three methods the check calls are implemented.
type everyMarkerMissing struct {
	dbservices.FullBlockReader
}

func (everyMarkerMissing) Integrity(context.Context, kv.Getter) error { return nil }
func (everyMarkerMissing) FrozenBlocks() uint64                       { return 0 }
func (everyMarkerMissing) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{}, false, nil
}

// A reported gap leaves no hash to read the header with, so the check has to move to the next block
// rather than fall through and read one. Falling through kills the run on the first problem, which
// is exactly the datadir an operator passes --failFast=false for.
func TestNoGapsInCanonicalHeadersSurvivesReportedGaps(t *testing.T) {
	ctx := context.Background()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Headers, 5)
	}))

	err := NoGapsInCanonicalHeaders(ctx, db, everyMarkerMissing{}, false)

	require.Error(t, err, "a run that reported gaps must not come back clean")
	require.Contains(t, err.Error(), string(HeaderNoGaps))
}

func TestNoGapsInCanonicalHeadersFailFastStopsAtTheFirstGap(t *testing.T) {
	ctx := context.Background()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Headers, 5)
	}))

	err := NoGapsInCanonicalHeaders(ctx, db, everyMarkerMissing{}, true)

	require.ErrorContains(t, err, "canonical marker not found: 1")
}
