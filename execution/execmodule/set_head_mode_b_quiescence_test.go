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

package execmodule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

var errStopAfterQuiescence = errors.New("stop: quiescence probe done")

// writeProbeUnwinder answers the only question that matters for the
// mode-B prologue: can anything else in the process take a write tx
// while the unwind is waiting for background builds to quiesce? The
// build goroutines it waits for read the db behind the aggregator's
// commit gate, and the gate's writer side blocks on a held write tx.
type writeProbeUnwinder struct {
	db            kv.TemporalRwDB
	writeAcquired bool
}

func (u *writeProbeUnwinder) BlockAligned() bool    { return true }
func (u *writeProbeUnwinder) BlockBuildFiles(bool)  {}
func (u *writeProbeUnwinder) FinalizeUnwind() error { return nil }
func (u *writeProbeUnwinder) AbortUnwind()          {}

func (u *writeProbeUnwinder) WaitForBuildAndMergeQuiescence(time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tx, err := u.db.BeginTemporalRw(ctx)
	if err != nil {
		return nil
	}
	u.writeAcquired = true
	tx.Rollback()
	return nil
}

func (u *writeProbeUnwinder) Unwind(context.Context, uint64, UnwindArgs) error {
	return errStopAfterQuiescence
}

// TestSetHeadModeB_QuiescenceWaitDoesNotHoldWriteTx pins the ordering
// that keeps mode-B from deadlocking against its own background
// builders: build quiescence is established before the unwind's write
// tx is begun, never while it is held.
//
// A builder that is already past the entry gate sets buildingFiles and
// then reads the db behind the aggregator commit gate. A write tx held
// across the wait stalls the gate's writer, the writer parks the
// builder's RLock, buildingFiles never clears, and the wait can only
// end by timing out — fifteen minutes during which the node does
// nothing at all.
func TestSetHeadModeB_QuiescenceWaitDoesNotHoldWriteTx(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	const (
		headBlock      = 2000
		firstChangeset = 1000
		unwindToBlock  = 500
	)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := stages.SaveStageProgress(tx, stages.Execution, headBlock); err != nil {
			return err
		}
		// A lone 40-byte key is what ReadLowestUnwindableBlock keys
		// off, and it puts the diffset floor above the target so
		// SetHead picks mode B.
		return tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(firstChangeset, common.Hash{}), []byte{0})
	}))

	snaps := blocksnapshots.NewRoSnapshots(ethconfig.BlocksFreezing{}, dirs.Snap, log.Root())
	t.Cleanup(snaps.Close)

	unwinder := &writeProbeUnwinder{db: db}
	e := &ExecModule{
		db:          db,
		blockReader: freezeblocks.NewBlockReader(snaps, nil),
		semaphore:   semaphore.NewWeighted(1),
		logger:      log.Root(),
		unwinder:    unwinder,
	}

	err := e.SetHead(t.Context(), unwindToBlock)
	require.ErrorIs(t, err, errStopAfterQuiescence,
		"the probe should have carried SetHead into the mode-B unwind")
	require.True(t, unwinder.writeAcquired,
		"a write tx was held across the build-quiescence wait — a builder blocked on the "+
			"aggregator commit gate can never clear buildingFiles, so the wait can only time out")
}
