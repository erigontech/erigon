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

package antiquary

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Startup is the only removal a default node gets: the dump sites run under snapgen, which is off
// by default, and downloaded files overlap too.
func TestLoopRemovesCoveredBlockSegmentsAtStartup(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeStateFixture(t, dirs.Snap, "beaconblocks", 0, 20_000, logger)
	subSeg := writeStateFixture(t, dirs.Snap, "beaconblocks", 0, 10_000, logger)
	subIdx := strings.TrimSuffix(subSeg, ".seg") + ".idx"

	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, logger)
	t.Cleanup(sn.Close)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	// A cursor past the visible tip keeps the startup reindex from reading the fixture as blocks.
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteLastBeaconSnapshot(tx, 20_000)
	}))

	ctx, cancel := context.WithCancel(t.Context())
	d := &recordingDownloader{}
	a := NewAntiquary(ctx, nil, nil, nil, &clparams.MainnetBeaconConfig, dirs, d, db, nil, sn, nil, nil, logger, false, true, false, false, nil)
	a.NotifyBackfilled()

	done := make(chan error, 1)
	go func() { done <- a.Loop() }()
	require.Eventually(t, func() bool {
		_, err := os.Stat(subSeg)
		return errors.Is(err, os.ErrNotExist)
	}, 10*time.Second, 10*time.Millisecond, "the covered segment must be unlinked at startup")
	cancel()
	if err := <-done; err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}

	require.ElementsMatch(t, []string{filepath.Base(subSeg), filepath.Base(subIdx)}, d.deletedNames())
	require.NoFileExists(t, subIdx)
}
