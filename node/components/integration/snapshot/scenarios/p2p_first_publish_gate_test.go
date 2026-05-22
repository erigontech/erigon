//go:build p2p_integration

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

package scenarios_test

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// TestP2P_FirstPublishGate is the end-to-end check for the publisher
// startup pre-flight gate (docs/plans/20260522-publisher-startup-preflight.md)
// exercised against the real storage.Provider.
//
// The gated node runs NewP2PNodeWithStorageProvider — its bus,
// orchestrator, lifecycle driver and initial-validation watcher are the
// production component. With EnableGatedAutoPublishV2 its first chain.v2
// advertisement is held until BOTH flow.InitialDownloadsComplete and
// flow.InitialValidationComplete have fired.
//
// Flow:
//  1. A seeder offers meta + salt files and advertises a V2 manifest.
//  2. The gated node peers and downloads them; each lands via RecordFile
//     and the lifecycle driver's OnMetaReady path (infohash check →
//     Advertisable), with no E3 index build.
//  3. statePending drains → InitialStateReady → InitialDownloadsComplete.
//  4. The Provider's watchInitialValidation observes downloads-complete,
//     polls the inventory until every Local file is Advertisable, and
//     fires InitialValidationComplete.
//  5. The first-publish gate opens; the gated auto-publisher publishes.
//
// Assertions: the gated node has published nothing at the moment
// InitialDownloadsComplete fires (gate still shut — validation not done),
// and it does publish once both events have fired.
func TestP2P_FirstPublishGate(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	seeder := harness.NewP2PNode(t, logger)
	gated := harness.NewP2PNodeWithStorageProvider(t, logger)
	gated.EnableGatedAutoPublishV2(200 * time.Millisecond)

	require.Empty(t, gated.V2Publisher().History(), "no publish before sync")

	// Seeder offers meta + salt — the files that take the no-index
	// OnMetaReady path on the consumer.
	seedSubdirFile(t, seeder, "erigondb.toml",
		[]byte("step_size = 32\nsteps_in_frozen_file = 64\n"),
		&snapshot.FileEntry{Name: "erigondb.toml", Kind: snapshot.KindMeta})
	seedSubdirFile(t, seeder, "salt-state.txt",
		multiPieceFixtureBytes("gate-salt-state", 4<<10),
		&snapshot.FileEntry{Name: "salt-state.txt", Kind: snapshot.KindSalt})
	seedSubdirFile(t, seeder, "salt-blocks.txt",
		multiPieceFixtureBytes("gate-salt-blocks", 4<<10),
		&snapshot.FileEntry{Name: "salt-blocks.txt", Kind: snapshot.KindSalt})

	v2Hash := seeder.PublishV2Manifest()
	require.NotEqual(t, [20]byte{}, v2Hash)

	_, btPort := seeder.LocalTorrentAddr()
	seeder.SetDevP2PENREntry(enr.ChainToml{InfoHash: v2Hash, DomainSteps: 64, MergeDepth: 64})
	seeder.SetDevP2PENREntry(enr.BT(btPort))

	gated.AddSeederPeer(seeder)

	var (
		downloadsDone         atomic.Bool
		validationDone        atomic.Bool
		historyLenAtDownloads atomic.Int32
	)
	require.NoError(t, gated.Bus.Subscribe(func(flow.InitialDownloadsComplete) {
		// The gate is gated on InitialValidationComplete too, and the
		// validation watcher only starts polling AFTER downloads-complete
		// — so no publish can have happened yet.
		historyLenAtDownloads.Store(int32(len(gated.V2Publisher().History())))
		downloadsDone.Store(true)
		t.Log("InitialDownloadsComplete fired")
	}))
	require.NoError(t, gated.Bus.Subscribe(func(flow.InitialValidationComplete) {
		validationDone.Store(true)
		t.Log("InitialValidationComplete fired")
	}))

	gated.AddDevP2PPeer(seeder.DevP2PSelf())

	// Wait for the gated first publish to land.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if len(gated.V2Publisher().History()) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NotEmpty(t, gated.V2Publisher().History(),
		"gated node never published within 60s (downloadsDone=%v validationDone=%v)",
		downloadsDone.Load(), validationDone.Load())

	// Drain async subscribers so the recorded flags are settled.
	gated.Bus.WaitAsync()

	require.True(t, downloadsDone.Load(), "InitialDownloadsComplete must have fired")
	require.True(t, validationDone.Load(), "InitialValidationComplete must have fired")
	require.Equal(t, int32(0), historyLenAtDownloads.Load(),
		"gate must still be shut when InitialDownloadsComplete fires — validation not yet done")
}
