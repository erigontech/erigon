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

package stagedsync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/shards"
)

// progressDownloader is an in-process downloader: it reports byte progress and
// drops it on reset, like the real one publishing/clearing its stats snapshot.
type progressDownloader struct {
	mu     sync.Mutex
	done   uint64
	total  uint64
	resets int
}

func (d *progressDownloader) set(done, total uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.done, d.total = done, total
}

func (d *progressDownloader) resetCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.resets
}

func (d *progressDownloader) Completed() (uint64, uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.done, d.total
}

func (d *progressDownloader) ResetProgress() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resets++
	d.done, d.total = 0, 0
}

func (d *progressDownloader) Seed(context.Context, []string) error   { return nil }
func (d *progressDownloader) Delete(context.Context, []string) error { return nil }
func (d *progressDownloader) Download(context.Context, *downloaderproto.DownloadRequest) error {
	return nil
}

// plainDownloader lacks the progress capability, like an external downloader
// reached over gRPC.
type plainDownloader struct {
	dbservices.DownloaderClient
}

type frozenBlockReader struct {
	dbservices.FullBlockReader
	frozen uint64
}

func (r frozenBlockReader) FrozenBlocks() uint64 { return r.frozen }

// seedPreverified registers a two-file snapshot set (plus any extra TOML lines)
// so KnownCfg reports a non-zero ExpectBlocks: the registry is empty until a
// chain TOML is loaded at runtime, and the reporter is a no-op without a target.
func seedPreverified(t *testing.T, chainName string, extraLines ...string) uint64 {
	t.Helper()
	empty, _ := snapcfg.KnownCfg(chainName)
	require.Zero(t, empty.ExpectBlocks, "test chain must have no preverified set of its own")
	t.Cleanup(func() { snapcfg.SetToml(chainName, []byte{}, false) })

	toml := `"v1-000000-000500-headers.seg" = "aa"
"v1-000000-000500-bodies.seg" = "bb"
`
	for _, line := range extraLines {
		toml += line + "\n"
	}
	snapcfg.SetToml(chainName, []byte(toml), false)

	cfg, known := snapcfg.KnownCfg(chainName)
	require.True(t, known)
	require.NotZero(t, cfg.ExpectBlocks)
	return cfg.ExpectBlocks
}

type reporterHarness struct {
	cfg          SnapshotsCfg
	downloader   *progressDownloader
	expectBlocks uint64
	events       chan *remoteproto.SyncingReply
}

// newReporterHarness wires the reporter to a real Notifications and temporal DB;
// only the downloader and the frozen-block count are faked.
func newReporterHarness(t *testing.T, chainName string, frozen uint64, downloader dbservices.DownloaderClient) *reporterHarness {
	t.Helper()

	snapshotDownloadProgressInterval = 5 * time.Millisecond
	t.Cleanup(func() { snapshotDownloadProgressInterval = 2 * time.Second })

	notifications := shards.NewNotifications(nil)
	events, cancelSubscription := notifications.Events.AddSyncStateSubscription()
	t.Cleanup(cancelSubscription)

	h := &reporterHarness{
		cfg: SnapshotsCfg{
			db:                 temporaltest.NewTestDB(t, datadir.New(t.TempDir())),
			chainConfig:        &chain.Config{ChainName: chainName},
			snapshotDownloader: downloader,
			blockReader:        frozenBlockReader{frozen: frozen},
			notifier:           notifications,
		},
		events: events,
	}
	if d, ok := downloader.(*progressDownloader); ok {
		h.downloader = d
	}
	return h
}

func newSeededReporterHarness(t *testing.T, frozen uint64) *reporterHarness {
	t.Helper()
	h := newReporterHarness(t, networkname.Bloatnet, frozen, &progressDownloader{})
	h.expectBlocks = seedPreverified(t, networkname.Bloatnet)
	return h
}

// currentBlock reads the progress the reporter recorded, independently of event
// dedup.
func (h *reporterHarness) currentBlock(t *testing.T) uint64 {
	t.Helper()
	var reply *remoteproto.SyncingReply
	err := h.cfg.db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		reply, err = h.cfg.notifier.BuildSyncingReply(tx, h.cfg.blockReader.FrozenBlocks())
		return err
	})
	require.NoError(t, err)
	return reply.CurrentBlock
}

func (h *reporterHarness) awaitEvent(t *testing.T) *remoteproto.SyncingReply {
	t.Helper()
	select {
	case reply := <-h.events:
		return reply
	case <-time.After(10 * time.Second):
		t.Fatal("no sync-state event published")
		return nil
	}
}

func (h *reporterHarness) requireNoEvent(t *testing.T) {
	t.Helper()
	select {
	case reply := <-h.events:
		t.Fatalf("unexpected sync-state event: currentBlock=%d", reply.CurrentBlock)
	case <-time.After(20 * snapshotDownloadProgressInterval):
	}
}

// The metadata gate keeps the first honest sample away for a while (minutes on
// slow webseeds). Without an up-front publish the reply would flash the stage
// list in that window and then drop it for the rest of the download.
func TestSnapshotDownloadProgressReporterPublishesDownloadShapeAtStart(t *testing.T) {
	h := newSeededReporterHarness(t, 0)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	t.Cleanup(func() { stop(nil, true) })

	reply := h.awaitEvent(t)
	require.True(t, reply.Syncing)
	require.Zero(t, reply.CurrentBlock)
	require.Empty(t, reply.Stages)
	require.Equal(t, h.expectBlocks, reply.LastNewBlockSeen)
}

func TestSnapshotDownloadProgressReporterMapsByteRatioToBlocks(t *testing.T) {
	h := newSeededReporterHarness(t, 0)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	t.Cleanup(func() { stop(nil, true) })
	h.downloader.set(250, 1000)

	reply := h.awaitEvent(t)
	require.True(t, reply.Syncing)
	require.InDelta(t, 0.25*float64(h.expectBlocks), float64(reply.CurrentBlock), 1)
	require.Equal(t, h.expectBlocks, reply.LastNewBlockSeen)
}

func TestSnapshotDownloadProgressReporterResetsStaleProgressAtStart(t *testing.T) {
	h := newSeededReporterHarness(t, 0)

	// A terminal sample left behind by the header-chain phase: publishing it
	// as-is would report the full snapshot set as downloaded.
	h.downloader.set(1000, 1000)
	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	defer func() { stop(nil, true) }()

	require.Equal(t, 1, h.downloader.resetCount())
	require.Zero(t, h.currentBlock(t))
}

func TestSnapshotDownloadProgressReporterSkipsCompleteSample(t *testing.T) {
	h := newSeededReporterHarness(t, 0)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	t.Cleanup(func() { stop(nil, true) })
	h.downloader.set(1000, 1000)

	h.requireNoEvent(t)
	require.Zero(t, h.currentBlock(t))
}

func TestSnapshotDownloadProgressReporterSkipsUnknownTotal(t *testing.T) {
	h := newSeededReporterHarness(t, 0)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	t.Cleanup(func() { stop(nil, true) })
	h.downloader.set(400, 0)

	h.requireNoEvent(t)
	require.Zero(t, h.currentBlock(t))
}

func TestSnapshotDownloadProgressReporterStopPinsFullProgressOnSuccess(t *testing.T) {
	const frozen = 499_000
	h := newSeededReporterHarness(t, frozen)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	h.downloader.set(400, 1000)
	h.awaitEvent(t)

	stop(nil, true)

	require.Equal(t, uint64(frozen), h.currentBlock(t))
}

// A download that stopped at 40% must not report 100%: an operator watching
// eth_syncing through a failing stage would see it oscillate real% → 100%.
func TestSnapshotDownloadProgressReporterStopKeepsLastSampleOnFailure(t *testing.T) {
	h := newSeededReporterHarness(t, 499_000)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	h.downloader.set(400, 1000)
	h.awaitEvent(t)

	stop(errors.New("webseed outage"), false)

	require.InDelta(t, 0.4*float64(h.expectBlocks), float64(h.currentBlock(t)), 1)
}

// Shutdown mid-download must not publish on the cancelled ctx either: the final
// db.View would only fail and log.
func TestSnapshotDownloadProgressReporterStopOnShutdownPublishesNothing(t *testing.T) {
	h := newSeededReporterHarness(t, 499_000)
	ctx, cancel := context.WithCancel(t.Context())

	stop := startSnapshotDownloadProgressReporter(ctx, h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	h.downloader.set(400, 1000)
	h.awaitEvent(t)

	cancel()
	stop(context.Canceled, false)

	h.requireNoEvent(t)
	require.InDelta(t, 0.4*float64(h.expectBlocks), float64(h.currentBlock(t)), 1)
}

func TestSnapshotDownloadProgressReporterStopWithoutSamplePinsFullProgress(t *testing.T) {
	const frozen = 499_000
	h := newSeededReporterHarness(t, frozen)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	stop(nil, true)

	require.Equal(t, uint64(frozen), h.currentBlock(t))
}

// Completion can land inside the last sampling window, while the downloader's
// metadata gate still reports (0, 0). The success pin must not depend on
// downloader state, or it clears the progress instead of pinning it.
func TestSnapshotDownloadProgressReporterStopPinsWhenDownloaderReportsNoProgress(t *testing.T) {
	const frozen = 499_000
	h := newSeededReporterHarness(t, frozen)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	h.downloader.set(400, 1000)
	h.awaitEvent(t)
	h.downloader.set(0, 0)

	stop(nil, true)

	require.Equal(t, uint64(frozen), h.currentBlock(t))
}

// Without state files (--snap.skip-state-snapshot-download) there is no
// commitment block and execution starts from genesis: pinning would claim the
// frozen tip for the whole re-execution, so the stop func must clear instead.
func TestSnapshotDownloadProgressReporterStopClearsWithoutCommitmentBlock(t *testing.T) {
	h := newSeededReporterHarness(t, 499_000)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	h.downloader.set(400, 1000)
	h.awaitEvent(t)

	stop(nil, false)

	require.Zero(t, h.currentBlock(t))
}

// The snapshots cover only the download target, so an FCU arriving mid-download
// must not scale the byte ratio onto the live head.
func TestSnapshotDownloadProgressReporterDoesNotScaleToLiveHead(t *testing.T) {
	h := newSeededReporterHarness(t, 0)
	h.cfg.notifier.NewLastBlockSeen(h.expectBlocks + 1_000_000)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	t.Cleanup(func() { stop(nil, true) })
	h.downloader.set(500, 1000)

	reply := h.awaitEvent(t)
	require.InDelta(t, 0.5*float64(h.expectBlocks), float64(reply.CurrentBlock), 1)
}

// With the download capped, the byte total covers only the capped file set, so
// the target must be capped too or progress overshoots.
func TestSnapshotDownloadProgressReporterCapsTargetAtDownloadToBlock(t *testing.T) {
	const toBlock = 200_000
	h := newSeededReporterHarness(t, 0)
	h.cfg.syncConfig.SnapshotDownloadToBlock = toBlock

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	t.Cleanup(func() { stop(nil, true) })
	h.downloader.set(500, 1000)

	reply := h.awaitEvent(t)
	require.InDelta(t, 0.5*float64(toBlock-1), float64(reply.CurrentBlock), 1)
}

func TestSnapshotDownloadProgressReporterDownloadToBlockAboveTipKeepsTarget(t *testing.T) {
	h := newSeededReporterHarness(t, 0)
	h.cfg.syncConfig.SnapshotDownloadToBlock = 10 * h.expectBlocks

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	t.Cleanup(func() { stop(nil, true) })
	h.downloader.set(500, 1000)

	reply := h.awaitEvent(t)
	require.InDelta(t, 0.5*float64(h.expectBlocks), float64(reply.CurrentBlock), 1)
}

// CL segments (beaconblocks, blobsidecars) are numbered by slot: on chains
// where slots exceed block numbers, a target derived from every .seg would
// report blocks past the EL tip and snap backwards at the success pin.
func TestSnapshotDownloadProgressReporterTargetIgnoresSlotNumberedSegments(t *testing.T) {
	h := newReporterHarness(t, networkname.Bloatnet, 0, &progressDownloader{})
	seedPreverified(t, networkname.Bloatnet, `"v1.1-000000-000800-beaconblocks.seg" = "cc"`)
	const headersTip = 499_999

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	h.awaitEvent(t) // the start event marking the download active
	t.Cleanup(func() { stop(nil, true) })
	h.downloader.set(500, 1000)

	reply := h.awaitEvent(t)
	require.InDelta(t, 0.5*float64(headersTip), float64(reply.CurrentBlock), 1)
	require.Equal(t, uint64(headersTip), reply.LastNewBlockSeen)
}

func TestSnapshotDownloadProgressReporterNoopWithoutProgressCapability(t *testing.T) {
	h := newReporterHarness(t, networkname.Bloatnet, 499_000, plainDownloader{})
	seedPreverified(t, networkname.Bloatnet)

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	stop(nil, false)

	h.requireNoEvent(t)
	require.Zero(t, h.currentBlock(t))
}

func TestSnapshotDownloadProgressReporterNoopWithoutKnownTarget(t *testing.T) {
	h := newReporterHarness(t, networkname.Bloatnet, 0, &progressDownloader{})

	h.downloader.set(250, 1000)
	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	stop(nil, false)

	require.Zero(t, h.downloader.resetCount())
	h.requireNoEvent(t)
	require.Zero(t, h.currentBlock(t))
}

func TestSnapshotDownloadProgressReporterNoopWithoutNotifier(t *testing.T) {
	h := newSeededReporterHarness(t, 0)
	h.cfg.notifier = nil

	stop := startSnapshotDownloadProgressReporter(t.Context(), h.cfg)
	stop(nil, false)

	require.Zero(t, h.downloader.resetCount())
}
