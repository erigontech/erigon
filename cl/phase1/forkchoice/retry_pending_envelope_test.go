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

package forkchoice

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/hashicorp/golang-lru/v2"
)

type retryPendingForkGraph struct {
	payloadVoteForkGraph
	block *cltypes.SignedBeaconBlock
	gate  chan struct{} // HasEnvelope blocks on it, inside the coordinated section
	// HasEnvelope counts its calls inside the coordinated section; while gate blocks the first call, that is
	// the number of attempts.
	attempts *atomic.Int32
}

func (g retryPendingForkGraph) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return g.block, g.block != nil
}

func (g retryPendingForkGraph) GetHeader(common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if g.block == nil || g.block.Block == nil {
		return nil, false
	}
	return &cltypes.BeaconBlockHeader{Slot: g.block.Block.Slot, Root: g.block.Block.StateRoot}, true
}

// With a gate, HasEnvelope blocks until it opens and then reports the envelope as present, so
// the apply ends on the already-persisted path without touching a config the stub lacks.
func (g retryPendingForkGraph) HasEnvelope(common.Hash) bool {
	if g.attempts != nil {
		g.attempts.Add(1)
	}
	if g.gate != nil {
		<-g.gate
		return true
	}
	return false
}

func newRetryPendingStore(t *testing.T, peerDas *das_mock.MockPeerDas) (*ForkChoiceStore, *lru.Cache[common.Hash, *cltypes.SignedExecutionPayloadEnvelope]) {
	t.Helper()
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 7, Body: &cltypes.BeaconBody{
		Version: clparams.GloasVersion,
		SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](4, 48),
		}},
	}}}
	block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	retryAt, err := lru.New[common.Hash, *pendingEnvelopeRetryBooking](2)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		forkGraph:                      retryPendingForkGraph{block: block},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		peerDas:                        peerDas,
		dataAvailableEnvelopeRetryAt:   retryAt,
		emitters:                       beaconevents.NewEventEmitter(),
	}
	return f, pending
}

func TestRetryPendingExecutionPayloadEnvelopeIgnoresRootsWithoutPendingEnvelope(t *testing.T) {
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	f, _ := newRetryPendingStore(t, peerDas)

	require.True(t, f.RetryPendingExecutionPayloadEnvelope(t.Context(), common.HexToHash("0x1")).IsZero())
}

// While the columns are missing, the retry probes them once per probe interval, and a caller
// that comes earlier gets the time of the next probe.
func TestRetryPendingExecutionPayloadEnvelopeProbesMissingColumnDataOncePerInterval(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(false, nil).Times(1)
	f, pending := newRetryPendingStore(t, peerDas)
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{})

	start := time.Now()
	next := f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
	require.WithinDuration(t, start.Add(missingEnvelopeDataProbeInterval), next, 100*time.Millisecond)
	require.Equal(t, next, f.RetryPendingExecutionPayloadEnvelope(t.Context(), root))
	require.True(t, pending.Contains(root))
}

func TestRetryPendingExecutionPayloadEnvelopeAppliesOnceColumnDataBecomesAvailable(t *testing.T) {
	cfg, blockState, block, envelope := validBlobEnvelopeFixture(t)
	root := envelope.Message.BeaconBlockRoot

	f := newPayloadVoteTestStore(t, root, false, false)
	gasLimits, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	f.executionPayloadGasLimit = gasLimits
	f.beaconCfg = cfg
	graph := &persistedEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	f.forkGraph = graph
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	pending.Add(root, envelope)
	f.pendingEnvelopes = pending
	f.pendingLocalSelfBuildEnvelopes = local
	retryAt, err := lru.New[common.Hash, *pendingEnvelopeRetryBooking](2)
	require.NoError(t, err)
	f.dataAvailableEnvelopeRetryAt = retryAt
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	gomock.InOrder(
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(false, nil),
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(true, nil),
		// The apply checks the columns again after admission: the custody set can grow meanwhile.
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(true, nil),
	)
	f.peerDas = peerDas

	// The columns are still missing at the first retry and complete by the next one.
	require.False(t, f.RetryPendingExecutionPayloadEnvelope(t.Context(), root).IsZero())
	require.True(t, pending.Contains(root))
	retryAt.Remove(root) // the probe interval has passed
	f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)

	require.True(t, graph.HasEnvelope(root))
	require.True(t, f.isPayloadAvailable(root))
	status, ok := f.GetRecentExecutionPayloadStatusByRoot(root)
	require.True(t, ok)
	require.Equal(t, execution_client.PayloadStatus(execution_client.PayloadStatusNotValidated), status)
	require.False(t, pending.Contains(root))
}

// A caller that comes while an attempt is in flight is not admitted: it gets the time of the next
// attempt, one retry interval after the one in flight started.
func TestRetryPendingExecutionPayloadEnvelopeAdmitsOneAttemptPerInterval(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil).Times(1)
	f, pending := newRetryPendingStore(t, peerDas)
	release := make(chan struct{})
	f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: release}
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})

	start := time.Now()
	first := make(chan struct{})
	go func() {
		f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
		close(first)
	}()
	require.Eventually(t, func() bool {
		_, busy := f.pendingEnvelopeApplyGate.Load(root)
		return busy
	}, 5*time.Second, time.Millisecond)

	next := f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
	require.WithinDuration(t, start.Add(dataAvailableEnvelopeRetryInterval), next, 500*time.Millisecond)
	_, busy := f.pendingEnvelopeApplyGate.Load(root)
	require.True(t, busy, "the first attempt is still in flight")
	close(release)
	awaitSignal(t, first, "the first retry")
}

// A caller that comes while another one probes the columns is not admitted: the retry is booked
// before the probe, so it gets the time of the next retry without probing again.
func TestRetryPendingExecutionPayloadEnvelopeBooksBeforeProbingTheColumns(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	probing, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).DoAndReturn(func(uint64, common.Hash) (bool, error) {
		close(probing)
		<-release
		return false, nil
	}).Times(1)
	f, pending := newRetryPendingStore(t, peerDas)
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{})

	go func() {
		f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
		close(done)
	}()
	awaitSignal(t, probing, "the first probe")
	require.False(t, f.RetryPendingExecutionPayloadEnvelope(t.Context(), root).IsZero())
	close(release)
	awaitSignal(t, done, "the first retry")
}

// A producer that keeps reporting columns of one root makes the wake probe that root each time, and no other:
// the rest stay on the probe interval of the poll.
func TestRetryPendingExecutionPayloadEnvelopesOnColumnsStoredLeavesUnrelatedRootsToThePoll(t *testing.T) {
	const wakes = 20
	woken, unrelated := common.HexToHash("0x1"), common.HexToHash("0x2")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), woken).Return(false, nil).Times(wakes)
	f, pending := newRetryPendingStore(t, peerDas)
	pending.Add(woken, &cltypes.SignedExecutionPayloadEnvelope{})
	pending.Add(unrelated, &cltypes.SignedExecutionPayloadEnvelope{})

	for range wakes {
		f.emitters.WakePendingEnvelopeRetry(woken)
		f.RetryPendingExecutionPayloadEnvelopesOnColumnsStored(t.Context(), 0)
	}
	require.Empty(t, f.emitters.TakePendingEnvelopeRetryRoots())
}

func TestRetryPendingExecutionPayloadEnvelopesOnColumnsStoredIgnoresRootsWithoutPendingEnvelope(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	f, _ := newRetryPendingStore(t, peerDas)

	f.emitters.WakePendingEnvelopeRetry(root)
	f.RetryPendingExecutionPayloadEnvelopesOnColumnsStored(t.Context(), 0)

	_, booked := f.dataAvailableEnvelopeRetryAt.Peek(root)
	require.False(t, booked)
}

func TestPrunePendingEnvelopeRetryRootsKeepsParkedRootsAtOrAboveTheFloor(t *testing.T) {
	parked, unparked := common.HexToHash("0x1"), common.HexToHash("0x2")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), parked).Return(true, nil).AnyTimes()
	f, pending := newRetryPendingStore(t, peerDas)
	pending.Add(parked, &cltypes.SignedExecutionPayloadEnvelope{})

	f.emitters.WakePendingEnvelopeRetry(unparked)
	require.False(t, f.PrunePendingEnvelopeRetryRoots(0), "no woken root has a parked envelope")
	require.Empty(t, f.emitters.TakePendingEnvelopeRetryRoots())

	f.emitters.WakePendingEnvelopeRetry(parked)
	require.False(t, f.PrunePendingEnvelopeRetryRoots(8), "the block is below the floor")
	require.Empty(t, f.emitters.TakePendingEnvelopeRetryRoots())

	f.emitters.WakePendingEnvelopeRetry(parked)
	f.emitters.WakePendingEnvelopeRetry(unparked)
	require.True(t, f.PrunePendingEnvelopeRetryRoots(7))
	require.Equal(t, []common.Hash{parked}, f.emitters.TakePendingEnvelopeRetryRoots(), "the pass still finds the parked root")
}

func TestPrunePendingEnvelopeRetryRootsKeepsOnlyRootsThatCanBeAttemptedNow(t *testing.T) {
	root := common.HexToHash("0x1")
	hourAhead, hourAgo := time.Now().Add(time.Hour), time.Now().Add(-time.Hour)
	for _, tc := range []struct {
		name       string
		booking    *pendingEnvelopeRetryBooking
		available  bool
		want       bool
		wantProbes int
	}{
		{"columns missing", nil, false, false, 1},
		{"columns available", nil, true, true, 1},
		{"a probe booking does not hold the wake", &pendingEnvelopeRetryBooking{at: hourAhead}, true, true, 1},
		{"an attempt is booked ahead", &pendingEnvelopeRetryBooking{at: hourAhead, attempt: true}, true, false, 0},
		{"the attempt interval is over", &pendingEnvelopeRetryBooking{at: hourAgo, attempt: true}, true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var probes int
			peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
			peerDas.EXPECT().IsDataAvailable(uint64(7), root).DoAndReturn(func(uint64, common.Hash) (bool, error) {
				probes++
				return tc.available, nil
			}).AnyTimes()
			f, pending := newRetryPendingStore(t, peerDas)
			pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{})
			if tc.booking != nil {
				f.dataAvailableEnvelopeRetryAt.Add(root, tc.booking)
			}
			f.emitters.WakePendingEnvelopeRetry(root)

			require.Equal(t, tc.want, f.PrunePendingEnvelopeRetryRoots(0))

			require.Equal(t, tc.wantProbes, probes)
			if tc.want {
				require.Equal(t, []common.Hash{root}, f.emitters.TakePendingEnvelopeRetryRoots())
			} else {
				require.Empty(t, f.emitters.TakePendingEnvelopeRetryRoots(), "a later column store reports the root again")
			}
			booking, booked := f.dataAvailableEnvelopeRetryAt.Peek(root)
			require.Equal(t, tc.booking != nil, booked, "the check books nothing")
			require.Same(t, tc.booking, booking)
		})
	}
}

func TestPrunePendingEnvelopeRetryRootsTreatsMissingPeerDasAsUnavailable(t *testing.T) {
	root := common.HexToHash("0x1")
	f, pending := newRetryPendingStore(t, nil)
	f.peerDas = nil
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{})

	f.emitters.WakePendingEnvelopeRetry(root)
	require.False(t, f.PrunePendingEnvelopeRetryRoots(0))
}

func TestPrunePendingEnvelopeRetryRootsKeepsAZeroBlobRootWithoutProbingColumns(t *testing.T) {
	root := common.HexToHash("0x1")
	f, pending := newRetryPendingStore(t, das_mock.NewMockPeerDas(gomock.NewController(t)))
	f.forkGraph = retryPendingForkGraph{block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 7, Body: &cltypes.BeaconBody{
		Version: clparams.GloasVersion,
		SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](4, 48),
		}},
	}}}}
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{})

	f.emitters.WakePendingEnvelopeRetry(root)
	require.True(t, f.PrunePendingEnvelopeRetryRoots(0))
}

// A booking that vanished while the caller probed (evicted, or dropped by an interrupted attempt) does not
// stop the caller from attempting: only an attempt that another caller booked since does.
func TestRetryPendingExecutionPayloadEnvelopeAttemptsWhenItsBookingVanished(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	f, pending := newRetryPendingStore(t, peerDas)
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).DoAndReturn(func(uint64, common.Hash) (bool, error) {
		f.dataAvailableEnvelopeRetryAt.Remove(root)
		return true, nil
	}).AnyTimes()
	opened := make(chan struct{})
	close(opened) // the apply ends on the persisted path instead of the stub's missing config
	f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: opened}
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})

	f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)

	require.False(t, pending.Contains(root), "the attempt ran")
}

// The attempt that follows a vanished probe booking holds the interval itself, so a caller that comes while the
// attempt is in flight does not attempt again.
func TestRetryPendingExecutionPayloadEnvelopeBooksItsAttemptWhenItsBookingVanished(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	f, pending := newRetryPendingStore(t, peerDas)
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).DoAndReturn(func(uint64, common.Hash) (bool, error) {
		f.dataAvailableEnvelopeRetryAt.Remove(root)
		return true, nil
	}).Times(1)
	release := make(chan struct{})
	f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: release}
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})
	done := make(chan struct{})
	go func() {
		f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
		close(done)
	}()
	require.Eventually(t, func() bool {
		_, busy := f.pendingEnvelopeApplyGate.Load(root)
		return busy
	}, 5*time.Second, time.Millisecond)

	booking, booked := f.dataAvailableEnvelopeRetryAt.Peek(root)
	close(release)
	awaitSignal(t, done, "the retry")
	require.True(t, booked, "the attempt in flight holds a booking")
	require.True(t, booking.attempt)
}

// A caller whose probe booking was replaced yields only to a booking that attempts: the caller that saw the columns
// attempts after a wake that saw none, and not after a wake that attempts, even when both bookings carry the same time.
func TestRetryPendingExecutionPayloadEnvelopeYieldsToAColumnWake(t *testing.T) {
	for _, wakeSeesData := range []bool{true, false} {
		t.Run(fmt.Sprintf("wakeSeesData=%t", wakeSeesData), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root := common.HexToHash("0x1")
				probing, releaseProbe := make(chan struct{}), make(chan struct{})
				var probes atomic.Int32
				peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
				peerDas.EXPECT().IsDataAvailable(uint64(7), root).DoAndReturn(func(uint64, common.Hash) (bool, error) {
					if probes.Add(1) == 1 {
						close(probing)
						<-releaseProbe
						return true, nil
					}
					return wakeSeesData, nil
				}).AnyTimes()
				f, pending := newRetryPendingStore(t, peerDas)
				holdApply := make(chan struct{})
				var attempts atomic.Int32
				f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: holdApply, attempts: &attempts}
				pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})
				releaseProbeOnce, releaseApplyOnce := sync.OnceFunc(func() { close(releaseProbe) }), sync.OnceFunc(func() { close(holdApply) })
				defer releaseProbeOnce()
				defer releaseApplyOnce()

				targeted, woken := make(chan struct{}), make(chan struct{})
				go func() {
					f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
					close(targeted)
				}()
				<-probing
				f.emitters.WakePendingEnvelopeRetry(root)
				go func() {
					f.RetryPendingExecutionPayloadEnvelopesOnColumnsStored(t.Context(), 0)
					close(woken)
				}()
				synctest.Wait()
				_, applying := f.pendingEnvelopeApplyGate.Load(root)
				require.Equal(t, wakeSeesData, applying, "the wake applies the envelope only when it sees the columns")

				releaseProbeOnce()
				synctest.Wait()
				returned := false
				select {
				case <-targeted:
					returned = true
				default:
				}
				require.Equal(t, wakeSeesData, returned, "the replaced caller returns only when the wake attempts")
				require.EqualValues(t, 1, attempts.Load(), "one attempt per interval")
				releaseApplyOnce()
				synctest.Wait()
				for name, done := range map[string]chan struct{}{"the targeted retry": targeted, "the wake": woken} {
					select {
					case <-done:
					default:
						t.Fatalf("%s did not finish", name)
					}
				}
			})
		})
	}
}

// Without PeerDAS the columns of a blob-carrying envelope count as missing, so the retry probes
// again later and does not apply the envelope.
func TestRetryPendingExecutionPayloadEnvelopeTreatsMissingPeerDasAsUnavailable(t *testing.T) {
	root := common.HexToHash("0x1")
	f, pending := newRetryPendingStore(t, nil)
	f.peerDas = nil
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})

	start := time.Now()
	next := f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
	require.WithinDuration(t, start.Add(missingEnvelopeDataProbeInterval), next, 100*time.Millisecond)
	require.True(t, pending.Contains(root))
	_, applying := f.pendingEnvelopeApplyGate.Load(root)
	require.False(t, applying)
}

func TestCheckDataAvailabilityTreatsMissingPeerDasAsUnavailable(t *testing.T) {
	f, _ := newRetryPendingStore(t, nil)
	f.peerDas = nil
	block, _ := f.forkGraph.GetBlock(common.Hash{}) // commits to one blob
	require.ErrorIs(t, f.checkDataAvailability(block, common.Hash{}), ErrEIP7594ColumnDataNotAvailable)
}

// An attempt that the caller's context ends before a verdict does not hold the retry interval,
// so the next caller does not lose up to one interval of its budget.
func TestRetryPendingExecutionPayloadEnvelopeReleasesTheIntervalOfAnInterruptedAttempt(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil).Times(1)
	f, pending := newRetryPendingStore(t, peerDas)
	release := make(chan struct{})
	f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: release}
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		f.RetryPendingExecutionPayloadEnvelope(ctx, root)
		close(done)
	}()
	require.Eventually(t, func() bool {
		_, busy := f.pendingEnvelopeApplyGate.Load(root)
		return busy
	}, 5*time.Second, time.Millisecond)
	_, booked := f.dataAvailableEnvelopeRetryAt.Peek(root)
	require.True(t, booked, "the attempt in flight holds the interval")
	cancel()
	close(release)
	awaitSignal(t, done, "the interrupted retry")

	_, booked = f.dataAvailableEnvelopeRetryAt.Peek(root)
	require.False(t, booked)
}

// gateWaitSignal closes waiting when the apply under test starts to wait at a busy per-root gate:
// before admission, only that branch of enterPendingEnvelopeApply calls Done.
type gateWaitSignal struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *gateWaitSignal) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

// applyWaitingAtTheGate starts an apply while the test holds the gate and returns once it waits
// there, so the test changes the queue only after the apply is blocked.
func applyWaitingAtTheGate(t *testing.T, f *ForkChoiceStore, root common.Hash) <-chan struct{} {
	t.Helper()
	ctx := &gateWaitSignal{Context: t.Context(), waiting: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		f.applyPendingEnvelope(ctx, root, false)
		close(done)
	}()
	awaitSignal(t, ctx.waiting, "the apply waiting at the gate")
	return done
}

func TestEnterPendingEnvelopeApplyAdmitsAWaiterAfterTheHolderLeaves(t *testing.T) {
	f := &ForkChoiceStore{}
	root := common.HexToHash("0x1")
	require.True(t, f.enterPendingEnvelopeApply(context.Background(), root))

	waiter := make(chan bool, 1)
	go func() {
		admitted := f.enterPendingEnvelopeApply(context.Background(), root)
		waiter <- admitted
		f.leavePendingEnvelopeApply(root)
	}()
	select {
	case <-waiter:
		t.Fatal("a waiter was admitted while the holder was still applying")
	case <-time.After(50 * time.Millisecond):
	}
	f.leavePendingEnvelopeApply(root)
	select {
	case admitted := <-waiter:
		require.True(t, admitted, "the waiter runs itself after the holder leaves")
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter was not admitted after the holder left")
	}

	// A waiter whose context ends first is not admitted.
	require.True(t, f.enterPendingEnvelopeApply(context.Background(), root))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, f.enterPendingEnvelopeApply(ctx, root))
	f.leavePendingEnvelopeApply(root)
}

// A caller whose budget already expired must not take the gate even when it is idle: the apply
// would run commitments, BLS and the state transition before the EL rejects the dead context.
func TestEnterPendingEnvelopeApplyRefusesADeadContextAtAnIdleGate(t *testing.T) {
	f := &ForkChoiceStore{}
	root := common.HexToHash("0x1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.False(t, f.enterPendingEnvelopeApply(ctx, root))
	_, busy := f.pendingEnvelopeApplyGate.Load(root)
	require.False(t, busy)
}

// Every caller of applyPendingEnvelope, block import included, holds the per-root gate.
func TestApplyPendingEnvelopeHoldsThePerRootGate(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil).AnyTimes()
	f, pending := newRetryPendingStore(t, peerDas)
	release := make(chan struct{})
	f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: release}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}}
	pending.Add(root, envelope)

	done := make(chan struct{})
	go func() {
		f.applyPendingEnvelope(t.Context(), root, false)
		close(done)
	}()
	require.Eventually(t, func() bool {
		_, busy := f.pendingEnvelopeApplyGate.Load(root)
		return busy
	}, 5*time.Second, time.Millisecond)
	close(release)
	awaitSignal(t, done, "the waiting apply")
	_, busy := f.pendingEnvelopeApplyGate.Load(root)
	require.False(t, busy)
}

type countingForkGraph struct {
	retryPendingForkGraph
	hasEnvelopeCalls *atomic.Int32
}

func (g countingForkGraph) HasEnvelope(root common.Hash) bool {
	g.hasEnvelopeCalls.Add(1)
	return g.retryPendingForkGraph.HasEnvelope(root)
}

// A waiter admitted after the holder settled the same parked copy must not apply it again.
func TestApplyPendingEnvelopeSkipsACopyTheHolderSettled(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil).AnyTimes()
	f, pending := newRetryPendingStore(t, peerDas)
	calls := &atomic.Int32{}
	opened := make(chan struct{})
	close(opened) // an unexpected apply ends on the persisted path instead of the stub's missing config
	f.forkGraph = countingForkGraph{retryPendingForkGraph: retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: opened}, hasEnvelopeCalls: calls}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}}
	pending.Add(root, envelope)

	require.True(t, f.enterPendingEnvelopeApply(t.Context(), root))
	done := applyWaitingAtTheGate(t, f, root)
	pending.Remove(root) // the holder settled the copy
	f.leavePendingEnvelopeApply(root)
	awaitSignal(t, done, "the waiting apply")

	require.Zero(t, calls.Load(), "the waiter must return without touching the envelope")
}

// A waiter whose copy was replaced while it waited applies the newer parked copy instead of
// leaving it for the next external trigger.
func TestApplyPendingEnvelopeAppliesANewerParkedCopy(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil).AnyTimes()
	f, pending := newRetryPendingStore(t, peerDas)
	calls := &atomic.Int32{}
	opened := make(chan struct{})
	close(opened) // the apply ends on the persisted path instead of the stub's missing config
	f.forkGraph = countingForkGraph{retryPendingForkGraph: retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: opened}, hasEnvelopeCalls: calls}
	older := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}}
	newer := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root, BuilderIndex: 1}}
	pending.Add(root, older)

	require.True(t, f.enterPendingEnvelopeApply(t.Context(), root))
	done := applyWaitingAtTheGate(t, f, root)
	pending.Add(root, newer) // the holder parked a newer copy
	f.leavePendingEnvelopeApply(root)
	awaitSignal(t, done, "the waiting apply")

	require.NotZero(t, calls.Load(), "the waiter must apply the newer parked copy")
	require.False(t, pending.Contains(root), "the newer copy left the queue")
}

// The admitted apply picks the queue itself: a waiter whose self-built copy the holder settled
// applies the gossip copy parked meanwhile.
func TestApplyPendingEnvelopePicksTheQueueOnceAdmitted(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil).AnyTimes()
	f, pending := newRetryPendingStore(t, peerDas)
	opened := make(chan struct{})
	close(opened) // the apply ends on the persisted path instead of the stub's missing config
	f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: opened}
	f.pendingLocalSelfBuildEnvelopes.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})

	require.True(t, f.enterPendingEnvelopeApply(t.Context(), root))
	done := applyWaitingAtTheGate(t, f, root)
	f.pendingLocalSelfBuildEnvelopes.Remove(root) // the holder settled the self-built copy
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root, BuilderIndex: 1}})
	f.leavePendingEnvelopeApply(root)
	awaitSignal(t, done, "the waiting apply")

	require.False(t, pending.Contains(root), "the gossip copy was applied")
}

// Callers that respect the booking, each with a column probe longer than the probe interval, overtake each
// other's probe booking before their probes end. The caller that saw the columns must still attempt.
func TestRetryPendingExecutionPayloadEnvelopeSlowProbesDoNotStarveTheAttempt(t *testing.T) {
	const (
		slowProbe    = 2 * missingEnvelopeDataProbeInterval
		pollInterval = 200 * time.Millisecond
		proposalPoll = 100 * time.Millisecond
	)
	synctest.Test(t, func(t *testing.T) {
		root := common.HexToHash("0x1")
		peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
		peerDas.EXPECT().IsDataAvailable(uint64(7), root).DoAndReturn(func(uint64, common.Hash) (bool, error) {
			time.Sleep(slowProbe)
			return true, nil
		}).AnyTimes()
		f, pending := newRetryPendingStore(t, peerDas)
		var attempts atomic.Int32
		opened := make(chan struct{})
		close(opened) // the apply ends on the persisted path instead of the stub's missing config
		f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: opened, attempts: &attempts}
		pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		wg.Go(func() {
			for ctx.Err() == nil && pending.Contains(root) {
				next := f.RetryPendingExecutionPayloadEnvelope(ctx, root)
				select {
				case <-ctx.Done():
				case <-time.After(min(time.Until(next), proposalPoll)):
				}
			}
		})
		wg.Go(func() {
			ticker := time.NewTicker(pollInterval)
			defer ticker.Stop()
			for ctx.Err() == nil && pending.Contains(root) {
				select {
				case <-ctx.Done():
				case <-ticker.C:
					f.RetryDataAvailablePendingExecutionPayloadEnvelopes(ctx, 0)
				}
			}
		})
		wg.Wait()

		require.NotZero(t, attempts.Load(), "the envelope must be attempted while its columns stay available")
	})
}
