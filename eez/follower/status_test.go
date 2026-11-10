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

package follower_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/types"
)

// phases records the follower's phase at each fork choice and L1 log query,
// once per run of one phase.
type phases struct {
	mu  sync.Mutex
	f   *follower.Follower
	got []follower.Phase
}

func (p *phases) record() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.f == nil {
		return
	}
	if ph := p.f.Status().Phase; len(p.got) == 0 || p.got[len(p.got)-1] != ph {
		p.got = append(p.got, ph)
	}
}

func (p *phases) seen() []follower.Phase {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.got)
}

// hookChain runs hook before each fork choice.
type hookChain struct {
	deriver.Chain
	hook func()
}

func (c hookChain) ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error {
	c.hook()
	return c.Chain.ForkChoice(ctx, head, safe, finalized)
}

// hookReader runs hook before each L1 log query.
type hookReader struct {
	*recording.MemoryChain
	hook func()
}

func (r hookReader) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	r.hook()
	return r.MemoryChain.Logs(ctx, from, to, address, topics)
}

func awaitPhase(t *testing.T, f *follower.Follower, want follower.Phase) {
	t.Helper()
	require.Eventually(t, func() bool { return f.Status().Phase == want }, 10*time.Second, 5*time.Millisecond, "phase %s", want)
}

func last(b *derivertest.Batch) *types.Block { return b.Blocks[len(b.Blocks)-1] }

// TestStatusBootFromPeers: a follower in peers mode reports its mode, each
// phase of its boot, L1's finalized block F and the root H_F there as boot
// reads them, H_F's number once the history is local, and the source that
// moved the head last.
func TestStatusBootFromPeers(t *testing.T) {
	t.Run("waiting", func(t *testing.T) {
		c := derivertest.NewComposer(t)
		synced := composeBatches(t, c, 1, 3)
		n, _, _ := newPeersNode(t, c, c.L1, false)
		s := n.follower.Status()
		require.Equal(t, follower.SyncPeers, s.Mode)
		require.Equal(t, follower.PhaseBooting, s.Phase)
		require.Nil(t, s.Anchor)
		require.Empty(t, s.HeadSource)
		require.Nil(t, s.OverlayPeers, "no overlay")
		require.Nil(t, s.SignedHeads, "no signed heads")

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- n.follower.Boot(ctx) }()
		awaitPhase(t, n.follower, follower.PhaseWaitingForFinality)
		require.Nil(t, n.follower.Status().Anchor)

		f := finalizeHead(t, c)
		awaitPhase(t, n.follower, follower.PhaseWaitingForPeers)
		root := last(synced[0]).Hash()
		require.Equal(t, &follower.Anchor{L1Block: f.Number, L1BlockHash: f.Hash, Root: &root}, n.follower.Status().Anchor)
		require.Empty(t, n.follower.Status().HeadSource, "a rewind to genesis at genesis moves nothing")
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})

	t.Run("synced and derived", func(t *testing.T) {
		c := derivertest.NewComposer(t)
		synced := composeBatches(t, c, 2, 4)
		f := finalizeHead(t, c)
		composeBatches(t, c, 1, 3)
		p := &phases{}
		m, chain := derivertest.NewL2(t)
		s := newPeerSentry(c.L2, true)
		n := (&node{m: m, dir: t.TempDir()}).restart(t, hookReader{c.L1, p.record}, hookChain{chain, p.record}, peersMode(newFetcher(t, s, chain)))
		p.f = n.follower

		require.NoError(t, n.follower.Boot(t.Context()))
		require.Equal(t, []follower.Phase{follower.PhaseBooting, follower.PhaseSyncing, follower.PhaseDeriving}, p.seen())
		st := n.follower.Status()
		require.Equal(t, follower.PhaseFollowing, st.Phase)
		root, height := last(synced[1]).Hash(), last(synced[1]).NumberU64()
		require.Equal(t, &follower.Anchor{L1Block: f.Number, L1BlockHash: f.Hash, Root: &root, L2Block: &height}, st.Anchor)
		require.Equal(t, follower.HeadDerivation, st.HeadSource)
	})

	t.Run("head from the history sync", func(t *testing.T) {
		c := derivertest.NewComposer(t)
		synced := composeBatches(t, c, 1, 3)
		finalizeHead(t, c)
		n, _, _ := newPeersNode(t, c, c.L1, true)

		require.NoError(t, n.follower.Boot(t.Context()))
		st := n.follower.Status()
		require.Equal(t, follower.HeadHistorySync, st.HeadSource)
		height := last(synced[0]).NumberU64()
		require.Equal(t, &height, st.Anchor.L2Block)
	})

	t.Run("registry not deployed at F", func(t *testing.T) {
		c := derivertest.NewComposer(t)
		composeBatches(t, c, 1, 3)
		f, err := c.L1.HeaderByNumber(t.Context(), derivertest.Config().L2.(*config.L2Config).RegistryDeployBlock-1)
		require.NoError(t, err)
		require.NoError(t, c.L1.Finalize(f.Number))
		n, _, _ := newPeersNode(t, c, c.L1, true)

		require.NoError(t, n.follower.Boot(t.Context()))
		var genesis uint64
		require.Equal(t, &follower.Anchor{L1Block: f.Number, L1BlockHash: f.Hash, L2Block: &genesis}, n.follower.Status().Anchor)
	})
}

// TestStatusL1Only: an L1-only follower reports its mode, derives in boot,
// and reports the root L1 finalized before its scan, with its number once
// the local chain holds it: after the scan, or at once on a restart.
func TestStatusL1Only(t *testing.T) {
	c := derivertest.NewComposer(t)
	batches := composeBatches(t, c, 1, 3)
	f := finalizeHead(t, c)
	p := &phases{}
	m, chain := derivertest.NewL2(t)
	n := (&node{m: m, dir: t.TempDir()}).restart(t, c.L1, hookChain{chain, p.record})
	p.f = n.follower
	s := n.follower.Status()
	require.Equal(t, follower.SyncL1Only, s.Mode)
	require.Equal(t, follower.PhaseBooting, s.Phase)

	require.NoError(t, n.follower.Boot(t.Context()))
	require.Equal(t, []follower.Phase{follower.PhaseBooting, follower.PhaseDeriving}, p.seen())
	s = n.follower.Status()
	require.Equal(t, follower.PhaseFollowing, s.Phase)
	root, height := last(batches[0]).Hash(), last(batches[0]).NumberU64()
	anchor := &follower.Anchor{L1Block: f.Number, L1BlockHash: f.Hash, Root: &root, L2Block: &height}
	require.Equal(t, anchor, s.Anchor)
	require.Equal(t, follower.HeadDerivation, s.HeadSource)
	require.Zero(t, s.Peers)

	composeBatches(t, c, 1, 2)
	var scanning *follower.Anchor
	var again *node
	again = n.restart(t, hookReader{c.L1, func() {
		if scanning == nil {
			scanning = again.follower.Status().Anchor
		}
	}}, chain)
	require.NoError(t, again.follower.Boot(t.Context()))
	require.Equal(t, anchor, scanning, "the root is local before the scan")
}

// TestStatusFailedBoot: a boot that fails is not following.
func TestStatusFailedBoot(t *testing.T) {
	other, err := recording.NewMemoryChain(1, &recording.Block{Header: l1.Header{Number: 12, Hash: common.Hash{1}}})
	require.NoError(t, err)
	n := newNode(t, other)
	require.ErrorIs(t, n.follower.Boot(t.Context()), follower.ErrL1ChainID)
	require.Equal(t, follower.PhaseBooting, n.follower.Status().Phase)
}

// TestStatusConcurrentWithBoot reads the status while boot writes it.
func TestStatusConcurrentWithBoot(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	finalizeHead(t, c)
	n := newNode(t, c.L1)
	done, read := make(chan struct{}), make(chan uint64)
	go func() {
		var height uint64
		for {
			select {
			case <-done:
				read <- height
				return
			default:
			}
			if a := n.follower.Status().Anchor; a != nil && a.L2Block != nil {
				height = *a.L2Block
			}
		}
	}()
	require.NoError(t, n.follower.Boot(t.Context()))
	close(done)
	require.Equal(t, uint64(3), <-read)
}

// countingPeers is a devp2p peer count of n that counts its calls and, while
// hung is set, answers only when its deadline passes.
type countingPeers struct {
	n, calls atomic.Uint64
	hung     atomic.Bool
}

func (p *countingPeers) count(ctx context.Context) (uint64, error) {
	p.calls.Add(1)
	if p.hung.Load() {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return p.n.Load(), nil
}

// TestStatusPeers: the status reports the latest devp2p peer count, counted
// in the background at most once per PeerRefresh and within a deadline, so
// that no caller waits on the count; Run refreshes it on its own.
func TestStatusPeers(t *testing.T) {
	newPeersNode := func(t *testing.T, p *countingPeers, refresh time.Duration) *node {
		c := derivertest.NewComposer(t)
		m, chain := derivertest.NewL2(t)
		return (&node{m: m, dir: t.TempDir()}).restart(t, c.L1, chain, func(cfg *follower.Config) {
			cfg.PeerCount, cfg.PeerRefresh = p.count, refresh
		})
	}

	t.Run("at most once per interval", func(t *testing.T) {
		p := &countingPeers{}
		p.n.Store(3)
		n := newPeersNode(t, p, time.Hour)
		for range 100 {
			n.follower.Status()
		}
		require.Eventually(t, func() bool { return n.follower.Status().Peers == 3 }, 10*time.Second, time.Millisecond)
		require.Equal(t, uint64(1), p.calls.Load())
	})

	t.Run("a hung count", func(t *testing.T) {
		p := &countingPeers{}
		p.hung.Store(true)
		n := newPeersNode(t, p, time.Millisecond)
		start := time.Now()
		for range 100 {
			require.Zero(t, n.follower.Status().Peers)
		}
		require.Less(t, time.Since(start), time.Second, "the status waits on no count")
		require.Eventually(t, func() bool {
			n.follower.Status()
			return p.calls.Load() >= 2
		}, 10*time.Second, time.Millisecond, "the deadline ends a hung count")
		p.hung.Store(false)
		p.n.Store(4)
		require.Eventually(t, func() bool { return n.follower.Status().Peers == 4 }, 10*time.Second, time.Millisecond)
	})

	t.Run("refreshed by Run", func(t *testing.T) {
		p := &countingPeers{}
		n := newPeersNode(t, p, 10*time.Millisecond)
		n.run(t)
		require.Eventually(t, func() bool { return p.calls.Load() >= 3 }, 10*time.Second, time.Millisecond)
	})
}

// TestStatusSignedHeads: a follower of signed heads reports the latest head
// of each source, highest version first, its devp2p and overlay peers, and
// which source moved the head last.
func TestStatusSignedHeads(t *testing.T) {
	f := newForks(t)
	_, peers := mapPeers(f.blocks()...)
	n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers, config: func(cfg *follower.Config) {
		cfg.PeerCount = func(context.Context) (uint64, error) { return 3, nil }
		cfg.OverlayPeerCount = func() int { return 2 }
	}})
	require.Eventually(t, func() bool { return n.follower.Status().Peers == 3 }, 10*time.Second, time.Millisecond)
	s := n.follower.Status()
	require.Equal(t, follower.PhaseFollowing, s.Phase)
	require.Equal(t, uint64(2), *s.OverlayPeers)
	require.NotNil(t, s.SignedHeads)
	require.Empty(t, s.SignedHeads)
	require.Equal(t, follower.HeadDerivation, s.HeadSource)

	b3, a3 := signedHead(f.b3.Blocks[0], 100, 2, true), signedHead(f.a3.Blocks[0], 200, 2, true)
	n.step(t, b3, a3)
	n.requireHead(t, f.a3.Blocks[0])
	s = n.follower.Status()
	require.Equal(t, []*seqsig.VerifiedHead{a3, b3}, s.SignedHeads)
	require.Equal(t, follower.HeadSignedHead, s.HeadSource)

	// A decision that keeps the head moves nothing.
	n.step(t)
	require.Equal(t, follower.HeadSignedHead, n.follower.Status().HeadSource)
	require.NoError(t, n.follower.Sync(t.Context()))
	require.Equal(t, follower.HeadSignedHead, n.follower.Status().HeadSource, "safe and finalized only")

	// L1 settles B: derivation rebuilds b2 over a2.
	f.c.Post(derivertest.Posting{Batch: f.b2})
	require.NoError(t, f.newHead(t, n))
	n.requireHead(t, f.b2.Blocks[0])
	require.Equal(t, follower.HeadDerivation, n.follower.Status().HeadSource)
}

// TestStatusSequencerHead: the head a sequencer's JSON-RPC sets is reported
// as such, until an L1 reorg moves it back; the status has no overlay peers
// and no signed heads.
func TestStatusSequencerHead(t *testing.T) {
	c := derivertest.NewComposer(t)
	posted := c.Post(derivertest.Posting{Batch: c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})})
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}})
	_, chain := derivertest.NewL2(t)
	cfg := testConfig
	cfg.Dir = t.TempDir()
	cfg.Sequencer = sequencer(t, c.L2)
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	require.Equal(t, follower.HeadDerivation, f.Status().HeadSource)

	require.NoError(t, f.StepOverlay(t.Context()))
	head, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, unsettled.Blocks[0].Hash(), head.Hash())
	s := f.Status()
	require.Equal(t, follower.HeadSequencerRPC, s.HeadSource)
	require.Nil(t, s.OverlayPeers)
	require.Nil(t, s.SignedHeads)

	require.NoError(t, f.Handle(t.Context(), l1.Reorg{Ancestor: l1.Header{Number: posted.Header.Number - 1}}))
	head, err = chain.Head(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, unsettled.Blocks[0].Hash(), head.Hash())
	require.Equal(t, follower.HeadDerivation, f.Status().HeadSource)
}

func phaseGauge(p follower.Phase) uint64 {
	return metrics.GetOrCreateGauge(fmt.Sprintf("eez_sync_phase{phase=%q}", p)).GetValueUint64()
}

// requirePhaseGauge waits for eez_sync_phase to be 1 for want alone.
func requirePhaseGauge(t *testing.T, want follower.Phase) {
	t.Helper()
	all := []follower.Phase{
		follower.PhaseBooting, follower.PhaseWaitingForFinality, follower.PhaseSyncing,
		follower.PhaseWaitingForPeers, follower.PhaseDeriving, follower.PhaseFollowing,
	}
	require.Eventually(t, func() bool {
		for _, p := range all {
			if (phaseGauge(p) == 1) != (p == want) {
				return false
			}
		}
		return true
	}, 10*time.Second, 5*time.Millisecond, "phase %s", want)
}

// TestPhaseMetrics: eez_sync_phase is 1 for the follower's phase alone.
func TestPhaseMetrics(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	n, _, _ := newPeersNode(t, c, c.L1, false)
	requirePhaseGauge(t, follower.PhaseBooting)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- n.follower.Boot(ctx) }()
	requirePhaseGauge(t, follower.PhaseWaitingForFinality)
	finalizeHead(t, c)
	requirePhaseGauge(t, follower.PhaseWaitingForPeers)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	require.NoError(t, newNode(t, c.L1).follower.Boot(t.Context()))
	requirePhaseGauge(t, follower.PhaseFollowing)
}

// TestPeerMetrics: eez_l2_peers follows the devp2p peer count while boot
// waits for finality and for peers, and once the follower follows, with no
// status read.
func TestPeerMetrics(t *testing.T) {
	gauge := metrics.GetOrCreateGauge("eez_l2_peers")
	requireGauge := func(want uint64) {
		t.Helper()
		require.Eventually(t, func() bool { return gauge.GetValueUint64() == want }, 10*time.Second, time.Millisecond, "eez_l2_peers %d", want)
	}
	gauge.SetUint64(0)
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	p := &countingPeers{}
	p.n.Store(5)
	refresh := func(cfg *follower.Config) { cfg.PeerCount, cfg.PeerRefresh = p.count, 10*time.Millisecond }

	m, chain := derivertest.NewL2(t)
	s := newPeerSentry(c.L2, false)
	booting := (&node{m: m, dir: t.TempDir()}).restart(t, c.L1, chain, peersMode(newFetcher(t, s, chain)), refresh)
	stop, _ := booting.run(t)
	requirePhaseGauge(t, follower.PhaseWaitingForFinality)
	requireGauge(5)
	finalizeHead(t, c)
	requirePhaseGauge(t, follower.PhaseWaitingForPeers)
	p.n.Store(6)
	requireGauge(6)
	stop()

	m, chain = derivertest.NewL2(t)
	following := (&node{m: m, dir: t.TempDir()}).restart(t, c.L1, chain, refresh)
	following.run(t)
	requirePhaseGauge(t, follower.PhaseFollowing)
	p.n.Store(7)
	requireGauge(7)
}
