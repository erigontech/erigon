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

package bscsync

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/types"
)

const fetchLatency = 10 * time.Second

var errTransient = errors.New("fake: transient")

type downloadRun struct {
	inserted []*types.Block
	elapsed  time.Duration
	err      error
}

func rangeStart(i uint64) uint64 { return 1 + i*rangeSize }

func rangesChain(n uint64) []*types.Block { return fakeChain(int(n*rangeSize) + 1) }

func forkAt(chain []*types.Block, n int) []*types.Block {
	fork := slices.Clone(chain[:n])
	for len(fork) < len(chain) {
		fork = append(fork, fakeBlock(fork[len(fork)-1].HeaderNoCopy(), 1))
	}
	return fork
}

func targetSeq(last uint64, seq ...uint64) func() uint64 {
	return func() uint64 {
		if len(seq) == 0 {
			return last
		}
		n := seq[0]
		seq = seq[1:]
		return n
	}
}

func downloadChain(t *testing.T, svc *fakeP2P) downloadRun {
	return downloadChainTo(t, svc, func() uint64 { return uint64(len(svc.chain) - 1) })
}

func downloadChainTo(t *testing.T, svc *fakeP2P, target func() uint64) downloadRun {
	chain := svc.chain
	prev := chain[0].HeaderNoCopy()
	var run downloadRun
	insert := func(_ context.Context, blocks []*types.Block) error {
		require.NotEmpty(t, blocks)
		for _, b := range blocks {
			require.Equal(t, prev.Number.Uint64()+1, b.NumberU64())
			require.Equal(t, prev.Hash(), b.ParentHash())
			prev = b.HeaderNoCopy()
		}
		run.inserted = append(run.inserted, blocks...)
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Hour)
	defer cancel()
	start := time.Now()
	run.err = downloadRanges(ctx, testlog.Logger(t, log.LvlCrit), svc, 1, target, chain[0].HeaderNoCopy(), 8, insert)
	run.elapsed = time.Since(start)
	return run
}

func requireDownloaded(t *testing.T, chain []*types.Block, run downloadRun) {
	require.NoError(t, run.err)
	require.Len(t, run.inserted, len(chain)-1)
	require.Equal(t, chain[len(chain)-1].Hash(), run.inserted[len(run.inserted)-1].Hash())
}

func TestDownloadRangesReassignsSlowRange(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(5)
		svc := &fakeP2P{peers: 3, chain: chain, serve: func(ctx context.Context, peer, _ uint64) ([]*types.Block, error) {
			if peer == 1 {
				return nil, common.Sleep(ctx, time.Hour)
			}
			return nil, common.Sleep(ctx, fetchLatency)
		}}

		run := downloadChain(t, svc)

		require.Equal(t, []uint64{1, 2}, svc.headerFetchers(rangeStart(0)))
		require.Equal(t, rangeDeadline+fetchLatency, run.elapsed)
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesReassignsRangeWithoutHeaders(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(3)
		svc := &fakeP2P{peers: 3, chain: chain, serve: func(ctx context.Context, peer, _ uint64) ([]*types.Block, error) {
			if peer == 1 {
				return nil, errNoHeaders
			}
			return nil, common.Sleep(ctx, fetchLatency)
		}}

		run := downloadChain(t, svc)

		require.Equal(t, 2*fetchLatency, run.elapsed)
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesKeepsRangesAboveGap(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(3)
		var failed atomic.Bool
		svc := &fakeP2P{peers: 3, chain: chain, serve: func(ctx context.Context, peer, _ uint64) ([]*types.Block, error) {
			if peer == 1 && failed.CompareAndSwap(false, true) {
				return nil, errTransient
			}
			return nil, common.Sleep(ctx, fetchLatency)
		}}

		run := downloadChain(t, svc)

		require.Equal(t, []uint64{2}, svc.headerFetchers(rangeStart(1)))
		require.Equal(t, []uint64{3}, svc.headerFetchers(rangeStart(2)))
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesGivesRangeBelowMissToPeer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(2)
		svc := &fakeP2P{peers: 2, chain: chain, serve: func(ctx context.Context, peer, start uint64) ([]*types.Block, error) {
			if err := common.Sleep(ctx, time.Second); err != nil {
				return nil, err
			}
			switch {
			case peer == 1 && start == rangeStart(0):
				return nil, errTransient
			case peer == 2 && start >= rangeStart(1):
				return nil, errNoHeaders
			}
			return nil, nil
		}}

		run := downloadChain(t, svc)

		require.Equal(t, []uint64{1, 2}, svc.headerFetchers(rangeStart(0)))
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesRefetchesRangeThatDoesNotLink(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(3)
		fork := forkAt(chain, 1000)
		svc := &fakeP2P{peers: 3, chain: chain, serve: func(ctx context.Context, peer, _ uint64) ([]*types.Block, error) {
			if err := common.Sleep(ctx, fetchLatency); err != nil {
				return nil, err
			}
			if peer == 2 {
				return fork, nil
			}
			return nil, nil
		}}

		run := downloadChain(t, svc)

		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesGivesTimedOutRangeToFastestPeer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(3)
		svc := &fakeP2P{peers: 3, chain: chain, serve: func(ctx context.Context, peer, _ uint64) ([]*types.Block, error) {
			switch peer {
			case 1:
				return nil, common.Sleep(ctx, time.Hour)
			case 2:
				return nil, common.Sleep(ctx, 4*fetchLatency)
			}
			return nil, common.Sleep(ctx, fetchLatency)
		}}

		run := downloadChain(t, svc)

		require.Equal(t, []uint64{1, 3}, svc.headerFetchers(rangeStart(0)))
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesDoublesDeadlineOfTimedOutRange(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(1)
		svc := &fakeP2P{peers: 2, chain: chain, serve: func(ctx context.Context, _, _ uint64) ([]*types.Block, error) {
			return nil, common.Sleep(ctx, rangeDeadline+time.Minute)
		}}

		run := downloadChain(t, svc)

		require.Equal(t, []uint64{1, 2}, svc.headerFetchers(rangeStart(0)))
		require.Equal(t, 2*rangeDeadline+time.Minute, run.elapsed)
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesRetriesRangeFailedByEveryPeer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(1)
		var failed atomic.Bool
		svc := &fakeP2P{peers: 1, chain: chain, serve: func(context.Context, uint64, uint64) ([]*types.Block, error) {
			if failed.CompareAndSwap(false, true) {
				return nil, errTransient
			}
			return nil, nil
		}}

		run := downloadChain(t, svc)

		require.Equal(t, peerBackoff, run.elapsed)
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesTimeoutDoesNotMarkPeerMissing(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(1)
		var slow atomic.Bool
		svc := &fakeP2P{peers: 1, chain: chain, serve: func(ctx context.Context, _, _ uint64) ([]*types.Block, error) {
			if slow.CompareAndSwap(false, true) {
				return nil, common.Sleep(ctx, time.Hour)
			}
			return nil, nil
		}}

		run := downloadChain(t, svc)

		require.Equal(t, rangeDeadline+peerBackoff, run.elapsed)
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesBoundsRangesAboveGap(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(20)
		svc := &fakeP2P{peers: 3, chain: chain, serve: func(ctx context.Context, peer, start uint64) ([]*types.Block, error) {
			if peer == 1 && start == rangeStart(0) {
				return nil, common.Sleep(ctx, time.Hour)
			}
			return nil, common.Sleep(ctx, fetchLatency)
		}}

		run := downloadChain(t, svc)

		var highest uint64
		for i, fetch := range svc.fetches {
			if fetch.start == rangeStart(0) && slices.ContainsFunc(svc.fetches[:i], func(f headerFetch) bool { return f.start == fetch.start }) {
				break
			}
			highest = max(highest, fetch.start)
		}
		require.Equal(t, rangeStart(7), highest)
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesShrinksRangeAboveDroppedTarget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(3)[:3001]
		svc := &fakeP2P{peers: 4, chain: chain}

		run := downloadChainTo(t, svc, targetSeq(2968, 3072))

		requireDownloaded(t, chain[:2969], run)
	})
}

func TestDownloadRangesFetchesRemainderWhenTargetRisesAgain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(3)
		svc := &fakeP2P{peers: 1, chain: chain}

		run := downloadChainTo(t, svc, targetSeq(3072, 3072, 2968, 2968))

		require.Equal(t, []uint64{1}, svc.headerFetchers(2969))
		requireDownloaded(t, chain, run)
	})
}

func TestDownloadRangesHoldsRangeAboveDroppedTarget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(3)
		svc := &fakeP2P{peers: 2, chain: chain, serve: func(ctx context.Context, peer, start uint64) ([]*types.Block, error) {
			if peer == 1 && start == rangeStart(0) {
				return nil, common.Sleep(ctx, fetchLatency)
			}
			return nil, nil
		}}

		run := downloadChainTo(t, svc, targetSeq(2000, 3072))

		require.Empty(t, svc.headerFetchers(rangeStart(2)))
		requireDownloaded(t, chain[:2049], run)
	})
}

func TestDownloadRangesReturnsWithMoreFetchesInFlightThanRanges(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		chain := rangesChain(3)
		svc := &fakeP2P{peers: 6, chain: chain, serve: func(ctx context.Context, peer, start uint64) ([]*types.Block, error) {
			switch {
			case peer == 1 && start == rangeStart(0):
				return nil, common.Sleep(ctx, fetchLatency)
			case start == rangeStart(1) && peer <= 4:
				return nil, errTransient
			}
			return nil, common.Sleep(ctx, time.Hour)
		}}
		var inserted int
		insert := func(_ context.Context, blocks []*types.Block) error {
			inserted += len(blocks)
			return nil
		}

		done := make(chan error, 1)
		go func() {
			done <- downloadRanges(t.Context(), testlog.Logger(t, log.LvlCrit), svc, 1, targetSeq(1024, 2048, 1100, 1050, 2048), chain[0].HeaderNoCopy(), 2, insert)
		}()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(30 * time.Minute):
			require.FailNow(t, "downloadRanges did not return")
		}
		require.Equal(t, 1024, inserted)
	})
}
