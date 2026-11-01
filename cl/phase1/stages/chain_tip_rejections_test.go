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

package stages

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
)

func TestChainTipRejectionsSummarizesPerReasonOncePerInterval(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	r := newChainTipRejections(30*time.Second, func() time.Time { return now })
	boom := errors.New("boom")

	// The first rejection is due at once.
	r.record("process block failed", 100, common.Hash{1}, boom)
	require.Equal(t, []chainTipRejectionSummary{{reason: "process block failed", chainTipRejection: chainTipRejection{
		attempts: 1, lastSlot: 100, lastRoot: common.Hash{1}, lastSeen: now, lastErr: boom, lastErrSlot: 100, lastErrRoot: common.Hash{1},
	}}}, r.flushIfDue())

	// Within the interval rejections are only aggregated; a later rejection without an error keeps
	// the last error with the block it came from.
	now = now.Add(10 * time.Second)
	r.record("process block failed", 101, common.Hash{2}, boom)
	r.record("process block failed", 102, common.Hash{3}, nil)
	r.record("parent not in fork graph", 103, common.Hash{4}, nil)
	require.Nil(t, r.flushIfDue())

	now = now.Add(25 * time.Second)
	summaries := r.flushIfDue()
	require.Len(t, summaries, 2)
	require.Equal(t, "parent not in fork graph", summaries[0].reason)
	failed := summaries[1]
	require.Equal(t, "process block failed", failed.reason)
	require.Equal(t, 2, failed.attempts)
	require.Equal(t, common.Hash{3}, failed.lastRoot)
	require.Equal(t, boom, failed.lastErr)
	require.Equal(t, common.Hash{2}, failed.lastErrRoot)
	require.Equal(t, uint64(101), failed.lastErrSlot)
	require.Nil(t, r.flushIfDue())
	require.Nil(t, (*chainTipRejections)(nil).flushIfDue())
}

// Rejections gathered long before the summary, as before a forward sync, are not reported as current.
func TestChainTipRejectionsDropsStaleReasons(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	r := newChainTipRejections(30*time.Second, func() time.Time { return now })
	r.lastLog = now
	r.record("process block failed", 100, common.Hash{1}, nil)
	now = now.Add(10 * time.Minute)
	r.record("parent not in fork graph", 200, common.Hash{2}, nil)

	summaries := r.flushIfDue()
	require.Len(t, summaries, 1)
	require.Equal(t, "parent not in fork graph", summaries[0].reason)
}

func testRejectedBlock(slot uint64) *cltypes.SignedBeaconBlock {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	block.Block.Slot = slot
	return block
}

// A block that arrives early or ahead of its parent's envelope is imported moments later by
// gossip, and a failure of the stage's own context is not the block's; neither is counted. The
// cause is still kept for the block's descendants.
func TestRejectChainTipBlockSkipsTransientAndOwnContextFailures(t *testing.T) {
	cfg := &Cfg{chainTipRejections: newChainTipRejections(time.Hour, nil)}
	cfg.chainTipRejections.lastLog = time.Now()
	rejected := map[common.Hash]error{}
	ended, cancel := context.WithCancel(t.Context())
	cancel()

	rejectChainTipBlock(t.Context(), cfg, rejected, testRejectedBlock(1), common.Hash{1}, "process block failed", fmt.Errorf("on block: %w", forkchoice.ErrBlockTooEarly))
	rejectChainTipBlock(t.Context(), cfg, rejected, testRejectedBlock(2), common.Hash{2}, "process block failed", fmt.Errorf("on block: %w", forkchoice.ErrParentEnvelopePending))
	rejectChainTipBlock(ended, cfg, rejected, testRejectedBlock(3), common.Hash{3}, "process block failed", fmt.Errorf("update: %w", context.Canceled))
	// During shutdown, the sentinel of a stopped anchor recovery or envelope fetch is not counted either.
	rejectChainTipBlock(ended, cfg, rejected, testRejectedBlock(5), common.Hash{5}, errAnchorEnvelopeUnavailable.Error(), errAnchorEnvelopeUnavailable)
	require.Empty(t, cfg.chainTipRejections.reasons)
	require.Len(t, rejected, 4)

	// A deadline that is the block's own, with the stage context alive, counts.
	rejectChainTipBlock(t.Context(), cfg, rejected, testRejectedBlock(4), common.Hash{4}, "parent payload not accepted", fmt.Errorf("replay budget exhausted: %w", context.DeadlineExceeded))
	require.Len(t, cfg.chainTipRejections.reasons, 1)
}

// A reason that is its own cause is not repeated as the error.
func TestRejectChainTipBlockDoesNotRepeatTheReasonAsTheError(t *testing.T) {
	cfg := &Cfg{chainTipRejections: newChainTipRejections(time.Hour, nil)}
	cfg.chainTipRejections.lastLog = time.Now()
	rejected := map[common.Hash]error{}

	rejectChainTipBlock(t.Context(), cfg, rejected, testRejectedBlock(1), common.Hash{1}, errAnchorEnvelopeUnavailable.Error(), errAnchorEnvelopeUnavailable)

	require.Equal(t, errAnchorEnvelopeUnavailable, rejected[common.Hash{1}])
	require.Nil(t, cfg.chainTipRejections.reasons[errAnchorEnvelopeUnavailable.Error()].lastErr)
}

// A retry builds the same cause as a new error; it must not count as a changed cause.
func TestSameCauseComparesByMessage(t *testing.T) {
	require.True(t, sameCause(errors.New("parent payload invalid"), errors.New("parent payload invalid")))
	require.True(t, sameCause(nil, nil))
	require.False(t, sameCause(nil, errors.New("parent payload invalid")))
	require.False(t, sameCause(errors.New("parent payload invalid"), errors.New("parent payload has no EL verdict")))
}
