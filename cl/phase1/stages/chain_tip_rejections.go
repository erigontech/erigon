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
	"maps"
	"slices"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const chainTipRejectionLogInterval = 30 * time.Second

var (
	errAnchorEnvelopeUnavailable = errors.New("anchor envelope unavailable")
	errParentEnvelopeUnavailable = errors.New("parent envelope unavailable")
)

// chainTipRejection is what one reason gathered since the last summary. The last error keeps the
// block it came from, so a later rejection without an error does not hide it.
type chainTipRejection struct {
	attempts    int
	lastSlot    uint64
	lastRoot    common.Hash
	lastSeen    time.Time
	lastErr     error
	lastErrSlot uint64
	lastErrRoot common.Hash
}

type chainTipRejectionSummary struct {
	reason string
	chainTipRejection
}

// chainTipRejections aggregates why chain-tip blocks are not imported and logs a Warn per reason
// at most once per interval, so the cause of a stuck head shows at the default log level. Only
// the stage goroutine uses it.
type chainTipRejections struct {
	interval time.Duration
	now      func() time.Time
	lastLog  time.Time
	reasons  map[string]*chainTipRejection
}

func newChainTipRejections(interval time.Duration, now func() time.Time) *chainTipRejections {
	if now == nil {
		now = time.Now
	}
	return &chainTipRejections{interval: interval, now: now, reasons: map[string]*chainTipRejection{}}
}

// record notes one attempt to import a block; a block retried on every poll counts each time.
func (r *chainTipRejections) record(reason string, slot uint64, root common.Hash, err error) {
	entry := r.reasons[reason]
	if entry == nil {
		entry = &chainTipRejection{}
		r.reasons[reason] = entry
	}
	entry.attempts++
	entry.lastSlot, entry.lastRoot, entry.lastSeen = slot, root, r.now()
	if err != nil {
		entry.lastErr, entry.lastErrSlot, entry.lastErrRoot = err, slot, root
	}
}

// flushIfDue returns the reasons gathered since the last summary once the interval has elapsed,
// leaving out those not seen for two intervals, such as rejections from before a forward sync.
func (r *chainTipRejections) flushIfDue() []chainTipRejectionSummary {
	if r == nil {
		return nil
	}
	now := r.now()
	if len(r.reasons) == 0 || now.Sub(r.lastLog) < r.interval {
		return nil
	}
	r.lastLog = now
	var summaries []chainTipRejectionSummary
	for _, reason := range slices.Sorted(maps.Keys(r.reasons)) {
		if entry := r.reasons[reason]; now.Sub(entry.lastSeen) <= 2*r.interval {
			summaries = append(summaries, chainTipRejectionSummary{reason: reason, chainTipRejection: *entry})
		}
	}
	r.reasons = map[string]*chainTipRejection{}
	return summaries
}

func (r *chainTipRejections) logIfDue(headSlot uint64) {
	summaries := r.flushIfDue()
	for i := range summaries {
		s := &summaries[i]
		fields := []any{"reason", s.reason, "attempts", s.attempts, "lastSlot", s.lastSlot, "lastBlockRoot", s.lastRoot, "headSlot", headSlot}
		if s.lastErr != nil {
			fields = append(fields, "lastErr", s.lastErr, "lastErrSlot", s.lastErrSlot, "lastErrBlockRoot", s.lastErrRoot)
		}
		log.Warn("[Caplin] chain tip blocks rejected", fields...)
	}
}

func chainTipHeadSlot(cfg *Cfg) uint64 {
	if cfg.syncedData == nil {
		return 0
	}
	return cfg.syncedData.HeadSlot()
}

// rejectChainTipBlock records why a block was not imported: as the cause its descendants report,
// and in the summary. Blocks that are early or wait for their parent's envelope are imported
// moments later by gossip, and neither a failure caused by the stage's own context nor any failure
// during shutdown is the block's; none of them is counted. A required envelope still missing at the
// stage deadline is counted under its own reason.
func rejectChainTipBlock(ctx context.Context, cfg *Cfg, rejected map[common.Hash]error, block *cltypes.SignedBeaconBlock, root common.Hash, reason string, cause error) {
	if prev, seen := rejected[root]; !seen || !sameCause(prev, cause) {
		log.Debug("[chainTipSync] block not imported", "reason", reason, "slot", block.Block.Slot, "blockRoot", root, "err", cause)
	}
	rejected[root] = cause
	if cfg.chainTipRejections == nil ||
		errors.Is(cause, forkchoice.ErrBlockTooEarly) || errors.Is(cause, forkchoice.ErrParentEnvelopePending) ||
		errors.Is(ctx.Err(), context.Canceled) ||
		ctx.Err() != nil && (errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded)) {
		return
	}
	if cause != nil && cause.Error() == reason {
		cause = nil
	}
	cfg.chainTipRejections.record(reason, block.Block.Slot, root, cause)
	cfg.chainTipRejections.logIfDue(chainTipHeadSlot(cfg))
}

// sameCause compares causes by message, since a retry builds the same cause as a new error.
func sameCause(a, b error) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Error() == b.Error()
}
