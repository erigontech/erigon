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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

// chainTipRejectionLogInterval bounds how often rejected chain-tip blocks are reported.
const chainTipRejectionLogInterval = 30 * time.Second

type chainTipRejection struct {
	count    int
	lastSlot uint64
	lastErr  string
}

// chainTipRejections aggregates why chain-tip blocks are not imported and reports the
// summary at Warn level at most once per interval, so a stuck head is visible at the
// default log level without logging every rejected block.
type chainTipRejections struct {
	mu       sync.Mutex
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

// record notes one rejected block. It returns the Warn summary once the interval has elapsed,
// or nil when the rejection is only aggregated. The returned fields are ready for logging.
func (r *chainTipRejections) record(reason string, slot uint64, err error) []any {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.reasons[reason]
	if entry == nil {
		entry = &chainTipRejection{}
		r.reasons[reason] = entry
	}
	entry.count++
	entry.lastSlot = slot
	if err != nil {
		entry.lastErr = err.Error()
	}
	now := r.now()
	if r.lastLog.IsZero() {
		r.lastLog = now.Add(-r.interval)
	}
	if now.Sub(r.lastLog) < r.interval {
		return nil
	}
	r.lastLog = now
	return r.flushLocked()
}

func (r *chainTipRejections) flushLocked() []any {
	reasons := make([]string, 0, len(r.reasons))
	for reason := range r.reasons {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	fields := make([]any, 0, 2*len(reasons))
	for _, reason := range reasons {
		entry := r.reasons[reason]
		detail := strings.Builder{}
		detail.WriteString("count=")
		detail.WriteString(strconv.Itoa(entry.count))
		detail.WriteString(" lastSlot=")
		detail.WriteString(strconv.FormatUint(entry.lastSlot, 10))
		if entry.lastErr != "" {
			detail.WriteString(" err=")
			detail.WriteString(entry.lastErr)
		}
		fields = append(fields, reason, detail.String())
	}
	r.reasons = map[string]*chainTipRejection{}
	return fields
}

// logChainTipRejection records a rejection and emits the periodic Warn summary when due.
func logChainTipRejection(cfg *Cfg, reason string, slot uint64, err error) {
	log.Debug("[chainTipSync] block not imported", "reason", reason, "slot", slot, "err", err)
	if cfg == nil || cfg.chainTipRejections == nil {
		return
	}
	if fields := cfg.chainTipRejections.record(reason, slot, err); fields != nil {
		log.Warn("[Caplin] chain tip blocks are not being imported", fields...)
	}
}
