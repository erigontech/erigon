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

package changeset

import (
	"context"
	"time"

	"github.com/erigontech/erigon/db/kv"
)

// ReadContext carries the per-read state that the temporal GetLatest read path
// needs but that does not belong on the public interface signature: the
// file-search upper bound (MaxStep, derived by SharedDomains from the mem-miss
// unwind boundary) and the read-metrics sink + timing.
//
// It is threaded through context.Context so the unified
// TemporalGetter.GetLatest(ctx, name, k) needs neither a parameter explosion
// nor the duck-typed MeteredGetter/MeteredGetterWithTxN interface variants it
// replaced. When no ReadContext is attached the read is unbounded (MaxStep
// effectively MaxUint64) and unmetered — exactly today's behaviour for the
// plain public callers.
type ReadContext struct {
	// MaxTxNum bounds the DB layer: a DB value whose txNum exceeds MaxTxNum
	// predates a not-yet-flushed unwind and must be skipped in favour of the
	// frozen files. The whole read path speaks txNum; the domain converts this
	// bound to a step internally where it compares against on-disk steps. Zero
	// means "unbounded" (treated as MaxUint64 by readers).
	MaxTxNum uint64

	// Metrics, when non-nil, receives per-layer read timing (cache/db/file).
	Metrics *DomainMetrics

	// Start is the read's start time, stamped by the caller for the metrics.
	Start time.Time
}

type readContextKey struct{}

// WithReadContext attaches rc to ctx for the duration of a GetLatest read.
func WithReadContext(ctx context.Context, rc *ReadContext) context.Context {
	return context.WithValue(ctx, readContextKey{}, rc)
}

// ReadContextFrom returns the ReadContext attached to ctx, or nil if none.
// A nil result means unbounded + unmetered.
func ReadContextFrom(ctx context.Context) *ReadContext {
	if ctx == nil {
		return nil
	}
	rc, _ := ctx.Value(readContextKey{}).(*ReadContext)
	return rc
}

// MaxStepOr returns the DB-read step bound: the ReadContext's MaxTxNum converted
// to a step, or the given default when no ReadContext is attached or its
// MaxTxNum is the zero (unbounded) value. This is the single place the txNum
// bound becomes a step — the rest of the read path stays in txNum.
func MaxStepOr(ctx context.Context, stepSize uint64, unbounded kv.Step) kv.Step {
	rc := ReadContextFrom(ctx)
	if rc == nil || rc.MaxTxNum == 0 {
		return unbounded
	}
	return kv.Step(rc.MaxTxNum / stepSize)
}
