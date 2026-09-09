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

package commitment

import (
	"time"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

// Commitment metrics are counters and a histogram rather than gauges holding a
// pre-divided rate: rate() belongs in the query, so the averaging window stays
// the reader's choice and a missed scrape costs a sample, not an interval.
//
// They are emitted per round, from the trie itself, because a round's Metrics
// is per-round on both engines and there is no cumulative series to difference:
// HexPatriciaHashed.Process resets at the top of every round, and the parallel
// trie now does the same. Publishing from the executor's log ticker instead
// would both mis-difference that snapshot and drop every round between ticks.
var (
	mxRounds = metrics.GetOrCreateCounter("commitment_rounds_total")

	// Buckets span a fast incremental block through a whale fold.
	mxRoundDuration = metrics.NewHistogram("commitment_round_duration_seconds",
		[]float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60})

	mxKeys       = metrics.GetOrCreateCounter("commitment_keys_total")
	mxFolds      = metrics.GetOrCreateCounter("commitment_folds_total")
	mxUnfolds    = metrics.GetOrCreateCounter("commitment_unfolds_total")
	mxBranchPuts = metrics.GetOrCreateCounter("commitment_branch_writes_total")
	mxReadBytes  = metrics.GetOrCreateCounter("commitment_branch_read_bytes_total")
	mxWriteBytes = metrics.GetOrCreateCounter("commitment_branch_write_bytes_total")

	// kind=address|storage. Cell traversals, not distinct keys: the parallel
	// engine re-walks subtrees on mount+replay, so this exceeds commitment_keys_total.
	mxTraversals = metrics.GetOrCreateCounterVec("commitment_key_traversals_total",
		[]string{"kind"}, "cell traversals during commitment, by key kind")

	// kind=account|storage|branch
	mxReads = metrics.GetOrCreateCounterVec("commitment_reads_total",
		[]string{"kind"}, "PatriciaContext reads during commitment")
)

func addU64(c metrics.Counter, v uint64) {
	if v > 0 {
		c.AddUint64(v)
	}
}

func addVec(v *metrics.CounterVec, kind string, n uint64) {
	if n > 0 {
		v.WithLabelValues(kind).Add(float64(n))
	}
}

// publishBranchWrites bills n branch writes of bytesOut bytes. Called where the
// write lands rather than at a round boundary, because deferred writes can be
// applied after their round has closed. m, when non-nil, also gets them for the
// trie's own log and CSV counters.
func publishBranchWrites(n, bytesOut int, m *Metrics) {
	if n <= 0 {
		return
	}
	mxBranchPuts.AddInt(n)
	if bytesOut > 0 {
		mxWriteBytes.AddInt(bytesOut)
	}
	if m != nil {
		m.updateBranch.Add(uint64(n))
		m.AddBranchWrite(bytesOut)
	}
}

// observeRound publishes one finished round. Branch writes are not published
// here — publishBranchWrites bills those where they land.
func observeRound(m *Metrics, start time.Time) {
	mxRounds.Inc()
	mxRoundDuration.ObserveDuration(start)
	if m == nil {
		return
	}
	v := m.AsValues()
	addU64(mxKeys, v.RoundKeys)
	addU64(mxFolds, v.Folds)
	addU64(mxUnfolds, v.Unfolds)
	addU64(mxReadBytes, v.BranchReadBytes)
	addVec(mxTraversals, "address", v.AddressKeys)
	addVec(mxTraversals, "storage", v.StorageKeys)
	addVec(mxReads, "account", v.LoadAccount)
	addVec(mxReads, "storage", v.LoadStorage)
	addVec(mxReads, "branch", v.LoadBranch)
}
