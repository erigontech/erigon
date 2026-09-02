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

package blockmetrics

import (
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/kvmetrics"
)

// Sample copies the cumulative domain counters; a block's cost is the
// difference between two. The commitment's own counters are carried alongside
// so they can be taken back out — the state root's reads are not execution's.
type Sample struct {
	accounts kvmetrics.DomainIOMetrics
	storage  kvmetrics.DomainIOMetrics
	code     kvmetrics.DomainIOMetrics

	nonExecAccounts kvmetrics.DomainIOMetrics
	nonExecStorage  kvmetrics.DomainIOMetrics
	nonExecCode     kvmetrics.DomainIOMetrics

	taken bool
}

// Take returns an untaken sample unless KV_READ_METRICS is on, since the read
// path skips the counters entirely without it and every delta would be zero.
// nonExec may be nil, in which case nothing is attributed to commitment.
func Take(dm, nonExec *kvmetrics.DomainMetrics) Sample {
	if dm == nil || !dbg.KVReadLevelledMetrics {
		return Sample{}
	}
	return Sample{
		accounts:        dm.SnapshotDomain(kv.AccountsDomain),
		storage:         dm.SnapshotDomain(kv.StorageDomain),
		code:            dm.SnapshotDomain(kv.CodeDomain),
		nonExecAccounts: nonExec.SnapshotDomain(kv.AccountsDomain),
		nonExecStorage:  nonExec.SnapshotDomain(kv.StorageDomain),
		nonExecCode:     nonExec.SnapshotDomain(kv.CodeDomain),
		taken:           true,
	}
}

// Since reports what execution alone did between the two samples: the whole
// delta minus the part commitment contributed.
func (s Sample) Since(before Sample) (accounts, storage, code DomainCounts, ok bool) {
	if !s.taken || !before.taken {
		return
	}
	return execOnly(diff(before.accounts, s.accounts), diff(before.nonExecAccounts, s.nonExecAccounts)),
		execOnly(diff(before.storage, s.storage), diff(before.nonExecStorage, s.nonExecStorage)),
		execOnly(diff(before.code, s.code), diff(before.nonExecCode, s.nonExecCode)),
		true
}

// execOnly subtracts commitment's share, clamped: the two aggregates are merged
// at different moments, so a snapshot can catch one ahead of the other.
func execOnly(total, nonExec DomainCounts) DomainCounts {
	return DomainCounts{
		Reads:     max(total.Reads-nonExec.Reads, 0),
		Writes:    max(total.Writes-nonExec.Writes, 0),
		CacheHits: max(total.CacheHits-nonExec.CacheHits, 0),
		CacheMiss: max(total.CacheMiss-nonExec.CacheMiss, 0),
		ReadTime:  max(total.ReadTime-nonExec.ReadTime, 0),
	}
}

// reads counts one read per served request. A stateCache hit returns without
// touching the mem/db/file counters, so it is added separately; a stateCache
// miss falls through to db or file and is already counted there.
func reads(m kvmetrics.DomainIOMetrics) int64 {
	return m.CacheReadCount + m.DbReadCount + m.FileReadCount + m.StateCacheHitCount
}

func readTime(m kvmetrics.DomainIOMetrics) time.Duration {
	return m.CacheReadDuration + m.DbReadDuration + m.FileReadDuration + m.StateCacheHitDuration
}

func diff(before, after kvmetrics.DomainIOMetrics) DomainCounts {
	return DomainCounts{
		Reads:     nonNeg(reads(after) - reads(before)),
		Writes:    nonNeg(after.CachePutCount - before.CachePutCount),
		CacheHits: nonNeg(after.StateCacheHitCount - before.StateCacheHitCount),
		CacheMiss: nonNeg(after.StateCacheMissCount - before.StateCacheMissCount),
		ReadTime:  readTime(after) - readTime(before),
	}
}

// nonNeg absorbs a counter reset between samples.
func nonNeg(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
