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

var domains = [...]kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain}

type Sample struct {
	total   [len(domains)]kvmetrics.DomainIOMetrics
	nonExec [len(domains)]kvmetrics.DomainIOMetrics
	taken   bool
}

func Take(dm, nonExecMetrics *kvmetrics.DomainMetrics) Sample {
	if dm == nil || !dbg.KVReadLevelledMetrics {
		return Sample{}
	}
	s := Sample{taken: true}
	for i, domain := range domains {
		s.total[i] = dm.SnapshotDomain(domain)
		s.nonExec[i] = nonExecMetrics.SnapshotDomain(domain)
	}
	return s
}

func (s Sample) Since(before Sample) (accounts, storage, code DomainCounts, ok bool) {
	if !s.taken || !before.taken {
		return
	}
	var out [len(domains)]DomainCounts
	for i := range domains {
		out[i] = execOnly(diff(before.total[i], s.total[i]), diff(before.nonExec[i], s.nonExec[i]))
	}
	return out[0], out[1], out[2], true
}

func execOnly(total, nonExec DomainCounts) DomainCounts {
	return DomainCounts{
		Reads:     max(total.Reads-nonExec.Reads, 0),
		Writes:    max(total.Writes-nonExec.Writes, 0),
		CacheHits: max(total.CacheHits-nonExec.CacheHits, 0),
		CacheMiss: max(total.CacheMiss-nonExec.CacheMiss, 0),
		ReadTime:  max(total.ReadTime-nonExec.ReadTime, 0),
	}
}

func reads(m kvmetrics.DomainIOMetrics) int64 {
	return m.CacheReadCount + m.DbReadCount + m.FileReadCount + m.StateCacheHitCount
}

func readTime(m kvmetrics.DomainIOMetrics) time.Duration {
	return m.CacheReadDuration + m.DbReadDuration + m.FileReadDuration + m.StateCacheHitDuration
}

func diff(before, after kvmetrics.DomainIOMetrics) DomainCounts {
	return DomainCounts{
		Reads:     reads(after) - reads(before),
		Writes:    after.CachePutCount - before.CachePutCount,
		CacheHits: after.StateCacheHitCount - before.StateCacheHitCount,
		CacheMiss: after.StateCacheMissCount - before.StateCacheMissCount,
		ReadTime:  readTime(after) - readTime(before),
	}
}
