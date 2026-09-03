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

type Sample struct {
	accounts kvmetrics.DomainIOMetrics
	storage  kvmetrics.DomainIOMetrics
	code     kvmetrics.DomainIOMetrics

	nonExecAccounts kvmetrics.DomainIOMetrics
	nonExecStorage  kvmetrics.DomainIOMetrics
	nonExecCode     kvmetrics.DomainIOMetrics

	taken bool
}

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

func (s Sample) Since(before Sample) (accounts, storage, code DomainCounts, ok bool) {
	if !s.taken || !before.taken {
		return
	}
	return execOnly(diff(before.accounts, s.accounts), diff(before.nonExecAccounts, s.nonExecAccounts)),
		execOnly(diff(before.storage, s.storage), diff(before.nonExecStorage, s.nonExecStorage)),
		execOnly(diff(before.code, s.code), diff(before.nonExecCode, s.nonExecCode)),
		true
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
		Reads:     max(reads(after)-reads(before), 0),
		Writes:    max(after.CachePutCount-before.CachePutCount, 0),
		CacheHits: max(after.StateCacheHitCount-before.StateCacheHitCount, 0),
		CacheMiss: max(after.StateCacheMissCount-before.StateCacheMissCount, 0),
		ReadTime:  readTime(after) - readTime(before),
	}
}
