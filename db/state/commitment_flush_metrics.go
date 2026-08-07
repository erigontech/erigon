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

package state

import (
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

// Commitment flush/build timing accumulators, split by the domain values vs the
// history side. Always-on so embedded and deembedded commitment layouts can be
// compared. Nanoseconds, summed across the whole process; logged cumulatively
// with the "[commitment-metrics]" tag.
var (
	commitInExecFlushDomainNs  atomic.Int64
	commitInExecFlushHistoryNs atomic.Int64
	commitFileBuildDomainNs    atomic.Int64
	commitFileBuildHistoryNs   atomic.Int64
)

func addCommitInExecFlush(domainNs, historyNs int64) {
	commitInExecFlushDomainNs.Add(domainNs)
	commitInExecFlushHistoryNs.Add(historyNs)
}

func addCommitFileBuild(domainNs, historyNs int64) {
	commitFileBuildDomainNs.Add(domainNs)
	commitFileBuildHistoryNs.Add(historyNs)
}

func logCommitMetrics(logger log.Logger, phase string) {
	ms := func(ns int64) time.Duration { return time.Duration(ns).Truncate(time.Millisecond) }
	logger.Info("[commitment-metrics]", "at", phase,
		"inexec_flush_domain", ms(commitInExecFlushDomainNs.Load()),
		"inexec_flush_history", ms(commitInExecFlushHistoryNs.Load()),
		"filebuild_domain", ms(commitFileBuildDomainNs.Load()),
		"filebuild_history", ms(commitFileBuildHistoryNs.Load()),
	)
}
