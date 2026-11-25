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

package commitmentdb

import (
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment"
)

var pinnedMuProbeEnabled = dbg.EnvBool("COMMITMENT_PINNEDMU_PROBE", false)

// pinnedMuProbe counts, per compute, the fold workers that fall back to the
// caller's tx and the time readers wait on the lock that serializes that tx.
type pinnedMuProbe struct {
	workers, drifted       atomic.Int64
	mainReads, workerReads atomic.Int64
	mainWait, workerWait   atomic.Int64
}

func (p *pinnedMuProbe) log(blockNum uint64, variant commitment.TrieVariant, fold time.Duration) {
	log.Info("[dbg] pinnedMu probe", "block", blockNum, "variant", variant,
		"workers", p.workers.Load(), "drifted", p.drifted.Load(),
		"mainReads", p.mainReads.Load(), "mainWait", time.Duration(p.mainWait.Load()),
		"workerReads", p.workerReads.Load(), "workerWait", time.Duration(p.workerWait.Load()),
		"fold", fold)
}
