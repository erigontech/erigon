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

package stagedsync

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

func IsInitialCycle(tx kv.Getter, cfg ethconfig.Sync, frozenBlocks uint64, knownTipHints ...uint64) (bool, error) {
	finishProgress, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return false, err
	}
	headersProgress, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return false, err
	}
	knownTip := KnownTip(headersProgress, frozenBlocks, knownTipHints...)
	return IsInitialCycleFromProgress(tx, knownTip, finishProgress, cfg.InitialCycleBlockTTL)
}

func IsInitialCycleFromProgress(tx kv.Getter, knownTip, finishProgress, ttl uint64) (bool, error) {
	lastTipReachedBlock, ok, err := rawdb.ReadLastTipReachedBlock(tx)
	if err != nil {
		return false, err
	}
	if !ok || finishProgress == 0 {
		return true, nil
	}
	// knownTip is the floor of "where the chain head is now" — bumped up by the marker
	// so an outdated headers/frozen view never makes us think we're at tip.
	//
	// Two independent invariants must hold to skip the initial cycle:
	//   1) the recorded tip-reached event is recent: knownTip - lastTipReachedBlock <= ttl
	//      (otherwise the marker is stale and tells us nothing about the present)
	//   2) current finish progress is still close to tip: knownTip - finishProgress <= ttl
	//      (otherwise we've fallen behind and need a full initial cycle again)
	knownTip = max(knownTip, lastTipReachedBlock)
	recentlyReachedTip := withinBlockTTL(knownTip, lastTipReachedBlock, ttl) && withinBlockTTL(knownTip, finishProgress, ttl)
	return !recentlyReachedTip, nil
}

func UpdateTipReached(tx kv.GetPut, frozenBlocks uint64, knownTipHints ...uint64) error {
	finishProgress, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return err
	}
	headersProgress, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	return UpdateTipReachedFromProgress(tx, KnownTip(headersProgress, frozenBlocks, knownTipHints...), finishProgress)
}

func UpdateTipReachedFromProgress(tx kv.GetPut, knownTip, finishProgress uint64) error {
	if finishProgress > 0 && finishProgress >= knownTip {
		lastTipReachedBlock, ok, err := rawdb.ReadLastTipReachedBlock(tx)
		if err != nil {
			return err
		}
		if ok && finishProgress <= lastTipReachedBlock {
			return nil
		}
		return rawdb.WriteTipReached(tx, finishProgress)
	}
	return nil
}

func KnownTip(headersProgress, frozenBlocks uint64, knownTipHints ...uint64) uint64 {
	knownTip := max(headersProgress, frozenBlocks)
	for _, hint := range knownTipHints {
		knownTip = max(knownTip, hint)
	}
	return knownTip
}

func withinBlockTTL(knownTip, progress, ttl uint64) bool {
	if knownTip <= progress {
		return true
	}
	return knownTip-progress <= ttl
}
