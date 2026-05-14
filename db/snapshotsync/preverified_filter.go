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

package snapshotsync

import (
	"strings"

	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/execution/chain"
)

// FilterPreverifiedByPruneMode returns the subset of preverified items
// the node should download given its prune.Mode + chain config. Used
// by the V2 bootstrap-from-preverified path in
// node/components/storage/provider.go so a publisher under
// --prune.mode=minimal doesn't pull the full archive (~1.9 TB of state
// history) it would just prune anyway.
//
// The filter intentionally captures only the prune-driven subset of
// SyncSnapshots' inline filtering loop. Other filters there (caplin
// mode, blobs, caplinState, headerchain, KeepExecutionProofs,
// PersistReceiptsCacheV2, SnapshotDownloadToBlock) belong to the
// running-node download path and don't apply identically at fresh
// bootstrap; the bootstrap caller is responsible for any additional
// kind-level filtering it wants.
//
// Rules applied here:
//
//  1. State-history files (idx/, history/, accessor/ — matched by
//     isStateHistory) are dropped when prune.History is enabled
//     (i.e. every prune mode except Archive). Archive keeps them.
//     This is the bug-N fix: without this drop, a --prune.mode=minimal
//     publisher at fresh bootstrap pulls the entire archive history
//     because there's no head-anchored prune horizon to filter against
//     yet (getMinimumBlocksToDownload requires frozen bodies on disk).
//
//  2. Pre-merge transactions are dropped per the existing
//     isTransactionsSegmentExpired contract (kept verbatim — the
//     condition is "Blocks == DefaultBlocksPruneMode AND IsPreMerge",
//     which only matches Full mode by current prune.Mode constants).
//
//  3. Receipt-cache and log-index files are NOT filtered here. The
//     running-node isReceiptsSegmentPruned check needs a kv.Tx +
//     TxNumsReader to compute the txNum at the prune horizon, neither
//     of which is available pre-Initialize. At fresh bootstrap the
//     receipts are kept; SyncSnapshots' running-loop applies the
//     finer filter once the node has bodies on disk.
//
// Single source of truth follow-up: SyncSnapshots itself should call
// this helper rather than re-implement rules 1–2 inline. Deferred —
// the running-loop also handles the receipts / blacklist axes and
// refactoring those into the helper widens scope. Tracked in
// docs/plans/time-to-get-back-generic-mist.md "Pickup state 2026-05-14".
func FilterPreverifiedByPruneMode(items snapcfg.PreverifiedItems, cc *chain.Config, pruneMode prune.Mode) snapcfg.PreverifiedItems {
	if !pruneMode.Initialised {
		// Defensive: an uninitialised mode should not silently
		// behave as archive. Callers must always pass an initialised
		// prune.Mode (cmd/utils maps every --prune.mode value to one).
		return items
	}
	out := make(snapcfg.PreverifiedItems, 0, len(items))
	for _, p := range items {
		if pruneMode.History.Enabled() && isStateHistory(p.Name) {
			continue
		}
		if strings.Contains(p.Name, "transactions") && isTransactionsSegmentExpired(cc, pruneMode, p) {
			continue
		}
		out = append(out, p)
	}
	return out
}
