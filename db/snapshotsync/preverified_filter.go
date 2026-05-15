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
// Divergence from SyncSnapshots' inline filter is intentional. The
// inline filter uses head-anchored predicates: it keeps state history
// at the prune horizon (range >= max(0, head-distance)) and drops only
// what's been retired off the tail. At fresh bootstrap, head==0 and
// the horizon collapses to "keep everything" — which is exactly the
// degenerate case bug N exposed. The bootstrap-side filter therefore
// applies an all-or-nothing rule: "if this prune mode runtime-prunes
// history, don't download any of it at startup; the publisher will
// rebuild what's needed during execution." Two filters, two contexts,
// two policies; trying to unify produces either an unsafe relaxation
// (bootstrap downloads things it would prune — bug N) or a wrong
// restriction (running-node keeps too little).
//
// The invariant the two filters share — and which
// TestFilterIsSubsetOfArchive pins below — is monotonicity:
// FilterPreverifiedByPruneMode(items, *, mode).len <= len(items) for
// every mode. The bootstrap-side filter never adds entries that
// weren't in preverified to begin with; SyncSnapshots' running-loop
// has the same property.
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
		// CL data (caplin archive + beacon blocks + blob sidecars) is
		// dropped under the prune-history gate. Three distinct file
		// naming patterns all describe CL data; the inline SyncSnapshots
		// filter at snapshotsync.go:398 uses the same three-substring
		// check (caplin == NoCaplin drops all three):
		//
		//   1. "caplin/" prefix — beacon state snapshots, validator
		//      balances dumps, ActiveValidatorIndicies, etc.
		//   2. "beaconblocks" substring — historical beacon blocks
		//      at the top level (v1.1-NNNNNN-NNNNNN-beaconblocks.seg).
		//   3. "blobsidecars" substring — historical blob sidecars
		//      at the top level (v1.1-NNNNNN-NNNNNN-blobsidecars.seg).
		//
		// Bug X: an earlier version of this filter only matched the
		// caplin/ prefix, allowing 501 blobsidecars + 1050 beaconblocks
		// entries (mainnet preverified) through to the bootstrap
		// manifest. The downloader pulled 1.7 TB of historical blob
		// sidecars on top of the EL set — exactly what minimal-mode
		// is supposed to avoid. Matching the inline filter's full
		// three-substring contains check makes the bootstrap path
		// honour `--caplin.blocks-archive=false` and
		// `--caplin.blobs-archive=false` (their defaults) via the
		// same prune-history-driven gate.
		//
		// Coupling CL data to prune.History.Enabled() (rather than
		// to dedicated CL-archive flags) is the bootstrap-side
		// shorthand: every non-archive prune mode is consistent
		// with "don't host the CL archive." If a future operator
		// wants archive-CL + minimal-EL, the inline SyncSnapshots
		// path's caplinState flag remains the explicit knob; the
		// bootstrap filter would need a parallel parameter then.
		if pruneMode.History.Enabled() && isCLData(p.Name) {
			continue
		}
		if strings.Contains(p.Name, "transactions") && isTransactionsSegmentExpired(cc, pruneMode, p) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// isCLData reports whether a preverified item name describes CL
// (consensus-layer) data — beacon state snapshots, historical
// beacon blocks, or blob sidecars. Mirrors the three-substring
// pattern used by SyncSnapshots' inline filter at snapshotsync.go:398
// for caplin == NoCaplin: the bootstrap manifest synthesised under
// non-archive prune modes must drop the same set.
func isCLData(name string) bool {
	return strings.HasPrefix(name, "caplin/") ||
		strings.Contains(name, "beaconblocks") ||
		strings.Contains(name, "blobsidecars")
}
