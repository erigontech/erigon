// Copyright 2024 The Erigon Authors
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

package integrity

type Check string

const (
	// Blocks spot-checks that block snapshots are readable. Samples blocks at intervals
	// to verify block data can be loaded correctly from snapshots. This is a basic read test
	// that doesn't validate block content, just that snapshots aren't corrupted.
	Blocks Check = "Blocks"

	// HeaderNoGaps verifies canonical block header chain has no gaps. Ensures every block
	// number from 1 to the tip has a header and body in the database. Detects missing blocks
	// or orphaned blocks in the canonical chain.
	HeaderNoGaps Check = "HeaderNoGaps"

	// BlocksTxnID verifies transaction ID consistency in block snapshots. Checks that
	// transaction numbering is sequential and consistent across the frozen block files.
	// Different from ReceiptsNoDups in that it validates the block snapshot layer itself.
	BlocksTxnID Check = "BlocksTxnID"

	// InvertedIndex validates inverted index files for the history domain (E3 format).
	// Checks that history inverted indexes can be read and correctly reference their
	// corresponding key-value data.
	InvertedIndex Check = "InvertedIndex"

	// HistoryNoSystemTxs verifies system transactions don't appear at block start.
	// Samples history data to ensure the first transaction in each block's history
	// is not a system transaction (except genesis). Used in Polygon chain validation.
	HistoryNoSystemTxs Check = "HistoryNoSystemTxs"

	// ReceiptsNoDups validates receipt data monotonicity. Checks that cumulative gas used
	// and log indexes are strictly increasing within each block. Detects duplicate or
	// out-of-order receipt data. Specifically validates LogIndexAfterTx and CumulativeGasUsed.
	ReceiptsNoDups Check = "ReceiptsNoDups"

	// RCacheNoDups validates receipt cache (in-memory cache layer) monotonicity.
	// Similar to ReceiptsNoDups but for the cached receipt data: verifies FirstLogIndexWithinBlock
	// and CumulativeGasUsed are monotonically increasing. Differs from ReceiptsNoDups in that
	// it works on the cached representation rather than the raw receipt domain.
	RCacheNoDups Check = "RCacheNoDups"

	// BorEvents validates Polygon Bor event snapshots (Heimdall events). Only runs on Bor chains.
	// Checks consistency of Bor bridge events. Skipped silently on non-Bor chains.
	BorEvents Check = "BorEvents"

	// BorSpans validates Polygon Bor span snapshots (validator spans). Only runs on Bor chains.
	// Checks consistency of Bor validator span data. Skipped silently on non-Bor chains.
	BorSpans Check = "BorSpans"

	// BorCheckpoints validates Polygon Bor checkpoint snapshots. Only runs on Bor chains.
	// Checks consistency of Bor checkpoint data. Skipped silently on non-Bor chains.
	BorCheckpoints Check = "BorCheckpoints"

	// CommitmentRoot verifies commitment state roots are present and correct. Checks that
	// each commitment snapshot file contains the state root key, and optionally recomputes
	// the root to verify it matches stored values. Ensures commitment snapshots have valid roots.
	CommitmentRoot Check = "CommitmentRoot"

	// CommitmentKvDeref validates commitment branch key references. Checks that every
	// reference from commitment branch keys (in commitment domain) to account/storage domain
	// keys can be dereferenced correctly. Detects missing accounts or storage entries that
	// are referenced by commitment branches. Different from CommitmentKvi in that it validates
	// the actual data references between domains.
	CommitmentKvDeref Check = "CommitmentKvDeref"

	// CommitmentHistVal samples and validates commitment history values. Randomly samples
	// 5% of commitment history data to verify that historical values are readable and
	// consistent. This is a statistical check used to catch corruption without checking every value.
	CommitmentHistVal Check = "CommitmentHistVal"

	// StateVerify verifies state correspondence between commitment and domains. Checks that
	// every key in account/storage domains is properly referenced by commitment branches,
	// and vice versa. Catches missing or extra keys. Uses forward check for base files
	// (commitment refs <= domain keys) and reverse check for incremental files (all domain keys
	// have commitment refs). Different from CommitmentKvDeref in that it checks complete
	// bidirectional correspondence rather than just reference validity.
	StateVerify Check = "StateVerify"

	// StateProgress verifies state files aren't ahead of block files. Checks that the
	// commitment domain progress doesn't exceed the frozen block files progress.
	// Prevents inconsistent states where state snapshots are more recent than block snapshots.
	StateProgress Check = "StateProgress"

	// Publishable validates snapshot publication readiness. Checks that all required snapshot
	// files exist and are properly structured: block snapshots, state snapshots, beacon/caplin
	// snapshots, and required metadata files (salt files). This is the final check before
	// publishing snapshots for external distribution.
	Publishable Check = "Publishable"
)

const (
	// CommitmentKvi validates KVI index files for commitment domain. Checks that the KVI
	// index files (.kvi) match their corresponding KV files (.kv): same key count and
	// correct offset mappings. Different from CommitmentKvDeref in that it only validates
	// the index structure itself.
	CommitmentKvi Check = "CommitmentKvi"
)

var FastChecks = []Check{
	Blocks, HeaderNoGaps, BlocksTxnID, InvertedIndex, StateProgress, Publishable, HistoryNoSystemTxs,
	BorEvents, BorSpans, BorCheckpoints, ReceiptsNoDups, RCacheNoDups, CommitmentRoot,
	StateVerify, CommitmentKvDeref,
}

var SlowChecks = []Check{CommitmentHistVal, StateVerify}
var DeprecatedChecks = []Check{
	CommitmentKvi, // super-passed by `StateVerify`
}

var AllChecks = append(append(append([]Check{}, FastChecks...), SlowChecks...), DeprecatedChecks...)
