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

package storage

import (
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// ensureCommitmentAtBlock is mode-B sub-op #3. Validates that the
// latest commitment entry sits exactly at toBlock — the equivalence
// case where the cold-start "processed frozen blocks" state is
// already correct. If the latest commitment is at an earlier block,
// returns the explicit "recompute not yet implemented" sentinel
// pointing at commit 2d.
//
// Sub-op #3's recompute case is genuinely new code (read state at
// toBlock's end-txNum via GetAsOf, walk the patricia trie to derive
// the new root, validate against the block header's stateRoot, write
// the new commitment entry). That work lands in commit 2d. The same
// primitive will be used by fork-from CLI when the cut block isn't
// at an existing commitment boundary — same state-diff problem.
//
// The validation case here is enough to unblock operators picking a
// toBlock that already has a commitment entry (e.g. a step boundary,
// or a previously-anchored cut). Arbitrary-block mode-B has to wait
// for 2d.
func (p *Provider) ensureCommitmentAtBlock(tx kv.TemporalTx, toBlock uint64) error {
	latest, err := commitmentdb.LatestBlockNumWithCommitment(tx)
	if err != nil {
		return fmt.Errorf("LatestBlockNumWithCommitment: %w", err)
	}
	if latest == toBlock {
		return nil
	}
	return fmt.Errorf("commitment recompute at non-step-boundary toBlock=%d (latest commitment is at block %d) not yet implemented — lands in commit 2d (GetAsOf + patricia rebuild + header-stateRoot validation, shared primitive with fork-from CLI per docs/plans/20260525-admin-sethead-unwind-design.md)", toBlock, latest)
}
