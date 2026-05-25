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

// ensureCommitmentAtBlock is mode-B sub-op #3 — a pure verification.
//
// After sub-op #2 has wiped the writable-domain MDBX shadow past
// toBlock's last txNum, LatestCommitmentState resolves through the
// commitment files: in aligned mode with toBlock at a step boundary,
// the file for that step has its KeyCommitmentState entry tagged at
// the step's last txNum = toBlock, so latest == toBlock by
// construction.
//
// If latest != toBlock here the chain is malformed (the file for
// toBlock's step is missing or its commitment entry is at the wrong
// coordinate), and the unwind has nowhere to anchor the new tip.
// We surface that loudly rather than papering over it with a
// re-derivation that would only mask the underlying corruption.
//
// The fork-from CLI's "cut at a non-boundary parent block" case has
// the same shape but operates on the fork's commitment file, not
// the active one; its primitive lives alongside that wiring, not
// here.
func (p *Provider) ensureCommitmentAtBlock(tx kv.TemporalTx, toBlock uint64) error {
	latest, err := commitmentdb.LatestBlockNumWithCommitment(tx)
	if err != nil {
		return fmt.Errorf("LatestBlockNumWithCommitment: %w", err)
	}
	if latest == toBlock {
		return nil
	}
	return fmt.Errorf("commitment-anchor verification failed: latest commitment is at block %d, expected %d — the commitment file for toBlock's step is missing or its entry is at the wrong coordinate (sub-op #2 wiped MDBX shadow past toBlock; no override is possible from the writable side)", latest, toBlock)
}
