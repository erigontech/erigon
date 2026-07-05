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
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

// CommitmentBranchExpander resolves shortened key refs in a commitment branch
// value against the accounts/storage files at a fixed [startTxNum, endTxNum)
// range. Callers that need to unpack many V's from the same commitment file
// (e.g. mode-B unwind's boundary regen) open one expander for the file, call
// Expand per V, then Close. This amortises the AggregatorRoTx open and
// file-item lookups over the whole file.
//
// An unresolvable ref is a hard error, not a fallback: it means the accounts
// or storage file the commitment file was written against has been merged or
// removed, and the ref no longer points at a live plain key. Callers that
// cannot recover from this must abort the regen loudly rather than emit an
// incomplete file.
type CommitmentBranchExpander struct {
	at                    *AggregatorRoTx
	accountFile           *FilesItem
	storageFile           *FilesItem
	accountsRo, storageRo *DomainRoTx
	startTxNum, endTxNum  uint64
	ownsRoTx              bool
}

// NewCommitmentBranchExpander opens an AggregatorRoTx and resolves the
// accounts/storage FilesItems at [startTxNum, endTxNum). The returned
// expander borrows the RoTx and must be Closed by the caller. Errors if
// either file is not visible at that range — the caller cannot safely emit
// full-plain-key output without both source files present.
func NewCommitmentBranchExpander(a *Aggregator, startTxNum, endTxNum uint64) (*CommitmentBranchExpander, error) {
	at := a.BeginFilesRo()
	acc := at.d[kv.AccountsDomain]
	sto := at.d[kv.StorageDomain]
	accountFile, err := acc.lookupVisibleFileByRange(startTxNum, endTxNum)
	if err != nil {
		at.Close()
		return nil, fmt.Errorf("NewCommitmentBranchExpander: accounts file at [%d, %d): %w", startTxNum, endTxNum, err)
	}
	storageFile, err := sto.lookupVisibleFileByRange(startTxNum, endTxNum)
	if err != nil {
		at.Close()
		return nil, fmt.Errorf("NewCommitmentBranchExpander: storage file at [%d, %d): %w", startTxNum, endTxNum, err)
	}
	return &CommitmentBranchExpander{
		at:          at,
		accountFile: accountFile,
		storageFile: storageFile,
		accountsRo:  acc,
		storageRo:   sto,
		startTxNum:  startTxNum,
		endTxNum:    endTxNum,
		ownsRoTx:    true,
	}, nil
}

// Expand resolves every shortened key ref inside branch to a full plain key.
// Empty branch or a branch with no refs pass through unchanged (the underlying
// ReplacePlainKeys callback keeps entries whose key length matches the plain
// form — length.Addr for accounts, length.Addr+length.Hash for storage).
//
// An unresolvable ref returns an error naming the ref bytes; caller must not
// emit a partial file.
func (e *CommitmentBranchExpander) Expand(branch commitment.BranchData) (commitment.BranchData, error) {
	if len(branch) == 0 {
		return branch, nil
	}
	return ExpandShortenedKeysInBranch(branch, e.accountsRo, e.storageRo, e.accountFile, e.storageFile, e.startTxNum, e.endTxNum)
}

// Close releases the AggregatorRoTx and drops file-item references.
func (e *CommitmentBranchExpander) Close() {
	if e == nil || !e.ownsRoTx {
		return
	}
	e.at.Close()
	e.ownsRoTx = false
	e.at = nil
	e.accountFile = nil
	e.storageFile = nil
	e.accountsRo = nil
	e.storageRo = nil
}
