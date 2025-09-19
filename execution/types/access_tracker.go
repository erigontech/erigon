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

package types

import (
	"github.com/erigontech/erigon-lib/common"
)

// AccessTracker defines the interface for tracking state accesses during block execution
// This interface is used by EIP-7928: Block-Level Access Lists
type AccessTracker interface {
	// RecordAccess records a state access for the given address and key
	RecordAccess(addr common.Address, key *common.Hash, accessType AccessType, index BlockAccessIndex, value []byte) error

	// GetBlockAccessList returns the complete Block Access List for the block
	GetBlockAccessList() *BlockAccessList

	// Reset clears the access tracker and prepares it for a new block with the given transaction count
	Reset(txCount int) error

	// GetCurrentIndex returns the current block access index
	GetCurrentIndex() BlockAccessIndex

	// SetCurrentIndex sets the current block access index
	SetCurrentIndex(index BlockAccessIndex) error
}

// NoopAccessTracker is an implementation that does nothing (for when EIP-7928 is not active)
type NoopAccessTracker struct{}

func NewNoopAccessTracker() *NoopAccessTracker {
	return &NoopAccessTracker{}
}

func (t *NoopAccessTracker) RecordAccess(addr common.Address, key *common.Hash, accessType AccessType, index BlockAccessIndex, value []byte) error {
	return nil
}

func (t *NoopAccessTracker) GetBlockAccessList() *BlockAccessList {
	return nil
}

func (t *NoopAccessTracker) Reset(txCount int) error {
	return nil
}

func (t *NoopAccessTracker) GetCurrentIndex() BlockAccessIndex {
	return 0
}

func (t *NoopAccessTracker) SetCurrentIndex(index BlockAccessIndex) error {
	return nil
}

// MemoryAccessTracker is an in-memory implementation of AccessTracker
type MemoryAccessTracker struct {
	blockAccessList *BlockAccessList
	currentIndex    BlockAccessIndex
}

func NewMemoryAccessTracker(txCount int) *MemoryAccessTracker {
	return &MemoryAccessTracker{
		blockAccessList: NewBlockAccessList(txCount),
		currentIndex:    0, // Start with pre-execution
	}
}

func (t *MemoryAccessTracker) RecordAccess(addr common.Address, key *common.Hash, accessType AccessType, index BlockAccessIndex, value []byte) error {
	if t.blockAccessList == nil {
		return nil
	}
	return t.blockAccessList.AddAccess(addr, key, accessType, index, value)
}

func (t *MemoryAccessTracker) GetBlockAccessList() *BlockAccessList {
	return t.blockAccessList
}

func (t *MemoryAccessTracker) Reset(txCount int) error {
	t.blockAccessList = NewBlockAccessList(txCount)
	t.currentIndex = 0
	return nil
}

func (t *MemoryAccessTracker) GetCurrentIndex() BlockAccessIndex {
	return t.currentIndex
}

func (t *MemoryAccessTracker) SetCurrentIndex(index BlockAccessIndex) error {
	t.currentIndex = index
	return nil
}
