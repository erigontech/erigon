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

package core

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types"
)

var (
	ErrInvalidBlockAccessList = errors.New("invalid block access list")
	ErrAccessListHashMismatch = errors.New("access list hash mismatch")
	ErrMissingAccessEntry     = errors.New("missing access entry")
	ErrInvalidAccessIndex     = errors.New("invalid access index")
	ErrAccessValueMismatch    = errors.New("access value mismatch")
	ErrExceedsMaxTransactions = errors.New("exceeds maximum transactions")
	ErrExceedsMaxAccounts     = errors.New("exceeds maximum accounts")
	ErrExceedsMaxStorageSlots = errors.New("exceeds maximum storage slots")
	ErrExceedsMaxCodeChanges  = errors.New("exceeds maximum code changes")
	ErrExceedsMaxCodeSize     = errors.New("exceeds maximum code size")
)

// BlockAccessListValidator handles validation of Block Access Lists according to EIP-7928
type BlockAccessListValidator struct {
	enabled bool
}

// NewBlockAccessListValidator creates a new BAL validator
func NewBlockAccessListValidator(enabled bool) *BlockAccessListValidator {
	return &BlockAccessListValidator{
		enabled: enabled,
	}
}

// ValidateBlockAccessList validates a Block Access List against actual execution
func (v *BlockAccessListValidator) ValidateBlockAccessList(
	providedBAL *types.BlockAccessList,
	executionBAL *types.BlockAccessList,
	header *types.Header,
) error {
	if !v.enabled {
		return nil
	}

	if providedBAL == nil {
		return ErrInvalidBlockAccessList
	}

	// Validate the provided BAL structure
	if err := v.validateBALStructure(providedBAL); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBlockAccessList, err)
	}

	// Validate the execution BAL structure
	if err := v.validateBALStructure(executionBAL); err != nil {
		return fmt.Errorf("%w: execution BAL invalid - %v", ErrInvalidBlockAccessList, err)
	}

	// Compare the two access lists
	if err := v.compareAccessLists(providedBAL, executionBAL); err != nil {
		return err
	}

	// Validate the hash matches header
	if header != nil && header.BlockAccessListHash != nil {
		computedHash := rlpHash(providedBAL)
		if computedHash != *header.BlockAccessListHash {
			return fmt.Errorf("%w: computed %x, expected %x",
				ErrAccessListHashMismatch, computedHash, *header.BlockAccessListHash)
		}
	}

	return nil
}

// validateBALStructure validates the structure of a Block Access List
func (v *BlockAccessListValidator) validateBALStructure(bal *types.BlockAccessList) error {
	if bal == nil {
		return ErrInvalidBlockAccessList
	}

	// Check transaction count
	if len(bal.Transactions) > types.MAX_TXS {
		return fmt.Errorf("%w: %d > %d", ErrExceedsMaxTransactions, len(bal.Transactions), types.MAX_TXS)
	}

	// Check total accounts
	totalAccounts := len(bal.GetAllAddresses())
	if totalAccounts > types.MAX_ACCOUNTS {
		return fmt.Errorf("%w: %d > %d", ErrExceedsMaxAccounts, totalAccounts, types.MAX_ACCOUNTS)
	}

	// Check storage slots and code constraints per address
	for _, addr := range bal.GetAllAddresses() {
		if err := v.validateAddressAccesses(bal, addr); err != nil {
			return fmt.Errorf("address %x: %w", addr, err)
		}
	}

	return nil
}

// validateAddressAccesses validates access constraints for a specific address
func (v *BlockAccessListValidator) validateAddressAccesses(bal *types.BlockAccessList, addr common.Address) error {
	storageSlots := make(map[common.Hash]struct{})
	codeChanges := 0
	var maxCodeSize int

	// Check all phases for this address
	phases := []map[common.Address]*types.AccessListForAddress{
		bal.PreExecution,
		bal.PostExecution,
	}

	// Add transaction phases
	for _, txPhase := range bal.Transactions {
		phases = append(phases, txPhase)
	}

	for _, phase := range phases {
		accessList := phase[addr]
		if accessList == nil {
			continue
		}

		// Check storage slots
		for _, entry := range accessList.Storage {
			if entry.Key != nil {
				storageSlots[*entry.Key] = struct{}{}
			}
		}

		// Check code changes and sizes
		if accessList.Code != nil {
			codeChanges++
			if len(accessList.Code.Value) > maxCodeSize {
				maxCodeSize = len(accessList.Code.Value)
			}
		}
	}

	// Validate constraints
	if len(storageSlots) > types.MAX_SLOTS {
		return fmt.Errorf("%w: %d > %d", ErrExceedsMaxStorageSlots, len(storageSlots), types.MAX_SLOTS)
	}

	if codeChanges > types.MAX_CODE_CHANGES {
		return fmt.Errorf("%w: %d > %d", ErrExceedsMaxCodeChanges, codeChanges, types.MAX_CODE_CHANGES)
	}

	if maxCodeSize > types.MAX_CODE_SIZE {
		return fmt.Errorf("%w: %d > %d", ErrExceedsMaxCodeSize, maxCodeSize, types.MAX_CODE_SIZE)
	}

	return nil
}

// compareAccessLists compares provided and execution access lists
func (v *BlockAccessListValidator) compareAccessLists(provided, execution *types.BlockAccessList) error {
	// Check transaction count matches
	if len(provided.Transactions) != len(execution.Transactions) {
		return fmt.Errorf("%w: transaction count mismatch, provided %d, execution %d",
			ErrInvalidBlockAccessList, len(provided.Transactions), len(execution.Transactions))
	}

	// Check all addresses in provided list are in execution list
	providedAddrs := provided.GetAllAddresses()
	executionAddrs := execution.GetAllAddresses()

	if len(providedAddrs) != len(executionAddrs) {
		return fmt.Errorf("%w: address count mismatch, provided %d, execution %d",
			ErrInvalidBlockAccessList, len(providedAddrs), len(executionAddrs))
	}

	// Compare accesses for each address
	for _, addr := range providedAddrs {
		if err := v.compareAddressAccesses(provided, execution, addr); err != nil {
			return fmt.Errorf("address %x: %w", addr, err)
		}
	}

	// Ensure all execution addresses are in provided list
	for _, addr := range executionAddrs {
		found := false
		for _, providedAddr := range providedAddrs {
			if addr == providedAddr {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: address %x found in execution but not in provided list",
				ErrMissingAccessEntry, addr)
		}
	}

	return nil
}

// compareAddressAccesses compares accesses for a specific address between provided and execution BALs
func (v *BlockAccessListValidator) compareAddressAccesses(provided, execution *types.BlockAccessList, addr common.Address) error {
	// Compare each phase
	phases := []struct {
		name string
		prov map[common.Address]*types.AccessListForAddress
		exec map[common.Address]*types.AccessListForAddress
	}{
		{"pre-execution", provided.PreExecution, execution.PreExecution},
		{"post-execution", provided.PostExecution, execution.PostExecution},
	}

	// Compare transaction phases
	for i := 0; i < len(provided.Transactions); i++ {
		phases = append(phases, struct {
			name string
			prov map[common.Address]*types.AccessListForAddress
			exec map[common.Address]*types.AccessListForAddress
		}{
			fmt.Sprintf("transaction %d", i+1),
			provided.Transactions[i],
			execution.Transactions[i],
		})
	}

	for _, phase := range phases {
		if err := v.comparePhaseAccesses(phase.name, phase.prov, phase.exec, addr); err != nil {
			return err
		}
	}

	return nil
}

// comparePhaseAccesses compares accesses for a specific phase
func (v *BlockAccessListValidator) comparePhaseAccesses(
	phaseName string,
	provided, execution map[common.Address]*types.AccessListForAddress,
	addr common.Address,
) error {
	provList := provided[addr]
	execList := execution[addr]

	// If one exists and the other doesn't, that's an error
	if (provList == nil) != (execList == nil) {
		return fmt.Errorf("%w: %s access list mismatch for address %x",
			ErrInvalidBlockAccessList, phaseName, addr)
	}

	// If neither exists, that's fine
	if provList == nil && execList == nil {
		return nil
	}

	// Compare balance access
	if err := v.compareAccessEntry(provList.Balance, execList.Balance, addr, "balance", phaseName); err != nil {
		return err
	}

	// Compare nonce access
	if err := v.compareAccessEntry(provList.Nonce, execList.Nonce, addr, "nonce", phaseName); err != nil {
		return err
	}

	// Compare code access
	if err := v.compareAccessEntry(provList.Code, execList.Code, addr, "code", phaseName); err != nil {
		return err
	}

	// Compare code hash access
	if err := v.compareAccessEntry(provList.CodeHash, execList.CodeHash, addr, "code hash", phaseName); err != nil {
		return err
	}

	// Compare storage accesses
	if err := v.compareStorageAccesses(provList.Storage, execList.Storage, addr, phaseName); err != nil {
		return err
	}

	return nil
}

// compareAccessEntry compares individual access entries
func (v *BlockAccessListValidator) compareAccessEntry(
	provEntry, execEntry *types.AccessEntry,
	addr common.Address,
	fieldName, phaseName string,
) error {
	if (provEntry == nil) != (execEntry == nil) {
		return fmt.Errorf("%w: %s %s access mismatch for address %x (phase: %s)",
			ErrMissingAccessEntry, fieldName,
			map[bool]string{true: "provided", false: "execution"}[provEntry != nil],
			addr, phaseName)
	}

	// If neither exists, that's fine
	if provEntry == nil && execEntry == nil {
		return nil
	}

	// Compare values
	if !compareByteSlices(provEntry.Value, execEntry.Value) {
		return fmt.Errorf("%w: %s value mismatch for address %x (phase: %s): provided %x, execution %x",
			ErrAccessValueMismatch, fieldName, addr, phaseName, provEntry.Value, execEntry.Value)
	}

	return nil
}

// compareStorageAccesses compares storage slot accesses
func (v *BlockAccessListValidator) compareStorageAccesses(
	provStorage, execStorage []*types.AccessEntry,
	addr common.Address,
	phaseName string,
) error {
	if len(provStorage) != len(execStorage) {
		return fmt.Errorf("%w: storage slot count mismatch for address %x (phase: %s): provided %d, execution %d",
			ErrInvalidBlockAccessList, addr, phaseName, len(provStorage), len(execStorage))
	}

	// Create maps for comparison
	provMap := make(map[common.Hash][]byte)
	execMap := make(map[common.Hash][]byte)

	for _, entry := range provStorage {
		if entry.Key == nil {
			return fmt.Errorf("%w: nil storage key in provided list for address %x",
				ErrInvalidBlockAccessList, addr)
		}
		provMap[*entry.Key] = entry.Value
	}

	for _, entry := range execStorage {
		if entry.Key == nil {
			return fmt.Errorf("%w: nil storage key in execution list for address %x",
				ErrInvalidBlockAccessList, addr)
		}
		execMap[*entry.Key] = entry.Value
	}

	// Compare all slots
	for key, provValue := range provMap {
		execValue, exists := execMap[key]
		if !exists {
			return fmt.Errorf("%w: storage slot %s for address %x missing from execution list (phase: %s)",
				ErrMissingAccessEntry, key.Hex(), addr, phaseName)
		}

		if !compareByteSlices(provValue, execValue) {
			return fmt.Errorf("%w: storage slot %s value mismatch for address %x (phase: %s): provided %x, execution %x",
				ErrAccessValueMismatch, key.Hex(), addr, phaseName, provValue, execValue)
		}
	}

	// Ensure no extra slots in execution
	for key := range execMap {
		if _, exists := provMap[key]; !exists {
			return fmt.Errorf("%w: storage slot %s for address %x found in execution but not in provided list (phase: %s)",
				ErrMissingAccessEntry, key.Hex(), addr, phaseName)
		}
	}

	return nil
}

// compareByteSlices compares two byte slices safely
func compareByteSlices(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// IsEnabled returns whether the validator is enabled
func (v *BlockAccessListValidator) IsEnabled() bool {
	return v.enabled
}
