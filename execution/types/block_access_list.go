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
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
)

const (
	// EIP-7928 constants
	MAX_TXS          = 30_000
	MAX_SLOTS        = 300_000
	MAX_ACCOUNTS     = 300_000
	MAX_CODE_SIZE    = 24_576
	MAX_CODE_CHANGES = 1
)

// BlockAccessIndex represents the position in the block execution sequence
// 0 = pre-execution, 1..n = transaction index, n+1 = post-execution
type BlockAccessIndex uint16

// AccessType represents the type of state access
type AccessType uint8

const (
	AccessNone AccessType = iota
	AccessBalance
	AccessNonce
	AccessCode
	AccessStorage
	AccessCodeHash
)

// AccessEntry represents a single state access entry in the Block Access List
type AccessEntry struct {
	Address    common.Address   // Address being accessed
	Key        *common.Hash     // Storage key (nil for account fields)
	AccessType AccessType       // Type of access
	Index      BlockAccessIndex // Block execution index
	Value      []byte           // Post-execution value
}

// AccessListForAddress contains all accesses for a specific address
type AccessListForAddress struct {
	Address  common.Address
	Balance  *AccessEntry   // Account balance
	Nonce    *AccessEntry   // Account nonce
	Code     *AccessEntry   // Account code
	CodeHash *AccessEntry   // Account code hash
	Storage  []*AccessEntry // Storage slots accessed
}

// BlockAccessList represents the complete block-level access list
// Structure: address -> field -> block_access_index -> change
type BlockAccessList struct {
	PreExecution  map[common.Address]*AccessListForAddress   // Pre-execution accesses (index 0)
	Transactions  []map[common.Address]*AccessListForAddress // Per-transaction accesses (index 1..n)
	PostExecution map[common.Address]*AccessListForAddress   // Post-execution accesses (index n+1)
}

// AccessListForAddressRLP is a simplified structure for RLP encoding
type AccessListForAddressRLP struct {
	Address  common.Address
	Balance  []byte
	Nonce    []byte
	Code     []byte
	CodeHash []byte
	Storage  []storageEntryRLP
}

// storageEntryRLP is a simplified structure for RLP encoding of storage entries
type storageEntryRLP struct {
	Key   common.Hash
	Value []byte
}

// NewBlockAccessList creates a new empty BlockAccessList
func NewBlockAccessList(txCount int) *BlockAccessList {
	return &BlockAccessList{
		PreExecution:  make(map[common.Address]*AccessListForAddress),
		Transactions:  make([]map[common.Address]*AccessListForAddress, txCount),
		PostExecution: make(map[common.Address]*AccessListForAddress),
	}
}

// GetOrCreateAccessListForAddress gets or creates an access list for a specific address and index
func (bal *BlockAccessList) GetOrCreateAccessListForAddress(addr common.Address, index BlockAccessIndex) *AccessListForAddress {
	var accesses map[common.Address]*AccessListForAddress

	switch {
	case index == 0:
		accesses = bal.PreExecution
	case index <= BlockAccessIndex(len(bal.Transactions)):
		if bal.Transactions[index-1] == nil {
			bal.Transactions[index-1] = make(map[common.Address]*AccessListForAddress)
		}
		accesses = bal.Transactions[index-1]
	case index == BlockAccessIndex(len(bal.Transactions))+1:
		accesses = bal.PostExecution
	default:
		return nil
	}

	if accesses[addr] == nil {
		accesses[addr] = &AccessListForAddress{
			Address: addr,
			Storage: make([]*AccessEntry, 0),
		}
	}
	return accesses[addr]
}

// AddAccess adds an access entry to the block access list
func (bal *BlockAccessList) AddAccess(addr common.Address, key *common.Hash, accessType AccessType, index BlockAccessIndex, value []byte) error {
	if index > BlockAccessIndex(len(bal.Transactions))+1 {
		return fmt.Errorf("invalid block access index: %d", index)
	}

	accessList := bal.GetOrCreateAccessListForAddress(addr, index)
	if accessList == nil {
		return fmt.Errorf("cannot create access list for address %x at index %d", addr, index)
	}

	entry := &AccessEntry{
		Address:    addr,
		Key:        key,
		AccessType: accessType,
		Index:      index,
		Value:      make([]byte, len(value)),
	}
	copy(entry.Value, value)

	// Add to appropriate field
	switch accessType {
	case AccessBalance:
		accessList.Balance = entry
	case AccessNonce:
		accessList.Nonce = entry
	case AccessCode:
		accessList.Code = entry
	case AccessCodeHash:
		accessList.CodeHash = entry
	case AccessStorage:
		if key == nil {
			return fmt.Errorf("storage access requires a key")
		}
		accessList.Storage = append(accessList.Storage, entry)
	default:
		return fmt.Errorf("unknown access type: %d", accessType)
	}

	return nil
}

// GetAllAddresses returns all unique addresses accessed in the block
func (bal *BlockAccessList) GetAllAddresses() []common.Address {
	addrMap := make(map[common.Address]struct{})

	// Collect from all phases
	for addr := range bal.PreExecution {
		addrMap[addr] = struct{}{}
	}

	for _, txAccesses := range bal.Transactions {
		for addr := range txAccesses {
			addrMap[addr] = struct{}{}
		}
	}

	for addr := range bal.PostExecution {
		addrMap[addr] = struct{}{}
	}

	addrs := make([]common.Address, 0, len(addrMap))
	for addr := range addrMap {
		addrs = append(addrs, addr)
	}

	// Sort for consistent ordering
	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i][:], addrs[j][:]) < 0
	})

	return addrs
}

// Validate validates the block access list against the defined constraints
func (bal *BlockAccessList) Validate() error {
	// Check transaction count limit
	if len(bal.Transactions) > MAX_TXS {
		return fmt.Errorf("transaction count %d exceeds maximum %d", len(bal.Transactions), MAX_TXS)
	}

	// Check total account limit
	totalAccounts := len(bal.GetAllAddresses())
	if totalAccounts > MAX_ACCOUNTS {
		return fmt.Errorf("total accounts %d exceeds maximum %d", totalAccounts, MAX_ACCOUNTS)
	}

	// Check storage slot limit
	totalSlots := 0
	for _, addr := range bal.GetAllAddresses() {
		// Count storage slots across all phases
		slots := bal.countStorageSlotsForAddress(addr)
		totalSlots += slots
		if totalSlots > MAX_SLOTS {
			return fmt.Errorf("total storage slots %d exceeds maximum %d", totalSlots, MAX_SLOTS)
		}
	}

	// Check code size and changes
	for _, addr := range bal.GetAllAddresses() {
		if err := bal.validateCodeForAddress(addr); err != nil {
			return err
		}
	}

	return nil
}

// countStorageSlotsForAddress counts total unique storage slots accessed for an address
func (bal *BlockAccessList) countStorageSlotsForAddress(addr common.Address) int {
	slots := make(map[common.Hash]struct{})

	// Check all phases
	if accessList := bal.PreExecution[addr]; accessList != nil {
		for _, entry := range accessList.Storage {
			if entry.Key != nil {
				slots[*entry.Key] = struct{}{}
			}
		}
	}

	for _, txAccesses := range bal.Transactions {
		if accessList := txAccesses[addr]; accessList != nil {
			for _, entry := range accessList.Storage {
				if entry.Key != nil {
					slots[*entry.Key] = struct{}{}
				}
			}
		}
	}

	if accessList := bal.PostExecution[addr]; accessList != nil {
		for _, entry := range accessList.Storage {
			if entry.Key != nil {
				slots[*entry.Key] = struct{}{}
			}
		}
	}

	return len(slots)
}

// validateCodeForAddress validates code changes for an address
func (bal *BlockAccessList) validateCodeForAddress(addr common.Address) error {
	codeChanges := 0

	// Check code changes across all phases
	var codeSizes []int

	if accessList := bal.PreExecution[addr]; accessList != nil && accessList.Code != nil {
		codeSizes = append(codeSizes, len(accessList.Code.Value))
		codeChanges++
	}

	for _, txAccesses := range bal.Transactions {
		if accessList := txAccesses[addr]; accessList != nil && accessList.Code != nil {
			codeSizes = append(codeSizes, len(accessList.Code.Value))
			codeChanges++
		}
	}

	if accessList := bal.PostExecution[addr]; accessList != nil && accessList.Code != nil {
		codeSizes = append(codeSizes, len(accessList.Code.Value))
		codeChanges++
	}

	// Check code change limit
	if codeChanges > MAX_CODE_CHANGES {
		return fmt.Errorf("code changes %d for address %x exceeds maximum %d", codeChanges, addr, MAX_CODE_CHANGES)
	}

	// Check code size limit
	for _, size := range codeSizes {
		if size > MAX_CODE_SIZE {
			return fmt.Errorf("code size %d for address %x exceeds maximum %d", size, addr, MAX_CODE_SIZE)
		}
	}

	return nil
}

// EncodeRLP implements rlp.Encoder for BlockAccessList
func (bal *BlockAccessList) EncodeRLP(w io.Writer) error {
	// Convert phases to RLP-encodable structures
	preExec := bal.phaseToRLPList(bal.PreExecution)
	transactions := make([][]*AccessListForAddressRLP, len(bal.Transactions))
	for i, txAccesses := range bal.Transactions {
		transactions[i] = bal.phaseToRLPList(txAccesses)
	}
	postExec := bal.phaseToRLPList(bal.PostExecution)

	// Encode as a list of three elements
	return rlp.Encode(w, []interface{}{preExec, transactions, postExec})
}

// decodeRLPPhase decodes a single phase from RLP structures
func (bal *BlockAccessList) decodeRLPPhase(rlpEntries []*AccessListForAddressRLP, phase map[common.Address]*AccessListForAddress, index BlockAccessIndex) error {
	for _, rlpEntry := range rlpEntries {
		accessList := &AccessListForAddress{Address: rlpEntry.Address}

		// Decode balance
		if len(rlpEntry.Balance) > 0 {
			accessList.Balance = &AccessEntry{
				Address:    rlpEntry.Address,
				Value:      rlpEntry.Balance,
				AccessType: AccessBalance,
				Index:      index,
			}
		}

		// Decode nonce
		if len(rlpEntry.Nonce) > 0 {
			accessList.Nonce = &AccessEntry{
				Address:    rlpEntry.Address,
				Value:      rlpEntry.Nonce,
				AccessType: AccessNonce,
				Index:      index,
			}
		}

		// Decode code
		if len(rlpEntry.Code) > 0 {
			accessList.Code = &AccessEntry{
				Address:    rlpEntry.Address,
				Value:      rlpEntry.Code,
				AccessType: AccessCode,
				Index:      index,
			}
		}

		// Decode code hash
		if len(rlpEntry.CodeHash) > 0 {
			accessList.CodeHash = &AccessEntry{
				Address:    rlpEntry.Address,
				Value:      rlpEntry.CodeHash,
				AccessType: AccessCodeHash,
				Index:      index,
			}
		}

		// Decode storage
		for _, storageEntry := range rlpEntry.Storage {
			accessList.Storage = append(accessList.Storage, &AccessEntry{
				Address:    rlpEntry.Address,
				Key:        &storageEntry.Key,
				Value:      storageEntry.Value,
				AccessType: AccessStorage,
				Index:      index,
			})
		}

		phase[rlpEntry.Address] = accessList
	}

	return nil
}

// phaseToRLPList converts a phase to RLP-encodable structures
func (bal *BlockAccessList) phaseToRLPList(phase map[common.Address]*AccessListForAddress) []*AccessListForAddressRLP {
	if len(phase) == 0 {
		return nil
	}

	// Get sorted addresses for consistent encoding
	addrs := make([]common.Address, 0, len(phase))
	for addr := range phase {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i][:], addrs[j][:]) < 0
	})

	// Convert to RLP structures
	result := make([]*AccessListForAddressRLP, 0, len(phase))
	for _, addr := range addrs {
		accessList := phase[addr]
		rlpStruct := &AccessListForAddressRLP{
			Address:  addr,
			Balance:  bal.accessEntryToBytes(accessList.Balance),
			Nonce:    bal.accessEntryToBytes(accessList.Nonce),
			Code:     bal.accessEntryToBytes(accessList.Code),
			CodeHash: bal.accessEntryToBytes(accessList.CodeHash),
		}

		// Convert storage entries
		for _, entry := range accessList.Storage {
			if entry.Key != nil {
				rlpStruct.Storage = append(rlpStruct.Storage, storageEntryRLP{
					Key:   *entry.Key,
					Value: entry.Value,
				})
			}
		}

		result = append(result, rlpStruct)
	}

	return result
}

// accessEntryToBytes converts an AccessEntry to bytes (or nil)
func (bal *BlockAccessList) accessEntryToBytes(entry *AccessEntry) []byte {
	if entry == nil {
		return nil
	}
	return entry.Value
}

// phaseSize calculates the RLP encoding size of a phase
func (bal *BlockAccessList) phaseSize(phase map[common.Address]*AccessListForAddress) int {
	var size int
	for addr := range phase {
		size += bal.accessListForAddressSize(phase[addr])
	}
	return size
}

// accessListForAddressSize calculates the RLP encoding size of an AccessListForAddress
func (bal *BlockAccessList) accessListForAddressSize(accessList *AccessListForAddress) int {
	size := 21 // Address (20 bytes + 1 byte prefix)

	// Calculate access entries size
	entriesSize := 0

	// Balance
	if accessList.Balance != nil {
		entriesSize += rlp.StringLen(accessList.Balance.Value)
	} else {
		entriesSize++ // nil
	}

	// Nonce
	if accessList.Nonce != nil {
		entriesSize += rlp.StringLen(accessList.Nonce.Value)
	} else {
		entriesSize++ // nil
	}

	// Code
	if accessList.Code != nil {
		entriesSize += rlp.StringLen(accessList.Code.Value)
	} else {
		entriesSize++ // nil
	}

	// CodeHash
	if accessList.CodeHash != nil {
		entriesSize += rlp.StringLen(accessList.CodeHash.Value)
	} else {
		entriesSize++ // nil
	}

	// Storage
	storageSize := 0
	for _, entry := range accessList.Storage {
		storageSize += 33 // Key (32 bytes + 1 byte prefix)
		storageSize += rlp.StringLen(entry.Value)
	}
	entriesSize += rlp.ListPrefixLen(storageSize) + storageSize

	size += rlp.ListPrefixLen(entriesSize) + entriesSize
	return size
}

// encodePhase encodes a single phase of the access list
func (bal *BlockAccessList) encodePhase(w io.Writer, b []byte, phase map[common.Address]*AccessListForAddress) error {
	// Get sorted addresses for consistent encoding
	addrs := make([]common.Address, 0, len(phase))
	for addr := range phase {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i][:], addrs[j][:]) < 0
	})

	for _, addr := range addrs {
		accessList := phase[addr]

		// Encode address entry
		accessEntrySize := bal.accessListForAddressSize(accessList)
		if err := rlp.EncodeStructSizePrefix(accessEntrySize, w, b); err != nil {
			return err
		}

		// Encode address
		if err := rlp.EncodeOptionalAddress(&addr, w, b); err != nil {
			return err
		}

		// Encode access entries
		entriesSize := bal.calculateAccessEntriesSize(accessList)
		if err := rlp.EncodeStructSizePrefix(entriesSize, w, b); err != nil {
			return err
		}

		// Balance
		if accessList.Balance != nil {
			if err := rlp.EncodeString(accessList.Balance.Value, w, b); err != nil {
				return err
			}
		} else {
			b[0] = 128 // RLP encoding of empty string (nil)
			if _, err := w.Write(b[:1]); err != nil {
				return err
			}
		}

		// Nonce
		if accessList.Nonce != nil {
			if err := rlp.EncodeString(accessList.Nonce.Value, w, b); err != nil {
				return err
			}
		} else {
			b[0] = 128 // RLP encoding of empty string (nil)
			if _, err := w.Write(b[:1]); err != nil {
				return err
			}
		}

		// Code
		if accessList.Code != nil {
			if err := rlp.EncodeString(accessList.Code.Value, w, b); err != nil {
				return err
			}
		} else {
			b[0] = 128 // RLP encoding of empty string (nil)
			if _, err := w.Write(b[:1]); err != nil {
				return err
			}
		}

		// CodeHash
		if accessList.CodeHash != nil {
			if err := rlp.EncodeString(accessList.CodeHash.Value, w, b); err != nil {
				return err
			}
		} else {
			b[0] = 128 // RLP encoding of empty string (nil)
			if _, err := w.Write(b[:1]); err != nil {
				return err
			}
		}

		// Storage entries
		storageSize := bal.calculateStorageSize(accessList)
		if err := rlp.EncodeStructSizePrefix(storageSize, w, b); err != nil {
			return err
		}

		for _, entry := range accessList.Storage {
			// Encode key (32 bytes)
			b[0] = 128 + 32
			if _, err := w.Write(b[:1]); err != nil {
				return err
			}
			if _, err := w.Write(entry.Key[:]); err != nil {
				return err
			}
			// Encode value
			if err := rlp.EncodeString(entry.Value, w, b); err != nil {
				return err
			}
		}
	}

	return nil
}

// encodePhaseWithSize encodes a phase with a pre-calculated size
func (bal *BlockAccessList) encodePhaseWithSize(w io.Writer, b []byte, phase map[common.Address]*AccessListForAddress, size int) error {
	if err := rlp.EncodeStructSizePrefix(size, w, b); err != nil {
		return err
	}
	return bal.encodePhase(w, b, phase)
}

// calculateAccessEntriesSize calculates the size of the access entries (balance, nonce, code, codehash, storage)
func (bal *BlockAccessList) calculateAccessEntriesSize(accessList *AccessListForAddress) int {
	size := 0

	// Balance
	if accessList.Balance != nil {
		size += rlp.StringLen(accessList.Balance.Value)
	} else {
		size++ // nil
	}

	// Nonce
	if accessList.Nonce != nil {
		size += rlp.StringLen(accessList.Nonce.Value)
	} else {
		size++ // nil
	}

	// Code
	if accessList.Code != nil {
		size += rlp.StringLen(accessList.Code.Value)
	} else {
		size++ // nil
	}

	// CodeHash
	if accessList.CodeHash != nil {
		size += rlp.StringLen(accessList.CodeHash.Value)
	} else {
		size++ // nil
	}

	// Storage
	storageSize := bal.calculateStorageSize(accessList)
	size += rlp.ListPrefixLen(storageSize) + storageSize

	return size
}

// calculateStorageSize calculates the size of storage entries
func (bal *BlockAccessList) calculateStorageSize(accessList *AccessListForAddress) int {
	size := 0
	for _, entry := range accessList.Storage {
		size += 33 // Key (32 bytes + 1 byte prefix)
		size += rlp.StringLen(entry.Value)
	}
	return size
}

// DecodeRLP implements rlp.Decoder for BlockAccessList
func (bal *BlockAccessList) DecodeRLP(s *rlp.Stream) error {
	// Reset the block access list
	bal.PreExecution = make(map[common.Address]*AccessListForAddress)
	bal.Transactions = nil
	bal.PostExecution = make(map[common.Address]*AccessListForAddress)

	// Decode structure: [preExecution, transactions, postExecution]
	var rlpStruct struct {
		PreExecution  []*AccessListForAddressRLP
		Transactions  [][]*AccessListForAddressRLP
		PostExecution []*AccessListForAddressRLP
	}

	if err := s.Decode(&rlpStruct); err != nil {
		return err
	}

	// Decode pre-execution phase
	if err := bal.decodeRLPPhase(rlpStruct.PreExecution, bal.PreExecution, 0); err != nil {
		return fmt.Errorf("pre-execution phase decode error: %w", err)
	}

	// Decode transaction phases
	bal.Transactions = make([]map[common.Address]*AccessListForAddress, len(rlpStruct.Transactions))
	for i, txData := range rlpStruct.Transactions {
		bal.Transactions[i] = make(map[common.Address]*AccessListForAddress)
		if err := bal.decodeRLPPhase(txData, bal.Transactions[i], BlockAccessIndex(i+1)); err != nil {
			return fmt.Errorf("transaction %d phase decode error: %w", i, err)
		}
	}

	// Decode post-execution phase
	if err := bal.decodeRLPPhase(rlpStruct.PostExecution, bal.PostExecution, BlockAccessIndex(len(rlpStruct.Transactions)+1)); err != nil {
		return fmt.Errorf("post-execution phase decode error: %w", err)
	}

	return nil
}

// decodePhase decodes a single phase from RLP data
func (bal *BlockAccessList) decodePhase(data interface{}, phase map[common.Address]*AccessListForAddress, index BlockAccessIndex) error {
	entries, ok := data.([]interface{})
	if !ok {
		return fmt.Errorf("phase data is not a list")
	}

	for _, entry := range entries {
		addrData, ok := entry.([]interface{})
		if !ok || len(addrData) != 2 {
			return fmt.Errorf("invalid address entry format")
		}

		// Decode address
		addrBytes, ok := addrData[0].([]byte)
		if !ok || len(addrBytes) != 20 {
			return fmt.Errorf("invalid address bytes")
		}
		var addr common.Address
		copy(addr[:], addrBytes)

		// Decode access entries
		accessData, ok := addrData[1].([]interface{})
		if !ok || len(accessData) != 5 {
			return fmt.Errorf("invalid access entries format")
		}

		accessList := &AccessListForAddress{
			Address: addr,
			Storage: make([]*AccessEntry, 0),
		}
		phase[addr] = accessList

		// Decode balance
		if balanceBytes, ok := accessData[0].([]byte); ok && len(balanceBytes) > 0 {
			accessList.Balance = &AccessEntry{
				Address:    addr,
				AccessType: AccessBalance,
				Index:      index,
				Value:      balanceBytes,
			}
		}

		// Decode nonce
		if nonceBytes, ok := accessData[1].([]byte); ok && len(nonceBytes) > 0 {
			accessList.Nonce = &AccessEntry{
				Address:    addr,
				AccessType: AccessNonce,
				Index:      index,
				Value:      nonceBytes,
			}
		}

		// Decode code
		if codeBytes, ok := accessData[2].([]byte); ok && len(codeBytes) > 0 {
			accessList.Code = &AccessEntry{
				Address:    addr,
				AccessType: AccessCode,
				Index:      index,
				Value:      codeBytes,
			}
		}

		// Decode code hash
		if codeHashBytes, ok := accessData[3].([]byte); ok && len(codeHashBytes) > 0 {
			accessList.CodeHash = &AccessEntry{
				Address:    addr,
				AccessType: AccessCodeHash,
				Index:      index,
				Value:      codeHashBytes,
			}
		}

		// Decode storage entries
		storageData, ok := accessData[4].([]interface{})
		if ok {
			for _, storageEntry := range storageData {
				storagePair, ok := storageEntry.([]interface{})
				if !ok || len(storagePair) != 2 {
					return fmt.Errorf("invalid storage entry format")
				}

				keyBytes, ok := storagePair[0].([]byte)
				if !ok || len(keyBytes) != 32 {
					return fmt.Errorf("invalid storage key bytes")
				}

				valueBytes, ok := storagePair[1].([]byte)
				if !ok {
					return fmt.Errorf("invalid storage value bytes")
				}

				var key common.Hash
				copy(key[:], keyBytes)

				accessList.Storage = append(accessList.Storage, &AccessEntry{
					Address:    addr,
					Key:        &key,
					AccessType: AccessStorage,
					Index:      index,
					Value:      valueBytes,
				})
			}
		}
	}

	return nil
}

// String returns a string representation of the BlockAccessList
func (bal *BlockAccessList) String() string {
	return fmt.Sprintf("BlockAccessList{PreExecution: %d addresses, Transactions: %d, PostExecution: %d addresses}",
		len(bal.PreExecution), len(bal.Transactions), len(bal.PostExecution))
}

// AccessEntryString returns a string representation of an AccessEntry
func (ae *AccessEntry) String() string {
	var keyStr string
	if ae.Key != nil {
		keyStr = ae.Key.Hex()
	} else {
		keyStr = "nil"
	}
	return fmt.Sprintf("AccessEntry{Address: %s, Key: %s, Type: %d, Index: %d, Value: %s}",
		ae.Address.Hex(), keyStr, ae.AccessType, ae.Index, hexutil.Encode(ae.Value))
}
