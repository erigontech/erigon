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
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/holiman/uint256"
)

func TestBlockAccessListBasicOperations(t *testing.T) {
	bal := NewBlockAccessList(3)

	// Test adding balance access
	addr1 := common.HexToAddress("0x1234567890123456789012345678901234567890")
	value1 := uint256.NewInt(1000).Bytes()

	err := bal.AddAccess(addr1, nil, AccessBalance, 0, value1)
	if err != nil {
		t.Fatalf("Failed to add balance access: %v", err)
	}

	// Test adding nonce access
	value2 := []byte{5}
	err = bal.AddAccess(addr1, nil, AccessNonce, 1, value2)
	if err != nil {
		t.Fatalf("Failed to add nonce access: %v", err)
	}

	// Test adding storage access
	key := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	value3 := []byte("storage value")
	err = bal.AddAccess(addr1, &key, AccessStorage, 2, value3)
	if err != nil {
		t.Fatalf("Failed to add storage access: %v", err)
	}

	// Test adding code access
	value4 := []byte{0x60, 0x80, 0x60, 0x40} // Simple PUSH1 0x80 PUSH1 0x40
	err = bal.AddAccess(addr1, nil, AccessCode, 3, value4)
	if err != nil {
		t.Fatalf("Failed to add code access: %v", err)
	}

	// Test GetAllAddresses
	addresses := bal.GetAllAddresses()
	if len(addresses) != 1 {
		t.Fatalf("Expected 1 address, got %d", len(addresses))
	}
	if addresses[0] != addr1 {
		t.Fatalf("Expected address %x, got %x", addr1, addresses[0])
	}

	// Test validation
	err = bal.Validate()
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
}

func TestBlockAccessListMultipleAddresses(t *testing.T) {
	bal := NewBlockAccessList(2)

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	// Add accesses for multiple addresses
	bal.AddAccess(addr1, nil, AccessBalance, 0, uint256.NewInt(100).Bytes())
	bal.AddAccess(addr2, nil, AccessBalance, 1, uint256.NewInt(200).Bytes())

	addresses := bal.GetAllAddresses()
	if len(addresses) != 2 {
		t.Fatalf("Expected 2 addresses, got %d", len(addresses))
	}

	// Check that addresses are sorted
	if bytes.Compare(addresses[0][:], addresses[1][:]) >= 0 {
		t.Fatalf("Addresses are not sorted: %x, %x", addresses[0], addresses[1])
	}
}

func TestBlockAccessListStorageTracking(t *testing.T) {
	bal := NewBlockAccessList(1)

	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// Add multiple storage accesses to the same address
	key1 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	key2 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
	key3 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001") // Duplicate

	bal.AddAccess(addr, &key1, AccessStorage, 0, []byte("value1"))
	bal.AddAccess(addr, &key2, AccessStorage, 0, []byte("value2"))
	bal.AddAccess(addr, &key3, AccessStorage, 0, []byte("value3")) // Same key, different value

	// Test counting unique storage slots
	slots := bal.countStorageSlotsForAddress(addr)
	if slots != 2 {
		t.Fatalf("Expected 2 unique storage slots, got %d", slots)
	}
}

func TestBlockAccessListValidation(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(*BlockAccessList)
		shouldErr bool
		errMsg    string
	}{
		{
			name: "valid access list",
			setup: func(bal *BlockAccessList) {
				addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
				bal.AddAccess(addr, nil, AccessBalance, 0, uint256.NewInt(100).Bytes())
			},
			shouldErr: false,
		},
		{
			name: "too many transactions",
			setup: func(bal *BlockAccessList) {
				// Create more than MAX_TXS transactions
				bal.Transactions = make([]map[common.Address]*AccessListForAddress, MAX_TXS+1)
			},
			shouldErr: true,
			errMsg:    "exceeds maximum",
		},
		{
			name: "too many accounts",
			setup: func(bal *BlockAccessList) {
				// Add more than MAX_ACCOUNTS addresses
				for i := 0; i < MAX_ACCOUNTS+1; i++ {
					// Create unique addresses by using different byte patterns
					addrBytes := make([]byte, 20)
					addrBytes[0] = byte(i >> 16)
					addrBytes[1] = byte(i >> 8)
					addrBytes[2] = byte(i)
					addr := common.BytesToAddress(addrBytes)
					bal.AddAccess(addr, nil, AccessBalance, 0, uint256.NewInt(1).Bytes())
				}
			},
			shouldErr: true,
			errMsg:    "exceeds maximum",
		},
		{
			name: "too many storage slots",
			setup: func(bal *BlockAccessList) {
				addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
				// Add more than MAX_SLOTS storage accesses
				for i := 0; i < MAX_SLOTS+1; i++ {
					// Create unique storage keys
					keyBytes := make([]byte, 32)
					keyBytes[0] = byte(i >> 24)
					keyBytes[1] = byte(i >> 16)
					keyBytes[2] = byte(i >> 8)
					keyBytes[3] = byte(i)
					key := common.BytesToHash(keyBytes)
					bal.AddAccess(addr, &key, AccessStorage, 0, []byte("value"))
				}
			},
			shouldErr: true,
			errMsg:    "exceeds maximum",
		},
		{
			name: "code too large",
			setup: func(bal *BlockAccessList) {
				addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
				// Add code larger than MAX_CODE_SIZE
				largeCode := make([]byte, MAX_CODE_SIZE+1)
				bal.AddAccess(addr, nil, AccessCode, 0, largeCode)
			},
			shouldErr: true,
			errMsg:    "exceeds maximum",
		},
		{
			name: "too many code changes",
			setup: func(bal *BlockAccessList) {
				addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
				// Add more than MAX_CODE_CHANGES code changes
				bal.AddAccess(addr, nil, AccessCode, 0, []byte{0x60})
				bal.AddAccess(addr, nil, AccessCode, 1, []byte{0x61})
			},
			shouldErr: true,
			errMsg:    "exceeds maximum",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bal := NewBlockAccessList(1)
			tt.setup(bal)

			err := bal.Validate()
			if tt.shouldErr {
				if err == nil {
					t.Fatalf("Expected error, got none")
				}
				if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Fatalf("Expected error to contain '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
			}
		})
	}
}

func TestBlockAccessListRLPEncoding(t *testing.T) {
	// Create a complex BlockAccessList for testing
	bal := NewBlockAccessList(2)

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	// Add various accesses
	bal.AddAccess(addr1, nil, AccessBalance, 0, uint256.NewInt(1000).Bytes())
	bal.AddAccess(addr1, nil, AccessNonce, 1, []byte{5})

	key := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	bal.AddAccess(addr1, &key, AccessStorage, 1, []byte("storage value"))

	bal.AddAccess(addr2, nil, AccessCode, 0, []byte{0x60, 0x80, 0x60, 0x40})

	// Test RLP encoding/decoding
	var buf bytes.Buffer
	err := rlp.Encode(&buf, bal)
	if err != nil {
		t.Fatalf("Failed to encode BlockAccessList: %v", err)
	}

	// Decode
	decoded := &BlockAccessList{}
	stream := rlp.NewStream(bytes.NewReader(buf.Bytes()), uint64(buf.Len()))
	err = decoded.DecodeRLP(stream)
	if err != nil {
		t.Fatalf("Failed to decode BlockAccessList: %v", err)
	}

	// Compare the original and decoded
	if !bal.Equals(decoded) {
		fmt.Printf("Original PreExecution: %d entries\n", len(bal.PreExecution))
		fmt.Printf("Decoded PreExecution: %d entries\n", len(decoded.PreExecution))
		fmt.Printf("Original Transactions: %d entries\n", len(bal.Transactions))
		fmt.Printf("Decoded Transactions: %d entries\n", len(decoded.Transactions))
		fmt.Printf("Original PostExecution: %d entries\n", len(bal.PostExecution))
		fmt.Printf("Decoded PostExecution: %d entries\n", len(decoded.PostExecution))

		// Check if addresses match
		origAddrs := bal.GetAllAddresses()
		decodedAddrs := decoded.GetAllAddresses()
		fmt.Printf("Original addresses: %d\n", len(origAddrs))
		fmt.Printf("Decoded addresses: %d\n", len(decodedAddrs))

		t.Fatalf("Decoded BlockAccessList does not match original")
	}
}

func TestAccessTrackerInterface(t *testing.T) {
	// Test NoopAccessTracker
	noop := NewNoopAccessTracker()

	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	key := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")

	err := noop.RecordAccess(addr, &key, AccessBalance, 0, uint256.NewInt(100).Bytes())
	if err != nil {
		t.Fatalf("NoopAccessTracker RecordAccess failed: %v", err)
	}

	bal := noop.GetBlockAccessList()
	if bal != nil {
		t.Fatalf("NoopAccessTracker should return nil BlockAccessList")
	}

	// Test MemoryAccessTracker
	mem := NewMemoryAccessTracker(2)

	err = mem.RecordAccess(addr, &key, AccessBalance, 0, uint256.NewInt(100).Bytes())
	if err != nil {
		t.Fatalf("MemoryAccessTracker RecordAccess failed: %v", err)
	}

	err = mem.RecordAccess(addr, nil, AccessNonce, 1, []byte{5})
	if err != nil {
		t.Fatalf("MemoryAccessTracker RecordAccess failed: %v", err)
	}

	bal = mem.GetBlockAccessList()
	if bal == nil {
		t.Fatalf("MemoryAccessTracker should return non-nil BlockAccessList")
	}

	// Test index management
	if mem.GetCurrentIndex() != 0 {
		t.Fatalf("Expected initial index 0, got %d", mem.GetCurrentIndex())
	}

	err = mem.SetCurrentIndex(1)
	if err != nil {
		t.Fatalf("SetCurrentIndex failed: %v", err)
	}

	if mem.GetCurrentIndex() != 1 {
		t.Fatalf("Expected index 1, got %d", mem.GetCurrentIndex())
	}

	// Test reset
	err = mem.Reset(3)
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	if mem.GetCurrentIndex() != 0 {
		t.Fatalf("Expected reset index 0, got %d", mem.GetCurrentIndex())
	}
}

func TestBlockAccessListConstants(t *testing.T) {
	// Test that constants are reasonable
	if MAX_TXS <= 0 {
		t.Fatalf("MAX_TXS should be positive")
	}

	if MAX_SLOTS <= 0 {
		t.Fatalf("MAX_SLOTS should be positive")
	}

	if MAX_ACCOUNTS <= 0 {
		t.Fatalf("MAX_ACCOUNTS should be positive")
	}

	if MAX_CODE_SIZE <= 0 {
		t.Fatalf("MAX_CODE_SIZE should be positive")
	}

	if MAX_CODE_CHANGES <= 0 {
		t.Fatalf("MAX_CODE_CHANGES should be positive")
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Equals method for BlockAccessList (add this to the implementation if needed)
func (bal *BlockAccessList) Equals(other *BlockAccessList) bool {
	if bal == nil || other == nil {
		return bal == other
	}

	// Compare pre-execution
	if len(bal.PreExecution) != len(other.PreExecution) {
		return false
	}
	for addr, accessList := range bal.PreExecution {
		otherAccessList, exists := other.PreExecution[addr]
		if !exists || !accessList.Equals(otherAccessList) {
			return false
		}
	}

	// Compare transactions
	if len(bal.Transactions) != len(other.Transactions) {
		return false
	}
	for i, txAccesses := range bal.Transactions {
		otherTxAccesses := other.Transactions[i]
		if len(txAccesses) != len(otherTxAccesses) {
			return false
		}
		for addr, accessList := range txAccesses {
			otherAccessList, exists := otherTxAccesses[addr]
			if !exists || !accessList.Equals(otherAccessList) {
				return false
			}
		}
	}

	// Compare post-execution
	if len(bal.PostExecution) != len(other.PostExecution) {
		return false
	}
	for addr, accessList := range bal.PostExecution {
		otherAccessList, exists := other.PostExecution[addr]
		if !exists || !accessList.Equals(otherAccessList) {
			return false
		}
	}

	return true
}

// Equals method for AccessListForAddress (add this to the implementation if needed)
func (al *AccessListForAddress) Equals(other *AccessListForAddress) bool {
	if al == nil || other == nil {
		return al == other
	}

	if al.Address != other.Address {
		return false
	}

	// Compare balance
	if (al.Balance == nil) != (other.Balance == nil) {
		return false
	}
	if al.Balance != nil && !bytes.Equal(al.Balance.Value, other.Balance.Value) {
		return false
	}

	// Compare nonce
	if (al.Nonce == nil) != (other.Nonce == nil) {
		return false
	}
	if al.Nonce != nil && !bytes.Equal(al.Nonce.Value, other.Nonce.Value) {
		return false
	}

	// Compare code
	if (al.Code == nil) != (other.Code == nil) {
		return false
	}
	if al.Code != nil && !bytes.Equal(al.Code.Value, other.Code.Value) {
		return false
	}

	// Compare code hash
	if (al.CodeHash == nil) != (other.CodeHash == nil) {
		return false
	}
	if al.CodeHash != nil && !bytes.Equal(al.CodeHash.Value, other.CodeHash.Value) {
		return false
	}

	// Compare storage
	if len(al.Storage) != len(other.Storage) {
		return false
	}
	for i, entry := range al.Storage {
		otherEntry := other.Storage[i]
		if entry.Address != otherEntry.Address {
			return false
		}
		if (entry.Key == nil) != (otherEntry.Key == nil) {
			return false
		}
		if entry.Key != nil && *entry.Key != *otherEntry.Key {
			return false
		}
		if entry.AccessType != otherEntry.AccessType {
			return false
		}
		if entry.Index != otherEntry.Index {
			return false
		}
		if !bytes.Equal(entry.Value, otherEntry.Value) {
			return false
		}
	}

	return true
}
