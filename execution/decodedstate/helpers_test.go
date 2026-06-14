package decodedstate

import (
	"encoding/binary"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/holiman/uint256"
)

// ---------------------------------------------------------------------------
// Test constants
// ---------------------------------------------------------------------------

var (
	addrA = common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	addrB = common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	addrC = common.HexToAddress("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")

	// Proxy / implementation addresses for DELEGATECALL tests
	addrProxy  = common.HexToAddress("0x1111111111111111111111111111111111111111")
	addrProxy2 = common.HexToAddress("0x2222222222222222222222222222222222222222")
	addrImpl   = common.HexToAddress("0x3333333333333333333333333333333333333333")
	addrImpl2  = common.HexToAddress("0x4444444444444444444444444444444444444444")
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// hashU64 encodes v as a big-endian uint64 in the last 8 bytes of a 32-byte hash.
func hashU64(v uint64) common.Hash {
	var h common.Hash
	binary.BigEndian.PutUint64(h[24:], v)
	return h
}

// u256 returns a uint256.Int for the given uint64.
func u256(v uint64) uint256.Int {
	return *uint256.NewInt(v)
}

// slotHash returns the common.Hash representation of a small slot index.
func slotHash(slot uint64) common.Hash {
	return hashU64(slot)
}

// keyHash returns the common.Hash representation of a key value.
func keyHash(key uint64) common.Hash {
	return hashU64(key)
}

// addrHash returns a 32-byte hash with the address left-padded to 32 bytes.
func addrHash(addr common.Address) common.Hash {
	var h common.Hash
	copy(h[12:], addr[:])
	return h
}

// keccak256 computes Keccak-256 and returns a common.Hash.
func keccak256(data []byte) common.Hash {
	return common.BytesToHash(crypto.Keccak256(data))
}

// mappingInput builds the 64-byte SHA3 input for mapping(K=>V):
// abi.encode(key, slot) = key(32) || slot(32).
func mappingInput(key, slot common.Hash) []byte {
	buf := make([]byte, 64)
	copy(buf[0:32], key[:])
	copy(buf[32:64], slot[:])
	return buf
}

// mappingLocation returns the storage slot for mapping[key] at variable slot.
func mappingLocation(key, slot common.Hash) common.Hash {
	return keccak256(mappingInput(key, slot))
}

// nestedMappingLocation returns the storage slot for mapping[key1][key2]
// where the outer mapping is at variable slot.
func nestedMappingLocation(key1, key2, slot common.Hash) common.Hash {
	inner := mappingLocation(key1, slot)
	return mappingLocation(key2, inner)
}

// arrayBaseInput builds the 32-byte SHA3 input for a dynamic array: abi.encode(slot).
func arrayBaseInput(slot common.Hash) []byte {
	return slot[:]
}

// arrayBase returns keccak256(slot) — the base storage location for array elements.
func arrayBase(slot common.Hash) common.Hash {
	return keccak256(arrayBaseInput(slot))
}

// arrayElementLocation returns base + index.
func arrayElementLocation(slot common.Hash, index uint64) common.Hash {
	base := arrayBase(slot)
	var loc uint256.Int
	loc.SetBytes(base[:])
	loc.Add(&loc, uint256.NewInt(index))
	return loc.Bytes32()
}

// enabledFullConfig returns a Config with decoding enabled in full mode.
func enabledFullConfig() Config {
	return Config{Enabled: true, FullMode: true}
}

// whitelistConfig returns a Config that only tracks the given addresses.
func whitelistConfig(addrs ...common.Address) Config {
	return Config{Enabled: true, FullMode: false, Whitelist: addrs}
}
