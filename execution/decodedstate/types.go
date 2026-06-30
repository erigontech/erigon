package decodedstate

import (
	"github.com/erigontech/erigon/common"
	"github.com/holiman/uint256"
)

// EntryType distinguishes mapping entries from array entries.
type EntryType uint8

const (
	MappingEntry       EntryType = iota // simple mapping(K => V)
	NestedMappingEntry                  // mapping(K1 => mapping(K2 => V))
	ArrayEntry                          // uint256[], bytes, string
)

// DecodedEntry represents a single decoded state entry — a mapping key/value
// or dynamic array element captured during EVM execution.
type DecodedEntry struct {
	Contract    common.Address // address whose storage is being modified
	MappingSlot common.Hash    // original slot position of the mapping/array variable
	Keys        []common.Hash  // original pre-hash keys; len>=1 for mappings, nil for arrays
	Value       common.Hash    // the 32-byte stored value
	EntryType   EntryType
	ArrayIndex  uint64 // index within the array (only when EntryType == ArrayEntry)
}

// ChangeSetEntry records the previous value of a decoded entry before overwrite.
type ChangeSetEntry struct {
	BlockNum    uint64
	Contract    common.Address
	MappingSlot common.Hash
	Keys        []common.Hash
	PrevValue   common.Hash
	NewValue    common.Hash
	EntryType   EntryType
	ArrayIndex  uint64
}

// Config controls decoded state collection behaviour.
type Config struct {
	Enabled    bool
	FullMode   bool             // true = track all contracts; false = whitelist only
	Whitelist  []common.Address // tracked contracts (used when FullMode == false)
	PruneAfter uint64           // change-set retention in blocks; 0 = keep all
}

// Collector intercepts EVM execution to build decoded state entries.
// It is wired into the EVM as a tracer / hook set.
type Collector interface {
	// OnKeccak256 is called when the SHA3/KECCAK256 opcode executes.
	// addr is the address whose storage is being operated on (respects DELEGATECALL).
	OnKeccak256(addr common.Address, input []byte, output common.Hash)

	// OnSStore is called when the SSTORE opcode executes.
	OnSStore(addr common.Address, location common.Hash, value uint256.Int)

	// OnEnterCall is called when a new EVM call frame begins.
	// storageAddr is the address whose storage will be affected.
	// codeAddr is the address of the executing code.
	// isDelegateCall is true for DELEGATECALL/CALLCODE.
	OnEnterCall(storageAddr common.Address, codeAddr common.Address, isDelegateCall bool)

	// OnExitCall is called when an EVM call frame returns.
	OnExitCall(reverted bool)

	// Entries returns the decoded entries collected for the current transaction.
	Entries() []DecodedEntry

	// Reset clears collector state between transactions.
	Reset()

	// SetConfig updates the collector configuration.
	SetConfig(cfg Config)
}

// Store persists decoded state and change sets, supporting historical queries.
type Store interface {
	// WriteEntries writes decoded entries for a given block.
	// For each entry: value!=0 → upsert, value==0 → delete.
	// Change sets are recorded automatically (old values).
	WriteEntries(blockNum uint64, entries []DecodedEntry) error

	// GetLatest returns the current value for a mapping key.
	GetLatest(contract common.Address, mappingSlot, key common.Hash) (common.Hash, bool, error)

	// GetAsOf returns the value for a mapping key at a historical block.
	GetAsOf(contract common.Address, mappingSlot, key common.Hash, blockNum uint64) (common.Hash, bool, error)

	// EnumerateKeys returns all known keys for a mapping at the latest block.
	EnumerateKeys(contract common.Address, mappingSlot common.Hash) ([]common.Hash, error)

	// EnumerateKeysAsOf returns all known keys for a mapping at a historical block.
	EnumerateKeysAsOf(contract common.Address, mappingSlot common.Hash, blockNum uint64) ([]common.Hash, error)

	// GetDecodedStorage returns the full decoded state for a contract.
	GetDecodedStorage(contract common.Address) (map[common.Hash][]DecodedEntry, error)

	// GetDecodedStorageAsOf returns the full decoded state at a historical block.
	GetDecodedStorageAsOf(contract common.Address, blockNum uint64) (map[common.Hash][]DecodedEntry, error)

	// DeleteContract removes all decoded state entries for a contract.
	DeleteContract(contract common.Address) error

	// Unwind reverts decoded state to the given block number.
	Unwind(toBlock uint64) error

	// Prune removes change sets older than the given block number.
	Prune(olderThan uint64) error

	// LatestBlock returns the highest block number written to the store.
	LatestBlock() (uint64, error)

	// Close releases resources.
	Close() error
}

// RPCHandler exposes decoded state via JSON-RPC methods.
type RPCHandler interface {
	// EnumerateMappingKeys implements erigon_enumerateMappingKeys.
	// blockNum==nil means latest.
	EnumerateMappingKeys(contract common.Address, mappingSlot common.Hash, blockNum *uint64) ([]common.Hash, error)

	// GetDecodedStorage implements erigon_getDecodedStorage.
	GetDecodedStorage(contract common.Address, blockNum *uint64) (map[common.Hash][]DecodedEntry, error)

	// GetMappingValue implements erigon_getMappingValue.
	GetMappingValue(contract common.Address, mappingSlot, key common.Hash, blockNum *uint64) (common.Hash, error)

	// EnumerateMappingKeysAtTx returns mapping keys as of post-state after canonical txNumber.
	EnumerateMappingKeysAtTx(contract common.Address, mappingSlot common.Hash, txNumber uint64) ([]common.Hash, error)

	// GetDecodedStorageAtTx returns full decoded state as of post-state after canonical txNumber.
	GetDecodedStorageAtTx(contract common.Address, txNumber uint64) (map[common.Hash][]DecodedEntry, error)

	// GetMappingValueAtTx returns a mapping value as of post-state after canonical txNumber.
	GetMappingValueAtTx(contract common.Address, mappingSlot, key common.Hash, txNumber uint64) (common.Hash, error)
}
