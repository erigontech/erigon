package decodedstate

import (
	"github.com/erigontech/erigon/common"
	"github.com/holiman/uint256"
)

// hashCandidate tracks a SHA3 preimage that may be a mapping or array access.
type hashCandidate struct {
	output common.Hash // the keccak256 result
	addr   common.Address

	// For 64-byte input (mapping access): key(32) || slot(32)
	isMapping bool
	key       common.Hash // the mapping key
	slot      common.Hash // the slot (could be a base slot or an intermediate hash)

	// For 32-byte input (array/bytes-like base): slot(32)
	isArrayBase bool
	arraySlot   common.Hash // the original slot for the array

	// For nested mappings: link to parent candidate chain
	parentKeys []common.Hash // accumulated keys from parent mapping candidates
	baseSlot   common.Hash   // the original base mapping slot (resolved through chain)
}

// callFrame tracks decoded state candidates and entries for a single EVM call frame.
type callFrame struct {
	candidates map[common.Hash]*hashCandidate // output hash → candidate
	entries    []DecodedEntry                 // decoded entries produced in this frame

	// Array base tracking: base hash → (arraySlot, base uint256)
	arrayBases map[common.Hash]arrayBaseInfo
}

type arrayBaseInfo struct {
	slot common.Hash    // the original storage slot of the array variable
	base uint256.Int    // the numeric base value (keccak256(slot))
	addr common.Address // the storage owner address
}

// collector implements the Collector interface.
type collector struct {
	cfg          Config
	whitelistMap map[common.Address]struct{} // precompiled whitelist index
	frames       []*callFrame                // call frame stack; frames[0] is the root

	// Global candidate map across all frames (for cross-frame lookups)
	allCandidates map[common.Hash]*hashCandidate
	allArrayBases map[common.Hash]arrayBaseInfo

	// Committed entries (survived all revert checks)
	committed []DecodedEntry
}

// NewCollector returns a new Collector configured with cfg.
func NewCollector(cfg Config) Collector {
	wm := make(map[common.Address]struct{}, len(cfg.Whitelist))
	for _, a := range cfg.Whitelist {
		wm[a] = struct{}{}
	}
	c := &collector{
		cfg:           cfg,
		whitelistMap:  wm,
		allCandidates: make(map[common.Hash]*hashCandidate),
		allArrayBases: make(map[common.Hash]arrayBaseInfo),
	}
	return c
}

func (c *collector) isTracked(addr common.Address) bool {
	if !c.cfg.Enabled {
		return false
	}
	if c.cfg.FullMode {
		return true
	}
	_, ok := c.whitelistMap[addr]
	return ok
}

func (c *collector) currentFrame() *callFrame {
	if len(c.frames) == 0 {
		return nil
	}
	return c.frames[len(c.frames)-1]
}

func (c *collector) ensureFrame() *callFrame {
	if len(c.frames) == 0 {
		// No explicit OnEnterCall was made; create an implicit root frame.
		f := &callFrame{
			candidates: make(map[common.Hash]*hashCandidate),
			arrayBases: make(map[common.Hash]arrayBaseInfo),
		}
		c.frames = append(c.frames, f)
		return f
	}
	return c.frames[len(c.frames)-1]
}

func (c *collector) OnKeccak256(addr common.Address, input []byte, output common.Hash) {
	if !c.isTracked(addr) {
		return
	}

	frame := c.ensureFrame()

	switch len(input) {
	case 64:
		// Mapping candidate: key(32) || slot(32)
		var key, slot common.Hash
		copy(key[:], input[0:32])
		copy(slot[:], input[32:64])

		cand := &hashCandidate{
			output:    output,
			addr:      addr,
			isMapping: true,
			key:       key,
			slot:      slot,
		}

		// Check if slot is itself an approved hash (nested mapping)
		if parent, ok := c.allCandidates[slot]; ok && parent.isMapping {
			// This is a nested mapping: keccak256(key2 || intermediateHash)
			cand.parentKeys = make([]common.Hash, len(parent.parentKeys)+1)
			copy(cand.parentKeys, parent.parentKeys)
			cand.parentKeys[len(parent.parentKeys)] = parent.key
			// Inherit the resolved base slot from the parent chain.
			// If parent has parentKeys, it already resolved baseSlot.
			// If parent has no parentKeys (first level), parent.slot is the base.
			if len(parent.parentKeys) > 0 {
				cand.baseSlot = parent.baseSlot
			} else {
				cand.baseSlot = parent.slot
			}
		} else {
			// Simple mapping: slot is the base storage slot
			cand.baseSlot = slot
		}

		frame.candidates[output] = cand
		c.allCandidates[output] = cand

	case 32:
		// Array/bytes-like base candidate: keccak256(slot)
		var slot common.Hash
		copy(slot[:], input[0:32])

		cand := &hashCandidate{
			output:      output,
			addr:        addr,
			isArrayBase: true,
			arraySlot:   slot,
		}

		frame.candidates[output] = cand
		c.allCandidates[output] = cand

		// Store the base info for offset matching
		var base uint256.Int
		base.SetBytes(output[:])
		info := arrayBaseInfo{slot: slot, base: base, addr: addr}
		frame.arrayBases[output] = info
		c.allArrayBases[output] = info
	}
	// Other input lengths are ignored (not mapping or array access patterns)
}

func (c *collector) OnSStore(addr common.Address, location common.Hash, value uint256.Int) {
	if !c.isTracked(addr) {
		return
	}

	frame := c.ensureFrame()

	// Check 1: Is this location an exact mapping hash?
	if cand, ok := c.allCandidates[location]; ok && cand.isMapping && cand.addr == addr {
		var val common.Hash
		val = value.Bytes32()

		var entryType EntryType
		var keys []common.Hash

		if len(cand.parentKeys) > 0 {
			// Nested mapping
			entryType = NestedMappingEntry
			keys = make([]common.Hash, len(cand.parentKeys)+1)
			copy(keys, cand.parentKeys)
			keys[len(cand.parentKeys)] = cand.key
		} else {
			// Simple mapping
			entryType = MappingEntry
			keys = []common.Hash{cand.key}
		}

		entry := DecodedEntry{
			Contract:    addr,
			MappingSlot: cand.baseSlot,
			Keys:        keys,
			Value:       val,
			EntryType:   entryType,
		}
		frame.entries = append(frame.entries, entry)
		return
	}

	// Check 2: Is this location an exact array base hash (offset 0)?
	if info, ok := c.allArrayBases[location]; ok && info.addr == addr {
		var val common.Hash
		val = value.Bytes32()

		entry := DecodedEntry{
			Contract:    addr,
			MappingSlot: info.slot,
			Keys:        nil,
			Value:       val,
			EntryType:   ArrayEntry,
			ArrayIndex:  0,
		}
		frame.entries = append(frame.entries, entry)
		return
	}

	// Check 3: Is this location base + offset for an array?
	var loc uint256.Int
	loc.SetBytes(location[:])

	for _, info := range c.allArrayBases {
		if info.addr != addr {
			continue
		}
		if loc.Cmp(&info.base) >= 0 {
			var offset uint256.Int
			offset.Sub(&loc, &info.base)
			// Reasonable offset check (within a practical array range)
			if offset.IsUint64() {
				idx := offset.Uint64()
				// Sanity: offset should be reasonable (< 2^32 for practical arrays)
				if idx < 1<<32 {
					var val common.Hash
					val = value.Bytes32()

					entry := DecodedEntry{
						Contract:    addr,
						MappingSlot: info.slot,
						Keys:        nil,
						Value:       val,
						EntryType:   ArrayEntry,
						ArrayIndex:  idx,
					}
					frame.entries = append(frame.entries, entry)
					return
				}
			}
		}
	}

	// No match — this SSTORE is not a decoded mapping/array access
}

func (c *collector) OnEnterCall(storageAddr, codeAddr common.Address, isDelegateCall bool) {
	f := &callFrame{
		candidates: make(map[common.Hash]*hashCandidate),
		arrayBases: make(map[common.Hash]arrayBaseInfo),
	}
	c.frames = append(c.frames, f)
}

func (c *collector) OnExitCall(reverted bool) {
	if len(c.frames) == 0 {
		return
	}

	frame := c.frames[len(c.frames)-1]
	c.frames = c.frames[:len(c.frames)-1]

	if reverted {
		// Discard this frame's candidates and entries
		for hash := range frame.candidates {
			delete(c.allCandidates, hash)
		}
		for hash := range frame.arrayBases {
			delete(c.allArrayBases, hash)
		}
		return
	}

	// Merge into parent frame or committed
	if len(c.frames) > 0 {
		parent := c.frames[len(c.frames)-1]
		parent.entries = append(parent.entries, frame.entries...)
		// Candidates are already in allCandidates, just merge into parent frame
		for h, cand := range frame.candidates {
			parent.candidates[h] = cand
		}
		for h, info := range frame.arrayBases {
			parent.arrayBases[h] = info
		}
	} else {
		// No parent frame — these are committed (root frame exited = end of transaction)
		c.committed = append(c.committed, frame.entries...)
		// Clear candidates so they don't leak to the next transaction
		c.allCandidates = make(map[common.Hash]*hashCandidate)
		c.allArrayBases = make(map[common.Hash]arrayBaseInfo)
	}
}

func (c *collector) Entries() []DecodedEntry {
	// Collect from all remaining frames (no explicit OnExitCall for root)
	var all []DecodedEntry
	all = append(all, c.committed...)
	for _, f := range c.frames {
		if f == nil {
			continue
		}
		all = append(all, f.entries...)
	}

	// Deduplicate: for same (contract, slot, keys), keep last value
	type entryKey struct {
		contract common.Address
		slot     common.Hash
		keyStr   string
		idx      uint64
		isArray  bool
	}

	seen := make(map[entryKey]int) // key → index in result
	var result []DecodedEntry

	for _, e := range all {
		var ks string
		for _, k := range e.Keys {
			ks += string(k[:])
		}
		ek := entryKey{
			contract: e.Contract,
			slot:     e.MappingSlot,
			keyStr:   ks,
			idx:      e.ArrayIndex,
			isArray:  e.EntryType == ArrayEntry,
		}
		if idx, exists := seen[ek]; exists {
			result[idx] = e // overwrite with latest
		} else {
			seen[ek] = len(result)
			result = append(result, e)
		}
	}

	return result
}

func (c *collector) Reset() {
	c.frames = nil
	c.allCandidates = make(map[common.Hash]*hashCandidate)
	c.allArrayBases = make(map[common.Hash]arrayBaseInfo)
	c.committed = nil
}

func (c *collector) SetConfig(cfg Config) {
	c.cfg = cfg
}
