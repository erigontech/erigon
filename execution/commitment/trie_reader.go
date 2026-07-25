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

package commitment

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// TrieReader navigates the Patricia trie by hashed key without any mutable grid
// state. Each Lookup call starts from the root and descends independently.
type TrieReader struct {
	ctx           PatriciaContext
	accountKeyLen int16
	keccak        keccak.KeccakState
	hashBuf       [length.Hash]byte
}

// Cell accessor methods for external callers.

// AccountAddrLen returns the length of the account plain key.
func (c *cell) AccountAddrLen() int { return int(c.accountAddrLen) }

// StorageAddrLen returns the length of the storage plain key.
func (c *cell) StorageAddrLen() int { return int(c.storageAddrLen) }

// HashLen returns the length of the cell hash.
func (c *cell) HashLen() int { return int(c.hashLen) }

// AccountAddr returns the account plain key bytes (up to accountAddrLen).
func (c *cell) GetAccountAddr() []byte { return c.accountAddr[:c.accountAddrLen] }

// StorageAddr returns the storage plain key bytes (up to storageAddrLen).
func (c *cell) GetStorageAddr() []byte { return c.storageAddr[:c.storageAddrLen] }

// CellHash returns the cell hash bytes (up to hashLen).
func (c *cell) CellHash() []byte { return c.hash[:c.hashLen] }

// Extension returns the extension nibbles (up to hashedExtLen).
func (c *cell) Extension() []byte { return c.hashedExtension[:c.hashedExtLen] }

// NewTrieReader creates a TrieReader that uses ctx for branch lookups.
// accountKeyLen is the length of plain account keys (typically length.Addr = 20).
func NewTrieReader(ctx PatriciaContext, accountKeyLen int) *TrieReader {
	return &TrieReader{
		ctx:           ctx,
		accountKeyLen: int16(accountKeyLen),
		keccak:        keccak.NewFastKeccak(),
	}
}

// parseCellAt parses exactly one cell from branch cell data at position nibble.
// data is the cell data portion of branch data (after the 4-byte touchMap+afterMap header).
// bitmap is the afterMap indicating which nibbles are present.
func parseCellAt(data []byte, bitmap uint16, nibble int) (c cell, err error) {
	if bitmap&(uint16(1)<<nibble) == 0 {
		return c, fmt.Errorf("nibble %d not set in bitmap %016b", nibble, bitmap)
	}

	pos := 0
	for bitset := bitmap; bitset != 0; {
		bit := bitset & -bitset
		n := bits.TrailingZeros16(bit)

		if n == nibble {
			if pos >= len(data) {
				return c, fmt.Errorf("branch data truncated at nibble %d", nibble)
			}
			fieldBits := data[pos]
			pos++
			if _, err = c.fillFromFields(data, pos, cellFields(fieldBits)); err != nil {
				return c, fmt.Errorf("fillFromFields nibble %d: %w", nibble, err)
			}
			return c, nil
		}

		// Skip this cell's fields to advance pos.
		if pos >= len(data) {
			return c, fmt.Errorf("branch data truncated before nibble %d", nibble)
		}
		fieldBits := data[pos]
		pos = skipCellFields(data, pos+1, fieldBits)
		bitset ^= bit
	}

	return c, fmt.Errorf("nibble %d not reached in bitmap iteration", nibble)
}

// Lookup descends the trie for hashedKey and returns the leaf cell if found.
// hashedKey is a slice of nibbles (one nibble per byte, values 0-15).
// Returns (cell, true, nil) on hit, (zero cell, false, nil) on miss, or an error.
func (tr *TrieReader) Lookup(hashedKey []byte) (c cell, found bool, err error) {
	depth := 0
	for {
		if depth >= len(hashedKey) {
			return c, false, nil
		}

		nibble := int(hashedKey[depth])
		if nibble > 0x0f {
			return c, false, fmt.Errorf("invalid nibble %d at depth %d", nibble, depth)
		}

		prefix := nibbles.HexToCompact(hashedKey[:depth])
		branchData, _, err := tr.ctx.Branch(prefix)
		if err != nil {
			return c, false, fmt.Errorf("Branch at depth %d: %w", depth, err)
		}
		// Branch data layout: [touchMap(2)][afterMap(2)][cell_data...]
		if len(branchData) < 4 {
			return c, false, nil
		}

		afterMap := binary.BigEndian.Uint16(branchData[2:4])
		cellData := branchData[4:]

		if afterMap&(uint16(1)<<nibble) == 0 {
			return c, false, nil
		}

		c, err = parseCellAt(cellData, afterMap, nibble)
		if err != nil {
			return c, false, fmt.Errorf("parseCellAt depth %d nibble %x: %w", depth, nibble, err)
		}

		// Advance past the nibble we matched in the bitmap.
		depth++

		// Branch data may not include the full hashed extension for leaf cells.
		// Derive it from the plain key (accountAddr/storageAddr), just like
		// HexPatriciaHashed.unfoldBranchNode calls deriveHashedKeys.
		if (c.accountAddrLen > 0 || c.storageAddrLen > 0) && c.hashedExtLen == 0 {
			if err = c.deriveHashedKeys(int16(depth), tr.keccak, tr.accountKeyLen, tr.hashBuf[:]); err != nil {
				return c, false, fmt.Errorf("deriveHashedKeys at depth %d: %w", depth, err)
			}
		}

		// If the cell has an extension, verify it matches and advance depth.
		if c.hashedExtLen > 0 {
			extEnd := depth + int(c.hashedExtLen)
			if extEnd > len(hashedKey) {
				// Extension goes past the key — if the cell is a leaf, it's a match
				// for the key prefix (account found during storage-length lookup).
				if c.accountAddrLen > 0 || c.storageAddrLen > 0 {
					return c, true, nil
				}
				return c, false, nil
			}
			for i := int16(0); i < c.hashedExtLen; i++ {
				if hashedKey[depth+int(i)] != c.hashedExtension[i] {
					return c, false, nil
				}
			}
			depth = extEnd
		}

		// Leaf: account or storage address present and key fully consumed
		// or no deeper branching possible.
		if c.accountAddrLen > 0 || c.storageAddrLen > 0 {
			if c.hashLen == 0 || depth >= len(hashedKey) {
				return c, true, nil
			}
			// Cell has both an address and a hash with more key to consume:
			// this is an account node with a storage sub-trie. Continue.
			continue
		}

		// Branch hash: the cell references a deeper subtree, continue.
		if c.hashLen > 0 {
			continue
		}

		// No useful data in this cell — key not in trie.
		return c, false, nil
	}
}
