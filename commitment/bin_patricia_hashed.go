/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package commitment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"

	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/rlp"
)

const (
	maxKeySize  = 512
	keyHalfSize = maxKeySize / 2
	maxChild    = 2
)

// BinHashed implements commitment based on patricia merkle tree with radix 16,
// with keys pre-hashed by keccak256
type BinHashed struct {
	root BinCell // Root cell of the tree
	// Rows of the grid correspond to the level of depth in the patricia tree
	// Columns of the grid correspond to pointers to the nodes further from the root
	grid [maxKeySize][maxChild]BinCell // First 64 rows of this grid are for account trie, and next 64 rows are for storage trie
	// How many rows (starting from row 0) are currently active and have corresponding selected columns
	// Last active row does not have selected column
	activeRows int
	// Length of the key that reflects current positioning of the grid. It maybe larger than number of active rows,
	// if a account leaf cell represents multiple nibbles in the key
	currentKeyLen int
	currentKey    [maxKeySize]byte // For each row indicates which column is currently selected
	depths        [maxKeySize]int  // For each row, the depth of cells in that row
	rootChecked   bool             // Set to false if it is not known whether the root is empty, set to true if it is checked
	rootTouched   bool
	rootPresent   bool
	branchBefore  [maxKeySize]bool   // For each row, whether there was a branch node in the database loaded in unfold
	touchMap      [maxKeySize]uint16 // For each row, bitmap of cells that were either present before modification, or modified or deleted
	afterMap      [maxKeySize]uint16 // For each row, bitmap of cells that were present after modification
	// Function used to load branch node and fill up the cells
	// For each cell, it sets the cell type, clears the modified flag, fills the hash,
	// and for the extension, account, and leaf type, the `l` and `k`
	branchFn func(prefix []byte) ([]byte, error)
	// Function used to fetch account with given plain key
	accountFn func(plainKey []byte, cell *BinCell) error
	// Function used to fetch account with given plain key
	storageFn       func(plainKey []byte, cell *BinCell) error
	keccak          keccakState
	keccak2         keccakState
	accountKeyLen   int
	trace           bool
	numBuf          [binary.MaxVarintLen64]byte
	byteArrayWriter ByteArrayWriter
}

func NewBinHashed(accountKeyLen int,
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *BinCell) error,
	storageFn func(plainKey []byte, cell *BinCell) error,
) *BinHashed {
	return &BinHashed{
		keccak:        sha3.NewLegacyKeccak256().(keccakState),
		keccak2:       sha3.NewLegacyKeccak256().(keccakState),
		accountKeyLen: accountKeyLen,
		branchFn:      branchFn,
		accountFn:     accountFn,
		storageFn:     storageFn,
	}
}

func (hph *BinHashed) ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (map[string][]byte, error) {
	branchNodeUpdates := make(map[string][]byte)
	for i, hk := range hashedKeys {
		hashedKey := hexToBin(hk)
		plainKey := plainKeys[i]
		update := updates[i]
		if hph.trace {
			fmt.Printf("plainKey=[%x], hashedKey=[%x], currentKey=[%x], update=%s\n",
				plainKey, hashedKey, hph.currentKey[:hph.currentKeyLen], update)
		}
		// Keep folding until the currentKey is the prefix of the key we modify
		for hph.needFolding(hashedKey) {
			if branchData, updateKey, err := hph.fold(); err != nil {
				return nil, fmt.Errorf("fold: %w", err)
			} else if branchData != nil {
				branchNodeUpdates[string(updateKey)] = branchData
			}
		}
		// Now unfold until we step on an empty cell
		for unfolding := hph.needUnfolding(hashedKey); unfolding > 0; unfolding = hph.needUnfolding(hashedKey) {
			if err := hph.unfold(hashedKey, unfolding); err != nil {
				return nil, fmt.Errorf("unfold: %w", err)
			}
		}
		// Update the cell
		if update.Flags == DELETE_UPDATE {
			hph.deleteCell(hashedKey)
		} else {
			if update.Flags&BALANCE_UPDATE != 0 {
				hph.updateBalance(plainKey, hashedKey, &update.Balance)
			}
			if update.Flags&NONCE_UPDATE != 0 {
				hph.updateNonce(plainKey, hashedKey, update.Nonce)
			}
			if update.Flags&CODE_UPDATE != 0 {
				hph.updateCode(plainKey, hashedKey, update.CodeHashOrStorage[:])
			}
			if update.Flags&STORAGE_UPDATE != 0 {
				hph.updateStorage(plainKey, hashedKey, update.CodeHashOrStorage[:update.ValLength])
			}
		}
	}
	// Folding everything up to the root
	for hph.activeRows > 0 {
		if branchData, updateKey, err := hph.fold(); err != nil {
			return nil, fmt.Errorf("final fold: %w", err)
		} else if branchData != nil {
			branchNodeUpdates[string(updateKey)] = branchData
		}
	}
	return branchNodeUpdates, nil
}

func (hph *BinHashed) Variant() TrieVariant {
	return VariantReducedHexPatriciaTrie
}

func (hph *BinHashed) RootHash() ([]byte, error) {
	hash, err := hph.computeCellHash(&hph.root, 0, nil)
	if err != nil {
		return nil, err
	}
	return hash[1:], nil // first byte is 128+hash_len
}

func (hph *BinHashed) SetTrace(trace bool) {
	hph.trace = trace
}

// Reset allows BinHashed instance to be reused for the new commitment calculation
func (hph *BinHashed) Reset() {
	hph.rootChecked = false
	hph.root.hl = 0
	hph.root.downHashedLen = 0
	hph.root.apl = 0
	hph.root.spl = 0
	hph.root.extLen = 0
	copy(hph.root.CodeHash[:], EmptyCodeHash)
	hph.root.StorageLen = 0
	hph.root.Balance.Clear()
	hph.root.Nonce = 0
	hph.rootTouched = false
	hph.rootPresent = true
}

func wrapAccountStorageFn(fn func([]byte, *Cell) error) func(pk []byte, bc *BinCell) error {
	return func(pk []byte, bc *BinCell) error {
		cl := &Cell{}
		cl.Balance = *bc.Balance.Clone()
		cl.Nonce = bc.Nonce
		cl.StorageLen = bc.StorageLen
		cl.apl = bc.apl
		cl.spl = bc.spl
		cl.hl = bc.hl
		copy(cl.apk[:], bc.apk[:])
		copy(cl.spk[:], bc.spk[:])
		copy(cl.h[:], bc.h[:])
		copy(cl.extension[:], bc.extension[:])
		copy(cl.downHashedKey[:], bc.downHashedKey[:])
		copy(cl.CodeHash[:], bc.CodeHash[:])
		copy(cl.Storage[:], bc.Storage[:])

		if err := fn(pk, cl); err != nil {
			return err
		}

		bc.Balance = *cl.Balance.Clone()
		bc.Nonce = cl.Nonce
		bc.StorageLen = cl.StorageLen
		bc.apl = cl.apl
		bc.spl = cl.spl
		bc.hl = cl.hl
		copy(bc.apk[:], cl.apk[:])
		copy(bc.spk[:], cl.spk[:])
		copy(bc.h[:], cl.h[:])
		copy(bc.extension[:], cl.extension[:])
		copy(bc.downHashedKey[:], cl.downHashedKey[:])
		copy(bc.CodeHash[:], cl.CodeHash[:])
		copy(bc.Storage[:], cl.Storage[:])
		return nil
	}
}

func (hph *BinHashed) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) error,
	storageFn func(plainKey []byte, cell *Cell) error,
) {
	hph.branchFn = branchFn
	hph.accountFn = wrapAccountStorageFn(accountFn)
	hph.storageFn = wrapAccountStorageFn(storageFn)
}

func (hph *BinHashed) completeLeafHash(buf []byte, keyPrefix []byte, kp, kl, compactLen int, key []byte, compact0 byte, ni int, val rlp.RlpSerializable, singleton bool) ([]byte, error) {
	totalLen := kp + kl + val.DoubleRLPLen()
	var lenPrefix [4]byte
	pt := rlp.GenerateStructLen(lenPrefix[:], totalLen)
	embedded := !singleton && totalLen+pt < length.Hash
	var writer io.Writer
	if embedded {
		hph.byteArrayWriter.Setup(buf)
		writer = &hph.byteArrayWriter
	} else {
		hph.keccak.Reset()
		writer = hph.keccak
	}
	if _, err := writer.Write(lenPrefix[:pt]); err != nil {
		return nil, err
	}
	if _, err := writer.Write(keyPrefix[:kp]); err != nil {
		return nil, err
	}
	var b [1]byte
	b[0] = compact0
	if _, err := writer.Write(b[:]); err != nil {
		return nil, err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := writer.Write(b[:]); err != nil {
			return nil, err
		}
		ni += 2
	}
	var prefixBuf [8]byte
	if err := val.ToDoubleRLP(writer, prefixBuf[:]); err != nil {
		return nil, err
	}
	if embedded {
		buf = hph.byteArrayWriter.buf
	} else {
		var hashBuf [33]byte
		hashBuf[0] = 0x80 + length.Hash
		if _, err := hph.keccak.Read(hashBuf[1:]); err != nil {
			return nil, err
		}
		buf = append(buf, hashBuf[:]...)
	}
	return buf, nil
}

func (hph *BinHashed) leafHashWithKeyVal(buf []byte, key []byte, val rlp.RlpSerializableBytes, singleton bool) ([]byte, error) {
	// Compute the total length of binary representation
	var kp, kl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	compactLen = (len(key)-1)/2 + 1
	if len(key)&1 == 0 {
		compact0 = 0x30 + key[0] // Odd: (3<<4) + first nibble
		ni = 1
	} else {
		compact0 = 0x20
	}
	var keyPrefix [1]byte
	if compactLen > 1 {
		keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	buf, err := hph.completeLeafHash(buf, keyPrefix[:], kp, kl, compactLen, key, compact0, ni, val, singleton)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (hph *BinHashed) accountLeafHashWithKey(buf, key []byte, val rlp.RlpSerializable) ([]byte, error) {
	// Compute the total length of binary representation
	var kp, kl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 48 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		} else {
			compact0 = 32
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 16 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		}
	}
	var keyPrefix [1]byte
	if compactLen > 1 {
		keyPrefix[0] = byte(128 + compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	var err error
	if buf, err = hph.completeLeafHash(buf, keyPrefix[:], kp, kl, compactLen, key, compact0, ni, val, true); err != nil {
		return nil, err
	}
	return buf, nil
}

func (hph *BinHashed) extensionHash(buf []byte, key []byte, hash []byte) ([]byte, error) {
	// Compute the total length of binary representation
	var kp, kl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 0x30 + key[0] // Odd: (3<<4) + first nibble
			ni = 1
		} else {
			compact0 = 0x20
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 0x10 + key[0] // Odd: (1<<4) + first nibble
			ni = 1
		}
	}
	var keyPrefix [1]byte
	if compactLen > 1 {
		keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	totalLen := kp + kl + 33
	var lenPrefix [4]byte
	pt := rlp.GenerateStructLen(lenPrefix[:], totalLen)
	hph.keccak.Reset()
	if _, err := hph.keccak.Write(lenPrefix[:pt]); err != nil {
		return nil, err
	}
	if _, err := hph.keccak.Write(keyPrefix[:kp]); err != nil {
		return nil, err
	}
	var b [1]byte
	b[0] = compact0
	if _, err := hph.keccak.Write(b[:]); err != nil {
		return nil, err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := hph.keccak.Write(b[:]); err != nil {
			return nil, err
		}
		ni += 2
	}
	b[0] = 0x80 + length.Hash
	if _, err := hph.keccak.Write(b[:]); err != nil {
		return nil, err
	}
	if _, err := hph.keccak.Write(hash); err != nil {
		return nil, err
	}
	// Replace previous hash with the new one
	var hashBuf [33]byte
	if _, err := hph.keccak.Read(hashBuf[:length.Hash]); err != nil {
		return nil, err
	}
	buf = append(buf, hashBuf[:length.Hash]...)
	return buf, nil
}

func (hph *BinHashed) computeCellHashLen(cell *BinCell, depth int) int {
	if cell.spl > 0 && depth >= keyHalfSize {
		keyLen := maxKeySize - depth + 1 // Length of hex key with terminator character
		var kp, kl int
		compactLen := (keyLen-1)/2 + 1
		if compactLen > 1 {
			kp = 1
			kl = compactLen
		} else {
			kl = 1
		}
		val := rlp.RlpSerializableBytes(cell.Storage[:cell.StorageLen])
		totalLen := kp + kl + val.DoubleRLPLen()
		var lenPrefix [4]byte
		pt := rlp.GenerateStructLen(lenPrefix[:], totalLen)
		if totalLen+pt < length.Hash {
			return totalLen + pt
		}
	}
	return length.Hash + 1
}

func (hph *BinHashed) computeCellHash(cell *BinCell, depth int, buf []byte) ([]byte, error) {
	var err error
	var storageRootHash []byte
	if cell.spl > 0 {
		var hashedKeyOffset int
		if depth >= keyHalfSize {
			hashedKeyOffset = depth - keyHalfSize
		}
		singleton := depth <= keyHalfSize
		if err := hashKey(hph.keccak, cell.spk[hph.accountKeyLen:cell.spl], cell.downHashedKey[:], hashedKeyOffset); err != nil {
			return nil, err
		}
		cell.downHashedKey[keyHalfSize-hashedKeyOffset] = 16 // Add terminator
		if singleton {
			if hph.trace {
				fmt.Printf("leafHashWithKeyVal(singleton) for [%x]=>[%x]\n", cell.downHashedKey[:keyHalfSize-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
			}
			if storageRootHash, err = hph.leafHashWithKeyVal(nil, cell.downHashedKey[:keyHalfSize-hashedKeyOffset+1], rlp.RlpSerializableBytes(cell.Storage[:cell.StorageLen]), true); err != nil {
				return nil, err
			}
			storageRootHash = storageRootHash[1:]
		} else {
			if hph.trace {
				fmt.Printf("leafHashWithKeyVal for [%x]=>[%x]\n", cell.downHashedKey[:keyHalfSize-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
			}
			if buf, err = hph.leafHashWithKeyVal(buf, cell.downHashedKey[:keyHalfSize-hashedKeyOffset+1], rlp.RlpSerializableBytes(cell.Storage[:cell.StorageLen]), false); err != nil {
				return nil, err
			}
			return buf, nil
		}
	}
	if cell.apl > 0 {
		if err := hashKey(hph.keccak, cell.apk[:cell.apl], cell.downHashedKey[:], depth); err != nil {
			return nil, err
		}
		cell.downHashedKey[keyHalfSize-depth] = 16 // Add terminator
		if storageRootHash == nil {
			if cell.extLen > 0 {
				// Extension
				if cell.hl > 0 {
					if hph.trace {
						fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.h[:cell.hl])
					}
					if storageRootHash, err = hph.extensionHash(nil, cell.extension[:cell.extLen], cell.h[:cell.hl]); err != nil {
						return nil, err
					}
				} else {
					return nil, fmt.Errorf("computeCellHash extension without hash")
				}
			} else if cell.hl > 0 {
				storageRootHash = cell.h[:cell.hl]
			} else {
				storageRootHash = EmptyRootHash
			}
		}
		var valBuf [128]byte
		valLen := cell.accountForHashing(valBuf[:], storageRootHash)
		if hph.trace {
			fmt.Printf("accountLeafHashWithKey for [%x]=>[%x]\n", cell.downHashedKey[:keyHalfSize+1-depth], valBuf[:valLen])
		}
		if buf, err = hph.accountLeafHashWithKey(buf, cell.downHashedKey[:keyHalfSize+1-depth], rlp.RlpEncodedBytes(valBuf[:valLen])); err != nil {
			return nil, err
		}
		return buf, nil
	}
	buf = append(buf, 0x80+32)
	if cell.extLen > 0 {
		// Extension
		if cell.hl > 0 {
			if hph.trace {
				fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.h[:cell.hl])
			}
			if buf, err = hph.extensionHash(buf, cell.extension[:cell.extLen], cell.h[:cell.hl]); err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("computeCellHash extension without hash")
		}
	} else if cell.hl > 0 {
		buf = append(buf, cell.h[:cell.hl]...)
	} else {
		buf = append(buf, EmptyRootHash...)
	}
	return buf, nil
}

func (hph *BinHashed) needUnfolding(hashedKey []byte) int {
	var cell *BinCell
	var depth int
	if hph.activeRows == 0 {
		if hph.trace {
			fmt.Printf("needUnfolding root, rootChecked = %t\n", hph.rootChecked)
		}
		cell = &hph.root
		if cell.downHashedLen == 0 && cell.hl == 0 && !hph.rootChecked {
			// Need to attempt to unfold the root
			return 1
		}
	} else {
		col := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[hph.activeRows-1][col]
		depth = hph.depths[hph.activeRows-1]
		if hph.trace {
			fmt.Printf("needUnfolding cell (%d, %x), currentKey=[%x], depth=%d, cell.h=[%x]\n", hph.activeRows-1, col, hph.currentKey[:hph.currentKeyLen], depth, cell.h[:cell.hl])
		}
	}
	if len(hashedKey) <= depth {
		return 0
	}
	if cell.downHashedLen == 0 {
		if cell.hl == 0 {
			// cell is empty, no need to unfold further
			return 0
		} else {
			// unfold branch node
			return 1
		}
	}
	cpl := commonPrefixLen(hashedKey[depth:], cell.downHashedKey[:cell.downHashedLen-1])
	if hph.trace {
		fmt.Printf("cpl=%d, cell.downHashedKey=[%x], depth=%d, hashedKey[depth:]=[%x]\n", cpl, cell.downHashedKey[:cell.downHashedLen], depth, hashedKey[depth:])
	}
	unfolding := cpl + 1
	if depth < keyHalfSize && depth+unfolding > keyHalfSize {
		// This is to make sure that unfolding always breaks at the level where storage subtrees start
		unfolding = keyHalfSize - depth
		if hph.trace {
			fmt.Printf("adjusted unfolding=%d\n", unfolding)
		}
	}
	return unfolding
}

func (hph *BinHashed) unfoldBranchNode(row int, deleted bool, depth int) error {
	branchData, err := hph.branchFn(binToCompact(hph.currentKey[:hph.currentKeyLen]))
	if err != nil {
		return err
	}
	if !hph.rootChecked && hph.currentKeyLen == 0 && len(branchData) == 0 {
		// Special case - empty or deleted root
		hph.rootChecked = true
		return nil
	}
	hph.branchBefore[row] = true
	//fmt.Printf("unfoldBranchNode [%x]=>[%x]\n", hph.currentKey[:hph.currentKeyLen], branchData)
	bitmap := binary.BigEndian.Uint16(branchData[0:])
	pos := 2
	if deleted {
		// All cells come as deleted (touched but not present after)
		hph.afterMap[row] = 0
		hph.touchMap[row] = bitmap
	} else {
		hph.afterMap[row] = bitmap
		hph.touchMap[row] = 0
	}
	//fmt.Printf("unfoldBranchNode [unfoldBranchNode%x], afterMap = [%016b], touchMap = [%016b]\n", branchData, hph.afterMap[row], hph.touchMap[row])
	// Loop iterating over the set bits of modMask
	for bitset, j := bitmap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &hph.grid[row][nibble]
		fieldBits := branchData[pos]
		pos++
		var err error
		if pos, err = cell.fillFromFields(branchData, pos, PartFlags(fieldBits)); err != nil {
			return fmt.Errorf("prefix [%x], branchData[%x]: %w", hph.currentKey[:hph.currentKeyLen], branchData, err)
		}
		if hph.trace {
			fmt.Printf("cell (%d, %x) depth=%d, hash=[%x], a=[%x], s=[%x], ex=[%x]\n", row, nibble, depth, cell.h[:cell.hl], cell.apk[:cell.apl], cell.spk[:cell.spl], cell.extension[:cell.extLen])
		}
		if cell.apl > 0 {
			hph.accountFn(cell.apk[:cell.apl], cell)
			if hph.trace {
				fmt.Printf("accountFn[%x] return balance=%d, nonce=%d\n", cell.apk[:cell.apl], &cell.Balance, cell.Nonce)
			}
		}
		if cell.spl > 0 {
			hph.storageFn(cell.spk[:cell.spl], cell)
		}
		if err = cell.deriveHashedKeys(depth, hph.keccak, hph.accountKeyLen); err != nil {
			return err
		}
		bitset ^= bit
	}
	return nil
}

func (hph *BinHashed) unfold(hashedKey []byte, unfolding int) error {
	if hph.trace {
		fmt.Printf("unfold %d: activeRows: %d\n", unfolding, hph.activeRows)
	}
	var upCell *BinCell
	var touched, present bool
	var col byte
	var upDepth, depth int
	if hph.activeRows == 0 {
		if hph.rootChecked && hph.root.hl == 0 && hph.root.downHashedLen == 0 {
			// No unfolding for empty root
			return nil
		}
		upCell = &hph.root
		touched = hph.rootTouched
		present = hph.rootPresent
		if hph.trace {
			fmt.Printf("root, touched %t, present %t\n", touched, present)
		}
	} else {
		upDepth = hph.depths[hph.activeRows-1]
		col = hashedKey[upDepth-1]
		upCell = &hph.grid[hph.activeRows-1][col]
		touched = hph.touchMap[hph.activeRows-1]&(uint16(1)<<col) != 0
		present = hph.afterMap[hph.activeRows-1]&(uint16(1)<<col) != 0
		if hph.trace {
			fmt.Printf("upCell (%d, %x), touched %t, present %t\n", hph.activeRows-1, col, touched, present)
		}
		hph.currentKey[hph.currentKeyLen] = col
		hph.currentKeyLen++
	}
	row := hph.activeRows
	for i := 0; i < maxChild; i++ {
		hph.grid[row][i].fillEmpty()
	}
	hph.touchMap[row] = 0
	hph.afterMap[row] = 0
	hph.branchBefore[row] = false
	if upCell.downHashedLen == 0 {
		depth = upDepth + 1
		if err := hph.unfoldBranchNode(row, touched && !present /* deleted */, depth); err != nil {
			return err
		}
	} else if upCell.downHashedLen >= unfolding {
		depth = upDepth + unfolding
		nibble := upCell.downHashedKey[unfolding-1]
		if touched {
			hph.touchMap[row] = uint16(1) << nibble
		}
		if present {
			hph.afterMap[row] = uint16(1) << nibble
		}
		cell := &hph.grid[row][nibble]
		cell.fillFromUpperCell(upCell, depth, unfolding)
		if hph.trace {
			fmt.Printf("cell (%d, %x) depth=%d, a=[%x], upa=[%x]\n", row, nibble, depth, cell.apk[:cell.apl], upCell.apk[:upCell.apl])
		}
		if row >= keyHalfSize {
			cell.apl = 0
		}
		if unfolding > 1 {
			copy(hph.currentKey[hph.currentKeyLen:], upCell.downHashedKey[:unfolding-1])
		}
		hph.currentKeyLen += unfolding - 1
	} else {
		// upCell.downHashedLen < unfolding
		depth = upDepth + upCell.downHashedLen
		nibble := upCell.downHashedKey[upCell.downHashedLen-1]
		if touched {
			hph.touchMap[row] = uint16(1) << nibble
		}
		if present {
			hph.afterMap[row] = uint16(1) << nibble
		}
		cell := &hph.grid[row][nibble]
		cell.fillFromUpperCell(upCell, depth, upCell.downHashedLen)
		if hph.trace {
			fmt.Printf("cell (%d, %x) depth=%d, a=[%x], upa=[%x]\n", row, nibble, depth, cell.apk[:cell.apl], upCell.apk[:upCell.apl])
		}
		if row >= keyHalfSize {
			cell.apl = 0
		}
		if upCell.downHashedLen > 1 {
			copy(hph.currentKey[hph.currentKeyLen:], upCell.downHashedKey[:upCell.downHashedLen-1])
		}
		hph.currentKeyLen += upCell.downHashedLen - 1
	}
	hph.depths[hph.activeRows] = depth
	hph.activeRows++
	return nil
}

func (hph *BinHashed) needFolding(hashedKey []byte) bool {
	return !bytes.HasPrefix(hashedKey, hph.currentKey[:hph.currentKeyLen])
}

func binToCompact(bin []byte) []byte {
	compact := make([]byte, 2+(len(bin)+7)/8)
	binary.BigEndian.PutUint16(compact, uint16(len(bin)))
	for i := 0; i < len(bin); i++ {
		if bin[i] != 0 {
			compact[2+i/8] |= (byte(1) << (i % 8))
		}
	}
	return compact
}

func compactToBin(compact []byte) []byte {
	bin := make([]byte, binary.BigEndian.Uint16(compact))
	for i := 0; i < len(bin); i++ {
		if compact[2+i/8]&(byte(1)<<(i%8)) == 0 {
			bin[i] = 0
		} else {
			bin[i] = 1
		}
	}
	return bin
}

// The purpose of fold is to reduce hph.currentKey[:hph.currentKeyLen]. It should be invoked
// until that current key becomes a prefix of hashedKey that we will proccess next
// (in other words until the needFolding function returns 0)
func (hph *BinHashed) fold() ([]byte, []byte, error) {
	updateKeyLen := hph.currentKeyLen
	if hph.activeRows == 0 {
		return nil, nil, fmt.Errorf("cannot fold - no active rows")
	}
	if hph.trace {
		fmt.Printf("fold: activeRows: %d, currentKey: [%x], touchMap: %016b, afterMap: %016b\n", hph.activeRows, hph.currentKey[:hph.currentKeyLen], hph.touchMap[hph.activeRows-1], hph.afterMap[hph.activeRows-1])
	}
	// Move information to the row above
	row := hph.activeRows - 1
	var upCell *BinCell
	var col int
	var upDepth int
	if hph.activeRows == 1 {
		if hph.trace {
			fmt.Printf("upcell is root\n")
		}
		upCell = &hph.root
	} else {
		upDepth = hph.depths[hph.activeRows-2]
		col = int(hph.currentKey[upDepth-1])
		if hph.trace {
			fmt.Printf("upcell is (%d x %x), upDepth=%d\n", row-1, col, upDepth)
		}
		upCell = &hph.grid[row-1][col]
	}
	depth := hph.depths[hph.activeRows-1]
	var branchData []byte
	var bitmapBuf [4]byte
	// updateKey, _ := bitstring(hph.currentKey[:updateKeyLen]).reconstructHex()
	updateKey := binToCompact(hph.currentKey[:updateKeyLen])
	if hph.trace {
		fmt.Printf("touchMap[%d]=%016b, afterMap[%d]=%016b\n", row, hph.touchMap[row], row, hph.afterMap[row])
	}
	partsCount := bits.OnesCount16(hph.afterMap[row])
	switch partsCount {
	case 0:
		// Everything deleted
		if hph.touchMap[row] != 0 {
			if row == 0 {
				// Root is deleted because the tree is empty
				hph.rootTouched = true
				hph.rootPresent = false
			} else if upDepth == keyHalfSize {
				// Special case - all storage items of an account have been deleted, but it does not automatically delete the account, just makes it empty storage
				// Therefore we are not propagating deletion upwards, but turn it into a modification
				hph.touchMap[row-1] |= (uint16(1) << col)
			} else {
				// Deletion is propagated upwards
				hph.touchMap[row-1] |= (uint16(1) << col)
				hph.afterMap[row-1] &^= (uint16(1) << col)
			}
		}
		upCell.hl = 0
		upCell.apl = 0
		upCell.spl = 0
		upCell.extLen = 0
		upCell.downHashedLen = 0
		if hph.branchBefore[row] {
			binary.BigEndian.PutUint16(bitmapBuf[0:], hph.touchMap[row]) // touchMap
			binary.BigEndian.PutUint16(bitmapBuf[2:], 0)                 // afterMap
			branchData = append(branchData, bitmapBuf[:]...)
		}
		hph.activeRows--
		if upDepth > 0 {
			hph.currentKeyLen = upDepth - 1
		} else {
			hph.currentKeyLen = 0
		}
	case 1:
		// Leaf or extension node
		if hph.touchMap[row] != 0 {
			// any modifications
			if row == 0 {
				hph.rootTouched = true
			} else {
				// Modifiction is propagated upwards
				hph.touchMap[row-1] |= (uint16(1) << col)
			}
		}
		nibble := bits.TrailingZeros16(hph.afterMap[row])
		cell := &hph.grid[row][nibble]
		upCell.extLen = 0
		upCell.fillFromLowerCell(cell, depth, hph.currentKey[upDepth:hph.currentKeyLen], nibble)
		// Delete if it existed
		if hph.branchBefore[row] {
			binary.BigEndian.PutUint16(bitmapBuf[0:], hph.touchMap[row]) // touchMap
			binary.BigEndian.PutUint16(bitmapBuf[2:], 0)                 // afterMap
			branchData = append(branchData, bitmapBuf[:]...)
		}
		hph.activeRows--
		if upDepth > 0 {
			hph.currentKeyLen = upDepth - 1
		} else {
			hph.currentKeyLen = 0
		}
	default:
		// Branch node
		if hph.touchMap[row] != 0 {
			// any modifications
			if row == 0 {
				hph.rootTouched = true
			} else {
				// Modifiction is propagated upwards
				hph.touchMap[row-1] |= (uint16(1) << col)
			}
		}
		bitmap := hph.touchMap[row] & hph.afterMap[row]
		if !hph.branchBefore[row] {
			// There was no branch node before, so we need to touch even the singular child that existed
			hph.touchMap[row] |= hph.afterMap[row]
			bitmap |= hph.afterMap[row]
		}
		// Calculate total length of all hashes
		totalBranchLen := 17 - partsCount // For every empty cell, one byte
		for bitset, j := hph.afterMap[row], 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			cell := &hph.grid[row][nibble]
			totalBranchLen += hph.computeCellHashLen(cell, depth)
			bitset ^= bit
		}
		binary.BigEndian.PutUint16(bitmapBuf[0:], hph.touchMap[row])
		binary.BigEndian.PutUint16(bitmapBuf[2:], hph.afterMap[row])
		branchData = append(branchData, bitmapBuf[:]...)
		hph.keccak2.Reset()
		var lenPrefix [4]byte
		pt := rlp.GenerateStructLen(lenPrefix[:], totalBranchLen)
		if _, err := hph.keccak2.Write(lenPrefix[:pt]); err != nil {
			return nil, nil, err
		}
		var lastNibble int
		var b [1]byte
		var cellHashBuf [33]byte
		for bitset, j := hph.afterMap[row], 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			b[0] = 0x80
			for i := lastNibble; i < nibble; i++ {
				if _, err := hph.keccak2.Write(b[:]); err != nil {
					return nil, nil, err
				}
				if hph.trace {
					fmt.Printf("%x: empty(%d,%x)\n", i, row, i)
				}
			}
			lastNibble = nibble + 1
			cell := &hph.grid[row][nibble]

			cellHash, err := hph.computeCellHash(cell, depth, cellHashBuf[:0])
			if err != nil {
				return nil, nil, err
			}

			if hph.trace {
				fmt.Printf("%x: computeCellHash(%d,%x,depth=%d)=[%x]\n", nibble, row, nibble, depth, cellHash)
			}
			if _, err = hph.keccak2.Write(cellHash); err != nil {
				return nil, nil, err
			}
			if bitmap&bit != 0 {
				var fieldBits PartFlags
				if cell.extLen > 0 && cell.spl == 0 {
					fieldBits |= HASHEDKEY_PART
				}
				if cell.apl > 0 {
					fieldBits |= ACCOUNT_PLAIN_PART
				}
				if cell.spl > 0 {
					fieldBits |= STORAGE_PLAIN_PART
				}
				if cell.hl > 0 {
					fieldBits |= HASH_PART
				}
				branchData = append(branchData, byte(fieldBits))
				if cell.extLen > 0 && cell.spl == 0 {
					n := binary.PutUvarint(hph.numBuf[:], uint64(cell.extLen))
					branchData = append(branchData, hph.numBuf[:n]...)
					branchData = append(branchData, cell.extension[:cell.extLen]...)
				}
				if cell.apl > 0 {
					n := binary.PutUvarint(hph.numBuf[:], uint64(cell.apl))
					branchData = append(branchData, hph.numBuf[:n]...)
					branchData = append(branchData, cell.apk[:cell.apl]...)
				}
				if cell.spl > 0 {
					n := binary.PutUvarint(hph.numBuf[:], uint64(cell.spl))
					branchData = append(branchData, hph.numBuf[:n]...)
					branchData = append(branchData, cell.spk[:cell.spl]...)
				}
				if cell.hl > 0 {
					n := binary.PutUvarint(hph.numBuf[:], uint64(cell.hl))
					branchData = append(branchData, hph.numBuf[:n]...)
					branchData = append(branchData, cell.h[:cell.hl]...)
				}
			}
			bitset ^= bit
		}
		b[0] = 0x80
		for i := lastNibble; i < maxChild+1; i++ {
			if _, err := hph.keccak2.Write(b[:]); err != nil {
				return nil, nil, err
			}
			if hph.trace {
				fmt.Printf("%x: empty(%d,%x)\n", i, row, i)
			}
		}
		upCell.extLen = depth - upDepth - 1
		if upCell.extLen > 0 {
			copy(upCell.extension[:], hph.currentKey[upDepth:hph.currentKeyLen])
		}
		if depth < keyHalfSize {
			upCell.apl = 0
		}
		upCell.spl = 0
		upCell.hl = 32
		if _, err := hph.keccak2.Read(upCell.h[:]); err != nil {
			return nil, nil, err
		}
		if hph.trace {
			fmt.Printf("} [%x]\n", upCell.h[:])
		}
		hph.activeRows--
		if upDepth > 0 {
			hph.currentKeyLen = upDepth - 1
		} else {
			hph.currentKeyLen = 0
		}
	}
	if branchData != nil {
		if hph.trace {
			fmt.Printf("fold: update key: [%x], branchData: [%x]\n", compactToBin(updateKey), branchData)
		}
	}
	return branchData, updateKey, nil
}

func (hph *BinHashed) deleteCell(hashedKey []byte) {
	if hph.trace {
		fmt.Printf("deleteCell, activeRows = %d\n", hph.activeRows)
	}
	var cell *BinCell
	if hph.activeRows == 0 {
		// Remove the root
		cell = &hph.root
		hph.rootTouched = true
		hph.rootPresent = false
	} else {
		row := hph.activeRows - 1
		if hph.depths[row] < len(hashedKey) {
			if hph.trace {
				fmt.Printf("deleteCell skipping spurious delete depth=%d, len(hashedKey)=%d\n", hph.depths[row], len(hashedKey))
			}
			return
		}
		col := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[row][col]
		if hph.afterMap[row]&(uint16(1)<<col) != 0 {
			// Prevent "spurios deletions", i.e. deletion of absent items
			hph.touchMap[row] |= (uint16(1) << col)
			hph.afterMap[row] &^= (uint16(1) << col)
			if hph.trace {
				fmt.Printf("deleteCell setting (%d, %x)\n", row, col)
			}
		} else {
			if hph.trace {
				fmt.Printf("deleteCell ignoring (%d, %x)\n", row, col)
			}
		}
	}
	cell.extLen = 0
	cell.Balance.Clear()
	copy(cell.CodeHash[:], EmptyCodeHash)
	cell.Nonce = 0
}

func (hph *BinHashed) updateAccount(plainKey, hashedKey []byte) *BinCell {
	var cell *BinCell
	var col, depth int
	if hph.activeRows == 0 {
		hph.activeRows++
	}
	row := hph.activeRows - 1
	depth = hph.depths[hph.activeRows-1]
	col = int(hashedKey[hph.currentKeyLen])
	cell = &hph.grid[row][col]
	hph.touchMap[row] |= (uint16(1) << col)
	hph.afterMap[row] |= (uint16(1) << col)
	if hph.trace {
		fmt.Printf("updateAccount setting (%d, %x), depth=%d\n", row, col, depth)
	}
	if cell.downHashedLen == 0 {
		copy(cell.downHashedKey[:], hashedKey[depth:])
		cell.downHashedLen = len(hashedKey) - depth
		if hph.trace {
			fmt.Printf("set downHasheKey=[%x]\n", cell.downHashedKey[:cell.downHashedLen])
		}
	} else {
		if hph.trace {
			fmt.Printf("left downHasheKey=[%x]\n", cell.downHashedKey[:cell.downHashedLen])
		}
	}
	cell.apl = len(plainKey)
	copy(cell.apk[:], plainKey)
	return cell
}

func (hph *BinHashed) updateBalance(plainKey, hashedKey []byte, balance *uint256.Int) {
	if hph.trace {
		fmt.Printf("updateBalance [%x] [%x] = %d, activeRows = %d\n", plainKey, hashedKey, balance, hph.activeRows)
	}
	cell := hph.updateAccount(plainKey, hashedKey)
	cell.Balance.Set(balance)
}

func (hph *BinHashed) updateCode(plainKey, hashedKey, codeHash []byte) {
	if hph.trace {
		fmt.Printf("updateCode, activeRows = %d\n", hph.activeRows)
	}
	cell := hph.updateAccount(plainKey, hashedKey)
	copy(cell.CodeHash[:], codeHash)
}

func (hph *BinHashed) updateNonce(plainKey, hashedKey []byte, nonce uint64) {
	if hph.trace {
		fmt.Printf("updateNonce, activeRows = %d\n", hph.activeRows)
	}
	cell := hph.updateAccount(plainKey, hashedKey)
	cell.Nonce = nonce
}

func (hph *BinHashed) updateStorage(plainKey, hashedKey, value []byte) {
	if hph.trace {
		fmt.Printf("updateStorage, activeRows = %d\n", hph.activeRows)
	}
	var col, depth int
	var cell *BinCell
	if hph.activeRows == 0 {
		hph.activeRows++
	}
	depth = hph.depths[hph.activeRows-1]
	col = int(hashedKey[hph.currentKeyLen])
	cell = &hph.grid[hph.activeRows-1][col]
	hph.touchMap[hph.activeRows-1] |= (uint16(1) << col)
	hph.afterMap[hph.activeRows-1] |= (uint16(1) << col)
	if hph.trace {
		fmt.Printf("updateStorage setting (%d, %x), touchMap[%d]=%016b, depth=%d\n", hph.activeRows-1, col, hph.activeRows-1, hph.touchMap[hph.activeRows-1], depth)
	}
	if cell.downHashedLen == 0 {
		copy(cell.downHashedKey[:], hashedKey[depth:])
		cell.downHashedLen = len(hashedKey) - depth
		if hph.trace {
			fmt.Printf("set downHasheKey=[%x]\n", cell.downHashedKey[:cell.downHashedLen])
		}
	} else {
		if hph.trace {
			fmt.Printf("left downHasheKey=[%x]\n", cell.downHashedKey[:cell.downHashedLen])
		}
	}
	copy(cell.spk[:], plainKey)
	cell.spl = len(plainKey)
	cell.StorageLen = len(value)
	if len(value) > 0 {
		copy(cell.Storage[:], value)
	}
}

type BinCell struct {
	h             [32]byte // cell hash
	hl            int      // Length of the hash (or embedded)
	apk           [20]byte // account plain key
	apl           int      // length of account plain key
	spk           [52]byte // storage plain key
	spl           int      // length of the storage plain key
	downHashedKey [maxKeySize]byte
	downHashedLen int
	extension     [64]byte
	extLen        int
	Nonce         uint64
	Balance       uint256.Int
	CodeHash      [32]byte // hash of the bytecode
	Storage       [32]byte
	StorageLen    int
}

func (cell *BinCell) fillEmpty() {
	cell.apl = 0
	cell.spl = 0
	cell.downHashedLen = 0
	cell.extLen = 0
	cell.hl = 0
	cell.Nonce = 0
	cell.Balance.Clear()
	copy(cell.CodeHash[:], EmptyCodeHash)
	cell.StorageLen = 0
}

func (cell *BinCell) fillFromUpperCell(upCell *BinCell, depth, depthIncrement int) {
	if upCell.downHashedLen >= depthIncrement {
		cell.downHashedLen = upCell.downHashedLen - depthIncrement
	} else {
		cell.downHashedLen = 0
	}
	if upCell.downHashedLen > depthIncrement {
		copy(cell.downHashedKey[:], upCell.downHashedKey[depthIncrement:upCell.downHashedLen])
	}
	if upCell.extLen >= depthIncrement {
		cell.extLen = upCell.extLen - depthIncrement
	} else {
		cell.extLen = 0
	}
	if upCell.extLen > depthIncrement {
		copy(cell.extension[:], upCell.extension[depthIncrement:upCell.extLen])
	}
	if depth <= keyHalfSize {
		cell.apl = upCell.apl
		if upCell.apl > 0 {
			copy(cell.apk[:], upCell.apk[:cell.apl])
			cell.Balance.Set(&upCell.Balance)
			cell.Nonce = upCell.Nonce
			copy(cell.CodeHash[:], upCell.CodeHash[:])
			cell.extLen = upCell.extLen
			if upCell.extLen > 0 {
				copy(cell.extension[:], upCell.extension[:upCell.extLen])
			}
		}
	} else {
		cell.apl = 0
	}
	cell.spl = upCell.spl
	if upCell.spl > 0 {
		copy(cell.spk[:], upCell.spk[:upCell.spl])
		cell.StorageLen = upCell.StorageLen
		if upCell.StorageLen > 0 {
			copy(cell.Storage[:], upCell.Storage[:upCell.StorageLen])
		}
	}
	cell.hl = upCell.hl
	if upCell.hl > 0 {
		copy(cell.h[:], upCell.h[:upCell.hl])
	}
}

func (cell *BinCell) fillFromLowerCell(lowCell *BinCell, lowDepth int, preExtension []byte, nibble int) {
	if lowCell.apl > 0 || lowDepth < keyHalfSize {
		cell.apl = lowCell.apl
	}
	if lowCell.apl > 0 {
		copy(cell.apk[:], lowCell.apk[:cell.apl])
		cell.Balance.Set(&lowCell.Balance)
		cell.Nonce = lowCell.Nonce
		copy(cell.CodeHash[:], lowCell.CodeHash[:])
	}
	cell.spl = lowCell.spl
	if lowCell.spl > 0 {
		copy(cell.spk[:], lowCell.spk[:cell.spl])
		cell.StorageLen = lowCell.StorageLen
		if lowCell.StorageLen > 0 {
			copy(cell.Storage[:], lowCell.Storage[:lowCell.StorageLen])
		}
	}
	if lowCell.hl > 0 {
		if (lowCell.apl == 0 && lowDepth < keyHalfSize) || (lowCell.spl == 0 && lowDepth > keyHalfSize) {
			// Extension is related to either accounts branch node, or storage branch node, we prepend it by preExtension | nibble
			if len(preExtension) > 0 {
				copy(cell.extension[:], preExtension)
			}
			cell.extension[len(preExtension)] = byte(nibble)
			if lowCell.extLen > 0 {
				copy(cell.extension[1+len(preExtension):], lowCell.extension[:lowCell.extLen])
			}
			cell.extLen = lowCell.extLen + 1 + len(preExtension)
		} else {
			// Extension is related to a storage branch node, so we copy it upwards as is
			cell.extLen = lowCell.extLen
			if lowCell.extLen > 0 {
				copy(cell.extension[:], lowCell.extension[:lowCell.extLen])
			}
		}
	}
	cell.hl = lowCell.hl
	if lowCell.hl > 0 {
		copy(cell.h[:], lowCell.h[:lowCell.hl])
	}
}

func binHashKey(keccak keccakState, plainKey []byte, dest []byte, hashedKeyOffset int) error {
	keccak.Reset()
	var hashBufBack [32]byte
	hashBuf := hashBufBack[:]
	if _, err := keccak.Write(plainKey); err != nil {
		return err
	}
	if _, err := keccak.Read(hashBuf); err != nil {
		return err
	}
	for k := hashedKeyOffset; k < 256; k++ {
		if hashBuf[k/8]&(1<<(7-k%8)) == 0 {
			dest[k-hashedKeyOffset] = 0
		} else {
			dest[k-hashedKeyOffset] = 1
		}
	}
	return nil
}

func (cell *BinCell) deriveHashedKeys(depth int, keccak keccakState, accountKeyLen int) error {
	extraLen := 0
	if cell.apl > 0 {
		if depth > keyHalfSize {
			return fmt.Errorf("deriveHashedKeys accountPlainKey present at depth > 512")
		}
		extraLen = keyHalfSize - depth
	}
	if cell.spl > 0 {
		if depth >= keyHalfSize {
			extraLen = maxKeySize - depth
		} else {
			extraLen += keyHalfSize
		}
	}
	if extraLen > 0 {
		if cell.downHashedLen > 0 {
			copy(cell.downHashedKey[extraLen:], cell.downHashedKey[:cell.downHashedLen])
		}
		cell.downHashedLen += extraLen
		var hashedKeyOffset, downOffset int
		if cell.apl > 0 {
			if err := binHashKey(keccak, cell.apk[:cell.apl], cell.downHashedKey[:], depth); err != nil {
				return err
			}
			downOffset = keyHalfSize - depth
		}
		if cell.spl > 0 {
			if depth >= keyHalfSize {
				hashedKeyOffset = depth - keyHalfSize
			}
			if err := binHashKey(keccak, cell.spk[accountKeyLen:cell.spl], cell.downHashedKey[downOffset:], hashedKeyOffset); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cell *BinCell) fillFromFields(data []byte, pos int, fieldBits PartFlags) (int, error) {
	if fieldBits&HASHEDKEY_PART != 0 {
		l, n := binary.Uvarint(data[pos:])
		if n == 0 {
			return 0, fmt.Errorf("fillFromFields buffer too small for hashedKey len")
		} else if n < 0 {
			return 0, fmt.Errorf("fillFromFields value overflow for hashedKey len")
		}
		pos += n
		if len(data) < pos+int(l) {
			return 0, fmt.Errorf("fillFromFields buffer too small for hashedKey")
		}
		cell.downHashedLen = int(l)
		cell.extLen = int(l)
		if l > 0 {
			copy(cell.downHashedKey[:], data[pos:pos+int(l)])
			copy(cell.extension[:], data[pos:pos+int(l)])
			pos += int(l)
		}
	} else {
		cell.downHashedLen = 0
		cell.extLen = 0
	}
	if fieldBits&ACCOUNT_PLAIN_PART != 0 {
		l, n := binary.Uvarint(data[pos:])
		if n == 0 {
			return 0, fmt.Errorf("fillFromFields buffer too small for accountPlainKey len")
		} else if n < 0 {
			return 0, fmt.Errorf("fillFromFields value overflow for accountPlainKey len")
		}
		pos += n
		if len(data) < pos+int(l) {
			return 0, fmt.Errorf("fillFromFields buffer too small for accountPlainKey")
		}
		cell.apl = int(l)
		if l > 0 {
			copy(cell.apk[:], data[pos:pos+int(l)])
			pos += int(l)
		}
	} else {
		cell.apl = 0
	}
	if fieldBits&STORAGE_PLAIN_PART != 0 {
		l, n := binary.Uvarint(data[pos:])
		if n == 0 {
			return 0, fmt.Errorf("fillFromFields buffer too small for storagePlainKey len")
		} else if n < 0 {
			return 0, fmt.Errorf("fillFromFields value overflow for storagePlainKey len")
		}
		pos += n
		if len(data) < pos+int(l) {
			return 0, fmt.Errorf("fillFromFields buffer too small for storagePlainKey")
		}
		cell.spl = int(l)
		if l > 0 {
			copy(cell.spk[:], data[pos:pos+int(l)])
			pos += int(l)
		}
	} else {
		cell.spl = 0
	}
	if fieldBits&HASH_PART != 0 {
		l, n := binary.Uvarint(data[pos:])
		if n == 0 {
			return 0, fmt.Errorf("fillFromFields buffer too small for hash len")
		} else if n < 0 {
			return 0, fmt.Errorf("fillFromFields value overflow for hash len")
		}
		pos += n
		if len(data) < pos+int(l) {
			return 0, fmt.Errorf("fillFromFields buffer too small for hash")
		}
		cell.hl = int(l)
		if l > 0 {
			copy(cell.h[:], data[pos:pos+int(l)])
			pos += int(l)
		}
	} else {
		cell.hl = 0
	}
	return pos, nil
}

func (cell *BinCell) accountForHashing(buffer, storageRootHash []byte) int {
	balanceBytes := 0
	if !cell.Balance.LtUint64(128) {
		balanceBytes = cell.Balance.ByteLen()
	}

	var nonceBytes int
	if cell.Nonce < 128 && cell.Nonce != 0 {
		nonceBytes = 0
	} else {
		nonceBytes = (bits.Len64(cell.Nonce) + 7) / 8
	}

	var structLength = uint(balanceBytes + nonceBytes + 2)
	structLength += 66 // Two 32-byte arrays + 2 prefixes

	var pos int
	if structLength < 56 {
		buffer[0] = byte(192 + structLength)
		pos = 1
	} else {
		lengthBytes := (bits.Len(structLength) + 7) / 8
		buffer[0] = byte(247 + lengthBytes)

		for i := lengthBytes; i > 0; i-- {
			buffer[i] = byte(structLength)
			structLength >>= 8
		}

		pos = lengthBytes + 1
	}

	// Encoding nonce
	if cell.Nonce < 128 && cell.Nonce != 0 {
		buffer[pos] = byte(cell.Nonce)
	} else {
		buffer[pos] = byte(128 + nonceBytes)
		var nonce = cell.Nonce
		for i := nonceBytes; i > 0; i-- {
			buffer[pos+i] = byte(nonce)
			nonce >>= 8
		}
	}
	pos += 1 + nonceBytes

	// Encoding balance
	if cell.Balance.LtUint64(128) && !cell.Balance.IsZero() {
		buffer[pos] = byte(cell.Balance.Uint64())
		pos++
	} else {
		buffer[pos] = byte(128 + balanceBytes)
		pos++
		cell.Balance.WriteToSlice(buffer[pos : pos+balanceBytes])
		pos += balanceBytes
	}

	// Encoding Root and CodeHash
	buffer[pos] = 128 + 32
	pos++
	copy(buffer[pos:], storageRootHash[:length.Hash])
	pos += 32
	buffer[pos] = 128 + 32
	pos++
	copy(buffer[pos:], cell.CodeHash[:])
	pos += 32
	return pos
}
