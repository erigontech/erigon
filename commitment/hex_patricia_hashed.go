/*
   Copyright 2022 The Erigon contributors

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
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"strings"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/rlp"
)

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

// HexPatriciaHashed implements commitment based on patricia merkle tree with radix 16,
// with keys pre-hashed by keccak256
type HexPatriciaHashed struct {
	root Cell // Root cell of the tree
	// How many rows (starting from row 0) are currently active and have corresponding selected columns
	// Last active row does not have selected column
	activeRows int
	// Length of the key that reflects current positioning of the grid. It maybe larger than number of active rows,
	// if an account leaf cell represents multiple nibbles in the key
	currentKeyLen int
	accountKeyLen int
	// Rows of the grid correspond to the level of depth in the patricia tree
	// Columns of the grid correspond to pointers to the nodes further from the root
	grid         [128][16]Cell // First 64 rows of this grid are for account trie, and next 64 rows are for storage trie
	currentKey   [128]byte     // For each row indicates which column is currently selected
	depths       [128]int      // For each row, the depth of cells in that row
	branchBefore [128]bool     // For each row, whether there was a branch node in the database loaded in unfold
	touchMap     [128]uint16   // For each row, bitmap of cells that were either present before modification, or modified or deleted
	afterMap     [128]uint16   // For each row, bitmap of cells that were present after modification
	keccak       keccakState
	keccak2      keccakState
	rootChecked  bool // Set to false if it is not known whether the root is empty, set to true if it is checked
	rootTouched  bool
	rootPresent  bool
	trace        bool
	// Function used to load branch node and fill up the cells
	// For each cell, it sets the cell type, clears the modified flag, fills the hash,
	// and for the extension, account, and leaf type, the `l` and `k`
	branchFn func(prefix []byte) ([]byte, error)
	// Function used to fetch account with given plain key
	accountFn func(plainKey []byte, cell *Cell) error
	// Function used to fetch storage with given plain key
	storageFn func(plainKey []byte, cell *Cell) error

	hashAuxBuffer [128]byte     // buffer to compute cell hash or write hash-related things
	auxBuffer     *bytes.Buffer // auxiliary buffer used during branch updates encoding
}

// represents state of the tree
type state struct {
	Root          []byte      // encoded root cell
	Depths        [128]int    // For each row, the depth of cells in that row
	TouchMap      [128]uint16 // For each row, bitmap of cells that were either present before modification, or modified or deleted
	AfterMap      [128]uint16 // For each row, bitmap of cells that were present after modification
	BranchBefore  [128]bool   // For each row, whether there was a branch node in the database loaded in unfold
	CurrentKey    [128]byte   // For each row indicates which column is currently selected
	CurrentKeyLen int8
	RootChecked   bool // Set to false if it is not known whether the root is empty, set to true if it is checked
	RootTouched   bool
	RootPresent   bool
}

func NewHexPatriciaHashed(accountKeyLen int,
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) error,
	storageFn func(plainKey []byte, cell *Cell) error,
) *HexPatriciaHashed {
	return &HexPatriciaHashed{
		keccak:        sha3.NewLegacyKeccak256().(keccakState),
		keccak2:       sha3.NewLegacyKeccak256().(keccakState),
		accountKeyLen: accountKeyLen,
		branchFn:      branchFn,
		accountFn:     accountFn,
		storageFn:     storageFn,
		auxBuffer:     bytes.NewBuffer(make([]byte, 8192)),
	}
}

type Cell struct {
	Balance       uint256.Int
	Nonce         uint64
	hl            int // Length of the hash (or embedded)
	StorageLen    int
	apl           int // length of account plain key
	spl           int // length of the storage plain key
	downHashedLen int
	extLen        int
	downHashedKey [128]byte
	extension     [64]byte
	spk           [length.Addr + length.Hash]byte // storage plain key
	h             [length.Hash]byte               // cell hash
	CodeHash      [length.Hash]byte               // hash of the bytecode
	Storage       [length.Hash]byte
	apk           [length.Addr]byte // account plain key
	Delete        bool
}

var (
	EmptyRootHash, _ = hex.DecodeString("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	EmptyCodeHash, _ = hex.DecodeString("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
)

func (cell *Cell) fillEmpty() {
	cell.apl = 0
	cell.spl = 0
	cell.downHashedLen = 0
	cell.extLen = 0
	cell.hl = 0
	cell.Nonce = 0
	cell.Balance.Clear()
	copy(cell.CodeHash[:], EmptyCodeHash)
	cell.StorageLen = 0
	cell.Delete = false
}

func (cell *Cell) fillFromUpperCell(upCell *Cell, depth, depthIncrement int) {
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
	if depth <= 64 {
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

func (cell *Cell) fillFromLowerCell(lowCell *Cell, lowDepth int, preExtension []byte, nibble int) {
	if lowCell.apl > 0 || lowDepth < 64 {
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
		if (lowCell.apl == 0 && lowDepth < 64) || (lowCell.spl == 0 && lowDepth > 64) {
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

func hashKey(keccak keccakState, plainKey []byte, dest []byte, hashedKeyOffset int) error {
	keccak.Reset()
	var hashBufBack [length.Hash]byte
	hashBuf := hashBufBack[:]
	if _, err := keccak.Write(plainKey); err != nil {
		return err
	}
	if _, err := keccak.Read(hashBuf); err != nil {
		return err
	}
	hashBuf = hashBuf[hashedKeyOffset/2:]
	var k int
	if hashedKeyOffset%2 == 1 {
		dest[0] = hashBuf[0] & 0xf
		k++
		hashBuf = hashBuf[1:]
	}
	for _, c := range hashBuf {
		dest[k] = (c >> 4) & 0xf
		k++
		dest[k] = c & 0xf
		k++
	}
	return nil
}

func (cell *Cell) deriveHashedKeys(depth int, keccak keccakState, accountKeyLen int) error {
	extraLen := 0
	if cell.apl > 0 {
		if depth > 64 {
			return fmt.Errorf("deriveHashedKeys accountPlainKey present at depth > 64")
		}
		extraLen = 64 - depth
	}
	if cell.spl > 0 {
		if depth >= 64 {
			extraLen = 128 - depth
		} else {
			extraLen += 64
		}
	}
	if extraLen > 0 {
		if cell.downHashedLen > 0 {
			copy(cell.downHashedKey[extraLen:], cell.downHashedKey[:cell.downHashedLen])
		}
		cell.downHashedLen += extraLen
		var hashedKeyOffset, downOffset int
		if cell.apl > 0 {
			if err := hashKey(keccak, cell.apk[:cell.apl], cell.downHashedKey[:], depth); err != nil {
				return err
			}
			downOffset = 64 - depth
		}
		if cell.spl > 0 {
			if depth >= 64 {
				hashedKeyOffset = depth - 64
			}
			if err := hashKey(keccak, cell.spk[accountKeyLen:cell.spl], cell.downHashedKey[downOffset:], hashedKeyOffset); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cell *Cell) fillFromFields(data []byte, pos int, fieldBits PartFlags) (int, error) {
	if fieldBits&HashedKeyPart != 0 {
		l, n := binary.Uvarint(data[pos:])
		if n == 0 {
			return 0, fmt.Errorf("fillFromFields buffer too small for hashedKey len")
		} else if n < 0 {
			return 0, fmt.Errorf("fillFromFields value overflow for hashedKey len")
		}
		pos += n
		if len(data) < pos+int(l) {
			return 0, fmt.Errorf("fillFromFields buffer too small for hashedKey exp %d got %d", pos+int(l), len(data))
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
	if fieldBits&AccountPlainPart != 0 {
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
	if fieldBits&StoragePlainPart != 0 {
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
	if fieldBits&HashPart != 0 {
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

func (cell *Cell) setStorage(value []byte) {
	cell.StorageLen = len(value)
	if len(value) > 0 {
		copy(cell.Storage[:], value)
	}
}

func (cell *Cell) setAccountFields(codeHash []byte, balance *uint256.Int, nonce uint64) {
	copy(cell.CodeHash[:], codeHash)

	cell.Balance.SetBytes(balance.Bytes())
	cell.Nonce = nonce
}

func (cell *Cell) accountForHashing(buffer []byte, storageRootHash [length.Hash]byte) int {
	balanceBytes := 0
	if !cell.Balance.LtUint64(128) {
		balanceBytes = cell.Balance.ByteLen()
	}

	var nonceBytes int
	if cell.Nonce < 128 && cell.Nonce != 0 {
		nonceBytes = 0
	} else {
		nonceBytes = common.BitLenToByteLen(bits.Len64(cell.Nonce))
	}

	var structLength = uint(balanceBytes + nonceBytes + 2)
	structLength += 66 // Two 32-byte arrays + 2 prefixes

	var pos int
	if structLength < 56 {
		buffer[0] = byte(192 + structLength)
		pos = 1
	} else {
		lengthBytes := common.BitLenToByteLen(bits.Len(structLength))
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
	copy(buffer[pos:], storageRootHash[:])
	pos += 32
	buffer[pos] = 128 + 32
	pos++
	copy(buffer[pos:], cell.CodeHash[:])
	pos += 32
	return pos
}

func (hph *HexPatriciaHashed) completeLeafHash(buf, keyPrefix []byte, kp, kl, compactLen int, key []byte, compact0 byte, ni int, val rlp.RlpSerializable, singleton bool) ([]byte, error) {
	totalLen := kp + kl + val.DoubleRLPLen()
	var lenPrefix [4]byte
	pt := rlp.GenerateStructLen(lenPrefix[:], totalLen)
	embedded := !singleton && totalLen+pt < length.Hash
	var writer io.Writer
	if embedded {
		//hph.byteArrayWriter.Setup(buf)
		hph.auxBuffer.Reset()
		writer = hph.auxBuffer
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
		buf = hph.auxBuffer.Bytes()
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

func (hph *HexPatriciaHashed) leafHashWithKeyVal(buf, key []byte, val rlp.RlpSerializableBytes, singleton bool) ([]byte, error) {
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
	return hph.completeLeafHash(buf, keyPrefix[:], kp, kl, compactLen, key, compact0, ni, val, singleton)
}

func (hph *HexPatriciaHashed) accountLeafHashWithKey(buf, key []byte, val rlp.RlpSerializable) ([]byte, error) {
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
	return hph.completeLeafHash(buf, keyPrefix[:], kp, kl, compactLen, key, compact0, ni, val, true)
}

func (hph *HexPatriciaHashed) extensionHash(key []byte, hash []byte) ([length.Hash]byte, error) {
	var hashBuf [length.Hash]byte

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
		return hashBuf, err
	}
	if _, err := hph.keccak.Write(keyPrefix[:kp]); err != nil {
		return hashBuf, err
	}
	var b [1]byte
	b[0] = compact0
	if _, err := hph.keccak.Write(b[:]); err != nil {
		return hashBuf, err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := hph.keccak.Write(b[:]); err != nil {
			return hashBuf, err
		}
		ni += 2
	}
	b[0] = 0x80 + length.Hash
	if _, err := hph.keccak.Write(b[:]); err != nil {
		return hashBuf, err
	}
	if _, err := hph.keccak.Write(hash); err != nil {
		return hashBuf, err
	}
	// Replace previous hash with the new one
	if _, err := hph.keccak.Read(hashBuf[:]); err != nil {
		return hashBuf, err
	}
	return hashBuf, nil
}

func (hph *HexPatriciaHashed) computeCellHashLen(cell *Cell, depth int) int {
	if cell.spl > 0 && depth >= 64 {
		keyLen := 128 - depth + 1 // Length of hex key with terminator character
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

func (hph *HexPatriciaHashed) computeCellHash(cell *Cell, depth int, buf []byte) ([]byte, error) {
	var err error
	var storageRootHash [length.Hash]byte
	storageRootHashIsSet := false
	if cell.spl > 0 {
		var hashedKeyOffset int
		if depth >= 64 {
			hashedKeyOffset = depth - 64
		}
		singleton := depth <= 64
		if err := hashKey(hph.keccak, cell.spk[hph.accountKeyLen:cell.spl], cell.downHashedKey[:], hashedKeyOffset); err != nil {
			return nil, err
		}
		cell.downHashedKey[64-hashedKeyOffset] = 16 // Add terminator
		if singleton {
			if hph.trace {
				fmt.Printf("leafHashWithKeyVal(singleton) for [%x]=>[%x]\n", cell.downHashedKey[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
			}
			aux := make([]byte, 0, 33)
			if aux, err = hph.leafHashWithKeyVal(aux, cell.downHashedKey[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], true); err != nil {
				return nil, err
			}
			storageRootHash = *(*[length.Hash]byte)(aux[1:])
			storageRootHashIsSet = true
		} else {
			if hph.trace {
				fmt.Printf("leafHashWithKeyVal for [%x]=>[%x]\n", cell.downHashedKey[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
			}
			return hph.leafHashWithKeyVal(buf, cell.downHashedKey[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], false)
		}
	}
	if cell.apl > 0 {
		if err := hashKey(hph.keccak, cell.apk[:cell.apl], cell.downHashedKey[:], depth); err != nil {
			return nil, err
		}
		cell.downHashedKey[64-depth] = 16 // Add terminator
		if !storageRootHashIsSet {
			if cell.extLen > 0 {
				// Extension
				if cell.hl > 0 {
					if hph.trace {
						fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.h[:cell.hl])
					}
					if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.h[:cell.hl]); err != nil {
						return nil, err
					}
				} else {
					return nil, fmt.Errorf("computeCellHash extension without hash")
				}
			} else if cell.hl > 0 {
				storageRootHash = cell.h
			} else {
				storageRootHash = *(*[length.Hash]byte)(EmptyRootHash)
			}
		}
		var valBuf [128]byte
		valLen := cell.accountForHashing(valBuf[:], storageRootHash)
		if hph.trace {
			fmt.Printf("accountLeafHashWithKey for [%x]=>[%x]\n", hph.hashAuxBuffer[:65-depth], valBuf[:valLen])
		}
		return hph.accountLeafHashWithKey(buf, cell.downHashedKey[:65-depth], rlp.RlpEncodedBytes(valBuf[:valLen]))
	}
	buf = append(buf, 0x80+32)
	if cell.extLen > 0 {
		// Extension
		if cell.hl > 0 {
			if hph.trace {
				fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.h[:cell.hl])
			}
			var hash [length.Hash]byte
			if hash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.h[:cell.hl]); err != nil {
				return nil, err
			}
			buf = append(buf, hash[:]...)
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

func (hph *HexPatriciaHashed) needUnfolding(hashedKey []byte) int {
	var cell *Cell
	var depth int
	if hph.activeRows == 0 {
		if hph.trace {
			fmt.Printf("needUnfolding root, rootChecked = %t\n", hph.rootChecked)
		}
		if hph.rootChecked && hph.root.downHashedLen == 0 && hph.root.hl == 0 {
			// Previously checked, empty root, no unfolding needed
			return 0
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
		}
		// unfold branch node
		return 1
	}
	cpl := commonPrefixLen(hashedKey[depth:], cell.downHashedKey[:cell.downHashedLen-1])
	if hph.trace {
		fmt.Printf("cpl=%d, cell.downHashedKey=[%x], depth=%d, hashedKey[depth:]=[%x]\n", cpl, cell.downHashedKey[:cell.downHashedLen], depth, hashedKey[depth:])
	}
	unfolding := cpl + 1
	if depth < 64 && depth+unfolding > 64 {
		// This is to make sure that unfolding always breaks at the level where storage subtrees start
		unfolding = 64 - depth
		if hph.trace {
			fmt.Printf("adjusted unfolding=%d\n", unfolding)
		}
	}
	return unfolding
}

// unfoldBranchNode returns true if unfolding has been done
func (hph *HexPatriciaHashed) unfoldBranchNode(row int, deleted bool, depth int) (bool, error) {
	branchData, err := hph.branchFn(hexToCompact(hph.currentKey[:hph.currentKeyLen]))
	if err != nil {
		return false, err
	}
	if !hph.rootChecked && hph.currentKeyLen == 0 && len(branchData) == 0 {
		// Special case - empty or deleted root
		hph.rootChecked = true
		return false, nil
	}
	if len(branchData) == 0 {
		log.Warn("got empty branch data during unfold", "key", hex.EncodeToString(hexToCompact(hph.currentKey[:hph.currentKeyLen])), "row", row, "depth", depth, "deleted", deleted)
	}
	hph.branchBefore[row] = true
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
	//fmt.Printf("unfoldBranchNode [%x], afterMap = [%016b], touchMap = [%016b]\n", branchData, hph.afterMap[row], hph.touchMap[row])
	// Loop iterating over the set bits of modMask
	for bitset, j := bitmap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &hph.grid[row][nibble]
		fieldBits := branchData[pos]
		pos++
		var err error
		if pos, err = cell.fillFromFields(branchData, pos, PartFlags(fieldBits)); err != nil {
			return false, fmt.Errorf("prefix [%x], branchData[%x]: %w", hph.currentKey[:hph.currentKeyLen], branchData, err)
		}
		if hph.trace {
			fmt.Printf("cell (%d, %x) depth=%d, hash=[%x], a=[%x], s=[%x], ex=[%x]\n", row, nibble, depth, cell.h[:cell.hl], cell.apk[:cell.apl], cell.spk[:cell.spl], cell.extension[:cell.extLen])
		}
		if cell.apl > 0 {
			hph.accountFn(cell.apk[:cell.apl], cell)
			if hph.trace {
				fmt.Printf("accountFn[%x] return balance=%d, nonce=%d code=%x\n", cell.apk[:cell.apl], &cell.Balance, cell.Nonce, cell.CodeHash[:])
			}
		}
		if cell.spl > 0 {
			hph.storageFn(cell.spk[:cell.spl], cell)
		}
		if err = cell.deriveHashedKeys(depth, hph.keccak, hph.accountKeyLen); err != nil {
			return false, err
		}
		bitset ^= bit
	}
	return true, nil
}

func (hph *HexPatriciaHashed) unfold(hashedKey []byte, unfolding int) error {
	if hph.trace {
		fmt.Printf("unfold %d: activeRows: %d\n", unfolding, hph.activeRows)
	}
	var upCell *Cell
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
			fmt.Printf("unfold root, touched %t, present %t, column %d\n", touched, present, col)
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
	for i := 0; i < 16; i++ {
		hph.grid[row][i].fillEmpty()
	}
	hph.touchMap[row] = 0
	hph.afterMap[row] = 0
	hph.branchBefore[row] = false
	if upCell.downHashedLen == 0 {
		depth = upDepth + 1
		if unfolded, err := hph.unfoldBranchNode(row, touched && !present /* deleted */, depth); err != nil {
			return err
		} else if !unfolded {
			// Return here to prevent activeRow from being incremented
			return nil
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
			fmt.Printf("cell (%d, %x) depth=%d\n", row, nibble, depth)
		}
		if row >= 64 {
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
			fmt.Printf("cell (%d, %x) depth=%d\n", row, nibble, depth)
		}
		if row >= 64 {
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

func (hph *HexPatriciaHashed) needFolding(hashedKey []byte) bool {
	return !bytes.HasPrefix(hashedKey, hph.currentKey[:hph.currentKeyLen])
}

// The purpose of fold is to reduce hph.currentKey[:hph.currentKeyLen]. It should be invoked
// until that current key becomes a prefix of hashedKey that we will proccess next
// (in other words until the needFolding function returns 0)
func (hph *HexPatriciaHashed) fold() (branchData BranchData, updateKey []byte, err error) {
	updateKeyLen := hph.currentKeyLen
	if hph.activeRows == 0 {
		return nil, nil, fmt.Errorf("cannot fold - no active rows")
	}
	if hph.trace {
		fmt.Printf("fold: activeRows: %d, currentKey: [%x], touchMap: %016b, afterMap: %016b\n", hph.activeRows, hph.currentKey[:hph.currentKeyLen], hph.touchMap[hph.activeRows-1], hph.afterMap[hph.activeRows-1])
	}
	// Move information to the row above
	row := hph.activeRows - 1
	var upCell *Cell
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
	updateKey = hexToCompact(hph.currentKey[:updateKeyLen])
	partsCount := bits.OnesCount16(hph.afterMap[row])

	if hph.trace {
		fmt.Printf("touchMap[%d]=%016b, afterMap[%d]=%016b\n", row, hph.touchMap[row], row, hph.afterMap[row])
	}
	switch partsCount {
	case 0:
		// Everything deleted
		if hph.touchMap[row] != 0 {
			if row == 0 {
				// Root is deleted because the tree is empty
				hph.rootTouched = true
				hph.rootPresent = false
			} else if upDepth == 64 {
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
			branchData, _, err = EncodeBranch(0, hph.touchMap[row], 0, func(nibble int, skip bool) (*Cell, error) { return nil, nil })
			if err != nil {
				return nil, updateKey, fmt.Errorf("failed to encode leaf node update: %w", err)
			}
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
			//branchData, _, err = hph.EncodeBranchDirectAccess(0, row, depth)
			branchData, _, err = EncodeBranch(0, hph.touchMap[row], 0, func(nibble int, skip bool) (*Cell, error) { return nil, nil })
			if err != nil {
				return nil, updateKey, fmt.Errorf("failed to encode leaf node update: %w", err)
			}
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

		hph.keccak2.Reset()
		pt := rlp.GenerateStructLen(hph.hashAuxBuffer[:], totalBranchLen)
		if _, err := hph.keccak2.Write(hph.hashAuxBuffer[:pt]); err != nil {
			return nil, nil, err
		}

		b := [...]byte{0x80}
		cellGetter := func(nibble int, skip bool) (*Cell, error) {
			if skip {
				if _, err := hph.keccak2.Write(b[:]); err != nil {
					return nil, fmt.Errorf("failed to write empty nibble to hash: %w", err)
				}
				if hph.trace {
					fmt.Printf("%x: empty(%d,%x)\n", nibble, row, nibble)
				}
				return nil, nil
			}
			cell := &hph.grid[row][nibble]
			cellHash, err := hph.computeCellHash(cell, depth, hph.hashAuxBuffer[:0])
			if err != nil {
				return nil, err
			}
			if hph.trace {
				fmt.Printf("%x: computeCellHash(%d,%x,depth=%d)=[%x]\n", nibble, row, nibble, depth, cellHash)
			}
			if _, err := hph.keccak2.Write(cellHash); err != nil {
				return nil, err
			}

			return cell, nil
		}

		var lastNibble int
		var err error
		_ = cellGetter

		//branchData, lastNibble, err = hph.EncodeBranchDirectAccess(bitmap, row, depth, branchData)
		branchData, lastNibble, err = EncodeBranch(bitmap, hph.touchMap[row], hph.afterMap[row], cellGetter)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode branch update: %w", err)
		}
		for i := lastNibble; i < 17; i++ {
			if _, err := hph.keccak2.Write(b[:]); err != nil {
				return nil, nil, err
			}
			if hph.trace {
				fmt.Printf("%x: empty(%d,%x)\n", i, row, i)
			}
		}
		upCell.extLen = depth - upDepth - 1
		upCell.downHashedLen = upCell.extLen
		if upCell.extLen > 0 {
			copy(upCell.extension[:], hph.currentKey[upDepth:hph.currentKeyLen])
			copy(upCell.downHashedKey[:], hph.currentKey[upDepth:hph.currentKeyLen])
		}
		if depth < 64 {
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
			fmt.Printf("fold: update key: %x, branchData: [%x]\n", CompactedKeyToHex(updateKey), branchData)
		}
	}
	return branchData, updateKey, nil
}

func (hph *HexPatriciaHashed) deleteCell(hashedKey []byte) {
	if hph.trace {
		fmt.Printf("deleteCell, activeRows = %d\n", hph.activeRows)
	}
	var cell *Cell
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

func (hph *HexPatriciaHashed) updateCell(plainKey, hashedKey []byte) *Cell {
	var cell *Cell
	var col, depth int
	if hph.activeRows == 0 {
		cell = &hph.root
		hph.rootTouched, hph.rootPresent = true, true
	} else {
		row := hph.activeRows - 1
		depth = hph.depths[row]
		col = int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[row][col]
		hph.touchMap[row] |= (uint16(1) << col)
		hph.afterMap[row] |= (uint16(1) << col)
		if hph.trace {
			fmt.Printf("updateCell setting (%d, %x), depth=%d\n", row, col, depth)
		}
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
	if len(hashedKey) == 2*length.Hash { // set account key
		cell.apl = len(plainKey)
		copy(cell.apk[:], plainKey)
	} else { // set storage key
		cell.spl = len(plainKey)
		copy(cell.spk[:], plainKey)
	}
	return cell
}

func (hph *HexPatriciaHashed) RootHash() ([]byte, error) {
	hash, err := hph.computeCellHash(&hph.root, 0, nil)
	if err != nil {
		return nil, err
	}
	return hash[1:], nil // first byte is 128+hash_len
}

func (hph *HexPatriciaHashed) ReviewKeys(plainKeys, hashedKeys [][]byte) (rootHash []byte, branchNodeUpdates map[string]BranchData, err error) {
	branchNodeUpdates = make(map[string]BranchData)

	stagedCell := new(Cell)
	for i, hashedKey := range hashedKeys {
		plainKey := plainKeys[i]
		if hph.trace {
			fmt.Printf("plainKey=[%x], hashedKey=[%x], currentKey=[%x]\n", plainKey, hashedKey, hph.currentKey[:hph.currentKeyLen])
		}
		// Keep folding until the currentKey is the prefix of the key we modify
		for hph.needFolding(hashedKey) {
			if branchData, updateKey, err := hph.fold(); err != nil {
				return nil, nil, fmt.Errorf("fold: %w", err)
			} else if branchData != nil {
				branchNodeUpdates[string(updateKey)] = branchData
			}
		}
		// Now unfold until we step on an empty cell
		for unfolding := hph.needUnfolding(hashedKey); unfolding > 0; unfolding = hph.needUnfolding(hashedKey) {
			if err := hph.unfold(hashedKey, unfolding); err != nil {
				return nil, nil, fmt.Errorf("unfold: %w", err)
			}
		}

		// Update the cell
		stagedCell.fillEmpty()
		if len(plainKey) == hph.accountKeyLen {
			if err := hph.accountFn(plainKey, stagedCell); err != nil {
				return nil, nil, fmt.Errorf("accountFn for key %x failed: %w", plainKey, err)
			}
			if !stagedCell.Delete {
				cell := hph.updateCell(plainKey, hashedKey)
				cell.setAccountFields(stagedCell.CodeHash[:], &stagedCell.Balance, stagedCell.Nonce)

				if hph.trace {
					fmt.Printf("accountFn reading key %x => balance=%v nonce=%v codeHash=%x\n", cell.apk, cell.Balance.Uint64(), cell.Nonce, cell.CodeHash)
				}
			}
		} else {
			if err = hph.storageFn(plainKey, stagedCell); err != nil {
				return nil, nil, fmt.Errorf("storageFn for key %x failed: %w", plainKey, err)
			}
			if !stagedCell.Delete {
				hph.updateCell(plainKey, hashedKey).setStorage(stagedCell.Storage[:stagedCell.StorageLen])
				if hph.trace {
					fmt.Printf("storageFn reading key %x => %x\n", plainKey, stagedCell.Storage[:stagedCell.StorageLen])
				}
			}
		}

		if stagedCell.Delete {
			if hph.trace {
				fmt.Printf("delete cell %x hash %x\n", plainKey, hashedKey)
			}
			hph.deleteCell(hashedKey)
		}
	}
	// Folding everything up to the root
	for hph.activeRows > 0 {
		if branchData, updateKey, err := hph.fold(); err != nil {
			return nil, nil, fmt.Errorf("final fold: %w", err)
		} else if branchData != nil {
			branchNodeUpdates[string(updateKey)] = branchData
		}
	}

	rootHash, err = hph.RootHash()
	if err != nil {
		return nil, branchNodeUpdates, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	return rootHash, branchNodeUpdates, nil
}

func (hph *HexPatriciaHashed) SetTrace(trace bool) { hph.trace = trace }

func (hph *HexPatriciaHashed) Variant() TrieVariant { return VariantHexPatriciaTrie }

// Reset allows HexPatriciaHashed instance to be reused for the new commitment calculation
func (hph *HexPatriciaHashed) Reset() {
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

func (hph *HexPatriciaHashed) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) error,
	storageFn func(plainKey []byte, cell *Cell) error,
) {
	hph.branchFn = branchFn
	hph.accountFn = accountFn
	hph.storageFn = storageFn
}

type stateRootFlag int8

var (
	stateRootPresent stateRootFlag = 1
	stateRootChecked stateRootFlag = 2
	stateRootTouched stateRootFlag = 4
)

func (s *state) Encode(buf []byte) ([]byte, error) {
	var rootFlags stateRootFlag
	if s.RootPresent {
		rootFlags |= stateRootPresent
	}
	if s.RootChecked {
		rootFlags |= stateRootChecked
	}
	if s.RootTouched {
		rootFlags |= stateRootTouched
	}

	ee := bytes.NewBuffer(buf)
	if err := binary.Write(ee, binary.BigEndian, s.CurrentKeyLen); err != nil {
		return nil, fmt.Errorf("encode currentKeyLen: %w", err)
	}
	if err := binary.Write(ee, binary.BigEndian, int8(rootFlags)); err != nil {
		return nil, fmt.Errorf("encode rootFlags: %w", err)
	}
	if n, err := ee.Write(s.CurrentKey[:]); err != nil || n != len(s.CurrentKey) {
		return nil, fmt.Errorf("encode currentKey: %w", err)
	}
	if err := binary.Write(ee, binary.BigEndian, uint16(len(s.Root))); err != nil {
		return nil, fmt.Errorf("encode root len: %w", err)
	}
	if n, err := ee.Write(s.Root); err != nil || n != len(s.Root) {
		return nil, fmt.Errorf("encode root: %w", err)
	}
	d := make([]byte, len(s.Depths))
	for i := 0; i < len(s.Depths); i++ {
		d[i] = byte(s.Depths[i])
	}
	if n, err := ee.Write(d); err != nil || n != len(s.Depths) {
		return nil, fmt.Errorf("encode depths: %w", err)
	}
	if err := binary.Write(ee, binary.BigEndian, s.TouchMap); err != nil {
		return nil, fmt.Errorf("encode touchMap: %w", err)
	}
	if err := binary.Write(ee, binary.BigEndian, s.AfterMap); err != nil {
		return nil, fmt.Errorf("encode afterMap: %w", err)
	}

	var before1, before2 uint64
	for i := 0; i < 64; i++ {
		if s.BranchBefore[i] {
			before1 |= 1 << i
		}
	}
	for i, j := 64, 0; i < 128; i, j = i+1, j+1 {
		if s.BranchBefore[i] {
			before2 |= 1 << j
		}
	}
	if err := binary.Write(ee, binary.BigEndian, before1); err != nil {
		return nil, fmt.Errorf("encode branchBefore_1: %w", err)
	}
	if err := binary.Write(ee, binary.BigEndian, before2); err != nil {
		return nil, fmt.Errorf("encode branchBefore_2: %w", err)
	}
	return ee.Bytes(), nil
}

func (s *state) Decode(buf []byte) error {
	aux := bytes.NewBuffer(buf)
	if err := binary.Read(aux, binary.BigEndian, &s.CurrentKeyLen); err != nil {
		return fmt.Errorf("currentKeyLen: %w", err)
	}
	var rootFlags stateRootFlag
	if err := binary.Read(aux, binary.BigEndian, &rootFlags); err != nil {
		return fmt.Errorf("rootFlags: %w", err)
	}

	if rootFlags&stateRootPresent != 0 {
		s.RootPresent = true
	}
	if rootFlags&stateRootTouched != 0 {
		s.RootTouched = true
	}
	if rootFlags&stateRootChecked != 0 {
		s.RootChecked = true
	}
	if n, err := aux.Read(s.CurrentKey[:]); err != nil || n != 128 {
		return fmt.Errorf("currentKey: %w", err)
	}
	var rootSize uint16
	if err := binary.Read(aux, binary.BigEndian, &rootSize); err != nil {
		return fmt.Errorf("root size: %w", err)
	}
	s.Root = make([]byte, rootSize)
	if _, err := aux.Read(s.Root); err != nil {
		return fmt.Errorf("root: %w", err)
	}
	d := make([]byte, len(s.Depths))
	if err := binary.Read(aux, binary.BigEndian, &d); err != nil {
		return fmt.Errorf("depths: %w", err)
	}
	for i := 0; i < len(s.Depths); i++ {
		s.Depths[i] = int(d[i])
	}
	if err := binary.Read(aux, binary.BigEndian, &s.TouchMap); err != nil {
		return fmt.Errorf("touchMap: %w", err)
	}
	if err := binary.Read(aux, binary.BigEndian, &s.AfterMap); err != nil {
		return fmt.Errorf("afterMap: %w", err)
	}
	var branch1, branch2 uint64
	if err := binary.Read(aux, binary.BigEndian, &branch1); err != nil {
		return fmt.Errorf("branchBefore1: %w", err)
	}
	if err := binary.Read(aux, binary.BigEndian, &branch2); err != nil {
		return fmt.Errorf("branchBefore2: %w", err)
	}

	for i := 0; i < 64; i++ {
		if branch1&(1<<i) != 0 {
			s.BranchBefore[i] = true
		}
	}
	for i, j := 64, 0; i < 128; i, j = i+1, j+1 {
		if branch2&(1<<j) != 0 {
			s.BranchBefore[i] = true
		}
	}
	return nil
}

func (c *Cell) bytes() []byte {
	var pos = 1
	size := 1 + c.hl + 1 + c.apl + c.spl + 1 + c.downHashedLen + 1 + c.extLen + 1 // max size
	buf := make([]byte, size)

	var flags uint8
	if c.hl != 0 {
		flags |= 1
		buf[pos] = byte(c.hl)
		pos++
		copy(buf[pos:pos+c.hl], c.h[:])
		pos += c.hl
	}
	if c.apl != 0 {
		flags |= 2
		buf[pos] = byte(c.hl)
		pos++
		copy(buf[pos:pos+c.apl], c.apk[:])
		pos += c.apl
	}
	if c.spl != 0 {
		flags |= 4
		buf[pos] = byte(c.spl)
		pos++
		copy(buf[pos:pos+c.spl], c.spk[:])
		pos += c.spl
	}
	if c.downHashedLen != 0 {
		flags |= 8
		buf[pos] = byte(c.downHashedLen)
		pos++
		copy(buf[pos:pos+c.downHashedLen], c.downHashedKey[:])
		pos += c.downHashedLen
	}
	if c.extLen != 0 {
		flags |= 16
		buf[pos] = byte(c.extLen)
		pos++
		copy(buf[pos:pos+c.downHashedLen], c.downHashedKey[:])
		//pos += c.downHashedLen
	}
	buf[0] = flags
	return buf
}

func (c *Cell) decodeBytes(buf []byte) error {
	if len(buf) < 1 {
		return fmt.Errorf("invalid buffer size to contain Cell (at least 1 byte expected)")
	}
	c.fillEmpty()

	var pos int
	flags := buf[pos]
	pos++

	if flags&1 != 0 {
		c.hl = int(buf[pos])
		pos++
		copy(c.h[:], buf[pos:pos+c.hl])
		pos += c.hl
	}
	if flags&2 != 0 {
		c.apl = int(buf[pos])
		pos++
		copy(c.apk[:], buf[pos:pos+c.apl])
		pos += c.apl
	}
	if flags&4 != 0 {
		c.spl = int(buf[pos])
		pos++
		copy(c.spk[:], buf[pos:pos+c.spl])
		pos += c.spl
	}
	if flags&8 != 0 {
		c.downHashedLen = int(buf[pos])
		pos++
		copy(c.downHashedKey[:], buf[pos:pos+c.downHashedLen])
		pos += c.downHashedLen
	}
	if flags&16 != 0 {
		c.extLen = int(buf[pos])
		pos++
		copy(c.extension[:], buf[pos:pos+c.extLen])
		//pos += c.extLen
	}
	return nil
}

// Encode current state of hph into bytes
func (hph *HexPatriciaHashed) EncodeCurrentState(buf []byte) ([]byte, error) {
	s := state{
		CurrentKeyLen: int8(hph.currentKeyLen),
		RootChecked:   hph.rootChecked,
		RootTouched:   hph.rootTouched,
		RootPresent:   hph.rootPresent,
		Root:          make([]byte, 0),
	}

	s.Root = hph.root.bytes()
	copy(s.CurrentKey[:], hph.currentKey[:])
	copy(s.Depths[:], hph.depths[:])
	copy(s.BranchBefore[:], hph.branchBefore[:])
	copy(s.TouchMap[:], hph.touchMap[:])
	copy(s.AfterMap[:], hph.afterMap[:])

	return s.Encode(buf)
}

// buf expected to be encoded hph state. Decode state and set up hph to that state.
func (hph *HexPatriciaHashed) SetState(buf []byte) error {
	if hph.activeRows != 0 {
		return fmt.Errorf("has active rows, could not reset state")
	}

	var s state
	if err := s.Decode(buf); err != nil {
		return err
	}

	hph.Reset()

	if err := hph.root.decodeBytes(s.Root); err != nil {
		return err
	}

	hph.currentKeyLen = int(s.CurrentKeyLen)
	hph.rootChecked = s.RootChecked
	hph.rootTouched = s.RootTouched
	hph.rootPresent = s.RootPresent

	copy(hph.currentKey[:], s.CurrentKey[:])
	copy(hph.depths[:], s.Depths[:])
	copy(hph.branchBefore[:], s.BranchBefore[:])
	copy(hph.touchMap[:], s.TouchMap[:])
	copy(hph.afterMap[:], s.AfterMap[:])

	return nil
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

func hexToCompact(key []byte) []byte {
	zeroByte, keyPos, keyLen := makeCompactZeroByte(key)
	bufLen := keyLen/2 + 1 // always > 0
	buf := make([]byte, bufLen)
	buf[0] = zeroByte
	return decodeKey(key[keyPos:], buf)
}

func makeCompactZeroByte(key []byte) (compactZeroByte byte, keyPos, keyLen int) {
	keyLen = len(key)
	if hasTerm(key) {
		keyLen--
		compactZeroByte = 0x20
	}
	var firstNibble byte
	if len(key) > 0 {
		firstNibble = key[0]
	}
	if keyLen&1 == 1 {
		compactZeroByte |= 0x10 | firstNibble // Odd: (1<<4) + first nibble
		keyPos++
	}

	return
}

func decodeKey(key, buf []byte) []byte {
	keyLen := len(key)
	if hasTerm(key) {
		keyLen--
	}
	for keyIndex, bufIndex := 0, 1; keyIndex < keyLen; keyIndex, bufIndex = keyIndex+2, bufIndex+1 {
		if keyIndex == keyLen-1 {
			buf[bufIndex] = buf[bufIndex] & 0x0f
		} else {
			buf[bufIndex] = key[keyIndex+1]
		}
		buf[bufIndex] |= key[keyIndex] << 4
	}
	return buf
}

func CompactedKeyToHex(compact []byte) []byte {
	if len(compact) == 0 {
		return compact
	}
	base := keybytesToHexNibbles(compact)
	// delete terminator flag
	if base[0] < 2 {
		base = base[:len(base)-1]
	}
	// apply odd flag
	chop := 2 - base[0]&1
	return base[chop:]
}

func keybytesToHexNibbles(str []byte) []byte {
	l := len(str)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return nibbles
}

// hasTerm returns whether a hex key has the terminator flag.
func hasTerm(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 16
}

func commonPrefixLen(b1, b2 []byte) int {
	var i int
	for i = 0; i < len(b1) && i < len(b2); i++ {
		if b1[i] != b2[i] {
			break
		}
	}
	return i
}

func (hph *HexPatriciaHashed) ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (rootHash []byte, branchNodeUpdates map[string]BranchData, err error) {
	branchNodeUpdates = make(map[string]BranchData)

	for i, plainKey := range plainKeys {
		hashedKey := hashedKeys[i]
		if hph.trace {
			fmt.Printf("plainKey=[%x], hashedKey=[%x], currentKey=[%x]\n", plainKey, hashedKey, hph.currentKey[:hph.currentKeyLen])
		}
		// Keep folding until the currentKey is the prefix of the key we modify
		for hph.needFolding(hashedKey) {
			if branchData, updateKey, err := hph.fold(); err != nil {
				return nil, nil, fmt.Errorf("fold: %w", err)
			} else if branchData != nil {
				branchNodeUpdates[string(updateKey)] = branchData
			}
		}
		// Now unfold until we step on an empty cell
		for unfolding := hph.needUnfolding(hashedKey); unfolding > 0; unfolding = hph.needUnfolding(hashedKey) {
			if err := hph.unfold(hashedKey, unfolding); err != nil {
				return nil, nil, fmt.Errorf("unfold: %w", err)
			}
		}

		update := updates[i]
		// Update the cell
		if update.Flags == DeleteUpdate {
			hph.deleteCell(hashedKey)
			if hph.trace {
				fmt.Printf("key %x deleted\n", plainKey)
			}
		} else {
			cell := hph.updateCell(plainKey, hashedKey)
			if hph.trace {
				fmt.Printf("accountFn updated key %x =>", plainKey)
			}
			if update.Flags&BalanceUpdate != 0 {
				if hph.trace {
					fmt.Printf(" balance=%d", update.Balance.Uint64())
				}
				cell.Balance.Set(&update.Balance)
			}
			if update.Flags&NonceUpdate != 0 {
				if hph.trace {
					fmt.Printf(" nonce=%d", update.Nonce)
				}
				cell.Nonce = update.Nonce
			}
			if update.Flags&CodeUpdate != 0 {
				if hph.trace {
					fmt.Printf(" codeHash=%x", update.CodeHashOrStorage)
				}
				copy(cell.CodeHash[:], update.CodeHashOrStorage[:])
			}
			if hph.trace {
				fmt.Printf("\n")
			}
			if update.Flags&StorageUpdate != 0 {
				cell.setStorage(update.CodeHashOrStorage[:update.ValLength])
				if hph.trace {
					fmt.Printf("\rstorageFn filled key %x => %x\n", plainKey, update.CodeHashOrStorage[:update.ValLength])
				}
			}
		}
	}
	// Folding everything up to the root
	for hph.activeRows > 0 {
		if branchData, updateKey, err := hph.fold(); err != nil {
			return nil, nil, fmt.Errorf("final fold: %w", err)
		} else if branchData != nil {
			branchNodeUpdates[string(updateKey)] = branchData
		}
	}

	rootHash, err = hph.RootHash()
	if err != nil {
		return nil, branchNodeUpdates, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	return rootHash, branchNodeUpdates, nil
}

// nolint
// Hashes provided key and expands resulting hash into nibbles (each byte split into two nibbles by 4 bits)
func (hph *HexPatriciaHashed) hashAndNibblizeKey(key []byte) []byte {
	hashedKey := make([]byte, length.Hash)

	hph.keccak.Reset()
	hph.keccak.Write(key[:length.Addr])
	copy(hashedKey[:length.Hash], hph.keccak.Sum(nil))

	if len(key[length.Addr:]) > 0 {
		hashedKey = append(hashedKey, make([]byte, length.Hash)...)
		hph.keccak.Reset()
		hph.keccak.Write(key[length.Addr:])
		copy(hashedKey[length.Hash:], hph.keccak.Sum(nil))
	}

	nibblized := make([]byte, len(hashedKey)*2)
	for i, b := range hashedKey {
		nibblized[i*2] = (b >> 4) & 0xf
		nibblized[i*2+1] = b & 0xf
	}
	return nibblized
}

type UpdateFlags uint8

const (
	CodeUpdate    UpdateFlags = 1
	DeleteUpdate  UpdateFlags = 2
	BalanceUpdate UpdateFlags = 4
	NonceUpdate   UpdateFlags = 8
	StorageUpdate UpdateFlags = 16
)

func (uf UpdateFlags) String() string {
	var sb strings.Builder
	if uf == DeleteUpdate {
		sb.WriteString("Delete")
	} else {
		if uf&BalanceUpdate != 0 {
			sb.WriteString("+Balance")
		}
		if uf&NonceUpdate != 0 {
			sb.WriteString("+Nonce")
		}
		if uf&CodeUpdate != 0 {
			sb.WriteString("+Code")
		}
		if uf&StorageUpdate != 0 {
			sb.WriteString("+Storage")
		}
	}
	return sb.String()
}

type Update struct {
	Flags             UpdateFlags
	Balance           uint256.Int
	Nonce             uint64
	CodeHashOrStorage [length.Hash]byte
	ValLength         int
}

func (u *Update) DecodeForStorage(enc []byte) {
	u.Nonce = 0
	u.Balance.Clear()
	copy(u.CodeHashOrStorage[:], EmptyCodeHash)

	pos := 0
	nonceBytes := int(enc[pos])
	pos++
	if nonceBytes > 0 {
		u.Nonce = bytesToUint64(enc[pos : pos+nonceBytes])
		pos += nonceBytes
	}
	balanceBytes := int(enc[pos])
	pos++
	if balanceBytes > 0 {
		u.Balance.SetBytes(enc[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	codeHashBytes := int(enc[pos])
	pos++
	if codeHashBytes > 0 {
		copy(u.CodeHashOrStorage[:], enc[pos:pos+codeHashBytes])
	}
}

func (u *Update) Encode(buf []byte, numBuf []byte) []byte {
	buf = append(buf, byte(u.Flags))
	if u.Flags&BalanceUpdate != 0 {
		buf = append(buf, byte(u.Balance.ByteLen()))
		buf = append(buf, u.Balance.Bytes()...)
	}
	if u.Flags&NonceUpdate != 0 {
		n := binary.PutUvarint(numBuf, u.Nonce)
		buf = append(buf, numBuf[:n]...)
	}
	if u.Flags&CodeUpdate != 0 {
		buf = append(buf, u.CodeHashOrStorage[:]...)
	}
	if u.Flags&StorageUpdate != 0 {
		n := binary.PutUvarint(numBuf, uint64(u.ValLength))
		buf = append(buf, numBuf[:n]...)
		if u.ValLength > 0 {
			buf = append(buf, u.CodeHashOrStorage[:u.ValLength]...)
		}
	}
	return buf
}

func (u *Update) Decode(buf []byte, pos int) (int, error) {
	if len(buf) < pos+1 {
		return 0, fmt.Errorf("decode Update: buffer too small for flags")
	}
	u.Flags = UpdateFlags(buf[pos])
	pos++
	if u.Flags&BalanceUpdate != 0 {
		if len(buf) < pos+1 {
			return 0, fmt.Errorf("decode Update: buffer too small for balance len")
		}
		balanceLen := int(buf[pos])
		pos++
		if len(buf) < pos+balanceLen {
			return 0, fmt.Errorf("decode Update: buffer too small for balance")
		}
		u.Balance.SetBytes(buf[pos : pos+balanceLen])
		pos += balanceLen
	}
	if u.Flags&NonceUpdate != 0 {
		var n int
		u.Nonce, n = binary.Uvarint(buf[pos:])
		if n == 0 {
			return 0, fmt.Errorf("decode Update: buffer too small for nonce")
		}
		if n < 0 {
			return 0, fmt.Errorf("decode Update: nonce overflow")
		}
		pos += n
	}
	if u.Flags&CodeUpdate != 0 {
		if len(buf) < pos+32 {
			return 0, fmt.Errorf("decode Update: buffer too small for codeHash")
		}
		copy(u.CodeHashOrStorage[:], buf[pos:pos+32])
		pos += 32
	}
	if u.Flags&StorageUpdate != 0 {
		l, n := binary.Uvarint(buf[pos:])
		if n == 0 {
			return 0, fmt.Errorf("decode Update: buffer too small for storage len")
		}
		if n < 0 {
			return 0, fmt.Errorf("decode Update: storage lee overflow")
		}
		pos += n
		if len(buf) < pos+int(l) {
			return 0, fmt.Errorf("decode Update: buffer too small for storage")
		}
		u.ValLength = int(l)
		copy(u.CodeHashOrStorage[:], buf[pos:pos+int(l)])
		pos += int(l)
	}
	return pos, nil
}

func (u *Update) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Flags: [%s]", u.Flags))
	if u.Flags&BalanceUpdate != 0 {
		sb.WriteString(fmt.Sprintf(", Balance: [%d]", &u.Balance))
	}
	if u.Flags&NonceUpdate != 0 {
		sb.WriteString(fmt.Sprintf(", Nonce: [%d]", u.Nonce))
	}
	if u.Flags&CodeUpdate != 0 {
		sb.WriteString(fmt.Sprintf(", CodeHash: [%x]", u.CodeHashOrStorage))
	}
	if u.Flags&StorageUpdate != 0 {
		sb.WriteString(fmt.Sprintf(", Storage: [%x]", u.CodeHashOrStorage[:u.ValLength]))
	}
	return sb.String()
}
