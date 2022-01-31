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
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"strings"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/rlp"
	"golang.org/x/crypto/sha3"
)

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

type ByteArrayWriter struct {
	buf []byte
}

func (w *ByteArrayWriter) Setup(buf []byte) {
	w.buf = buf
}

func (w *ByteArrayWriter) Write(data []byte) (int, error) {
	w.buf = append(w.buf, data...)
	return len(data), nil
}

// HexPatriciaHashed implements commitment based on patricia merkle tree with radix 16,
// with keys pre-hashed by keccak256
type HexPatriciaHashed struct {
	root Cell // Root cell of the tree
	// Rows of the grid correspond to the level of depth in the patricia tree
	// Columns of the grid correspond to pointers to the nodes further from the root
	grid [128][16]Cell // First 64 rows of this grid are for account trie, and next 64 rows are for storage trie
	// How many rows (starting from row 0) are currently active and have corresponding selected columns
	// Last active row does not have selected column
	activeRows int
	// Length of the key that reflects current positioning of the grid. It maybe larger than number of active rows,
	// if a account leaf cell represents multiple nibbles in the key
	currentKeyLen int
	currentKey    [128]byte // For each row indicates which column is currently selected
	depths        [128]int  // For each row, the depth of cells in that row
	rootChecked   bool      // Set to false if it is not known whether the root is empty, set to true if it is checked
	rootMod       bool
	rootDel       bool
	beforeBitmap  [128]uint16 // For each row, bitmap of cells that were present before modification
	modBitmap     [128]uint16 // For each row, bitmap of cells that were modified (not deleted)
	delBitmap     [128]uint16 // For each row, bitmap of cells that were deleted
	// Function used to load branch node and fill up the cells
	// For each cell, it sets the cell type, clears the modified flag, fills the hash,
	// and for the extension, account, and leaf type, the `l` and `k`
	lockFn   func()
	unlockFn func()
	branchFn func(prefix []byte) ([]byte, error)
	// Function used to fetch account with given plain key. It loads
	accountFn func(plainKey []byte, cell *Cell) ([]byte, error)
	// Function used to fetch account with given plain key
	storageFn       func(plainKey []byte, cell *Cell) ([]byte, error)
	keccak          keccakState
	keccak2         keccakState
	accountKeyLen   int
	trace           bool
	numBuf          [binary.MaxVarintLen64]byte
	byteArrayWriter ByteArrayWriter
	hashBuf         [33]byte // RLP representation of hash (or un-hashes value)
	keyPrefix       [1]byte
	lenPrefix       [4]byte
	valBuf          [128]byte // Enough to accommodate hash encoding of any account
	b               [1]byte   // Buffer for single byte
	prefixBuf       [8]byte
}

func NewHexPatriciaHashed(accountKeyLen int,
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) ([]byte, error),
	storageFn func(plainKey []byte, cell *Cell) ([]byte, error),
	lockFn func(),
	unlockFn func(),
) *HexPatriciaHashed {
	return &HexPatriciaHashed{
		keccak:        sha3.NewLegacyKeccak256().(keccakState),
		keccak2:       sha3.NewLegacyKeccak256().(keccakState),
		accountKeyLen: accountKeyLen,
		branchFn:      branchFn,
		accountFn:     accountFn,
		storageFn:     storageFn,
		lockFn:        lockFn,
		unlockFn:      unlockFn,
	}
}

type Cell struct {
	h             [32]byte // cell hash
	hl            int      // Length of the hash (or embedded)
	apk           [20]byte // account plain key
	apl           int      // length of account plain key
	spk           [52]byte // storage plain key
	spl           int      // length of the storage plain key
	downHashedKey [128]byte
	downHashedLen int
	extension     [64]byte
	extLen        int
	Nonce         uint64
	Balance       uint256.Int
	CodeHash      [32]byte // hash of the bytecode
	Storage       [32]byte
	StorageLen    int
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

func hashKey(keccak keccakState, plainKey []byte, hashBuf []byte, dest []byte, hashedKeyOffset int) error {
	keccak.Reset()
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

func (cell *Cell) deriveHashedKeys(depth int, keccak keccakState, hashBuf []byte, accountKeyLen int) error {
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
			if err := hashKey(keccak, cell.apk[:cell.apl], hashBuf, cell.downHashedKey[:], depth); err != nil {
				return err
			}
			downOffset = 64 - depth
		}
		if cell.spl > 0 {
			if depth >= 64 {
				hashedKeyOffset = depth - 64
			}
			if err := hashKey(keccak, cell.spk[accountKeyLen:cell.spl], hashBuf, cell.downHashedKey[downOffset:], hashedKeyOffset); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cell *Cell) fillFromFields(data []byte, pos int, fieldBits PartFlags) (int, error) {
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

func (hph *HexPatriciaHashed) SetTrace(trace bool) {
	hph.trace = trace
}

// hasTerm returns whether a hex key has the terminator flag.
func hasTerm(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 16
}

func (hph *HexPatriciaHashed) completeLeafHash(buf []byte, kp, kl, compactLen int, key []byte, compact0 byte, ni int, val rlp.RlpSerializable, singleton bool) ([]byte, error) {
	totalLen := kp + kl + val.DoubleRLPLen()
	pt := rlp.GenerateStructLen(hph.lenPrefix[:], totalLen)
	embedded := !singleton && totalLen+pt < length.Hash
	var writer io.Writer
	if embedded {
		hph.byteArrayWriter.Setup(buf)
		writer = &hph.byteArrayWriter
	} else {
		hph.keccak.Reset()
		writer = hph.keccak
	}
	if _, err := writer.Write(hph.lenPrefix[:pt]); err != nil {
		return nil, err
	}
	if _, err := writer.Write(hph.keyPrefix[:kp]); err != nil {
		return nil, err
	}
	hph.b[0] = compact0
	if _, err := writer.Write(hph.b[:]); err != nil {
		return nil, err
	}
	for i := 1; i < compactLen; i++ {
		hph.b[0] = key[ni]*16 + key[ni+1]
		if _, err := writer.Write(hph.b[:]); err != nil {
			return nil, err
		}
		ni += 2
	}
	if err := val.ToDoubleRLP(writer, hph.prefixBuf[:]); err != nil {
		return nil, err
	}
	if embedded {
		buf = hph.byteArrayWriter.buf
	} else {
		hph.hashBuf[0] = 0x80 + length.Hash
		if _, err := hph.keccak.Read(hph.hashBuf[1:]); err != nil {
			return nil, err
		}
		buf = append(buf, hph.hashBuf[:]...)
	}
	return buf, nil
}

func (hph *HexPatriciaHashed) leafHashWithKeyVal(buf []byte, key []byte, val rlp.RlpSerializableBytes, singleton bool) ([]byte, error) {
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
	if compactLen > 1 {
		hph.keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	buf, err := hph.completeLeafHash(buf, kp, kl, compactLen, key, compact0, ni, val, singleton)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (cell *Cell) accountForHashing(buffer []byte, storageRootHash []byte) int {
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

func (hph *HexPatriciaHashed) accountLeafHashWithKey(buf []byte, key []byte, val rlp.RlpSerializable) ([]byte, error) {
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
	if compactLen > 1 {
		hph.keyPrefix[0] = byte(128 + compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	var err error
	if buf, err = hph.completeLeafHash(buf, kp, kl, compactLen, key, compact0, ni, val, true); err != nil {
		return nil, err
	}
	return buf, nil
}

func (hph *HexPatriciaHashed) extensionHash(buf []byte, key []byte, hash []byte) ([]byte, error) {
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
	if compactLen > 1 {
		hph.keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	totalLen := kp + kl + 33
	pt := rlp.GenerateStructLen(hph.lenPrefix[:], totalLen)
	hph.keccak.Reset()
	if _, err := hph.keccak.Write(hph.lenPrefix[:pt]); err != nil {
		return nil, err
	}
	if _, err := hph.keccak.Write(hph.keyPrefix[:kp]); err != nil {
		return nil, err
	}
	hph.b[0] = compact0
	if _, err := hph.keccak.Write(hph.b[:]); err != nil {
		return nil, err
	}
	for i := 1; i < compactLen; i++ {
		hph.b[0] = key[ni]*16 + key[ni+1]
		if _, err := hph.keccak.Write(hph.b[:]); err != nil {
			return nil, err
		}
		ni += 2
	}
	hph.b[0] = 0x80 + length.Hash
	if _, err := hph.keccak.Write(hph.b[:]); err != nil {
		return nil, err
	}
	if _, err := hph.keccak.Write(hash); err != nil {
		return nil, err
	}
	// Replace previous hash with the new one
	if _, err := hph.keccak.Read(hph.hashBuf[:length.Hash]); err != nil {
		return nil, err
	}
	if buf == nil {
		return hph.hashBuf[:length.Hash], nil
	}
	buf = append(buf, hph.hashBuf[:length.Hash]...)
	return buf, nil
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
		pt := rlp.GenerateStructLen(hph.lenPrefix[:], totalLen)
		if totalLen+pt < length.Hash {
			return totalLen + pt
		}
	}
	return length.Hash + 1
}

func (hph *HexPatriciaHashed) computeCellHash(cell *Cell, depth int, buf []byte) ([]byte, error) {
	var err error
	var storageRootHash []byte
	if cell.spl > 0 {
		var hashedKeyOffset int
		if depth >= 64 {
			hashedKeyOffset = depth - 64
		}
		singleton := depth <= 64
		if err := hashKey(hph.keccak, cell.spk[hph.accountKeyLen:cell.spl], hph.hashBuf[:length.Hash], cell.downHashedKey[:], hashedKeyOffset); err != nil {
			return nil, err
		}
		cell.downHashedKey[64-hashedKeyOffset] = 16 // Add terminator
		if singleton {
			if hph.trace {
				fmt.Printf("leafHashWithKeyVal(singleton) for [%x]=>[%x]\n", cell.downHashedKey[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
			}
			if storageRootHash, err = hph.leafHashWithKeyVal(nil, cell.downHashedKey[:64-hashedKeyOffset+1], rlp.RlpSerializableBytes(cell.Storage[:cell.StorageLen]), true); err != nil {
				return nil, err
			}
			storageRootHash = storageRootHash[1:]
		} else {
			if hph.trace {
				fmt.Printf("leafHashWithKeyVal for [%x]=>[%x]\n", cell.downHashedKey[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
			}
			if buf, err = hph.leafHashWithKeyVal(buf, cell.downHashedKey[:64-hashedKeyOffset+1], rlp.RlpSerializableBytes(cell.Storage[:cell.StorageLen]), false); err != nil {
				return nil, err
			}
			return buf, nil
		}
	}
	if cell.apl > 0 {
		if err := hashKey(hph.keccak, cell.apk[:cell.apl], hph.hashBuf[:length.Hash], cell.downHashedKey[:], depth); err != nil {
			return nil, err
		}
		cell.downHashedKey[64-depth] = 16 // Add terminator
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
		valLen := cell.accountForHashing(hph.valBuf[:], storageRootHash)
		if hph.trace {
			fmt.Printf("accountLeafHashWithKey for [%x]=>[%x]\n", cell.downHashedKey[:65-depth], hph.valBuf[:valLen])
		}
		if buf, err = hph.accountLeafHashWithKey(buf, cell.downHashedKey[:65-depth], rlp.RlpEncodedBytes(hph.valBuf[:valLen])); err != nil {
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

type PartFlags uint8

const (
	HASHEDKEY_PART     PartFlags = 1
	ACCOUNT_PLAIN_PART PartFlags = 2
	STORAGE_PLAIN_PART PartFlags = 4
	HASH_PART          PartFlags = 8
)

func branchToString(branchData []byte) string {
	var sb strings.Builder
	pos := 0
	partBitmap := binary.BigEndian.Uint16(branchData[pos:])
	pos += 2
	fieldsPos := pos
	partCount := bits.OnesCount16(partBitmap)
	pos += (partCount + 1) / 2 // 1 bytes per two parts
	fmt.Fprintf(&sb, "mask %016b\n", partBitmap)
	var cell Cell
	for bitset, j := partBitmap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		fieldBits := branchData[fieldsPos+j/2]
		if j%2 == 1 {
			fieldBits >>= 4
		} else {
			fieldBits &= 0xf
		}
		var err error
		if pos, err = cell.fillFromFields(branchData, pos, PartFlags(fieldBits)); err != nil {
			// This is used for test output, so ok to panic
			panic(err)
		}
		fmt.Fprintf(&sb, "   %x => ", nibble)
		sb.WriteString("{")
		var comma string
		if cell.downHashedLen > 0 {
			fmt.Fprintf(&sb, "hashedKey=[%x]", cell.downHashedKey[:cell.downHashedLen])
			comma = ","
		}
		if cell.apl > 0 {
			fmt.Fprintf(&sb, "%saccountPlainKey=[%x]", comma, cell.apk[:cell.apl])
			comma = ","
		}
		if cell.spl > 0 {
			fmt.Fprintf(&sb, "%sstoragePlainKey=[%x]", comma, cell.spk[:cell.spl])
			comma = ","
		}
		if cell.hl > 0 {
			fmt.Fprintf(&sb, "%shash=[%x]", comma, cell.h[:cell.hl])
			comma = ","
		}
		sb.WriteString("}\n")
		bitset ^= bit
	}
	return sb.String()
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
}

func (hph *HexPatriciaHashed) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) ([]byte, error),
	storageFn func(plainKey []byte, cell *Cell) ([]byte, error),
	lockFn func(),
	unlockFn func(),
) {
	hph.branchFn = branchFn
	hph.accountFn = accountFn
	hph.storageFn = storageFn
	hph.lockFn = lockFn
	hph.unlockFn = unlockFn
}

func (hph *HexPatriciaHashed) needUnfolding(hashedKey []byte) int {
	var cell *Cell
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
	if depth < 64 && depth+unfolding > 64 {
		// This is to make sure that unfolding always breaks at the level where storage subtrees start
		unfolding = 64 - depth
		if hph.trace {
			fmt.Printf("adjusted unfolding=%d\n", unfolding)
		}
	}
	return unfolding
}

func (hph *HexPatriciaHashed) unfold(hashedKey []byte, unfolding int) error {
	if hph.trace {
		fmt.Printf("unfold %d: activeRows: %d\n", unfolding, hph.activeRows)
	}
	var upCell *Cell
	var before, modified, deleted bool
	var col byte
	var upDepth, depth int
	if hph.activeRows == 0 {
		if hph.rootChecked && hph.root.hl == 0 && hph.root.downHashedLen == 0 {
			// No unfolding for empty root
			return nil
		}
		upCell = &hph.root
		before = true
		modified = hph.rootMod
		deleted = hph.rootDel
	} else {
		upDepth = hph.depths[hph.activeRows-1]
		col = hashedKey[upDepth-1]
		upCell = &hph.grid[hph.activeRows-1][col]
		before = hph.beforeBitmap[hph.activeRows-1]&(uint16(1)<<col) != 0
		modified = hph.modBitmap[hph.activeRows-1]&(uint16(1)<<col) != 0
		deleted = hph.delBitmap[hph.activeRows-1]&(uint16(1)<<col) != 0
		if hph.trace {
			fmt.Printf("upCell (%d, %x), before %t, modified %t, deleted %t\n", hph.activeRows-1, col, before, modified, deleted)
		}
		hph.currentKey[hph.currentKeyLen] = col
		hph.currentKeyLen++
	}
	row := hph.activeRows
	for i := 0; i < 16; i++ {
		hph.grid[row][i].fillEmpty()
	}
	hph.beforeBitmap[row] = 0
	hph.modBitmap[row] = 0
	hph.delBitmap[row] = 0
	if upCell.downHashedLen == 0 {
		depth = upDepth + 1
		hph.lockFn()
		branchData, err := hph.branchFn(hexToCompact(hph.currentKey[:hph.currentKeyLen]))
		if err != nil {
			hph.unlockFn()
			return err
		}
		if !hph.rootChecked && hph.currentKeyLen == 0 && len(branchData) == 0 {
			// Special case - empty or deleted root
			hph.unlockFn()
			hph.rootChecked = true
			return nil
		}
		if len(branchData) == 0 {
			fmt.Printf("branchData empty for [%x]\n", hph.currentKey[:hph.currentKeyLen])
		}
		pos := 0
		partBitmap := binary.BigEndian.Uint16(branchData[pos:])
		pos += 2
		hph.beforeBitmap[row] = partBitmap
		if deleted {
			hph.delBitmap[row] = partBitmap
		}
		// Next, for each part, we have bitmap of fields
		partCount := bits.OnesCount16(partBitmap)
		fieldsPos := pos
		pos += (partCount + 1) / 2 // 1 bytes per two parts
		// Loop iterating over the set bits of modMask
		for bitset, j := partBitmap, 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			cell := &hph.grid[row][nibble]
			fieldBits := branchData[fieldsPos+j/2]
			if j%2 == 1 {
				fieldBits >>= 4
			}
			if pos, err = cell.fillFromFields(branchData, pos, PartFlags(fieldBits)); err != nil {
				hph.unlockFn()
				return err
			}
			if hph.trace {
				fmt.Printf("cell (%d, %x) depth=%d, hash=[%x], a=[%x], s=[%x], ex=[%x]\n", row, nibble, depth, cell.h[:cell.hl], cell.apk[:cell.apl], cell.spk[:cell.spl], cell.extension[:cell.extLen])
			}
			if cell.apl > 0 {
				var k []byte
				if k, err = hph.accountFn(cell.apk[:cell.apl], cell); err != nil {
					hph.unlockFn()
					return err
				}
				cell.apl = len(k)
				copy(cell.apk[:], k)
				if hph.trace {
					fmt.Printf("accountFn[%x] return balance=%d, nonce=%d\n", cell.apk[:cell.apl], &cell.Balance, cell.Nonce)
				}
			}
			if cell.spl > 0 {
				var k []byte
				if k, err = hph.storageFn(cell.spk[:cell.spl], cell); err != nil {
					hph.unlockFn()
					return err
				}
				cell.spl = len(k)
				copy(cell.spk[:], k)
			}
			if err = cell.deriveHashedKeys(depth, hph.keccak, hph.hashBuf[:length.Hash], hph.accountKeyLen); err != nil {
				hph.unlockFn()
				return err
			}
			bitset ^= bit
		}
		hph.unlockFn()
	} else if upCell.downHashedLen >= unfolding {
		depth = upDepth + unfolding
		nibble := upCell.downHashedKey[unfolding-1]
		if before {
			hph.beforeBitmap[row] = uint16(1) << nibble
		}
		if deleted {
			hph.delBitmap[row] = uint16(1) << nibble
			if hph.trace {
				fmt.Printf("delBitmap[%d]=%016b\n", row, hph.delBitmap[row])
			}
		} else if modified {
			hph.modBitmap[row] = uint16(1) << nibble
			if hph.trace {
				fmt.Printf("modBitmap[%d]=%016b\n", row, hph.modBitmap[row])
			}
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
		if before {
			hph.beforeBitmap[row] = uint16(1) << nibble
		}
		if deleted {
			hph.delBitmap[row] = uint16(1) << nibble
			if hph.trace {
				fmt.Printf("delBitmap[%d]=%016b\n", row, hph.delBitmap[row])
			}
		} else if modified {
			hph.modBitmap[row] = uint16(1) << nibble
			if hph.trace {
				fmt.Printf("modBitmap[%d]=%016b\n", row, hph.modBitmap[row])
			}
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

func (hph *HexPatriciaHashed) foldRoot() ([]byte, error) {
	if hph.trace {
		fmt.Printf("foldRoot: activeRows: %d\n", hph.activeRows)
	}
	if hph.activeRows != 0 {
		return nil, fmt.Errorf("cannot fold root - there are still active rows: %d", hph.activeRows)
	}
	if hph.root.downHashedLen == 0 {
		return nil, nil
	}
	var branchData []byte
	binary.BigEndian.PutUint16(hph.numBuf[:], 1)
	branchData = append(branchData, hph.numBuf[:2]...)
	fieldsPos := 2
	branchData = append(branchData, 0)
	var fieldBits PartFlags
	cell := &hph.root
	if cell.extLen > 0 {
		fieldBits |= HASHEDKEY_PART
		n := binary.PutUvarint(hph.numBuf[:], uint64(cell.extLen))
		branchData = append(branchData, hph.numBuf[:n]...)
		branchData = append(branchData, cell.extension[:cell.extLen]...)
	}
	if cell.apl > 0 {
		fieldBits |= ACCOUNT_PLAIN_PART
		n := binary.PutUvarint(hph.numBuf[:], uint64(cell.apl))
		branchData = append(branchData, hph.numBuf[:n]...)
		branchData = append(branchData, cell.apk[:cell.apl]...)
	}
	if cell.spl > 0 {
		fieldBits |= STORAGE_PLAIN_PART
		n := binary.PutUvarint(hph.numBuf[:], uint64(cell.spl))
		branchData = append(branchData, hph.numBuf[:n]...)
		branchData = append(branchData, cell.spk[:cell.spl]...)
	}
	if cell.hl > 0 {
		fieldBits |= HASH_PART
		n := binary.PutUvarint(hph.numBuf[:], uint64(cell.hl))
		branchData = append(branchData, hph.numBuf[:n]...)
		branchData = append(branchData, cell.h[:cell.hl]...)
	}
	branchData[fieldsPos] = byte(fieldBits)
	return branchData, nil
}

func (hph *HexPatriciaHashed) needFolding(hashedKey []byte) bool {
	return !bytes.HasPrefix(hashedKey, hph.currentKey[:hph.currentKeyLen])
}

// The purpose of fold is to reduce hph.currentKey[:hph.currentKeyLen]. It should be invoked
// until that current key becomes a prefix of hashedKey that we will proccess next
// (in other words until the needFolding function returns 0)
func (hph *HexPatriciaHashed) fold() ([]byte, []byte, error) {
	updateKey := hexToCompact(hph.currentKey[:hph.currentKeyLen])
	if hph.activeRows == 0 {
		return nil, nil, fmt.Errorf("cannot fold - no active rows")
	}
	if hph.trace {
		fmt.Printf("fold: activeRows: %d, currentKey: [%x], modBitmap: %016b, delBitmap: %016b\n", hph.activeRows, hph.currentKey[:hph.currentKeyLen], hph.modBitmap[hph.activeRows-1], hph.delBitmap[hph.activeRows-1])
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
	var branchData []byte
	if hph.trace {
		fmt.Printf("beforeBitmap[%d]=%016b, modBitmap[%d]=%016b, delBitmap[%d]=%016b\n", row, hph.beforeBitmap[row], row, hph.modBitmap[row], row, hph.delBitmap[row])
	}
	bitmap := (hph.beforeBitmap[row] | hph.modBitmap[row]) ^ hph.delBitmap[row]
	partsCount := bits.OnesCount16(bitmap)
	switch partsCount {
	case 0:
		// Everything deleted
		if hph.delBitmap[row] != 0 {
			if row == 0 {
				hph.rootDel = true
			} else if upDepth != 64 {
				hph.delBitmap[row-1] |= (uint16(1) << col)
				if hph.trace {
					fmt.Printf("del delBitmap[%d]=%016b\n", row-1, hph.delBitmap[row-1])
				}
			}
		}
		upCell.hl = 0
		upCell.apl = 0
		upCell.spl = 0
		upCell.extLen = 0
		upCell.downHashedLen = 0
		if bits.OnesCount16(hph.beforeBitmap[row]) > 1 {
			// Deletion
			branchData = []byte{}
		}
		hph.activeRows--
		if upDepth > 0 {
			hph.currentKeyLen = upDepth - 1
		}
	case 1:
		// Leaf or extension node
		if hph.modBitmap[row] != 0 || hph.delBitmap[row] != 0 {
			// any modifications
			if row == 0 {
				hph.rootMod = true
			} else {
				hph.modBitmap[row-1] |= (uint16(1) << col)
				hph.delBitmap[row-1] &^= (uint16(1) << col)
				if hph.trace {
					fmt.Printf("leaf/ext modBitmap[%d]=%016b, delBitmap[%d]=%016b\n", row-1, hph.modBitmap[row-1], row-1, hph.delBitmap[row-1])
				}
			}
		}
		nibble := bits.TrailingZeros16(bitmap)
		cell := &hph.grid[row][nibble]
		upCell.extLen = 0
		upCell.fillFromLowerCell(cell, depth, hph.currentKey[upDepth:hph.currentKeyLen], nibble)
		if bits.OnesCount16(hph.beforeBitmap[row]) > 1 {
			// Deletion
			branchData = []byte{}
		}
		hph.activeRows--
		if upDepth > 0 {
			hph.currentKeyLen = upDepth - 1
		} else {
			hph.currentKeyLen = 0
		}
	default:
		// Branch node
		if hph.modBitmap[row] != 0 || hph.delBitmap[row] != 0 {
			// any modifications
			if row == 0 {
				hph.rootMod = true
			} else {
				hph.modBitmap[row-1] |= (uint16(1) << col)
				hph.delBitmap[row-1] &^= (uint16(1) << col)
				if hph.trace {
					fmt.Printf("branch modBitmap[%d]=%016b, delBitmap[%d]=%016b\n", row-1, hph.modBitmap[row-1], row-1, hph.delBitmap[row-1])
				}
			}
		}
		// Calculate total length of all hashes
		totalBranchLen := 17 - partsCount // For every empty cell, one byte
		for bitset, j := bitmap, 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			cell := &hph.grid[row][nibble]
			totalBranchLen += hph.computeCellHashLen(cell, depth)
			bitset ^= bit
		}
		// Parts bitmap
		binary.BigEndian.PutUint16(hph.numBuf[:], bitmap)
		branchData = append(branchData, hph.numBuf[:2]...)
		fieldsPos := 2
		// Add field flags
		binary.BigEndian.PutUint64(hph.numBuf[:], 0) // Fill numBuf with zeros
		branchData = append(branchData, hph.numBuf[:(partsCount+1)/2]...)
		hph.keccak2.Reset()
		pt := rlp.GenerateStructLen(hph.lenPrefix[:], totalBranchLen)
		if _, err := hph.keccak2.Write(hph.lenPrefix[:pt]); err != nil {
			return nil, nil, err
		}
		if hph.trace {
			fmt.Printf("branchHash [%x] {\n", updateKey)
		}
		var lastNibble int
		for bitset, j := bitmap, 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			hph.b[0] = 0x80
			for i := lastNibble; i < nibble; i++ {
				if _, err := hph.keccak2.Write(hph.b[:]); err != nil {
					return nil, nil, err
				}
				if hph.trace {
					fmt.Printf("%x: empty(%d,%x)\n", i, row, i)
				}
			}
			lastNibble = nibble + 1
			cell := &hph.grid[row][nibble]
			cellHash, err := hph.computeCellHash(cell, depth, nil)
			if err != nil {
				return nil, nil, err
			}
			if hph.trace {
				fmt.Printf("%x: computeCellHash(%d,%x,depth=%d)=[%x]\n", nibble, row, nibble, depth, cellHash)
			}
			if _, err = hph.keccak2.Write(cellHash); err != nil {
				return nil, nil, err
			}
			var fieldBits PartFlags
			if cell.extLen > 0 && cell.spl == 0 {
				fieldBits |= HASHEDKEY_PART
				n := binary.PutUvarint(hph.numBuf[:], uint64(cell.extLen))
				branchData = append(branchData, hph.numBuf[:n]...)
				branchData = append(branchData, cell.extension[:cell.extLen]...)
			}
			if cell.apl > 0 {
				fieldBits |= ACCOUNT_PLAIN_PART
				n := binary.PutUvarint(hph.numBuf[:], uint64(cell.apl))
				branchData = append(branchData, hph.numBuf[:n]...)
				branchData = append(branchData, cell.apk[:cell.apl]...)
			}
			if cell.spl > 0 {
				fieldBits |= STORAGE_PLAIN_PART
				n := binary.PutUvarint(hph.numBuf[:], uint64(cell.spl))
				branchData = append(branchData, hph.numBuf[:n]...)
				branchData = append(branchData, cell.spk[:cell.spl]...)
			}
			if cell.hl > 0 {
				fieldBits |= HASH_PART
				n := binary.PutUvarint(hph.numBuf[:], uint64(cell.hl))
				branchData = append(branchData, hph.numBuf[:n]...)
				branchData = append(branchData, cell.h[:cell.hl]...)
			}
			if j%2 == 1 {
				fieldBits <<= 4
			}
			branchData[fieldsPos+(j/2)] |= byte(fieldBits)
			bitset ^= bit
		}
		hph.b[0] = 0x80
		for i := lastNibble; i < 17; i++ {
			if _, err := hph.keccak2.Write(hph.b[:]); err != nil {
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
			fmt.Printf("fold: update key: %x\n", updateKey)
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
		hph.rootDel = true
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
		if hph.beforeBitmap[row]&(uint16(1)<<col) != 0 {
			hph.delBitmap[row] |= (uint16(1) << col)
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

func (hph *HexPatriciaHashed) updateAccount(plainKey, hashedKey []byte) *Cell {
	var cell *Cell
	var col int
	var depth int
	if hph.activeRows == 0 {
		// Update the root
		cell = &hph.root
		hph.rootMod = true
		hph.rootDel = false
	} else {
		row := hph.activeRows - 1
		depth = hph.depths[hph.activeRows-1]
		col = int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[row][col]
		hph.modBitmap[row] |= (uint16(1) << col)
		hph.delBitmap[row] &^= (uint16(1) << col)
		if hph.trace {
			fmt.Printf("updateAccount setting (%d, %x), depth=%d\n", row, col, depth)
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
	cell.apl = len(plainKey)
	copy(cell.apk[:], plainKey)
	return cell
}

func (hph *HexPatriciaHashed) updateBalance(plainKey, hashedKey []byte, balance *uint256.Int) {
	if hph.trace {
		fmt.Printf("updateBalance [%x] [%x] = %d, activeRows = %d\n", plainKey, hashedKey, balance, hph.activeRows)
	}
	cell := hph.updateAccount(plainKey, hashedKey)
	cell.Balance.Set(balance)
}

func (hph *HexPatriciaHashed) updateCode(plainKey, hashedKey []byte, codeHash []byte) {
	if hph.trace {
		fmt.Printf("updateCode, activeRows = %d\n", hph.activeRows)
	}
	cell := hph.updateAccount(plainKey, hashedKey)
	copy(cell.CodeHash[:], codeHash)
}

func (hph *HexPatriciaHashed) updateNonce(plainKey, hashedKey []byte, nonce uint64) {
	if hph.trace {
		fmt.Printf("updateNonce, activeRows = %d\n", hph.activeRows)
	}
	cell := hph.updateAccount(plainKey, hashedKey)
	cell.Nonce = nonce
}

func (hph *HexPatriciaHashed) updateStorage(plainKey []byte, hashedKey []byte, value []byte) {
	if hph.trace {
		fmt.Printf("updateStorage, activeRows = %d\n", hph.activeRows)
	}
	var col int
	var depth int
	var cell *Cell
	if hph.activeRows == 0 {
		// Update the root
		cell = &hph.root
	} else {
		depth = hph.depths[hph.activeRows-1]
		col = int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[hph.activeRows-1][col]
		hph.modBitmap[hph.activeRows-1] |= (uint16(1) << col)
		hph.delBitmap[hph.activeRows-1] &^= (uint16(1) << col)
		if hph.trace {
			fmt.Printf("updateStorage setting (%d, %x), modBitmap[%d]=%016b, depth=%d\n", hph.activeRows-1, col, hph.activeRows-1, hph.modBitmap[hph.activeRows-1], depth)
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
	copy(cell.spk[:], plainKey)
	cell.spl = len(plainKey)
	cell.StorageLen = len(value)
	if len(value) > 0 {
		copy(cell.Storage[:], value)
	}
}

func (hph *HexPatriciaHashed) RootHash() ([]byte, error) {
	if hph.root.hl > 0 {
		return hph.root.h[:hph.root.hl], nil
	}
	hash, err := hph.computeCellHash(&hph.root, 0, nil)
	if err != nil {
		return nil, err
	}
	return hash, nil
}

type UpdateFlags uint8

const (
	CODE_UPDATE    UpdateFlags = 1
	DELETE_UPDATE  UpdateFlags = 2
	BALANCE_UPDATE UpdateFlags = 4
	NONCE_UPDATE   UpdateFlags = 8
	STORAGE_UPDATE UpdateFlags = 16
)

func (uf UpdateFlags) String() string {
	var sb strings.Builder
	if uf == DELETE_UPDATE {
		sb.WriteString("Delete")
	} else {
		if uf&BALANCE_UPDATE != 0 {
			sb.WriteString("+Balance")
		}
		if uf&NONCE_UPDATE != 0 {
			sb.WriteString("+Nonce")
		}
		if uf&CODE_UPDATE != 0 {
			sb.WriteString("+Code")
		}
		if uf&STORAGE_UPDATE != 0 {
			sb.WriteString("+Storage")
		}
	}
	return sb.String()
}

type Update struct {
	Flags             UpdateFlags
	Balance           uint256.Int
	Nonce             uint64
	CodeHashOrStorage [32]byte
	ValLength         int
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

func (u Update) encode(buf []byte, numBuf []byte) []byte {
	buf = append(buf, byte(u.Flags))
	if u.Flags&BALANCE_UPDATE != 0 {
		buf = append(buf, byte(u.Balance.ByteLen()))
		buf = append(buf, u.Balance.Bytes()...)
	}
	if u.Flags&NONCE_UPDATE != 0 {
		n := binary.PutUvarint(numBuf, u.Nonce)
		buf = append(buf, numBuf[:n]...)
	}
	if u.Flags&CODE_UPDATE != 0 {
		buf = append(buf, u.CodeHashOrStorage[:]...)
	}
	if u.Flags&STORAGE_UPDATE != 0 {
		n := binary.PutUvarint(numBuf, uint64(u.ValLength))
		buf = append(buf, numBuf[:n]...)
		if u.ValLength > 0 {
			buf = append(buf, u.CodeHashOrStorage[:u.ValLength]...)
		}
	}
	return buf
}

func (u *Update) decode(buf []byte, pos int) (int, error) {
	if len(buf) < pos+1 {
		return 0, fmt.Errorf("decode Update: buffer too small for flags")
	}
	u.Flags = UpdateFlags(buf[pos])
	pos++
	if u.Flags&BALANCE_UPDATE != 0 {
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
	if u.Flags&NONCE_UPDATE != 0 {
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
	if u.Flags&CODE_UPDATE != 0 {
		if len(buf) < pos+32 {
			return 0, fmt.Errorf("decode Update: buffer too small for codeHash")
		}
		copy(u.CodeHashOrStorage[:], buf[pos:pos+32])
		pos += 32
	}
	if u.Flags&STORAGE_UPDATE != 0 {
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

func (u Update) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Flags: [%s]", u.Flags))
	if u.Flags&BALANCE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", Balance: [%d]", &u.Balance))
	}
	if u.Flags&NONCE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", Nonce: [%d]", u.Nonce))
	}
	if u.Flags&CODE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", CodeHash: [%x]", u.CodeHashOrStorage))
	}
	if u.Flags&STORAGE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", Storage: [%x]", u.CodeHashOrStorage[:u.ValLength]))
	}
	return sb.String()
}

func (hph *HexPatriciaHashed) ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (map[string][]byte, error) {
	branchNodeUpdates := make(map[string][]byte)
	for i, hashedKey := range hashedKeys {
		plainKey := plainKeys[i]
		update := updates[i]
		if hph.trace {
			fmt.Printf("plainKey=[%x], hashedKey=[%x], currentKey=[%x], update=%s\n",
				plainKey, hashedKey, hph.currentKey[:hph.currentKeyLen], update)
		}
		// Keep folding until the currentKey is the prefix of the key we modify
		for hph.needFolding(hashedKey) {
			if branchNodeUpdate, updateKey, err := hph.fold(); err != nil {
				return nil, fmt.Errorf("fold: %w", err)
			} else if branchNodeUpdate != nil {
				branchNodeUpdates[string(updateKey)] = branchNodeUpdate
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
	if hph.activeRows == 0 {
		if branchNodeUpdate, err := hph.foldRoot(); err != nil {
			return nil, fmt.Errorf("foldRoot: %w", err)
		} else {
			branchNodeUpdates[""] = branchNodeUpdate
		}
	} else {
		for hph.activeRows > 0 {
			if branchNodeUpdate, updateKey, err := hph.fold(); err != nil {
				return nil, fmt.Errorf("final fold: %w", err)
			} else if branchNodeUpdate != nil {
				branchNodeUpdates[string(updateKey)] = branchNodeUpdate
			}
		}
	}
	return branchNodeUpdates, nil
}

// ExtractPlainKeys parses branchData and extract the plain keys for accounts and storage in the same order
// they appear witjin the branchData
func ExtractPlainKeys(branchData []byte) (accountPlainKeys [][]byte, storagePlainKeys [][]byte, err error) {
	pos := 0
	partBitmap := binary.BigEndian.Uint16(branchData[pos:])
	pos += 2
	// Next, for each part, we have bitmap of fields
	partCount := bits.OnesCount16(partBitmap)
	fieldsPos := pos
	pos += (partCount + 1) / 2 // 1 bytes per two parts
	// Loop iterating over the set bits of modMask
	for bitset, j := partBitmap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		fieldBits := PartFlags(branchData[fieldsPos+j/2])
		if j%2 == 1 {
			fieldBits >>= 4
		}
		if fieldBits&HASHEDKEY_PART != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, nil, fmt.Errorf("extractPlainKeys buffer too small for hashedKey len")
			} else if n < 0 {
				return nil, nil, fmt.Errorf("extractPlainKeys value overflow for hashedKey len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, nil, fmt.Errorf("extractPlainKeys buffer too small for hashedKey")
			}
			if l > 0 {
				pos += int(l)
			}
		}
		if fieldBits&ACCOUNT_PLAIN_PART != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, nil, fmt.Errorf("extractPlainKeys buffer too small for accountPlainKey len")
			} else if n < 0 {
				return nil, nil, fmt.Errorf("extractPlainKeys value overflow for accountPlainKey len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, nil, fmt.Errorf("extractPlainKeys buffer too small for accountPlainKey")
			}
			accountPlainKeys = append(accountPlainKeys, branchData[pos:pos+int(l)])
			if l > 0 {
				pos += int(l)
			}
		}
		if fieldBits&STORAGE_PLAIN_PART != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, nil, fmt.Errorf("extractPlainKeys buffer too small for storagePlainKey len")
			} else if n < 0 {
				return nil, nil, fmt.Errorf("extractPlainKeys value overflow for storagePlainKey len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, nil, fmt.Errorf("extractPlainKeys buffer too small for storagePlainKey")
			}
			storagePlainKeys = append(storagePlainKeys, branchData[pos:pos+int(l)])
			if l > 0 {
				pos += int(l)
			}
		}
		if fieldBits&HASH_PART != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, nil, fmt.Errorf("extractPlainKeys buffer too small for hash len")
			} else if n < 0 {
				return nil, nil, fmt.Errorf("extractPlainKeys value overflow for hash len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, nil, fmt.Errorf("extractPlainKeys buffer too small for hash")
			}
			if l > 0 {
				pos += int(l)
			}
		}
		bitset ^= bit
	}
	return
}

func ReplacePlainKeys(branchData []byte, accountPlainKeys [][]byte, storagePlainKeys [][]byte, numBuf []byte, newData []byte) ([]byte, error) {
	pos := 0
	partBitmap := binary.BigEndian.Uint16(branchData[pos:])
	newData = append(newData, branchData[pos:pos+2]...)
	pos += 2
	// Next, for each part, we have bitmap of fields
	partCount := bits.OnesCount16(partBitmap)
	fieldsPos := pos
	newData = append(newData, branchData[pos:pos+(partCount+1)/2]...) // Copy all fieldbits
	pos += (partCount + 1) / 2                                        // 1 bytes per two parts
	accountI := 0                                                     // Counter for accountPlainKeys slice
	storageI := 0                                                     // Counter for storagePlainKeys slice
	// Loop iterating over the set bits of modMask
	for bitset, j := partBitmap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		fieldBits := PartFlags(branchData[fieldsPos+j/2])
		if j%2 == 1 {
			fieldBits >>= 4
		}
		if fieldBits&HASHEDKEY_PART != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hashedKey len")
			} else if n < 0 {
				return nil, fmt.Errorf("replacePlainKeys value overflow for hashedKey len")
			}
			newData = append(newData, branchData[pos:pos+n]...)
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hashedKey")
			}
			if l > 0 {
				newData = append(newData, branchData[pos:pos+int(l)]...)
				pos += int(l)
			}
		}
		if fieldBits&ACCOUNT_PLAIN_PART != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for accountPlainKey len")
			} else if n < 0 {
				return nil, fmt.Errorf("replacePlainKeys value overflow for accountPlainKey len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for accountPlainKey")
			}
			accountPlainKeys = append(accountPlainKeys, branchData[pos:pos+int(l)])
			if l > 0 {
				pos += int(l)
			}
			n = binary.PutUvarint(numBuf, uint64(len(accountPlainKeys[accountI])))
			newData = append(newData, numBuf[:n]...)
			newData = append(newData, accountPlainKeys[accountI]...)
			accountI++
		}
		if fieldBits&STORAGE_PLAIN_PART != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for storagePlainKey len")
			} else if n < 0 {
				return nil, fmt.Errorf("replacePlainKeys value overflow for storagePlainKey len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for storagePlainKey")
			}
			storagePlainKeys = append(storagePlainKeys, branchData[pos:pos+int(l)])
			if l > 0 {
				pos += int(l)
			}
			n = binary.PutUvarint(numBuf, uint64(len(storagePlainKeys[storageI])))
			newData = append(newData, numBuf[:n]...)
			newData = append(newData, storagePlainKeys[storageI]...)
			storageI++
		}
		if fieldBits&HASH_PART != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hash len")
			} else if n < 0 {
				return nil, fmt.Errorf("replacePlainKeys value overflow for hash len")
			}
			newData = append(newData, branchData[pos:pos+n]...)
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hash")
			}
			if l > 0 {
				newData = append(newData, branchData[pos:pos+int(l)]...)
				pos += int(l)
			}
		}
		bitset ^= bit
	}
	return newData, nil
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
