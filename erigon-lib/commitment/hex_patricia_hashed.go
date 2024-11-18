// Copyright 2022 The Erigon Authors
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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/dbg"

	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common/hexutility"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/rlp"
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
	root cell // Root cell of the tree
	// How many rows (starting from row 0) are currently active and have corresponding selected columns
	// Last active row does not have selected column
	activeRows int
	// Length of the key that reflects current positioning of the grid. It may be larger than number of active rows,
	// if an account leaf cell represents multiple nibbles in the key
	currentKeyLen int
	accountKeyLen int
	// Rows of the grid correspond to the level of depth in the patricia tree
	// Columns of the grid correspond to pointers to the nodes further from the root
	grid          [128][16]cell // First 64 rows of this grid are for account trie, and next 64 rows are for storage trie
	currentKey    [128]byte     // For each row indicates which column is currently selected
	depths        [128]int      // For each row, the depth of cells in that row
	branchBefore  [128]bool     // For each row, whether there was a branch node in the database loaded in unfold
	touchMap      [128]uint16   // For each row, bitmap of cells that were either present before modification, or modified or deleted
	afterMap      [128]uint16   // For each row, bitmap of cells that were present after modification
	keccak        keccakState
	keccak2       keccakState
	rootChecked   bool // Set to false if it is not known whether the root is empty, set to true if it is checked
	rootTouched   bool
	rootPresent   bool
	trace         bool
	ctx           PatriciaContext
	hashAuxBuffer [128]byte     // buffer to compute cell hash or write hash-related things
	auxBuffer     *bytes.Buffer // auxiliary buffer used during branch updates encoding
	branchEncoder *BranchEncoder

	depthsToTxNum [129]uint64 // endTxNum of file with branch data for that depth
	hadToLoadL    map[uint64]skipStat
	mounted       bool
	mountedNib    int // < 16
}

func NewHexPatriciaHashed(accountKeyLen int, ctx PatriciaContext, tmpdir string) *HexPatriciaHashed {
	hph := &HexPatriciaHashed{
		ctx:           ctx,
		keccak:        sha3.NewLegacyKeccak256().(keccakState),
		keccak2:       sha3.NewLegacyKeccak256().(keccakState),
		accountKeyLen: accountKeyLen,
		auxBuffer:     bytes.NewBuffer(make([]byte, 8192)),
		hadToLoadL:    make(map[uint64]skipStat),
	}
	hph.branchEncoder = NewBranchEncoder(1024, filepath.Join(tmpdir, "branch-encoder"))
	return hph
}

type cell struct {
	hashedExtension [128]byte
	extension       [64]byte
	accountAddr     [length.Addr]byte               // account plain key
	storageAddr     [length.Addr + length.Hash]byte // storage plain key
	hash            [length.Hash]byte               // cell hash
	stateHash       [length.Hash]byte
	hashedExtLen    int       // length of the hashed extension, if any
	extLen          int       // length of the extension, if any
	accountAddrLen  int       // length of account plain key
	storageAddrLen  int       // length of the storage plain key
	hashLen         int       // Length of the hash (or embedded)
	stateHashLen    int       // stateHash length, if > 0 can reuse
	loaded          loadFlags // folded Cell have only hash, unfolded have all fields
	Update                    // state update
}

type loadFlags uint8

func (f loadFlags) String() string {
	var b strings.Builder
	if f == cellLoadNone {
		b.WriteString("false")
	} else {
		if f.account() {
			b.WriteString("Account ")
		}
		if f.storage() {
			b.WriteString("Storage ")
		}
	}
	return b.String()
}

func (f loadFlags) account() bool {
	return f&cellLoadAccount != 0
}

func (f loadFlags) storage() bool {
	return f&cellLoadStorage != 0
}

func (f loadFlags) addFlag(loadFlags loadFlags) loadFlags {
	if loadFlags == cellLoadNone {
		return f
	}
	return f | loadFlags
}

const (
	cellLoadNone    = loadFlags(0)
	cellLoadAccount = loadFlags(1)
	cellLoadStorage = loadFlags(2)
)

var (
	EmptyRootHash      = hexutility.MustDecodeHex("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	EmptyCodeHash      = hexutility.MustDecodeHex("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
	EmptyCodeHashArray = *(*[length.Hash]byte)(EmptyCodeHash)
)

func (cell *cell) reset() {
	cell.accountAddrLen = 0
	cell.storageAddrLen = 0
	cell.hashedExtLen = 0
	cell.extLen = 0
	cell.hashLen = 0
	cell.stateHashLen = 0
	cell.loaded = cellLoadNone
	clear(cell.hashedExtension[:])
	clear(cell.extension[:])
	clear(cell.accountAddr[:])
	clear(cell.storageAddr[:])
	clear(cell.hash[:])
	cell.Update.Reset()
}

func (cell *cell) String() string {
	b := new(strings.Builder)
	b.WriteString("{")
	b.WriteString(fmt.Sprintf("loaded=%v ", cell.loaded))
	if cell.Deleted() {
		b.WriteString("DELETED ")
	}
	if cell.accountAddrLen > 0 {
		b.WriteString(fmt.Sprintf("addr=%x ", cell.accountAddr[:cell.accountAddrLen]))
	}
	if cell.storageAddrLen > 0 {
		b.WriteString(fmt.Sprintf("addr[s]=%x", cell.storageAddr[:cell.storageAddrLen]))

	}
	if cell.hashLen > 0 {
		b.WriteString(fmt.Sprintf("h=%x", cell.hash[:cell.hashLen]))
	}
	b.WriteString("}")
	return b.String()
}

func (cell *cell) FullString() string {
	b := new(strings.Builder)
	b.WriteString("{")
	b.WriteString(fmt.Sprintf("loaded=%v ", cell.loaded))
	if cell.Deleted() {
		b.WriteString("DELETED ")
	}

	if cell.accountAddrLen > 0 {
		b.WriteString(fmt.Sprintf("addr=%x ", cell.accountAddr[:cell.accountAddrLen]))
		b.WriteString(fmt.Sprintf("balance=%s ", cell.Balance.String()))
		b.WriteString(fmt.Sprintf("nonce=%d ", cell.Nonce))
		if cell.CodeHash != EmptyCodeHashArray {
			b.WriteString(fmt.Sprintf("codeHash=%x ", cell.CodeHash[:]))
		} else {
			b.WriteString("codeHash=EMPTY ")
		}
	}
	if cell.storageAddrLen > 0 {
		b.WriteString(fmt.Sprintf("addr[s]=%x ", cell.storageAddr[:cell.storageAddrLen]))
		b.WriteString(fmt.Sprintf("storage=%x ", cell.Storage[:cell.StorageLen]))
	}
	if cell.hashLen > 0 {
		b.WriteString(fmt.Sprintf("h=%x ", cell.hash[:cell.hashLen]))
	}
	if cell.stateHashLen > 0 {
		b.WriteString(fmt.Sprintf("memHash=%x ", cell.stateHash[:cell.stateHashLen]))
	}
	if cell.extLen > 0 {
		b.WriteString(fmt.Sprintf("extension=%x ", cell.extension[:cell.extLen]))
	}
	if cell.hashedExtLen > 0 {
		b.WriteString(fmt.Sprintf("hashedExtension=%x ", cell.hashedExtension[:cell.hashedExtLen]))
	}

	b.WriteString("}")
	return b.String()
}

func (cell *cell) setFromUpdate(update *Update) {
	cell.Update.Merge(update)
	if update.Flags&StorageUpdate != 0 {
		cell.loaded = cell.loaded.addFlag(cellLoadStorage)
		mxTrieStateLoadRate.Inc()
		hadToLoad.Add(1)
	}
	if update.Flags&BalanceUpdate != 0 || update.Flags&NonceUpdate != 0 || update.Flags&CodeUpdate != 0 {
		cell.loaded = cell.loaded.addFlag(cellLoadAccount)
		mxTrieStateLoadRate.Inc()
		hadToLoad.Add(1)
	}
}

func (cell *cell) fillFromUpperCell(upCell *cell, depth, depthIncrement int) {
	if upCell.hashedExtLen >= depthIncrement {
		cell.hashedExtLen = upCell.hashedExtLen - depthIncrement
	} else {
		cell.hashedExtLen = 0
	}
	if upCell.hashedExtLen > depthIncrement {
		copy(cell.hashedExtension[:], upCell.hashedExtension[depthIncrement:upCell.hashedExtLen])
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
		cell.accountAddrLen = upCell.accountAddrLen
		if upCell.accountAddrLen > 0 {
			copy(cell.accountAddr[:], upCell.accountAddr[:cell.accountAddrLen])
			cell.Balance.Set(&upCell.Balance)
			cell.Nonce = upCell.Nonce
			copy(cell.CodeHash[:], upCell.CodeHash[:])
			cell.extLen = upCell.extLen
			if upCell.extLen > 0 {
				copy(cell.extension[:], upCell.extension[:upCell.extLen])
			}
		}
	} else {
		cell.accountAddrLen = 0
	}
	cell.storageAddrLen = upCell.storageAddrLen
	if upCell.storageAddrLen > 0 {
		copy(cell.storageAddr[:], upCell.storageAddr[:upCell.storageAddrLen])
		cell.StorageLen = upCell.StorageLen
		if upCell.StorageLen > 0 {
			copy(cell.Storage[:], upCell.Storage[:upCell.StorageLen])
		}
	}
	cell.hashLen = upCell.hashLen
	if upCell.hashLen > 0 {
		copy(cell.hash[:], upCell.hash[:upCell.hashLen])
	}
	cell.loaded = upCell.loaded
}

// fillFromLowerCell fills the cell with the data from the cell of the lower row during fold
func (cell *cell) fillFromLowerCell(lowCell *cell, lowDepth int, preExtension []byte, nibble int) {
	if lowCell.accountAddrLen > 0 || lowDepth < 64 {
		cell.accountAddrLen = lowCell.accountAddrLen
	}
	if lowCell.accountAddrLen > 0 {
		copy(cell.accountAddr[:], lowCell.accountAddr[:cell.accountAddrLen])
		cell.Balance.Set(&lowCell.Balance)
		cell.Nonce = lowCell.Nonce
		copy(cell.CodeHash[:], lowCell.CodeHash[:])
	}
	cell.storageAddrLen = lowCell.storageAddrLen
	if lowCell.storageAddrLen > 0 {
		copy(cell.storageAddr[:], lowCell.storageAddr[:cell.storageAddrLen])
		cell.StorageLen = lowCell.StorageLen
		if lowCell.StorageLen > 0 {
			copy(cell.Storage[:], lowCell.Storage[:lowCell.StorageLen])
		}
	}
	if lowCell.hashLen > 0 {
		if (lowCell.accountAddrLen == 0 && lowDepth < 64) || (lowCell.storageAddrLen == 0 && lowDepth > 64) {
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
	cell.hashLen = lowCell.hashLen
	if lowCell.hashLen > 0 {
		copy(cell.hash[:], lowCell.hash[:lowCell.hashLen])
	}
	cell.loaded = lowCell.loaded
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

func (cell *cell) deriveHashedKeys(depth int, keccak keccakState, accountKeyLen int) error {
	extraLen := 0
	if cell.accountAddrLen > 0 {
		if depth > 64 {
			return errors.New("deriveHashedKeys accountAddr present at depth > 64")
		}
		extraLen = 64 - depth
	}
	if cell.storageAddrLen > 0 {
		if depth >= 64 {
			extraLen = 128 - depth
		} else {
			extraLen += 64
		}
	}
	if extraLen > 0 {
		if cell.hashedExtLen > 0 {
			copy(cell.hashedExtension[extraLen:], cell.hashedExtension[:cell.hashedExtLen])
		}
		cell.hashedExtLen = min(extraLen+cell.hashedExtLen, len(cell.hashedExtension))
		var hashedKeyOffset, downOffset int
		if cell.accountAddrLen > 0 {
			if err := hashKey(keccak, cell.accountAddr[:cell.accountAddrLen], cell.hashedExtension[:], depth); err != nil {
				return err
			}
			downOffset = 64 - depth
		}
		if cell.storageAddrLen > 0 {
			if depth >= 64 {
				hashedKeyOffset = depth - 64
			}
			if depth == 0 {
				accountKeyLen = 0
			}
			if err := hashKey(keccak, cell.storageAddr[accountKeyLen:cell.storageAddrLen], cell.hashedExtension[downOffset:], hashedKeyOffset); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cell *cell) fillFromFields(data []byte, pos int, fieldBits cellFields) (int, error) {
	fields := []struct {
		flag      cellFields
		lenField  *int
		dataField []byte
		extraFunc func(int)
	}{
		{fieldExtension, &cell.hashedExtLen, cell.hashedExtension[:], func(l int) {
			cell.extLen = l
			if l > 0 {
				copy(cell.extension[:], cell.hashedExtension[:l])
			}
		}},
		{fieldAccountAddr, &cell.accountAddrLen, cell.accountAddr[:], nil},
		{fieldStorageAddr, &cell.storageAddrLen, cell.storageAddr[:], nil},
		{fieldHash, &cell.hashLen, cell.hash[:], nil},
		{fieldStateHash, &cell.stateHashLen, cell.stateHash[:], nil},
	}

	for _, f := range fields {
		if fieldBits&f.flag != 0 {
			l, n, err := readUvarint(data[pos:])
			if err != nil {
				return 0, err
			}
			pos += n

			if len(data) < pos+int(l) {
				return 0, fmt.Errorf("buffer too small for %v", f.flag)
			}

			*f.lenField = int(l)
			if l > 0 {
				copy(f.dataField, data[pos:pos+int(l)])
				pos += int(l)
			}
			if f.extraFunc != nil {
				f.extraFunc(int(l))
			}
		} else {
			*f.lenField = 0
			if f.flag == fieldExtension {
				cell.extLen = 0
			}
		}
	}

	if fieldBits&fieldAccountAddr != 0 {
		copy(cell.CodeHash[:], EmptyCodeHash)
	}
	return pos, nil
}

func readUvarint(data []byte) (uint64, int, error) {
	l, n := binary.Uvarint(data)
	if n == 0 {
		return 0, 0, errors.New("buffer too small for length")
	} else if n < 0 {
		return 0, 0, errors.New("value overflow for length")
	}
	return l, n, nil
}

func (cell *cell) accountForHashing(buffer []byte, storageRootHash [length.Hash]byte) int {
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
	pl := rlp.GenerateStructLen(lenPrefix[:], totalLen)
	canEmbed := !singleton && totalLen+pl < length.Hash
	var writer io.Writer
	if canEmbed {
		//hph.byteArrayWriter.Setup(buf)
		hph.auxBuffer.Reset()
		writer = hph.auxBuffer
	} else {
		hph.keccak.Reset()
		writer = hph.keccak
	}
	if _, err := writer.Write(lenPrefix[:pl]); err != nil {
		return nil, err
	}
	if _, err := writer.Write(keyPrefix[:kp]); err != nil {
		return nil, err
	}
	b := [1]byte{compact0}
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
	if canEmbed {
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

func (hph *HexPatriciaHashed) computeCellHashLen(cell *cell, depth int) int {
	if cell.storageAddrLen > 0 && depth >= 64 {
		if cell.stateHashLen > 0 {
			return cell.stateHashLen + 1
		}

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

func (hph *HexPatriciaHashed) computeCellHash(cell *cell, depth int, buf []byte) ([]byte, error) {
	var err error
	var storageRootHash [length.Hash]byte
	var storageRootHashIsSet bool
	if cell.storageAddrLen > 0 {
		var hashedKeyOffset int
		if depth >= 64 {
			hashedKeyOffset = depth - 64
		}
		singleton := depth <= 64
		koffset := hph.accountKeyLen
		if depth == 0 && cell.accountAddrLen == 0 {
			// if account key is empty, then we need to hash storage key from the key beginning
			koffset = 0
		}
		if err = hashKey(hph.keccak, cell.storageAddr[koffset:cell.storageAddrLen], cell.hashedExtension[:], hashedKeyOffset); err != nil {
			return nil, err
		}
		cell.hashedExtension[64-hashedKeyOffset] = 16 // Add terminator

		if cell.stateHashLen > 0 {
			res := append([]byte{160}, cell.stateHash[:cell.stateHashLen]...)
			hph.keccak.Reset()
			if hph.trace {
				fmt.Printf("REUSED stateHash %x spk %x\n", res, cell.storageAddr[:cell.storageAddrLen])
			}
			mxTrieStateSkipRate.Inc()
			skippedLoad.Add(1)
			if !singleton {
				return res, nil
			} else {
				storageRootHashIsSet = true
				storageRootHash = *(*[length.Hash]byte)(res[1:])
				//copy(storageRootHash[:], res[1:])
				//cell.stateHashLen = 0
			}
		} else {
			if !cell.loaded.storage() {
				update, err := hph.ctx.Storage(cell.storageAddr[:cell.storageAddrLen])
				if err != nil {
					return nil, err
				}
				cell.setFromUpdate(update)
				fmt.Printf("Storage %x was not loaded\n", cell.storageAddr[:cell.storageAddrLen])
			}
			if singleton {
				if hph.trace {
					fmt.Printf("leafHashWithKeyVal(singleton) for [%x]=>[%x]\n", cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
				}
				aux := make([]byte, 0, 33)
				if aux, err = hph.leafHashWithKeyVal(aux, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], true); err != nil {
					return nil, err
				}
				if hph.trace {
					fmt.Printf("leafHashWithKeyVal(singleton) storage hash [%x]\n", aux)
				}
				storageRootHash = *(*[length.Hash]byte)(aux[1:])
				storageRootHashIsSet = true
				cell.stateHashLen = 0
				hadToReset.Add(1)
			} else {
				if hph.trace {
					fmt.Printf("leafHashWithKeyVal for [%x]=>[%x] %v\n", cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], cell.String())
				}
				leafHash, err := hph.leafHashWithKeyVal(buf, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], false)
				if err != nil {
					return nil, err
				}

				copy(cell.stateHash[:], leafHash[1:])
				cell.stateHashLen = len(leafHash) - 1
				if hph.trace {
					fmt.Printf("STATE HASH storage memoized %x spk %x\n", leafHash, cell.storageAddr[:cell.storageAddrLen])
				}

				return leafHash, nil
			}
		}
	}
	if cell.accountAddrLen > 0 {
		if err := hashKey(hph.keccak, cell.accountAddr[:cell.accountAddrLen], cell.hashedExtension[:], depth); err != nil {
			return nil, err
		}
		cell.hashedExtension[64-depth] = 16 // Add terminator
		if !storageRootHashIsSet {
			if cell.extLen > 0 { // Extension
				if cell.hashLen == 0 {
					return nil, errors.New("computeCellHash extension without hash")
				}
				if hph.trace {
					fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
				}
				if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
					return nil, err
				}
				if hph.trace {
					fmt.Printf("EXTENSION HASH %x DROPS stateHash\n", storageRootHash)
				}
				cell.stateHashLen = 0
				hadToReset.Add(1)
			} else if cell.hashLen > 0 {
				storageRootHash = cell.hash
			} else {
				storageRootHash = *(*[length.Hash]byte)(EmptyRootHash)
			}
		}
		if !cell.loaded.account() {
			if cell.stateHashLen > 0 {
				res := append([]byte{160}, cell.stateHash[:cell.stateHashLen]...)
				hph.keccak.Reset()

				mxTrieStateSkipRate.Inc()
				skippedLoad.Add(1)
				if hph.trace {
					fmt.Printf("REUSED stateHash %x apk %x\n", res, cell.accountAddr[:cell.accountAddrLen])
				}
				return res, nil
			}
			// storage root update or extension update could invalidate older stateHash, so we need to reload state
			update, err := hph.ctx.Account(cell.accountAddr[:cell.accountAddrLen])
			if err != nil {
				return nil, err
			}
			cell.setFromUpdate(update)
		}

		var valBuf [128]byte
		valLen := cell.accountForHashing(valBuf[:], storageRootHash)
		if hph.trace {
			fmt.Printf("accountLeafHashWithKey for [%x]=>[%x]\n", cell.hashedExtension[:65-depth], rlp.RlpEncodedBytes(valBuf[:valLen]))
		}
		leafHash, err := hph.accountLeafHashWithKey(buf, cell.hashedExtension[:65-depth], rlp.RlpEncodedBytes(valBuf[:valLen]))
		if err != nil {
			return nil, err
		}
		if hph.trace {
			fmt.Printf("STATE HASH account memoized %x\n", leafHash)
		}
		copy(cell.stateHash[:], leafHash[1:])
		cell.stateHashLen = len(leafHash) - 1
		return leafHash, nil
	}

	buf = append(buf, 0x80+32)
	if cell.extLen > 0 { // Extension
		if cell.hashLen > 0 {
			if hph.trace {
				fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
			}
			var hash [length.Hash]byte
			if hash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
				return nil, err
			}
			buf = append(buf, hash[:]...)
		} else {
			return nil, errors.New("computeCellHash extension without hash")
		}
	} else if cell.hashLen > 0 {
		buf = append(buf, cell.hash[:cell.hashLen]...)
	} else if storageRootHashIsSet {
		buf = append(buf, storageRootHash[:]...)
		copy(cell.hash[:], storageRootHash[:])
		cell.hashLen = len(storageRootHash)
	} else {
		buf = append(buf, EmptyRootHash...)
	}
	return buf, nil
}

func (hph *HexPatriciaHashed) needUnfolding(hashedKey []byte) int {
	var cell *cell
	var depth int
	if hph.activeRows == 0 {
		if hph.trace {
			fmt.Printf("needUnfolding root, rootChecked = %t\n", hph.rootChecked)
		}
		if hph.root.hashedExtLen == 64 && hph.root.accountAddrLen > 0 && hph.root.storageAddrLen > 0 {
			// in case if root is a leaf node with storage and account, we need to derive storage part of a key
			if err := hph.root.deriveHashedKeys(depth, hph.keccak, hph.accountKeyLen); err != nil {
				log.Warn("deriveHashedKeys for root with storage", "err", err, "cell", hph.root.FullString())
				return 0
			}
			//copy(hph.currentKey[:], hph.root.hashedExtension[:])
			if hph.trace {
				fmt.Printf("derived prefix %x\n", hph.currentKey[:hph.currentKeyLen])
			}
		}
		if hph.root.hashedExtLen == 0 && hph.root.hashLen == 0 {
			if hph.rootChecked {
				return 0 // Previously checked, empty root, no unfolding needed
			}
			return 1 // Need to attempt to unfold the root
		}
		cell = &hph.root
	} else {
		col := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[hph.activeRows-1][col]
		depth = hph.depths[hph.activeRows-1]
		if hph.trace {
			fmt.Printf("currentKey [%x] needUnfolding cell (%d, %x, depth=%d) cell.hash=[%x]\n", hph.currentKey[:hph.currentKeyLen], hph.activeRows-1, col, depth, cell.hash[:cell.hashLen])
		}
	}
	if len(hashedKey) <= depth {
		return 0
	}
	if cell.hashedExtLen == 0 {
		if cell.hashLen == 0 {
			// cell is empty, no need to unfold further
			return 0
		}
		// unfold branch node
		return 1
	}
	cpl := commonPrefixLen(hashedKey[depth:], cell.hashedExtension[:cell.hashedExtLen-1])
	if hph.trace {
		fmt.Printf("cpl=%d cell.hashedExtension=[%x] hashedKey[depth=%d:]=[%x]\n", cpl, cell.hashedExtension[:cell.hashedExtLen], depth, hashedKey[depth:])
	}
	unfolding := cpl + 1
	if depth < 64 && depth+unfolding > 64 {
		// This is to make sure that unfolding always breaks at the level where storage subtrees start
		unfolding = 64 - depth
		if hph.trace {
			fmt.Printf("adjusted unfolding=%d <- %d\n", unfolding, cpl+1)
		}
	}
	return unfolding
}

// unfoldBranchNode returns true if unfolding has been done
func (hph *HexPatriciaHashed) unfoldBranchNode(row, depth int, deleted bool) (bool, error) {
	key := hexToCompact(hph.currentKey[:hph.currentKeyLen])
	branchData, fileEndTxNum, err := hph.ctx.Branch(key)
	if err != nil {
		return false, err
	}
	hph.depthsToTxNum[depth] = fileEndTxNum
	if len(branchData) >= 2 {
		branchData = branchData[2:] // skip touch map and keep the rest
	}
	if hph.trace {
		fmt.Printf("unfoldBranchNode prefix '%x', nibbles [%x] depth %d row %d '%x'\n", key, hph.currentKey[:hph.currentKeyLen], depth, row, branchData)
	}
	if !hph.rootChecked && hph.currentKeyLen == 0 && len(branchData) == 0 {
		// Special case - empty or deleted root
		hph.rootChecked = true
		return false, nil
	}
	if len(branchData) == 0 {
		log.Warn("got empty branch data during unfold", "key", hex.EncodeToString(key), "row", row, "depth", depth, "deleted", deleted)
		return false, fmt.Errorf("empty branch data read during unfold, prefix %x", hexToCompact(hph.currentKey[:hph.currentKeyLen]))
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
	//fmt.Printf("unfoldBranchNode prefix '%x' [%x], afterMap = [%016b], touchMap = [%016b]\n", key, branchData, hph.afterMap[row], hph.touchMap[row])
	// Loop iterating over the set bits of modMask
	for bitset, j := bitmap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &hph.grid[row][nibble]
		fieldBits := branchData[pos]
		pos++
		if pos, err = cell.fillFromFields(branchData, pos, cellFields(fieldBits)); err != nil {
			return false, fmt.Errorf("prefix [%x] branchData[%x]: %w", hph.currentKey[:hph.currentKeyLen], branchData, err)
		}
		if hph.trace {
			fmt.Printf("cell (%d, %x, depth=%d) %s\n", row, nibble, depth, cell.FullString())
		}

		// relies on plain account/storage key so need to be dereferenced before hashing
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
	var upCell *cell
	var touched, present bool
	var upDepth, depth int
	if hph.activeRows == 0 {
		if hph.rootChecked && hph.root.hashLen == 0 && hph.root.hashedExtLen == 0 {
			// No unfolding for empty root
			return nil
		}
		upCell = &hph.root
		touched = hph.rootTouched
		present = hph.rootPresent
		if hph.trace {
			fmt.Printf("unfold root: touched: %t present: %t %s\n", touched, present, upCell.FullString())
		}
	} else {
		upDepth = hph.depths[hph.activeRows-1]
		nib := hashedKey[upDepth-1]
		upCell = &hph.grid[hph.activeRows-1][nib]
		touched = hph.touchMap[hph.activeRows-1]&(uint16(1)<<nib) != 0
		present = hph.afterMap[hph.activeRows-1]&(uint16(1)<<nib) != 0
		if hph.trace {
			fmt.Printf("upCell (%d, %x, updepth=%d) touched: %t present: %t\n", hph.activeRows-1, nib, upDepth, touched, present)
		}
		hph.currentKey[hph.currentKeyLen] = nib
		hph.currentKeyLen++
	}
	row := hph.activeRows
	for i := 0; i < 16; i++ {
		hph.grid[row][i].reset()
	}
	hph.touchMap[row], hph.afterMap[row] = 0, 0
	hph.branchBefore[row] = false

	if upCell.hashedExtLen == 0 {
		depth = upDepth + 1
		unfolded, err := hph.unfoldBranchNode(row, depth, touched && !present)
		if err != nil {
			return err
		}
		if unfolded {
			hph.depths[hph.activeRows] = depth
			hph.activeRows++
		}
		// Return here to prevent activeRow from being incremented when !unfolded
		return nil
	}

	var nibble, copyLen int
	if upCell.hashedExtLen >= unfolding {
		depth = upDepth + unfolding
		nibble = int(upCell.hashedExtension[unfolding-1])
		copyLen = unfolding - 1
	} else {
		depth = upDepth + upCell.hashedExtLen
		nibble = int(upCell.hashedExtension[upCell.hashedExtLen-1])
		copyLen = upCell.hashedExtLen - 1
	}

	if touched {
		hph.touchMap[row] = uint16(1) << nibble
	}
	if present {
		hph.afterMap[row] = uint16(1) << nibble
	}

	cell := &hph.grid[row][nibble]
	cell.fillFromUpperCell(upCell, depth, min(unfolding, upCell.hashedExtLen))
	if hph.trace {
		fmt.Printf("unfolded cell (%d, %x, depth=%d) %s\n", row, nibble, depth, cell.FullString())
	}

	if row >= 64 {
		cell.accountAddrLen = 0
	}
	if copyLen > 0 {
		copy(hph.currentKey[hph.currentKeyLen:], upCell.hashedExtension[:copyLen])
	}
	hph.currentKeyLen += copyLen

	hph.depths[hph.activeRows] = depth
	hph.activeRows++
	return nil
}

func (hph *HexPatriciaHashed) needFolding(hashedKey []byte) bool {
	return !bytes.HasPrefix(hashedKey, hph.currentKey[:hph.currentKeyLen])
}

var (
	hadToLoad   atomic.Uint64
	skippedLoad atomic.Uint64
	hadToReset  atomic.Uint64
)

type skipStat struct {
	accLoaded, accSkipped, accReset, storReset, storLoaded, storSkipped uint64
}

func updatedNibs(num uint16) string {
	var nibbles []string
	for i := 0; i < 16; i++ {
		if num&(1<<i) != 0 {
			nibbles = append(nibbles, fmt.Sprintf("%X", i))
		}
	}
	return strings.Join(nibbles, ",")
}

const DepthWithoutNodeHashes = 35 //nolint

// The purpose of fold is to reduce hph.currentKey[:hph.currentKeyLen]. It should be invoked
// until that current key becomes a prefix of hashedKey that we will process next
// (in other words until the needFolding function returns 0)
func (hph *HexPatriciaHashed) fold() (err error) {
	updateKeyLen := hph.currentKeyLen
	if hph.activeRows == 0 {
		return errors.New("cannot fold - no active rows")
	}
	if hph.trace {
		fmt.Printf("fold [%x] activeRows: %d touchMap: %016b afterMap: %016b\n", hph.currentKey[:hph.currentKeyLen], hph.activeRows, hph.touchMap[hph.activeRows-1], hph.afterMap[hph.activeRows-1])
	}
	// Move information to the row above
	var upCell *cell
	var nibble, upDepth int
	row := hph.activeRows - 1
	if row == 0 {
		if hph.trace {
			fmt.Printf("fold: parent is root %s\n", hph.root.FullString())
		}
		upCell = &hph.root
	} else {
		upDepth = hph.depths[hph.activeRows-2]
		nibble = int(hph.currentKey[upDepth-1])
		if hph.trace {
			fmt.Printf("fold: parent (%d, %x, depth=%d)\n", row-1, nibble, upDepth)
		}
		upCell = &hph.grid[row-1][nibble]
	}

	depth := hph.depths[row]
	updateKey := hexToCompact(hph.currentKey[:updateKeyLen])
	partsCount := bits.OnesCount16(hph.afterMap[row])
	defer func() { hph.depthsToTxNum[depth] = 0 }()

	if hph.trace {
		fmt.Printf("fold: (row=%d, {%s}, depth=%d) prefix [%x] touchMap: %016b afterMap: %016b \n",
			row, updatedNibs(hph.touchMap[row]&hph.afterMap[row]), depth, hph.currentKey[:hph.currentKeyLen], hph.touchMap[row], hph.afterMap[row])
	}
	switch partsCount {
	case 0: // Everything deleted
		if hph.touchMap[row] != 0 {
			if row == 0 {
				// Root is deleted because the tree is empty
				hph.rootTouched = true
				hph.rootPresent = false
			} else if upDepth == 64 {
				// Special case - all storage items of an account have been deleted, but it does not automatically delete the account, just makes it empty storage
				// Therefore we are not propagating deletion upwards, but turn it into a modification
				hph.touchMap[row-1] |= uint16(1) << nibble
			} else {
				// Deletion is propagated upwards
				hph.touchMap[row-1] |= uint16(1) << nibble
				hph.afterMap[row-1] &^= uint16(1) << nibble
			}
		}

		upCell.reset()
		if hph.branchBefore[row] {
			_, err := hph.branchEncoder.CollectUpdate(hph.ctx, updateKey, 0, hph.touchMap[row], 0, RetrieveCellNoop)
			if err != nil {
				return fmt.Errorf("failed to encode leaf node update: %w", err)
			}
		}
		hph.activeRows--
		if upDepth > 0 {
			hph.currentKeyLen = upDepth - 1
		} else {
			hph.currentKeyLen = 0
		}
	case 1: // Leaf or extension node
		if hph.touchMap[row] != 0 {
			// any modifications
			if row == 0 {
				hph.rootTouched = true
			} else {
				// Modification is propagated upwards
				hph.touchMap[row-1] |= uint16(1) << nibble
			}
		}
		nibble := bits.TrailingZeros16(hph.afterMap[row])
		cell := &hph.grid[row][nibble]
		upCell.extLen = 0
		upCell.stateHashLen = 0
		upCell.fillFromLowerCell(cell, depth, hph.currentKey[upDepth:hph.currentKeyLen], nibble)
		// Delete if it existed
		if hph.branchBefore[row] {
			//fmt.Printf("delete existed row %d prefix %x\n", row, updateKey)
			_, err := hph.branchEncoder.CollectUpdate(hph.ctx, updateKey, 0, hph.touchMap[row], 0, RetrieveCellNoop)
			if err != nil {
				return fmt.Errorf("failed to encode leaf node update: %w", err)
			}
		}
		hph.activeRows--
		hph.currentKeyLen = max(upDepth-1, 0)
		if hph.trace {
			fmt.Printf("formed leaf (%d %x, depth=%d) [%x] %s\n", row, nibble, depth, updateKey, cell.FullString())
		}
	default: // Branch node
		if hph.touchMap[row] != 0 { // any modifications
			if row == 0 {
				hph.rootTouched = true
				hph.rootPresent = true
			} else {
				// Modification is propagated upwards
				hph.touchMap[row-1] |= uint16(1) << nibble
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

			/* memoization of state hashes*/
			counters := hph.hadToLoadL[hph.depthsToTxNum[depth]]
			if cell.stateHashLen > 0 && (hph.touchMap[row]&hph.afterMap[row]&uint16(1<<nibble) > 0 || cell.stateHashLen != length.Hash) {
				// drop state hash if updated or hashLen < 32 (corner case, may even not encode such leaf hashes)
				if hph.trace {
					fmt.Printf("DROP hash for (%d, %x, depth=%d) %s\n", row, nibble, depth, cell.FullString())
				}
				cell.stateHashLen = 0
				hadToReset.Add(1)
				if cell.accountAddrLen > 0 {
					counters.accReset++
				}
				if cell.storageAddrLen > 0 {
					counters.storReset++
				}
			}

			if cell.stateHashLen == 0 { // load state if needed
				if !cell.loaded.account() && cell.accountAddrLen > 0 {
					upd, err := hph.ctx.Account(cell.accountAddr[:cell.accountAddrLen])
					if err != nil {
						return fmt.Errorf("failed to get account: %w", err)
					}
					cell.setFromUpdate(upd)
					// if update is empty, loaded flag was not updated so do it manually
					cell.loaded = cell.loaded.addFlag(cellLoadAccount)
					counters.accLoaded++
				}
				if !cell.loaded.storage() && cell.storageAddrLen > 0 {
					upd, err := hph.ctx.Storage(cell.storageAddr[:cell.storageAddrLen])
					if err != nil {
						return fmt.Errorf("failed to get storage: %w", err)
					}
					cell.setFromUpdate(upd)
					// if update is empty, loaded flag was not updated so do it manually
					cell.loaded = cell.loaded.addFlag(cellLoadStorage)
					counters.storLoaded++
				}
				// computeCellHash can reset hash as well so have to check if node has been skipped  right after computeCellHash.
			}
			hph.hadToLoadL[hph.depthsToTxNum[depth]] = counters
			/* end of memoization */

			totalBranchLen += hph.computeCellHashLen(cell, depth)
			bitset ^= bit
		}

		hph.keccak2.Reset()
		pt := rlp.GenerateStructLen(hph.hashAuxBuffer[:], totalBranchLen)
		if _, err := hph.keccak2.Write(hph.hashAuxBuffer[:pt]); err != nil {
			return err
		}

		b := [...]byte{0x80}
		cellGetter := func(nibble int, skip bool) (*cell, error) {
			if skip {
				if _, err := hph.keccak2.Write(b[:]); err != nil {
					return nil, fmt.Errorf("failed to write empty nibble to hash: %w", err)
				}
				if hph.trace {
					fmt.Printf("  %x: empty(%d, %x, depth=%d)\n", nibble, row, nibble, depth)
				}
				return nil, nil
			}
			cell := &hph.grid[row][nibble]
			if cell.accountAddrLen > 0 && cell.stateHashLen == 0 && !cell.loaded.account() && !cell.Deleted() {
				//panic("account not loaded" + fmt.Sprintf("%x", cell.accountAddr[:cell.accountAddrLen]))
				log.Warn("account not loaded", "pref", updateKey, "c", fmt.Sprintf("(%d, %x, depth=%d", row, nibble, depth), "cell", cell.String())
			}
			if cell.storageAddrLen > 0 && cell.stateHashLen == 0 && !cell.loaded.storage() && !cell.Deleted() {
				//panic("storage not loaded" + fmt.Sprintf("%x", cell.storageAddr[:cell.storageAddrLen]))
				log.Warn("storage not loaded", "pref", updateKey, "c", fmt.Sprintf("(%d, %x, depth=%d", row, nibble, depth), "cell", cell.String())
			}

			loadedBefore := cell.loaded
			hashBefore := common.Copy(cell.stateHash[:cell.stateHashLen])

			cellHash, err := hph.computeCellHash(cell, depth, hph.hashAuxBuffer[:0])
			if err != nil {
				return nil, err
			}
			if hph.trace {
				fmt.Printf("  %x: computeCellHash(%d, %x, depth=%d)=[%x]\n", nibble, row, nibble, depth, cellHash)
			}

			if hashBefore != nil && (cell.accountAddrLen > 0 || cell.storageAddrLen > 0) {
				counters := hph.hadToLoadL[hph.depthsToTxNum[depth]]
				if !bytes.Equal(hashBefore, cell.stateHash[:cell.stateHashLen]) {
					if cell.accountAddrLen > 0 {
						counters.accReset++
						counters.accLoaded++
					}
					if cell.storageAddrLen > 0 {
						counters.storReset++
						counters.storLoaded++
					}
				} else {
					if cell.accountAddrLen > 0 && (!loadedBefore.account() && !cell.loaded.account()) {
						counters.accSkipped++
					}
					if cell.storageAddrLen > 0 && (!loadedBefore.storage() && !cell.loaded.storage()) {
						counters.storSkipped++
					}
				}
				hph.hadToLoadL[hph.depthsToTxNum[depth]] = counters
			}
			//if len(updateKey) > DepthWithoutNodeHashes {
			//	cell.hashLen = 0 // do not write hashes for storages in the branch node, should reset ext as well which can break unfolding.. -
			//  cell.extLen = 0
			//}
			if _, err := hph.keccak2.Write(cellHash); err != nil {
				return nil, err
			}

			return cell, nil
		}

		lastNibble, err := hph.branchEncoder.CollectUpdate(hph.ctx, updateKey, bitmap, hph.touchMap[row], hph.afterMap[row], cellGetter)
		if err != nil {
			return fmt.Errorf("failed to encode branch update: %w", err)
		}
		for i := lastNibble; i < 17; i++ {
			if _, err := hph.keccak2.Write(b[:]); err != nil {
				return err
			}
			if hph.trace {
				fmt.Printf("  %x: empty(%d, %x, depth=%d)\n", i, row, i, depth)
			}
		}
		upCell.extLen = depth - upDepth - 1
		upCell.hashedExtLen = upCell.extLen
		if upCell.extLen > 0 {
			copy(upCell.extension[:], hph.currentKey[upDepth:hph.currentKeyLen])
			copy(upCell.hashedExtension[:], hph.currentKey[upDepth:hph.currentKeyLen])
		}
		if depth < 64 {
			upCell.accountAddrLen = 0
		}
		upCell.storageAddrLen = 0
		upCell.hashLen = 32
		if _, err := hph.keccak2.Read(upCell.hash[:]); err != nil {
			return err
		}
		if hph.trace {
			fmt.Printf("} [%x]\n", upCell.hash[:])
		}
		hph.activeRows--
		if upDepth > 0 {
			hph.currentKeyLen = upDepth - 1
		} else {
			hph.currentKeyLen = 0
		}
	}
	return nil
}

func (hph *HexPatriciaHashed) deleteCell(hashedKey []byte) {
	if hph.trace {
		fmt.Printf("deleteCell, activeRows = %d\n", hph.activeRows)
	}
	var cell *cell
	if hph.activeRows == 0 { // Remove the root
		cell = &hph.root
		hph.rootTouched, hph.rootPresent = true, false
	} else {
		row := hph.activeRows - 1
		if hph.depths[row] < len(hashedKey) {
			if hph.trace {
				fmt.Printf("deleteCell skipping spurious delete depth=%d, len(hashedKey)=%d\n", hph.depths[row], len(hashedKey))
			}
			return
		}
		nibble := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[row][nibble]
		col := uint16(1) << nibble
		if hph.afterMap[row]&col != 0 {
			// Prevent "spurios deletions", i.e. deletion of absent items
			hph.touchMap[row] |= col
			hph.afterMap[row] &^= col
			if hph.trace {
				fmt.Printf("deleteCell setting (%d, %x)\n", row, nibble)
			}
		} else {
			if hph.trace {
				fmt.Printf("deleteCell ignoring (%d, %x)\n", row, nibble)
			}
		}
	}
	cell.reset()
}

// fetches cell by key and set touch/after maps. Requires that prefix to be already unfolded
func (hph *HexPatriciaHashed) updateCell(plainKey, hashedKey []byte, u *Update) (cell *cell) {
	if u.Deleted() {
		hph.deleteCell(hashedKey)
		return nil
	}

	var depth int
	if hph.activeRows == 0 {
		cell = &hph.root
		hph.rootTouched, hph.rootPresent = true, true
	} else {
		row := hph.activeRows - 1
		depth = hph.depths[row]
		nibble := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[row][nibble]
		col := uint16(1) << nibble

		hph.touchMap[row] |= col
		hph.afterMap[row] |= col
		if hph.trace {
			fmt.Printf("updateCell setting (%d, %x, depth=%d)\n", row, nibble, depth)
		}
	}
	if cell.hashedExtLen == 0 {
		copy(cell.hashedExtension[:], hashedKey[depth:])
		cell.hashedExtLen = len(hashedKey) - depth
		if hph.trace {
			fmt.Printf("set downHasheKey=[%x]\n", cell.hashedExtension[:cell.hashedExtLen])
		}
	} else {
		if hph.trace {
			fmt.Printf("keep downHasheKey=[%x]\n", cell.hashedExtension[:cell.hashedExtLen])
		}
	}
	if len(plainKey) == hph.accountKeyLen {
		cell.accountAddrLen = len(plainKey)
		copy(cell.accountAddr[:], plainKey)

		copy(cell.CodeHash[:], EmptyCodeHash) // todo check
	} else { // set storage key
		cell.storageAddrLen = len(plainKey)
		copy(cell.storageAddr[:], plainKey)
	}
	cell.stateHashLen = 0

	cell.setFromUpdate(u)
	if hph.trace {
		fmt.Printf("updateCell %x => %s\n", plainKey, u.String())
	}
	return cell
}

func (hph *HexPatriciaHashed) RootHash() ([]byte, error) {
	hph.root.stateHashLen = 0
	rootHash, err := hph.computeCellHash(&hph.root, 0, nil)
	if err != nil {
		return nil, err
	}
	return rootHash[1:], nil // first byte is 128+hash_len=160
}

func (hph *HexPatriciaHashed) followAndUpdate(hashedKey, plainKey []byte, stateUpdate *Update) (err error) {
	// fmt.Printf("mnt: %0x current: %x path %x\n", hph.mountedNib, hph.currentKey[:hph.currentKeyLen], hashedKey)
	// Keep folding until the currentKey is the prefix of the key we modify
	for hph.needFolding(hashedKey) {
		if err := hph.fold(); err != nil {
			return fmt.Errorf("fold: %w", err)
		}
	}
	// Now unfold until we step on an empty cell
	for unfolding := hph.needUnfolding(hashedKey); unfolding > 0; unfolding = hph.needUnfolding(hashedKey) {
		if err := hph.unfold(hashedKey, unfolding); err != nil {
			return fmt.Errorf("unfold: %w", err)
		}
	}

	if stateUpdate == nil {
		// Update the cell
		if len(plainKey) == hph.accountKeyLen {
			stateUpdate, err = hph.ctx.Account(plainKey)
			if err != nil {
				return fmt.Errorf("GetAccount for key %x failed: %w", plainKey, err)
			}
		} else {
			stateUpdate, err = hph.ctx.Storage(plainKey)
			if err != nil {
				return fmt.Errorf("GetStorage for key %x failed: %w", plainKey, err)
			}
		}
	}
	hph.updateCell(plainKey, hashedKey, stateUpdate)

	mxTrieProcessedKeys.Inc()
	return nil
}

func (hph *HexPatriciaHashed) foldMounted(nib int) (cell, int, error) {
	uptoroot := hph.activeRows == 0
	for hph.activeRows > 0 {
		if err := hph.fold(); err != nil {
			panic(err)
			return cell{}, 0, fmt.Errorf("final fold: %w", err)
		}
	}
	if uptoroot {
		return hph.root, 0, nil
	}
	_ = nib
	return hph.grid[0][hph.mountedNib], 1, nil
}
func (hph *HexPatriciaHashed) flush(ctx context.Context) error {
	return hph.branchEncoder.Load(hph.ctx, etl.TransformArgs{Quit: ctx.Done()})
}

func (hph *HexPatriciaHashed) Process(ctx context.Context, updates *Updates, logPrefix string) (rootHash []byte, err error) {
	var (
		m  runtime.MemStats
		ki uint64

		updatesCount = updates.Size()
		start        = time.Now()
		logEvery     = time.NewTicker(20 * time.Second)
	)
	defer logEvery.Stop()
	//hph.trace = true

	err = updates.HashSort(ctx, func(hashedKey, plainKey []byte, stateUpdate *Update) error {
		select {
		case <-logEvery.C:
			dbg.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s][agg] computing trie", logPrefix),
				"progress", fmt.Sprintf("%s/%s", common.PrettyCounter(ki), common.PrettyCounter(updatesCount)),
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

		default:
		}
		if hph.trace {
			fmt.Printf("\n%d/%d) plainKey [%x] hashedKey [%x] currentKey [%x]\n", ki+1, updatesCount, plainKey, hashedKey, hph.currentKey[:hph.currentKeyLen])
		}
		if err := hph.followAndUpdate(hashedKey, plainKey, stateUpdate); err != nil {
			return fmt.Errorf("followAndUpdate: %w", err)
		}

		ki++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hash sort failed: %w", err)
	}

	// Folding everything up to the root
	for hph.activeRows > 0 {
		if err := hph.fold(); err != nil {
			return nil, fmt.Errorf("final fold: %w", err)
		}
	}

	rootHash, err = hph.RootHash()
	if err != nil {
		return nil, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	if hph.trace {
		fmt.Printf("root hash %x updates %d\n", rootHash, updatesCount)
	}
	err = hph.branchEncoder.Load(hph.ctx, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return nil, fmt.Errorf("branch update failed: %w", err)
	}

	log.Debug("commitment finished, counters updated (no reset)",
		//"hadToLoad", common.PrettyCounter(hadToLoad.Load()), "skippedLoad", common.PrettyCounter(skippedLoad.Load()),
		//"hadToReset", common.PrettyCounter(hadToReset.Load()),
		"skip ratio", fmt.Sprintf("%.1f%%", 100*(float64(skippedLoad.Load())/float64(hadToLoad.Load()+skippedLoad.Load()))),
		"reset ratio", fmt.Sprintf("%.1f%%", 100*(float64(hadToReset.Load())/float64(hadToLoad.Load()))),
		"keys", common.PrettyCounter(ki), "spent", time.Since(start),
	)
	ends := make([]uint64, 0, len(hph.hadToLoadL))
	for k := range hph.hadToLoadL {
		ends = append(ends, k)
	}
	sort.Slice(ends, func(i, j int) bool { return ends[i] > ends[j] })
	var Li int
	for _, k := range ends {
		v := hph.hadToLoadL[k]
		accs := fmt.Sprintf("load=%s skip=%s (%.1f%%) reset %.1f%%", common.PrettyCounter(v.accLoaded), common.PrettyCounter(v.accSkipped), 100*(float64(v.accSkipped)/float64(v.accLoaded+v.accSkipped)), 100*(float64(v.accReset)/float64(v.accReset+v.accSkipped)))
		stors := fmt.Sprintf("load=%s skip=%s (%.1f%%) reset %.1f%%", common.PrettyCounter(v.storLoaded), common.PrettyCounter(v.storSkipped), 100*(float64(v.storSkipped)/float64(v.storLoaded+v.storSkipped)), 100*(float64(v.storReset)/float64(v.storReset+v.storSkipped)))
		if k == 0 {
			log.Debug("branchData memoization, new branches", "endStep", k, "accounts", accs, "storages", stors)
		} else {
			log.Debug("branchData memoization", "L", Li, "endStep", k, "accounts", accs, "storages", stors)
			Li++

			mxTrieStateLevelledSkipRatesAccount[min(Li, 5)].Add(float64(v.accSkipped))
			mxTrieStateLevelledSkipRatesStorage[min(Li, 5)].Add(float64(v.storSkipped))
			mxTrieStateLevelledLoadRatesAccount[min(Li, 5)].Add(float64(v.accLoaded))
			mxTrieStateLevelledLoadRatesStorage[min(Li, 5)].Add(float64(v.storLoaded))
		}
	}
	return rootHash, nil
}

func (hph *HexPatriciaHashed) SetTrace(trace bool) { hph.trace = trace }

func (hph *HexPatriciaHashed) Variant() TrieVariant { return VariantHexPatriciaTrie }

// Reset allows HexPatriciaHashed instance to be reused for the new commitment calculation
func (hph *HexPatriciaHashed) Reset() {
	hph.root.reset()
	hph.rootTouched = false
	hph.rootChecked = false
	hph.rootPresent = true
}

func (hph *HexPatriciaHashed) ResetContext(ctx PatriciaContext) {
	hph.ctx = ctx
}

type stateRootFlag int8

var (
	stateRootPresent stateRootFlag = 1
	stateRootChecked stateRootFlag = 2
	stateRootTouched stateRootFlag = 4
)

// represents state of the tree
type state struct {
	Root         []byte      // encoded root cell
	Depths       [128]int    // For each row, the depth of cells in that row
	TouchMap     [128]uint16 // For each row, bitmap of cells that were either present before modification, or modified or deleted
	AfterMap     [128]uint16 // For each row, bitmap of cells that were present after modification
	BranchBefore [128]bool   // For each row, whether there was a branch node in the database loaded in unfold
	RootChecked  bool        // Set to false if it is not known whether the root is empty, set to true if it is checked
	RootTouched  bool
	RootPresent  bool
}

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
	if err := binary.Write(ee, binary.BigEndian, int8(rootFlags)); err != nil {
		return nil, fmt.Errorf("encode rootFlags: %w", err)
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

func (cell *cell) Encode() []byte {
	var pos = 1
	size := pos + 5 + cell.hashLen + cell.accountAddrLen + cell.storageAddrLen + cell.hashedExtLen + cell.extLen // max size
	buf := make([]byte, size)

	var flags uint8
	if cell.hashLen != 0 {
		flags |= cellFlagHash
		buf[pos] = byte(cell.hashLen)
		pos++
		copy(buf[pos:pos+cell.hashLen], cell.hash[:])
		pos += cell.hashLen
	}
	if cell.accountAddrLen != 0 {
		flags |= cellFlagAccount
		buf[pos] = byte(cell.accountAddrLen)
		pos++
		copy(buf[pos:pos+cell.accountAddrLen], cell.accountAddr[:])
		pos += cell.accountAddrLen
	}
	if cell.storageAddrLen != 0 {
		flags |= cellFlagStorage
		buf[pos] = byte(cell.storageAddrLen)
		pos++
		copy(buf[pos:pos+cell.storageAddrLen], cell.storageAddr[:])
		pos += cell.storageAddrLen
	}
	if cell.hashedExtLen != 0 {
		flags |= cellFlagDownHash
		buf[pos] = byte(cell.hashedExtLen)
		pos++
		copy(buf[pos:pos+cell.hashedExtLen], cell.hashedExtension[:cell.hashedExtLen])
		pos += cell.hashedExtLen
	}
	if cell.extLen != 0 {
		flags |= cellFlagExtension
		buf[pos] = byte(cell.extLen)
		pos++
		copy(buf[pos:pos+cell.extLen], cell.extension[:])
		pos += cell.extLen //nolint
	}
	if cell.Deleted() {
		flags |= cellFlagDelete
	}
	buf[0] = flags
	return buf
}

const (
	cellFlagHash = uint8(1 << iota)
	cellFlagAccount
	cellFlagStorage
	cellFlagDownHash
	cellFlagExtension
	cellFlagDelete
)

func (cell *cell) Decode(buf []byte) error {
	if len(buf) < 1 {
		return errors.New("invalid buffer size to contain cell (at least 1 byte expected)")
	}
	cell.reset()

	var pos int
	flags := buf[pos]
	pos++

	if flags&cellFlagHash != 0 {
		cell.hashLen = int(buf[pos])
		pos++
		copy(cell.hash[:], buf[pos:pos+cell.hashLen])
		pos += cell.hashLen
	}
	if flags&cellFlagAccount != 0 {
		cell.accountAddrLen = int(buf[pos])
		pos++
		copy(cell.accountAddr[:], buf[pos:pos+cell.accountAddrLen])
		pos += cell.accountAddrLen
	}
	if flags&cellFlagStorage != 0 {
		cell.storageAddrLen = int(buf[pos])
		pos++
		copy(cell.storageAddr[:], buf[pos:pos+cell.storageAddrLen])
		pos += cell.storageAddrLen
	}
	if flags&cellFlagDownHash != 0 {
		cell.hashedExtLen = int(buf[pos])
		pos++
		copy(cell.hashedExtension[:], buf[pos:pos+cell.hashedExtLen])
		pos += cell.hashedExtLen
	}
	if flags&cellFlagExtension != 0 {
		cell.extLen = int(buf[pos])
		pos++
		copy(cell.extension[:], buf[pos:pos+cell.extLen])
		pos += cell.extLen //nolint
	}
	if flags&cellFlagDelete != 0 {
		log.Warn("deleted cell should not be encoded", "cell", cell.String())
		cell.Update.Flags = DeleteUpdate
	}
	return nil
}

// Encode current state of hph into bytes
func (hph *HexPatriciaHashed) EncodeCurrentState(buf []byte) ([]byte, error) {
	s := state{
		RootChecked: hph.rootChecked,
		RootTouched: hph.rootTouched,
		RootPresent: hph.rootPresent,
	}
	if hph.currentKeyLen > 0 {
		panic("currentKeyLen > 0")
	}

	s.Root = hph.root.Encode()
	copy(s.Depths[:], hph.depths[:])
	copy(s.BranchBefore[:], hph.branchBefore[:])
	copy(s.TouchMap[:], hph.touchMap[:])
	copy(s.AfterMap[:], hph.afterMap[:])

	return s.Encode(buf)
}

// buf expected to be encoded hph state. Decode state and set up hph to that state.
func (hph *HexPatriciaHashed) SetState(buf []byte) error {
	hph.Reset()

	if buf == nil {
		// reset state to 'empty'
		hph.currentKeyLen = 0
		hph.rootChecked = false
		hph.rootTouched = false
		hph.rootPresent = false
		hph.activeRows = 0

		for i := 0; i < len(hph.depths); i++ {
			hph.depths[i] = 0
			hph.branchBefore[i] = false
			hph.touchMap[i] = 0
			hph.afterMap[i] = 0
		}
		return nil
	}
	if hph.activeRows != 0 {
		return errors.New("target trie has active rows, could not reset state before fold")
	}

	var s state
	if err := s.Decode(buf); err != nil {
		return err
	}

	if err := hph.root.Decode(s.Root); err != nil {
		return err
	}
	hph.rootChecked = s.RootChecked
	hph.rootTouched = s.RootTouched
	hph.rootPresent = s.RootPresent

	copy(hph.depths[:], s.Depths[:])
	copy(hph.branchBefore[:], s.BranchBefore[:])
	copy(hph.touchMap[:], s.TouchMap[:])
	copy(hph.afterMap[:], s.AfterMap[:])

	if hph.root.accountAddrLen > 0 {
		if hph.ctx == nil {
			panic("nil ctx")
		}

		update, err := hph.ctx.Account(hph.root.accountAddr[:hph.root.accountAddrLen])
		if err != nil {
			return err
		}
		hph.root.setFromUpdate(update)
	}
	if hph.root.storageAddrLen > 0 {
		if hph.ctx == nil {
			panic("nil ctx")
		}
		update, err := hph.ctx.Storage(hph.root.storageAddr[:hph.root.storageAddrLen])
		if err != nil {
			return err
		}
		hph.root.setFromUpdate(update)
		//hph.root.deriveHashedKeys(0, hph.keccak, hph.accountKeyLen)
	}

	return nil
}

func HexTrieExtractStateRoot(enc []byte) ([]byte, error) {
	if len(enc) < 18 { // 8*2+2
		return nil, fmt.Errorf("invalid state length %x (min %d expected)", len(enc), 18)
	}

	//txn := binary.BigEndian.Uint64(enc)
	//bn := binary.BigEndian.Uint64(enc[8:])
	sl := binary.BigEndian.Uint16(enc[16:18])
	var s state
	if err := s.Decode(enc[18 : 18+sl]); err != nil {
		return nil, err
	}
	root := new(cell)
	if err := root.Decode(s.Root); err != nil {
		return nil, err
	}
	return root.hash[:], nil
}

func HexTrieStateToString(enc []byte) (string, error) {
	if len(enc) < 18 {
		return "", fmt.Errorf("invalid state length %x (min %d expected)", len(enc), 18)
	}
	txn := binary.BigEndian.Uint64(enc)
	bn := binary.BigEndian.Uint64(enc[8:])
	sl := binary.BigEndian.Uint16(enc[16:18])

	var s state
	sb := new(strings.Builder)
	if err := s.Decode(enc[18 : 18+sl]); err != nil {
		return "", err
	}
	fmt.Fprintf(sb, "block: %d txn: %d\n", bn, txn)
	// fmt.Fprintf(sb, " touchMaps: %v\n", s.TouchMap)
	// fmt.Fprintf(sb, " afterMaps: %v\n", s.AfterMap)
	// fmt.Fprintf(sb, " depths: %v\n", s.Depths)

	printAfterMap := func(sb *strings.Builder, name string, list []uint16, depths []int, existedBefore []bool) {
		fmt.Fprintf(sb, "\t::%s::\n\n", name)
		lastNonZero := 0
		for i := len(list) - 1; i >= 0; i-- {
			if list[i] != 0 {
				lastNonZero = i
				break
			}
		}
		for i, v := range list {
			newBranchSuf := ""
			if !existedBefore[i] {
				newBranchSuf = " NEW"
			}

			fmt.Fprintf(sb, " d=%3d %016b%s\n", depths[i], v, newBranchSuf)
			if i == lastNonZero {
				break
			}
		}
	}
	fmt.Fprintf(sb, " rootNode: %x [touched=%t, present=%t, checked=%t]\n", s.Root, s.RootTouched, s.RootPresent, s.RootChecked)

	root := new(cell)
	if err := root.Decode(s.Root); err != nil {
		return "", err
	}

	fmt.Fprintf(sb, "RootHash: %x\n", root.hash)
	printAfterMap(sb, "afterMap", s.AfterMap[:], s.Depths[:], s.BranchBefore[:])

	return sb.String(), nil
}

//func bytesToUint64(buf []byte) (x uint64) {
//	for i, b := range buf {
//		x = x<<8 + uint64(b)
//		if i == 7 {
//			return
//		}
//	}
//	return
//}

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

// nolint
// Hashes provided key and expands resulting hash into nibbles (each byte split into two nibbles by 4 bits)
func (hph *HexPatriciaHashed) hashAndNibblizeKey(key []byte) []byte {
	hashedKey := make([]byte, length.Hash)

	hph.keccak.Reset()
	fp := length.Addr
	if len(key) < length.Addr {
		fp = len(key)
	}
	hph.keccak.Write(key[:fp])
	hph.keccak.Read(hashedKey[:length.Hash])

	if len(key[fp:]) > 0 {
		hashedKey = append(hashedKey, make([]byte, length.Hash)...)
		hph.keccak.Reset()
		hph.keccak.Write(key[fp:])
		hph.keccak.Read(hashedKey[length.Hash:])
	}

	nibblized := make([]byte, len(hashedKey)*2)
	for i, b := range hashedKey {
		nibblized[i*2] = (b >> 4) & 0xf
		nibblized[i*2+1] = b & 0xf
	}
	return nibblized
}

type ParallelPatriciaHashed struct {
	root   *HexPatriciaHashed
	mounts [16]*HexPatriciaHashed
	ctx    PatriciaContext
	//ch [16]chan
}

func (p *ParallelPatriciaHashed) unfoldRoot() error {
	zero := []byte{0}
	// Now unfold until we step on an empty cell
	for unfolding := p.root.needUnfolding(zero); unfolding > 0; unfolding = p.root.needUnfolding(zero) {
		if err := p.root.unfold(zero, unfolding); err != nil {
			return fmt.Errorf("unfold: %w", err)
		}
	}

	for i := range p.mounts {
		if p.mounts[i] == nil {
			panic(fmt.Sprintf("nibble %x is nil", i))
		}
		hph := p.mounts[i]
		hph.Reset()

		hph.mountedNib = i
		for unfolding := hph.needUnfolding(zero); unfolding > 0; unfolding = hph.needUnfolding(zero) {
			if err := hph.unfold(zero, unfolding); err != nil {
				return fmt.Errorf("unfold: %w", err)
			}
		}
		p.mounts[i] = hph
	}
	return nil
}

func NewParallelPatriciaHashed(root *HexPatriciaHashed, ctx PatriciaContext, tmpdir string) (*ParallelPatriciaHashed, error) {
	p := &ParallelPatriciaHashed{root: root}

	for i := range p.mounts {
		hph := NewHexPatriciaHashed(length.Addr, ctx, tmpdir)
		hph.mountedNib = i
		p.mounts[i] = hph
	}

	if err := p.unfoldRoot(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *ParallelPatriciaHashed) Close() {
	for i := range p.mounts {
		p.mounts[i].Reset()
	}
}

func (p *ParallelPatriciaHashed) SetTrace(b bool) {
	p.root.SetTrace(b)
	for i := range p.mounts {
		p.mounts[i].SetTrace(b)
	}
}

func (p *ParallelPatriciaHashed) Process(ctx context.Context, updates *Updates, logPrefix string) (rootHash []byte, err error) {
	if err = p.unfoldRoot(); err != nil {
		return nil, err
	}

	// foldAndFlush := func(prevByte, curByte byte) error {
	// 	c, d, err := p.mounts[prevByte].foldMounted(int(prevByte))
	// 	if err != nil {
	// 		return err
	// 	}
	// 	_ = d

	// 	//fmt.Printf("\tmount d=%d n=%0x %s\n", d, prevByte, c.FullString())
	// 	p.root.grid[0][prevByte] = c
	// 	if err = p.mounts[prevByte].branchEncoder.Load(p.ctx, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
	// 		return err
	// 	}
	// 	p.mounts[prevByte].Reset()
	// 	p.mounts[prevByte].currentKeyLen = 0
	// 	if p.mounts[prevByte].activeRows >= 0 {
	// 		p.mounts[prevByte].depths[0] = 0
	// 		p.mounts[prevByte].activeRows = 0
	// 		p.mounts[prevByte].touchMap[0] = 0
	// 		p.mounts[prevByte].afterMap[0] = 0
	// 	}

	// 	p.root.touchMap[0] |= uint16(1) << prevByte
	// 	p.root.afterMap[0] |= uint16(1) << prevByte
	// 	p.root.depths[0] = 1
	// 	return nil
	// }

	// var prevByte byte
	// var prevset bool

	// err = updates.HashSort(ctx, func(hashedKey, plainKey []byte, stateUpdate *Update) error {
	// 	err = p.mounts[hashedKey[0]].followAndUpdate(hashedKey, plainKey, stateUpdate)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if !prevset {
	// 		prevByte = hashedKey[0]
	// 		prevset = true
	// 	}
	// 	if prevByte != hashedKey[0] {
	// 		if err := foldAndFlush(prevByte, hashedKey[0]); err != nil {
	// 			return err
	// 		}
	// 		prevByte = hashedKey[0]
	// 	}
	// 	return nil
	// })
	// if err != nil {
	// 	return nil, err
	// }

	// if err := foldAndFlush(prevByte, 0); err != nil {
	// 	return nil, err
	// }
	//
	err = updates.ParallelHashSort(ctx, p)
	if err != nil {
		return nil, err
	}

	if p.root.activeRows == 0 {
		p.root.activeRows = 1
	}

	// p.root.trace = true
	for p.root.activeRows > 0 {
		if err = p.root.fold(); err != nil {
			return nil, err
		}
	}
	rootHash, err = p.root.RootHash()
	if err != nil {
		return nil, err
	}

	err = p.root.branchEncoder.Load(p.ctx, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return nil, fmt.Errorf("branch update failed: %w", err)
	}

	return rootHash, nil
}
