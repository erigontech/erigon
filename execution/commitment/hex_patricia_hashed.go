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
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
	witnesstypes "github.com/erigontech/erigon/execution/types/witness"
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

	mounted      bool                 // true if this trie is mounted to some root trie
	mountedNib   int                  // if 0 <= nib <= 15 means mounted to some root. If -1, means it's a storage subtrie so must not be folded above depth 63
	mountedTries []*HexPatriciaHashed // list of mounted tries to unmount

	memoizationOff bool // if true, do not rely on memoized hashes
	//temp buffers
	accValBuf rlp.RlpEncodedBytes

	//processing metrics
	metrics       *Metrics
	depthsToTxNum [129]uint64 // endTxNum of file with branch data for that depth
	hadToLoadL    map[uint64]skipStat
}

// Clones current trie state to allow concurrent processing.
func (hph *HexPatriciaHashed) SpawnSubTrie(ctx PatriciaContext, forNibble int) *HexPatriciaHashed {
	subTrie := NewHexPatriciaHashed(hph.accountKeyLen, ctx)

	subTrie.mountTo(hph, forNibble)
	return subTrie
}

func NewHexPatriciaHashed(accountKeyLen int, ctx PatriciaContext) *HexPatriciaHashed {
	hph := &HexPatriciaHashed{
		ctx:           ctx,
		keccak:        sha3.NewLegacyKeccak256().(keccakState),
		keccak2:       sha3.NewLegacyKeccak256().(keccakState),
		accountKeyLen: accountKeyLen,
		auxBuffer:     bytes.NewBuffer(make([]byte, 8192)),
		hadToLoadL:    make(map[uint64]skipStat),
		accValBuf:     make(rlp.RlpEncodedBytes, 128),
		metrics:       NewMetrics(),
		branchEncoder: NewBranchEncoder(1024),
	}

	hph.branchEncoder.setMetrics(hph.metrics)
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

	// temporary buffers
	hashBuf [length.Hash]byte
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
	emptyRootHashBytes = empty.RootHash.Bytes()
)

func (cell *cell) hashAccKey(keccak keccakState, depth int) error {
	return hashKey(keccak, cell.accountAddr[:cell.accountAddrLen], cell.hashedExtension[:], depth, cell.hashBuf[:])
}

func (cell *cell) hashStorageKey(keccak keccakState, accountKeyLen, downOffset int, hashedKeyOffset int) error {
	return hashKey(keccak, cell.storageAddr[accountKeyLen:cell.storageAddrLen], cell.hashedExtension[downOffset:], hashedKeyOffset, cell.hashBuf[:])
}

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
		if cell.CodeHash != empty.CodeHash {
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
			cell.CodeHash = upCell.CodeHash
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
		cell.CodeHash = lowCell.CodeHash
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
			if err := cell.hashAccKey(keccak, depth); err != nil {
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
			if err := cell.hashStorageKey(keccak, accountKeyLen, downOffset, hashedKeyOffset); err != nil {
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
		cell.CodeHash = empty.CodeHash
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

func (hph *HexPatriciaHashed) completeLeafHash(buf []byte, compactLen int, key []byte, compact0 byte, ni int, val rlp.RlpSerializable, singleton bool) ([]byte, error) {
	// Compute the total length of binary representation
	var kp, kl int
	var keyPrefix [1]byte
	if compactLen > 1 {
		keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}

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
	return hph.completeLeafHash(buf, compactLen, key, compact0, ni, val, singleton)
}

func (hph *HexPatriciaHashed) accountLeafHashWithKey(buf, key []byte, val rlp.RlpSerializable) ([]byte, error) {
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
			compact0 = terminatorHexByte + key[0] // Odd (1<<4) + first nibble
			ni = 1
		}
	}
	return hph.completeLeafHash(buf, compactLen, key, compact0, ni, val, true)
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

func (hph *HexPatriciaHashed) witnessComputeCellHashWithStorage(cell *cell, depth int, buf []byte) ([]byte, bool, []byte, error) {
	var err error
	var storageRootHash [length.Hash]byte
	var storageRootHashIsSet bool
	if hph.memoizationOff {
		cell.stateHashLen = 0 // Reset stateHashLen to force recompute
	}
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
		if err = hashKey(hph.keccak, cell.storageAddr[koffset:cell.storageAddrLen], cell.hashedExtension[:], hashedKeyOffset, cell.hashBuf[:]); err != nil {
			return nil, storageRootHashIsSet, nil, err
		}
		cell.hashedExtension[64-hashedKeyOffset] = terminatorHexByte // Add terminator

		if cell.stateHashLen > 0 {
			res := append([]byte{160}, cell.stateHash[:cell.stateHashLen]...)
			hph.keccak.Reset()
			if hph.trace {
				fmt.Printf("REUSED stateHash %x spk %x\n", res, cell.storageAddr[:cell.storageAddrLen])
			}
			mxTrieStateSkipRate.Inc()
			skippedLoad.Add(1)
			if !singleton {
				return res, storageRootHashIsSet, nil, err
			} else {
				storageRootHashIsSet = true
				storageRootHash = *(*[length.Hash]byte)(res[1:])
				//copy(storageRootHash[:], res[1:])
				//cell.stateHashLen = 0
			}
		} else {
			if !cell.loaded.storage() {
				hph.metrics.StorageLoad(cell.storageAddr[:cell.storageAddrLen])
				update, err := hph.ctx.Storage(cell.storageAddr[:cell.storageAddrLen])
				if err != nil {
					return nil, storageRootHashIsSet, nil, err
				}
				cell.setFromUpdate(update)
				if hph.trace {
					fmt.Printf("Storage %x was not loaded\n", cell.storageAddr[:cell.storageAddrLen])
				}
			}
			if singleton {
				if hph.trace {
					fmt.Printf("leafHashWithKeyVal(singleton) for [%x]=>[%x]\n", cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
				}
				aux := make([]byte, 0, 33)
				if aux, err = hph.leafHashWithKeyVal(aux, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], true); err != nil {
					return nil, storageRootHashIsSet, nil, err
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
					return nil, storageRootHashIsSet, nil, err
				}

				copy(cell.stateHash[:], leafHash[1:])
				cell.stateHashLen = len(leafHash) - 1
				if hph.trace {
					fmt.Printf("STATE HASH storage memoized %x spk %x\n", leafHash, cell.storageAddr[:cell.storageAddrLen])
				}

				return leafHash, storageRootHashIsSet, storageRootHash[:], nil
			}
		}
	}
	if cell.accountAddrLen > 0 {
		if err := hashKey(hph.keccak, cell.accountAddr[:cell.accountAddrLen], cell.hashedExtension[:], depth, cell.hashBuf[:]); err != nil {
			return nil, storageRootHashIsSet, nil, err
		}
		cell.hashedExtension[64-depth] = terminatorHexByte // Add terminator
		if !storageRootHashIsSet {
			if cell.extLen > 0 { // Extension
				if cell.hashLen == 0 {
					return nil, storageRootHashIsSet, nil, errors.New("computeCellHash extension without hash")
				}
				if hph.trace {
					fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
				}
				if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
					return nil, storageRootHashIsSet, nil, err
				}
				if hph.trace {
					fmt.Printf("EXTENSION HASH %x DROPS stateHash\n", storageRootHash)
				}
				cell.stateHashLen = 0
				hadToReset.Add(1)
			} else if cell.hashLen > 0 {
				storageRootHash = cell.hash
				storageRootHashIsSet = true
			} else {
				storageRootHash = empty.RootHash
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
				return res, storageRootHashIsSet, storageRootHash[:], nil
			}
			// storage root update or extension update could invalidate older stateHash, so we need to reload state
			hph.metrics.AccountLoad(cell.accountAddr[:cell.accountAddrLen])
			update, err := hph.ctx.Account(cell.accountAddr[:cell.accountAddrLen])
			if err != nil {
				return nil, storageRootHashIsSet, storageRootHash[:], err
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
			return nil, storageRootHashIsSet, nil, err
		}
		if hph.trace {
			fmt.Printf("STATE HASH account memoized %x\n", leafHash)
		}
		copy(cell.stateHash[:], leafHash[1:])
		cell.stateHashLen = len(leafHash) - 1
		return leafHash, storageRootHashIsSet, storageRootHash[:], nil
	}

	buf = append(buf, 0x80+32)
	if cell.extLen > 0 { // Extension
		if cell.hashLen > 0 {
			if hph.trace {
				fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
			}
			var hash [length.Hash]byte
			if hash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
				return nil, storageRootHashIsSet, storageRootHash[:], err
			}
			buf = append(buf, hash[:]...)
		} else {
			return nil, storageRootHashIsSet, storageRootHash[:], errors.New("computeCellHash extension without hash")
		}
	} else if cell.hashLen > 0 {
		buf = append(buf, cell.hash[:cell.hashLen]...)
	} else if storageRootHashIsSet {
		buf = append(buf, storageRootHash[:]...)
		copy(cell.hash[:], storageRootHash[:])
		cell.hashLen = len(storageRootHash)
	} else {
		buf = append(buf, emptyRootHashBytes...)
	}
	return buf, storageRootHashIsSet, storageRootHash[:], nil
}

func (hph *HexPatriciaHashed) computeCellHash(cell *cell, depth int, buf []byte) ([]byte, error) {
	var err error
	var storageRootHash [length.Hash]byte
	var storageRootHashIsSet bool
	if hph.memoizationOff {
		cell.stateHashLen = 0 // Reset stateHashLen to force recompute
	}
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
		if err = cell.hashStorageKey(hph.keccak, koffset, 0, hashedKeyOffset); err != nil {
			return nil, err
		}
		cell.hashedExtension[64-hashedKeyOffset] = terminatorHexByte // Add terminator

		if cell.stateHashLen > 0 {
			hph.keccak.Reset()
			if hph.trace {
				fmt.Printf("REUSED stateHash %x spk %x\n", cell.stateHash[:cell.stateHashLen], cell.storageAddr[:cell.storageAddrLen])
			}
			mxTrieStateSkipRate.Inc()
			skippedLoad.Add(1)
			if !singleton {
				return append(append(buf[:0], byte(160)), cell.stateHash[:cell.stateHashLen]...), nil
			}
			storageRootHashIsSet = true
			storageRootHash = *(*[length.Hash]byte)(cell.stateHash[:cell.stateHashLen])
		} else {
			if !cell.loaded.storage() {
				return nil, fmt.Errorf("storage %x was not loaded as expected: cell %v", cell.storageAddr[:cell.storageAddrLen], cell.String())
				// update, err := hph.ctx.Storage(cell.storageAddr[:cell.storageAddrLen])
				// if err != nil {
				// 	return nil, err
				// }
				// cell.setFromUpdate(update)
			}

			leafHash, err := hph.leafHashWithKeyVal(buf, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], singleton)
			if err != nil {
				return nil, err
			}
			if hph.trace {
				fmt.Printf("leafHashWithKeyVal(singleton=%t) {%x} for [%x]=>[%x] %v\n",
					singleton, leafHash, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], cell.String())
			}
			if !singleton {
				copy(cell.stateHash[:], leafHash[1:])
				cell.stateHashLen = len(leafHash) - 1
				return leafHash, nil
			}
			storageRootHash = *(*[length.Hash]byte)(leafHash[1:])
			storageRootHashIsSet = true
			cell.stateHashLen = 0
			hadToReset.Add(1)
		}
	}
	if cell.accountAddrLen > 0 {
		if err := cell.hashAccKey(hph.keccak, depth); err != nil {
			return nil, err
		}
		cell.hashedExtension[64-depth] = terminatorHexByte // Add terminator
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
				storageRootHash = empty.RootHash
			}
		}
		if !cell.loaded.account() {
			if cell.stateHashLen > 0 {
				hph.keccak.Reset()

				mxTrieStateSkipRate.Inc()
				skippedLoad.Add(1)
				if hph.trace {
					fmt.Printf("REUSED stateHash %x apk %x\n", cell.stateHash[:cell.stateHashLen], cell.accountAddr[:cell.accountAddrLen])
				}
				return append(append(buf[:0], byte(160)), cell.stateHash[:cell.stateHashLen]...), nil
			}
			// storage root update or extension update could invalidate older stateHash, so we need to reload state
			hph.metrics.AccountLoad(cell.accountAddr[:cell.accountAddrLen])
			update, err := hph.ctx.Account(cell.accountAddr[:cell.accountAddrLen])
			if err != nil {
				return nil, err
			}
			cell.setFromUpdate(update)
		}

		valLen := cell.accountForHashing(hph.accValBuf, storageRootHash)
		buf, err = hph.accountLeafHashWithKey(buf, cell.hashedExtension[:65-depth], hph.accValBuf[:valLen])
		if err != nil {
			return nil, err
		}
		if hph.trace {
			fmt.Printf("accountLeafHashWithKey {%x} (memorised) for [%x]=>[%x]\n", buf, cell.hashedExtension[:65-depth], hph.accValBuf[:valLen])
		}
		copy(cell.stateHash[:], buf[1:])
		cell.stateHashLen = len(buf) - 1
		return buf, nil
	}

	buf = append(buf, 0x80+32)
	if cell.extLen > 0 { // Extension
		if cell.hashLen > 0 {
			if hph.trace {
				fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
			}
			if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
				return nil, err
			}
			buf = append(buf, storageRootHash[:]...)
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
		buf = append(buf, emptyRootHashBytes...)
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

func (c *cell) IsEmpty() bool {
	return c.hashLen == 0 && c.hashedExtLen == 0 && c.extLen == 0 && c.accountAddrLen == 0 && c.storageAddrLen == 0
}

func (c *cell) String() string {
	s := "("
	if c.hashLen > 0 {
		s += fmt.Sprintf("hash(len=%d)=%x, ", c.hashLen, c.hash)
	}
	if c.hashedExtLen > 0 {
		s += fmt.Sprintf("hashedExtension(len=%d)=%x, ", c.hashedExtLen, c.hashedExtension[:c.hashedExtLen])
	}
	if c.extLen > 0 {
		s += fmt.Sprintf("extension(len=%d)=%x, ", c.extLen, c.extension[:c.extLen])
	}
	if c.accountAddrLen > 0 {
		s += fmt.Sprintf("accountAddr=%x, ", c.accountAddr)
	}
	if c.storageAddrLen > 0 {
		s += fmt.Sprintf("storageAddr=%x, ", c.storageAddr)
	}

	s += ")"
	return s
}

func (hph *HexPatriciaHashed) PrintGrid() {
	fmt.Printf("GRID:\n")
	for row := 0; row < hph.activeRows; row++ {
		fmt.Printf("row %d depth %d:\n", row, hph.depths[row])
		for col := 0; col < 16; col++ {
			cell := &hph.grid[row][col]
			if cell.hashedExtLen > 0 || cell.accountAddrLen > 0 {
				var cellHash []byte
				cellHash, _, _, err := hph.witnessComputeCellHashWithStorage(cell, hph.depths[row], nil)
				if err != nil {
					panic("failed to compute cell hash")
				}
				fmt.Printf("\t %x: %v cellHash=%x, \n", col, cell, cellHash)
			} else {
				fmt.Printf("\t %x: %v , \n", col, cell)
			}
		}
		fmt.Printf("\n")
	}
	fmt.Printf("\n")
}

// this function is only related to the witness
func (hph *HexPatriciaHashed) witnessCreateAccountNode(c *cell, row int, hashedKey []byte, codeReads map[common.Hash]witnesstypes.CodeWithHash) (*trie.AccountNode, error) {
	_, storageIsSet, storageRootHash, err := hph.witnessComputeCellHashWithStorage(c, hph.depths[row], nil)
	if err != nil {
		return nil, err
	}
	accountUpdate, err := hph.ctx.Account(c.accountAddr[:c.accountAddrLen])
	if err != nil {
		return nil, err
	}
	var account accounts.Account
	account.Nonce = accountUpdate.Nonce
	account.Balance = accountUpdate.Balance
	account.Initialised = true
	account.Root = accountUpdate.Storage
	account.CodeHash = accountUpdate.CodeHash

	addrHash, err := compactKey(hashedKey[:64])
	if err != nil {
		return nil, err
	}

	// get code
	var code []byte
	codeWithHash, hasCode := codeReads[[32]byte(addrHash)]
	if !hasCode {
		code = nil
	} else {
		code = codeWithHash.Code
		// sanity check
		if account.CodeHash != codeWithHash.CodeHash {
			return nil, fmt.Errorf("account.CodeHash(%x)!=codeReads[%x].CodeHash(%x)", account.CodeHash, addrHash, codeWithHash.CodeHash)
		}
	}

	var accountNode *trie.AccountNode
	if !storageIsSet {
		account.Root = trie.EmptyRoot
		accountNode = &trie.AccountNode{Account: account, Storage: nil, RootCorrect: true, Code: code, CodeSize: -1}
	} else {
		accountNode = &trie.AccountNode{Account: account, Storage: trie.NewHashNode(storageRootHash), RootCorrect: true, Code: code, CodeSize: -1}
	}
	return accountNode, nil
}

func (hph *HexPatriciaHashed) nCellsInRow(row int) int { //nolint:unused
	count := 0
	for col := 0; col < 16; col++ {
		c := &hph.grid[row][col]
		if !c.IsEmpty() {
			count++
		}
	}
	return count
}

// Traverse the grid following `hashedKey` and produce the witness `triedeprecated.Trie` for that key
func (hph *HexPatriciaHashed) toWitnessTrie(hashedKey []byte, codeReads map[common.Hash]witnesstypes.CodeWithHash) (*trie.Trie, error) {
	var rootNode trie.Node = &trie.FullNode{}
	var currentNode trie.Node = rootNode
	keyPos := 0 // current position in hashedKey (usually same as row, but could be different due to extension nodes)

	if hph.root.hashedExtLen > 0 {
		currentNode = &trie.ShortNode{Key: common.Copy(hph.root.hashedExtension[:hph.root.hashedExtLen]), Val: &trie.FullNode{}}
		// currentNode = &trie.ShortNode{Val: &trie.FullNode{}}
		rootNode = currentNode             // use root node as the current node
		keyPos = hph.root.hashedExtLen - 1 // start from the end of the root extension
		fmt.Printf("[witness] root node %s, pos %d\n", hph.root.FullString(), keyPos)
	}

	for row := 0; row < hph.activeRows && keyPos < len(hashedKey); row++ {
		currentNibble := hashedKey[keyPos]
		// determine the type of the next node to expand (in the next iteration)
		var nextNode trie.Node
		// need to check node type along the key path
		cellToExpand := &hph.grid[row][currentNibble]
		// determine the next node
		if hph.root.hashedExtLen > 0 && currentNode == rootNode {
			currentNode = currentNode.(*trie.ShortNode).Val
			keyPos++
			continue

		} else if cellToExpand.hashedExtLen > 0 { // extension cell
			depthAdjusted := false
			extKeyLength := cellToExpand.hashedExtLen
			if hph.depths[row] < 64 && extKeyLength+hph.depths[row] > 64 { //&& cellToExpand.accountAddrLen > 0 {
				extKeyLength = 64 - hph.depths[row] // adjust depth to stop before storage trie
				depthAdjusted = true
				if hph.trace {
					fmt.Printf("[witness] adjusted hashExtLen=%d <- %d\n", extKeyLength, cellToExpand.hashedExtLen)
				}
			}

			keyPos += extKeyLength // jump ahead
			hashedExtKey := cellToExpand.hashedExtension[:extKeyLength]
			if keyPos+1 == len(hashedKey) || keyPos+1 == 64 {
				extKeyLength++ //  +1 for the terminator 0x10 ([16])  byte when on a terminal extension node
			}
			extensionKey := make([]byte, extKeyLength)
			copy(extensionKey, hashedExtKey)
			if keyPos+1 == len(hashedKey) || keyPos+1 == 64 {
				extensionKey[len(extensionKey)-1] = terminatorHexByte // append terminator byte
			}
			nextNode = &trie.ShortNode{Key: extensionKey} // Value will be in the next iteration
			if keyPos+1 == len(hashedKey) {
				if cellToExpand.storageAddrLen > 0 && !depthAdjusted {
					storageUpdate, err := hph.ctx.Storage(cellToExpand.storageAddr[:cellToExpand.storageAddrLen])
					if err != nil {
						return nil, err
					}
					storageValueNode := trie.ValueNode(storageUpdate.Storage[:storageUpdate.StorageLen])
					nextNode = &trie.ShortNode{Key: extensionKey, Val: storageValueNode}
				} else if cellToExpand.accountAddrLen > 0 {
					accNode, err := hph.witnessCreateAccountNode(cellToExpand, row, hashedKey, codeReads)
					if err != nil {
						return nil, err
					}
					nextNode = &trie.ShortNode{Key: extensionKey, Val: accNode}
					extNodeSubTrie := trie.NewInMemoryTrie(nextNode)
					subTrieRoot := extNodeSubTrie.Root()
					cellHash, _, _, _ := hph.witnessComputeCellHashWithStorage(cellToExpand, hph.depths[row], nil)
					if !bytes.Equal(subTrieRoot, cellHash[1:]) {
						return nil, fmt.Errorf("subTrieRoot(%x) != cellHash(%x)", subTrieRoot, cellHash[1:])
					}
					// // DEBUG patch with cell hash which we know to be correct
					//fmt.Printf("witness cell (%d, %0x, depth=%d) %s\n", row, currentNibble, hph.depths[row], cellToExpand.FullString())
					//nextNode = trie.NewHashNode(cellToExpand.stateHash[:])
				}
			}
		} else if cellToExpand.storageAddrLen > 0 { // storage cell
			storageUpdate, err := hph.ctx.Storage(cellToExpand.storageAddr[:cellToExpand.storageAddrLen])
			if err != nil {
				return nil, err
			}
			storageValueNode := trie.ValueNode(storageUpdate.Storage[:storageUpdate.StorageLen])
			nextNode = &storageValueNode //nolint:ineffassign, wastedassign
			break
		} else if cellToExpand.accountAddrLen > 0 { // account cell
			accNode, err := hph.witnessCreateAccountNode(cellToExpand, row, hashedKey, codeReads)
			if err != nil {
				return nil, err
			}
			nextNode = accNode
			keyPos++ // only move one nibble
		} else if cellToExpand.hashLen > 0 { // hash cell means we will expand using a full node
			nextNode = &trie.FullNode{}
			keyPos++
		} else if cellToExpand.IsEmpty() {
			nextNode = nil // no more expanding can happen (this could be due )
		} else { // default for now before we handle extLen
			nextNode = &trie.FullNode{}
			keyPos++

			if hph.trace {
				fmt.Printf("[witness] DefaultFullNode cell (%d, %0x, depth=%d) %s %+v\n", row, currentNibble, hph.depths[row], cellToExpand.FullString(), nextNode)
			}
		}

		if hph.trace {
			fmt.Printf("[witness] nextNode (%d, %0x, depth=%d) %T %+v %s keyPos %d\n", row, currentNibble, hph.depths[row], nextNode, nextNode, cellToExpand.FullString(), keyPos)
			fmt.Printf("[witness] currentNode (%d, %0x, depth=%d) %T %+v %s keyPos %d\n", row, currentNibble, hph.depths[row], currentNode, currentNode, cellToExpand.FullString(), keyPos)
		}

		// process the current node
		if fullNode, ok := currentNode.(*trie.FullNode); ok { // handle full node case
			for col := 0; col < 16; col++ {
				currentCell := &hph.grid[row][col]
				if currentCell.IsEmpty() {
					fullNode.Children[col] = nil
					continue
				}
				cellHash, _, _, err := hph.witnessComputeCellHashWithStorage(currentCell, hph.depths[row], nil)
				if err != nil {
					return nil, err
				}
				fullNode.Children[col] = trie.NewHashNode(cellHash[1:]) // because cellHash has 33 bytes and we want 32

				if hph.trace {
					fmt.Printf("[witness, pos %d] FullNodeChild Hash (%d, %0x, depth=%d) %s proof %+v\n", keyPos, row, col, hph.depths[row], currentCell.FullString(), fullNode.Children[col])
				}
			}
			fullNode.Children[currentNibble] = nextNode // ready to expand next nibble in the path
		} else if accNode, ok := currentNode.(*trie.AccountNode); ok {
			if len(hashedKey) <= 64 { // no storage, stop here
				nextNode = nil // nolint:ineffassign, wastedassign
				if hph.trace {
					fmt.Printf("[witness] AccountNode (break) (%d, %0x, depth=%d) %s proof %+v\n", row, currentNibble, hph.depths[row], cellToExpand.FullString(), accNode)
				}
				break
			}

			// there is storage so we need to expand further
			accNode.Storage = nextNode
			if hph.trace {
				fmt.Printf("[witness] AccountNode (+storage) (%d, %0x, depth=%d) %s proof %+v\n", row, currentNibble, hph.depths[row], cellToExpand.FullString(), accNode)
			}
		} else if extNode, ok := currentNode.(*trie.ShortNode); ok { // handle extension node case
			// expect only one item in this row, so take the first one
			// technically it should be at the last nibble of the key but we will adjust this later
			if extNode.Val != nil { // early termination
				break
			}
			extNode.Val = nextNode

			if hph.trace {
				fmt.Printf("[witness, pos %d] ShortNode (%d, %0x, depth=%d) %s proof %+v\n", keyPos, row, currentNibble, hph.depths[row], cellToExpand.FullString(), extNode)
			}
		} else {
			if hph.trace {
				fmt.Printf("[witness] current node is nil (%d, %0x, depth=%d) %s proof %+v\n", row, currentNibble, hph.depths[row], cellToExpand.FullString(), currentNode)
			}
			break // break if currentNode is nil
		}
		// we need to check if we are dealing with the next node being an account node and we have a storage key,
		// in that case start a new tree for the storage
		if nextAccNode, ok := nextNode.(*trie.AccountNode); ok && len(hashedKey) > 64 {
			nextNode = &trie.FullNode{}
			nextAccNode.Storage = nextNode
			if hph.trace {
				fmt.Printf("[witness] AccountNode (+StorageTrie) (%d, %0x, depth=%d) %s [proof %+v\n", row, currentNibble, hph.depths[row], cellToExpand.FullString(), nextAccNode)
			}
		}
		currentNode = nextNode
	}
	tr := trie.NewInMemoryTrie(rootNode)
	return tr, nil
}

// unfoldBranchNode returns true if unfolding has been done
func (hph *HexPatriciaHashed) unfoldBranchNode(row, depth int, deleted bool) (bool, error) {
	key := hexNibblesToCompactBytes(hph.currentKey[:hph.currentKeyLen])
	hph.metrics.BranchLoad(hph.currentKey[:hph.currentKeyLen])
	branchData, step, err := hph.ctx.Branch(key)
	if err != nil {
		return false, err
	}
	fileEndTxNum := uint64(step) // TODO: investigate why we cast step to txNum!
	hph.depthsToTxNum[depth] = fileEndTxNum
	if len(branchData) >= 2 {
		branchData = branchData[2:] // skip touch map and keep the rest
	}
	if hph.trace {
		fmt.Printf("unfoldBranchNode prefix '%x', nibbles [%x] depth %d row %d '%x'\n",
			key, hph.currentKey[:hph.currentKeyLen], depth, row, branchData)
	}
	if !hph.rootChecked && hph.currentKeyLen == 0 && len(branchData) == 0 {
		// Special case - empty or deleted root
		hph.rootChecked = true
		return false, nil
	}
	if len(branchData) == 0 {
		log.Warn("got empty branch data during unfold", "key", hex.EncodeToString(key), "row", row, "depth", depth, "deleted", deleted)
		if hph.trace {
			branchData, _, _ = hph.ctx.Branch(key)
			fmt.Printf("unfoldBranchNode prefix '%x', nibbles [%x] depth %d row %d '%x' %s\n", key, hph.currentKey[:hph.currentKeyLen], depth, row, branchData, BranchData(branchData).String())
		}
		return false, fmt.Errorf("empty branch data read during unfold, compact prefix %x nibbles %x", key, hph.currentKey[:hph.currentKeyLen])
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

const DepthWithoutNodeHashes = 35 //nolint

func (hph *HexPatriciaHashed) createCellGetter(b []byte, updateKey []byte, row, depth int) func(nibble int, skip bool) (*cell, error) {
	hashBefore := make([]byte, 32) // buffer reused between calls
	return func(nibble int, skip bool) (*cell, error) {
		if skip {
			if _, err := hph.keccak2.Write(b); err != nil {
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
		copy(hashBefore, cell.stateHash[:cell.stateHashLen])
		hashBefore = hashBefore[:cell.stateHashLen]

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
		if _, err := hph.keccak2.Write(cellHash); err != nil {
			return nil, err
		}

		return cell, nil
	}
}

const terminatorHexByte = 16 // max nibble value +1. Defines end of nibble line in the trie or splits address and storage space in trie.

// updateKind is a type of update that is being applied to the trie structure.
type updateKind uint8

const (
	// updateKindDelete means after we processed longest common prefix, row ended up empty.
	updateKindDelete updateKind = 0b0

	// updateKindPropagate is an update operation ended up with a single nibble which is leaf or extension node.
	// We do not store keys with only one cell as a value in db, instead we copy them upwards to the parent branch.
	//
	// In case current prefix existed before and node is fused to upper level, this causes deletion for current prefix
	// and update of branch value on upper level.
	// 	e.g.: leaf was at prefix 0xbeef, but we fuse it in level above, so
	//  - delete 0xbeef
	//  - update 0xbee
	updateKindPropagate updateKind = 0b01

	// updateKindBranch is an update operation ended up as a branch of 2+ cells.
	// That does not necessarily means that branch is NEW, it could be an existing branch that was updated.
	updateKindBranch updateKind = 0b10
)

// Kind defines how exactly given update should be folded upwards to the parent branch or root.
// It also returns number of nibbles that left in branch after the operation.
func afterMapUpdateKind(afterMap uint16) (kind updateKind, nibblesAfterUpdate int) {
	nibblesAfterUpdate = bits.OnesCount16(afterMap)
	switch nibblesAfterUpdate {
	case 0:
		return updateKindDelete, nibblesAfterUpdate
	case 1:
		return updateKindPropagate, nibblesAfterUpdate
	default:
		return updateKindBranch, nibblesAfterUpdate
	}
}

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
	upRow := row - 1
	if row == 0 {
		if hph.trace {
			fmt.Printf("fold: parent is root %s\n", hph.root.FullString())
		}
		upCell = &hph.root
	} else {
		upDepth = hph.depths[upRow]
		nibble = int(hph.currentKey[upDepth-1])
		if hph.trace {
			fmt.Printf("fold: parent (%d, %x, depth=%d)\n", upRow, nibble, upDepth)
		}
		upCell = &hph.grid[upRow][nibble]
	}

	depth := hph.depths[row]
	updateKey := hexNibblesToCompactBytes(hph.currentKey[:updateKeyLen])
	defer func() { hph.depthsToTxNum[depth] = 0 }()

	if hph.trace {
		fmt.Printf("fold: (row=%d, {%s}, depth=%d) prefix [%x] touchMap: %016b afterMap: %016b \n",
			row, updatedNibs(hph.touchMap[row]&hph.afterMap[row]), depth, hph.currentKey[:hph.currentKeyLen], hph.touchMap[row], hph.afterMap[row])
	}

	updateKind, nibblesLeftAfterUpdate := afterMapUpdateKind(hph.afterMap[row])
	switch updateKind {
	case updateKindDelete: // Everything deleted
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
	case updateKindPropagate: // Leaf or extension node
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
		// propagate cell into parent row
		upCell.fillFromLowerCell(cell, depth, hph.currentKey[upDepth:hph.currentKeyLen], nibble)

		if hph.branchBefore[row] { // encode Delete if prefix existed before
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
	case updateKindBranch:
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
		totalBranchLen := 17 - nibblesLeftAfterUpdate // For every empty cell, one byte
		for bitset, j := hph.afterMap[row], 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			cell := &hph.grid[row][nibble]

			if hph.memoizationOff {
				cell.stateHashLen = 0
			}
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
					hph.metrics.AccountLoad(cell.accountAddr[:cell.accountAddrLen])
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
					hph.metrics.StorageLoad(cell.storageAddr[:cell.storageAddrLen])
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
		cellGetter := hph.createCellGetter(b[:], updateKey, row, depth)
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
			// Prevent "spurious deletions", i.e. deletion of absent items
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
	hph.metrics.Updates(plainKey)

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

		cell.CodeHash = empty.CodeHash
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
	//if hph.trace {
	// fmt.Printf("mnt: %0x current: %x path %x\n", hph.mountedNib, hph.currentKey[:hph.currentKeyLen], hashedKey)
	//}
	// Keep folding until the currentKey is the prefix of the key we modify
	for hph.needFolding(hashedKey) {
		foldDone := hph.metrics.StartFolding(plainKey)
		if err := hph.fold(); err != nil {
			return fmt.Errorf("fold: %w", err)
		}
		foldDone()
	}
	// Now unfold until we step on an empty cell
	for unfolding := hph.needUnfolding(hashedKey); unfolding > 0; unfolding = hph.needUnfolding(hashedKey) {
		printLater := hph.currentKeyLen == 0 && hph.mounted && hph.trace
		unfoldDone := hph.metrics.StartUnfolding(plainKey)
		if err := hph.unfold(hashedKey, unfolding); err != nil {
			return fmt.Errorf("unfold: %w", err)
		}
		unfoldDone()
		if printLater {
			fmt.Printf("[%x] subtrie pref '%x' d=%d\n", hph.mountedNib, hph.currentKey[:hph.currentKeyLen], hph.depths[max(0, hph.activeRows-1)])
		}
		// fmt.Printf("mnt: %0x current: %x path %x\n", hph.mountedNib, hph.currentKey[:hph.currentKeyLen], hashedKey)
	}

	if stateUpdate == nil {
		// Update the cell
		if len(plainKey) == hph.accountKeyLen {
			hph.metrics.AccountLoad(plainKey)
			stateUpdate, err = hph.ctx.Account(plainKey)
			if err != nil {
				return fmt.Errorf("GetAccount for key %x failed: %w", plainKey, err)
			}
		} else {
			hph.metrics.StorageLoad(plainKey)
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

func (hph *HexPatriciaHashed) foldMounted(nib int) (cell, error) {
	if nib != hph.mountedNib {
		panic(fmt.Sprintf("foldMounted: nib (%x)!= mountedNib (%x)", nib, hph.mountedNib))
	}

	if hph.trace {
		fmt.Printf("====[%x] folding rows %d depths %+v\n", hph.mountedNib, hph.activeRows, hph.depths[:hph.activeRows])
		defer func() { fmt.Printf("=======[%x] folded =========\n", hph.mountedNib) }()
	}

	for hph.activeRows > 0 {
		// fmt.Printf("===[%x] folding prefix %x (len %d)\n", hph.mountedNib, hph.currentKey[:hph.currentKeyLen], hph.currentKeyLen)
		if hph.activeRows == 1 && hph.depths[hph.activeRows-1] == 1 {
			if hph.trace {
				fmt.Printf("mount early as nibble %02x %s\n", hph.mountedNib, hph.grid[0][hph.mountedNib].String())
			}
			// fmt.Printf("===[%x] stop folding at %x\n", hph.mountedNib, hph.currentKey[:hph.currentKeyLen])
			return hph.grid[0][hph.mountedNib], nil
		}
		if err := hph.fold(); err != nil {
			return cell{}, fmt.Errorf("final fold: %w", err)
		}
	}

	if hph.trace {
		fmt.Printf("===[%x] !@folded to the root\n", hph.mountedNib)
	}
	if hph.rootPresent && hph.rootTouched {
		if hph.trace {
			fmt.Printf("mount root as %02x %s\n", hph.mountedNib, hph.root.String())
		}
		return hph.root, nil
	}
	if hph.trace {
		fmt.Printf("mount as nibble %02x %s\n", hph.mountedNib, hph.grid[0][hph.mountedNib].String())
	}
	// todo potential bug
	return hph.grid[0][hph.mountedNib], nil
}

// Generate the block witness. This works by loading each key from the list of updates (they are not really updates since we won't modify the trie,
// but currently need to be defined like that for the fold/unfold algorithm) into the grid and traversing the grid to convert it into `triedeprecated.Trie`.
// All the individual tries are combined to create the final witness trie.
// Because the grid is lacking information about the code in smart contract accounts which is also part of the witness, we need to provide that as an input parameter to this function (`codeReads`)
func (hph *HexPatriciaHashed) GenerateWitness(ctx context.Context, updates *Updates, codeReads map[common.Hash]witnesstypes.CodeWithHash, expectedRootHash []byte, logPrefix string) (witnessTrie *trie.Trie, rootHash []byte, err error) {
	var (
		m  runtime.MemStats
		ki uint64

		updatesCount = updates.Size()
		logEvery     = time.NewTicker(20 * time.Second)
	)
	hph.memoizationOff, hph.trace = true, false
	// defer func() {
	// 	hph.memoizationOff, hph.trace = false, false
	// }()

	defer logEvery.Stop()
	var tries []*trie.Trie = make([]*trie.Trie, 0, len(updates.keys)) // slice of tries, i.e the witness for each key, these will be all merged into single trie
	err = updates.HashSort(ctx, func(hashedKey, plainKey []byte, stateUpdate *Update) error {
		select {
		case <-logEvery.C:
			dbg.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s][agg] computing trie", logPrefix),
				"progress", fmt.Sprintf("%s/%s", common.PrettyCounter(ki), common.PrettyCounter(updatesCount)),
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

		default:
		}

		var tr *trie.Trie
		if hph.trace {
			fmt.Printf("\n%d/%d) witnessing [%x] hashedKey [%x] currentKey [%x]\n", ki+1, updatesCount, plainKey, hashedKey, hph.currentKey[:hph.currentKeyLen])
		}

		var update *Update
		if len(plainKey) == hph.accountKeyLen { // account
			update, err = hph.ctx.Account(plainKey)
			if err != nil {
				return fmt.Errorf("account with plainkey=%x not found: %w", plainKey, err)
			}
			if hph.trace {
				addrHash := crypto.Keccak256(plainKey)
				fmt.Printf("account with plainKey=%x, addrHash=%x FOUND = %v\n", plainKey, addrHash, update)
			}
		} else {
			update, err = hph.ctx.Storage(plainKey)
			if err != nil {
				return fmt.Errorf("storage with plainkey=%x not found: %w", plainKey, err)
			}
			if hph.trace {
				fmt.Printf("storage found = %v\n", update.Storage[:update.StorageLen])
			}
		}

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
		//hph.PrintGrid()
		//hph.updateCell(plainKey, hashedKey, update)

		// convert grid to trie.Trie
		tr, err = hph.toWitnessTrie(hashedKey, codeReads) // build witness trie for this key, based on the current state of the grid
		if err != nil {
			return err
		}
		//computedRootHash := tr.Root()
		//// fmt.Printf("computedRootHash = %x\n", computedRootHash)
		//
		//if !bytes.Equal(computedRootHash, expectedRootHash) {
		//	err = fmt.Errorf("root hash mismatch computedRootHash(%x)!=expectedRootHash(%x)", computedRootHash, expectedRootHash)
		//	return err
		//}

		tries = append(tries, tr)
		ki++
		return nil
	})

	if err != nil {
		return nil, nil, fmt.Errorf("hash sort failed: %w", err)
	}

	// Folding everything up to the root
	for hph.activeRows > 0 {
		if err := hph.fold(); err != nil {
			return nil, nil, fmt.Errorf("final fold: %w", err)
		}
	}

	rootHash, err = hph.RootHash()
	if err != nil {
		return nil, nil, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	if hph.trace {
		fmt.Printf("root hash %x updates %d\n", rootHash, updatesCount)
	}

	// merge all individual tries
	witnessTrie, err = trie.MergeTries(tries)
	if err != nil {
		return nil, nil, err
	}

	witnessTrieRootHash := witnessTrie.Root()

	// fmt.Printf("mergedTrieRootHash = %x\n", witnessTrieRootHash)

	if !bytes.Equal(witnessTrieRootHash, expectedRootHash) {
		return nil, nil, fmt.Errorf("root hash mismatch witnessTrieRootHash(%x)!=expectedRootHash(%x)", witnessTrieRootHash, expectedRootHash)
	}

	return witnessTrie, rootHash, nil
}

func (hph *HexPatriciaHashed) Process(ctx context.Context, updates *Updates, logPrefix string) (rootHash []byte, err error) {
	var (
		m  runtime.MemStats
		ki uint64
		//hph.trace = true

		updatesCount = updates.Size()
		start        = time.Now()
		logEvery     = time.NewTicker(20 * time.Second)
	)

	if collectCommitmentMetrics {
		hph.metrics.Reset()
		hph.metrics.updates.Store(updatesCount)
		defer func() {
			hph.metrics.TotalProcessingTimeInc(start)
			hph.metrics.WriteToCSV()
		}()
	}

	defer func() { logEvery.Stop() }()

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
		foldDone := hph.metrics.StartFolding(nil)
		if err = hph.fold(); err != nil {
			return nil, fmt.Errorf("final fold: %w", err)
		}
		foldDone()
	}

	rootHash, err = hph.RootHash()
	if err != nil {
		return nil, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	if hph.trace {
		fmt.Printf("root hash %x updates %d\n", rootHash, updatesCount)
	}

	hph.metrics.CollectFileDepthStats(hph.hadToLoadL)
	if dbg.KVReadLevelledMetrics {
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

func HexTrieStateToShortString(enc []byte) (string, error) {
	if len(enc) < 18 {
		return "", fmt.Errorf("invalid state length %x (min %d expected)", len(enc), 18)
	}
	txn := binary.BigEndian.Uint64(enc)
	bn := binary.BigEndian.Uint64(enc[8:])
	sl := binary.BigEndian.Uint16(enc[16:18])

	var s state
	if err := s.Decode(enc[18 : 18+sl]); err != nil {
		return "", err
	}
	root := new(cell)
	if err := root.Decode(s.Root); err != nil {
		return "", err
	}
	return fmt.Sprintf("block: %d txn: %d rootHash: %x", bn, txn, root.hash[:]), nil
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

func (hph *HexPatriciaHashed) Grid() [128][16]cell {
	return hph.grid
}

func (hph *HexPatriciaHashed) PrintAccountsInGrid() {
	fmt.Printf("SEARCHING FOR ACCOUNTS IN GRID\n")
	for row := 0; row < 128; row++ {
		for col := 0; col < 16; col++ {
			c := hph.grid[row][col]
			if c.accountAddr[19] != 0 && c.accountAddr[0] != 0 {
				fmt.Printf("FOUND account %x in position (%d,%d)\n", c.accountAddr, row, col)
			}
		}
	}
}
