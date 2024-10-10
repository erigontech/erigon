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
	"io"
	"math/bits"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/rlp"
)

const (
	maxKeySize  = 512
	halfKeySize = maxKeySize / 2
	maxChild    = 2
)

type bitstring []uint8

// converts slice of nibbles (lowest 4 bits of each byte) to bitstring
func hexToBin(hex []byte) bitstring {
	bin := make([]byte, 4*len(hex))
	for i := range bin {
		if hex[i/4]&(1<<(3-i%4)) != 0 {
			bin[i] = 1
		}
	}
	return bin
}

// encodes bitstring to its compact representation
func binToCompact(bin []byte) []byte {
	compact := make([]byte, 2+common.BitLenToByteLen(len(bin)))
	binary.BigEndian.PutUint16(compact, uint16(len(bin)))
	for i := 0; i < len(bin); i++ {
		if bin[i] != 0 {
			compact[2+i/8] |= byte(1) << (i % 8)
		}
	}
	return compact
}

// decodes compact bitstring representation into actual bitstring
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

// BinHashed implements commitment based on patricia merkle tree with radix 16,
// with keys pre-hashed by keccak256
type BinPatriciaHashed struct {
	root BinaryCell // Root cell of the tree
	// Rows of the grid correspond to the level of depth in the patricia tree
	// Columns of the grid correspond to pointers to the nodes further from the root
	grid [maxKeySize][maxChild]BinaryCell // First halfKeySize rows of this grid are for account trie, and next halfKeySize rows are for storage trie
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
	keccak        keccakState
	keccak2       keccakState
	accountKeyLen int
	trace         bool
	hashAuxBuffer [maxKeySize]byte // buffer to compute cell hash or write hash-related things
	auxBuffer     *bytes.Buffer    // auxiliary buffer used during branch updates encoding

	// Function used to load branch node and fill up the cells
	// For each cell, it sets the cell type, clears the modified flag, fills the hash,
	// and for the extension, account, and leaf type, the `l` and `k`
	branchFn func(prefix []byte) ([]byte, error)
	// Function used to fetch account with given plain key
	accountFn func(plainKey []byte, cell *BinaryCell) error
	// Function used to fetch account with given plain key
	storageFn func(plainKey []byte, cell *BinaryCell) error
}

func NewBinPatriciaHashed(accountKeyLen int,
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) error,
	storageFn func(plainKey []byte, cell *Cell) error,
) *BinPatriciaHashed {
	return &BinPatriciaHashed{
		keccak:        sha3.NewLegacyKeccak256().(keccakState),
		keccak2:       sha3.NewLegacyKeccak256().(keccakState),
		accountKeyLen: accountKeyLen,
		branchFn:      branchFn,
		accountFn:     wrapAccountStorageFn(accountFn),
		storageFn:     wrapAccountStorageFn(storageFn),
		auxBuffer:     bytes.NewBuffer(make([]byte, 8192)),
	}
}

type BinaryCell struct {
	h             [length.Hash]byte               // cell hash
	hl            int                             // Length of the hash (or embedded)
	apk           [length.Addr]byte               // account plain key
	apl           int                             // length of account plain key
	spk           [length.Addr + length.Hash]byte // storage plain key
	spl           int                             // length of the storage plain key
	downHashedKey [maxKeySize]byte
	downHashedLen int
	extension     [halfKeySize]byte
	extLen        int
	Nonce         uint64
	Balance       uint256.Int
	CodeHash      [length.Hash]byte // hash of the bytecode
	Storage       [length.Hash]byte
	StorageLen    int
	Delete        bool
}

func (cell *BinaryCell) unwrapToHexCell() (cl *Cell) {
	cl = new(Cell)
	cl.Balance = *cell.Balance.Clone()
	cl.Nonce = cell.Nonce
	cl.StorageLen = cell.StorageLen
	cl.apl = cell.apl
	cl.spl = cell.spl
	cl.hl = cell.hl

	copy(cl.apk[:], cell.apk[:])
	copy(cl.spk[:], cell.spk[:])
	copy(cl.h[:], cell.h[:])

	if cell.extLen > 0 {
		compactedExt := binToCompact(cell.extension[:cell.extLen])
		copy(cl.extension[:], compactedExt)
		cl.extLen = len(compactedExt)
	}
	if cell.downHashedLen > 0 {
		compactedDHK := binToCompact(cell.downHashedKey[:cell.downHashedLen])
		copy(cl.downHashedKey[:], compactedDHK)
		cl.downHashedLen = len(compactedDHK)
	}

	copy(cl.CodeHash[:], cell.CodeHash[:])
	copy(cl.Storage[:], cell.Storage[:])
	cl.Delete = cell.Delete
	return cl
}

var ( // TODO REEAVL
	EmptyBinRootHash, _ = hex.DecodeString("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	EmptyBinCodeHash, _ = hex.DecodeString("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
)

func (cell *BinaryCell) fillEmpty() {
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

func (cell *BinaryCell) fillFromUpperCell(upBinaryCell *BinaryCell, depth, depthIncrement int) {
	if upBinaryCell.downHashedLen >= depthIncrement {
		cell.downHashedLen = upBinaryCell.downHashedLen - depthIncrement
	} else {
		cell.downHashedLen = 0
	}
	if upBinaryCell.downHashedLen > depthIncrement {
		copy(cell.downHashedKey[:], upBinaryCell.downHashedKey[depthIncrement:upBinaryCell.downHashedLen])
	}
	if upBinaryCell.extLen >= depthIncrement {
		cell.extLen = upBinaryCell.extLen - depthIncrement
	} else {
		cell.extLen = 0
	}
	if upBinaryCell.extLen > depthIncrement {
		copy(cell.extension[:], upBinaryCell.extension[depthIncrement:upBinaryCell.extLen])
	}
	if depth <= halfKeySize {
		cell.apl = upBinaryCell.apl
		if upBinaryCell.apl > 0 {
			copy(cell.apk[:], upBinaryCell.apk[:cell.apl])
			cell.Balance.Set(&upBinaryCell.Balance)
			cell.Nonce = upBinaryCell.Nonce
			copy(cell.CodeHash[:], upBinaryCell.CodeHash[:])
			cell.extLen = upBinaryCell.extLen
			if upBinaryCell.extLen > 0 {
				copy(cell.extension[:], upBinaryCell.extension[:upBinaryCell.extLen])
			}
		}
	} else {
		cell.apl = 0
	}
	cell.spl = upBinaryCell.spl
	if upBinaryCell.spl > 0 {
		copy(cell.spk[:], upBinaryCell.spk[:upBinaryCell.spl])
		cell.StorageLen = upBinaryCell.StorageLen
		if upBinaryCell.StorageLen > 0 {
			copy(cell.Storage[:], upBinaryCell.Storage[:upBinaryCell.StorageLen])
		}
	}
	cell.hl = upBinaryCell.hl
	if upBinaryCell.hl > 0 {
		copy(cell.h[:], upBinaryCell.h[:upBinaryCell.hl])
	}
}

func (cell *BinaryCell) fillFromLowerBinaryCell(lowBinaryCell *BinaryCell, lowDepth int, preExtension []byte, nibble int) {
	if lowBinaryCell.apl > 0 || lowDepth < halfKeySize {
		cell.apl = lowBinaryCell.apl
	}
	if lowBinaryCell.apl > 0 {
		copy(cell.apk[:], lowBinaryCell.apk[:cell.apl])
		cell.Balance.Set(&lowBinaryCell.Balance)
		cell.Nonce = lowBinaryCell.Nonce
		copy(cell.CodeHash[:], lowBinaryCell.CodeHash[:])
	}
	cell.spl = lowBinaryCell.spl
	if lowBinaryCell.spl > 0 {
		copy(cell.spk[:], lowBinaryCell.spk[:cell.spl])
		cell.StorageLen = lowBinaryCell.StorageLen
		if lowBinaryCell.StorageLen > 0 {
			copy(cell.Storage[:], lowBinaryCell.Storage[:lowBinaryCell.StorageLen])
		}
	}
	if lowBinaryCell.hl > 0 {
		if (lowBinaryCell.apl == 0 && lowDepth < halfKeySize) || (lowBinaryCell.spl == 0 && lowDepth > halfKeySize) {
			// Extension is related to either accounts branch node, or storage branch node, we prepend it by preExtension | nibble
			if len(preExtension) > 0 {
				copy(cell.extension[:], preExtension)
			}
			cell.extension[len(preExtension)] = byte(nibble)
			if lowBinaryCell.extLen > 0 {
				copy(cell.extension[1+len(preExtension):], lowBinaryCell.extension[:lowBinaryCell.extLen])
			}
			cell.extLen = lowBinaryCell.extLen + 1 + len(preExtension)
		} else {
			// Extension is related to a storage branch node, so we copy it upwards as is
			cell.extLen = lowBinaryCell.extLen
			if lowBinaryCell.extLen > 0 {
				copy(cell.extension[:], lowBinaryCell.extension[:lowBinaryCell.extLen])
			}
		}
	}
	cell.hl = lowBinaryCell.hl
	if lowBinaryCell.hl > 0 {
		copy(cell.h[:], lowBinaryCell.h[:lowBinaryCell.hl])
	}
}

func (cell *BinaryCell) deriveHashedKeys(depth int, keccak keccakState, accountKeyLen int) error {
	extraLen := 0
	if cell.apl > 0 {
		if depth > halfKeySize {
			return fmt.Errorf("deriveHashedKeys accountPlainKey present at depth > halfKeySize")
		}
		extraLen = halfKeySize - depth
	}
	if cell.spl > 0 {
		if depth >= halfKeySize {
			extraLen = maxKeySize - depth
		} else {
			extraLen += halfKeySize
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
			downOffset = halfKeySize - depth
		}
		if cell.spl > 0 {
			if depth >= halfKeySize {
				hashedKeyOffset = depth - halfKeySize
			}
			if err := binHashKey(keccak, cell.spk[accountKeyLen:cell.spl], cell.downHashedKey[downOffset:], hashedKeyOffset); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cell *BinaryCell) fillFromFields(data []byte, pos int, fieldBits PartFlags) (int, error) {
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

func (cell *BinaryCell) setStorage(value []byte) {
	cell.StorageLen = len(value)
	if len(value) > 0 {
		copy(cell.Storage[:], value)
	}
}

func (cell *BinaryCell) setAccountFields(codeHash []byte, balance *uint256.Int, nonce uint64) {
	copy(cell.CodeHash[:], codeHash)

	cell.Balance.SetBytes(balance.Bytes())
	cell.Nonce = nonce
}

func (cell *BinaryCell) accountForHashing(buffer []byte, storageRootHash [length.Hash]byte) int {
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

func (bph *BinPatriciaHashed) completeLeafHash(buf, keyPrefix []byte, kp, kl, compactLen int, key []byte, compact0 byte, ni int, val rlp.RlpSerializable, singleton bool) ([]byte, error) {
	totalLen := kp + kl + val.DoubleRLPLen()
	var lenPrefix [4]byte
	pt := rlp.GenerateStructLen(lenPrefix[:], totalLen)
	embedded := !singleton && totalLen+pt < length.Hash
	var writer io.Writer
	if embedded {
		//bph.byteArrayWriter.Setup(buf)
		bph.auxBuffer.Reset()
		writer = bph.auxBuffer
	} else {
		bph.keccak.Reset()
		writer = bph.keccak
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
		buf = bph.auxBuffer.Bytes()
	} else {
		var hashBuf [33]byte
		hashBuf[0] = 0x80 + length.Hash
		if _, err := bph.keccak.Read(hashBuf[1:]); err != nil {
			return nil, err
		}
		buf = append(buf, hashBuf[:]...)
	}
	return buf, nil
}

func (bph *BinPatriciaHashed) leafHashWithKeyVal(buf, key []byte, val rlp.RlpSerializableBytes, singleton bool) ([]byte, error) {
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
	return bph.completeLeafHash(buf, keyPrefix[:], kp, kl, compactLen, key, compact0, ni, val, singleton)
}

func (bph *BinPatriciaHashed) accountLeafHashWithKey(buf, key []byte, val rlp.RlpSerializable) ([]byte, error) {
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
	return bph.completeLeafHash(buf, keyPrefix[:], kp, kl, compactLen, key, compact0, ni, val, true)
}

func (bph *BinPatriciaHashed) extensionHash(key []byte, hash []byte) ([length.Hash]byte, error) {
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
	bph.keccak.Reset()
	if _, err := bph.keccak.Write(lenPrefix[:pt]); err != nil {
		return hashBuf, err
	}
	if _, err := bph.keccak.Write(keyPrefix[:kp]); err != nil {
		return hashBuf, err
	}
	var b [1]byte
	b[0] = compact0
	if _, err := bph.keccak.Write(b[:]); err != nil {
		return hashBuf, err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := bph.keccak.Write(b[:]); err != nil {
			return hashBuf, err
		}
		ni += 2
	}
	b[0] = 0x80 + length.Hash
	if _, err := bph.keccak.Write(b[:]); err != nil {
		return hashBuf, err
	}
	if _, err := bph.keccak.Write(hash); err != nil {
		return hashBuf, err
	}
	// Replace previous hash with the new one
	if _, err := bph.keccak.Read(hashBuf[:]); err != nil {
		return hashBuf, err
	}
	return hashBuf, nil
}

func (bph *BinPatriciaHashed) computeBinaryCellHashLen(cell *BinaryCell, depth int) int {
	if cell.spl > 0 && depth >= halfKeySize {
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

func (bph *BinPatriciaHashed) computeBinaryCellHash(cell *BinaryCell, depth int, buf []byte) ([]byte, error) {
	var err error
	var storageRootHash [length.Hash]byte
	storageRootHashIsSet := false
	if cell.spl > 0 {
		var hashedKeyOffset int
		if depth >= halfKeySize {
			hashedKeyOffset = depth - halfKeySize
		}
		singleton := depth <= halfKeySize
		if err := binHashKey(bph.keccak, cell.spk[bph.accountKeyLen:cell.spl], cell.downHashedKey[:], hashedKeyOffset); err != nil {
			return nil, err
		}
		cell.downHashedKey[halfKeySize-hashedKeyOffset] = 16 // Add terminator
		if singleton {
			if bph.trace {
				fmt.Printf("leafHashWithKeyVal(singleton) for [%x]=>[%x]\n", cell.downHashedKey[:halfKeySize-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
			}
			aux := make([]byte, 0, 33)
			if aux, err = bph.leafHashWithKeyVal(aux, cell.downHashedKey[:halfKeySize-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], true); err != nil {
				return nil, err
			}
			storageRootHash = *(*[length.Hash]byte)(aux[1:])
			storageRootHashIsSet = true
		} else {
			if bph.trace {
				fmt.Printf("leafHashWithKeyVal for [%x]=>[%x]\n", cell.downHashedKey[:halfKeySize-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
			}
			return bph.leafHashWithKeyVal(buf, cell.downHashedKey[:halfKeySize-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], false)
		}
	}
	if cell.apl > 0 {
		if err := binHashKey(bph.keccak, cell.apk[:cell.apl], cell.downHashedKey[:], depth); err != nil {
			return nil, err
		}
		cell.downHashedKey[halfKeySize-depth] = 16 // Add terminator
		if !storageRootHashIsSet {
			if cell.extLen > 0 {
				// Extension
				if cell.hl > 0 {
					if bph.trace {
						fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.h[:cell.hl])
					}
					if storageRootHash, err = bph.extensionHash(cell.extension[:cell.extLen], cell.h[:cell.hl]); err != nil {
						return nil, err
					}
				} else {
					return nil, fmt.Errorf("computeBinaryCellHash extension without hash")
				}
			} else if cell.hl > 0 {
				storageRootHash = cell.h
			} else {
				storageRootHash = *(*[length.Hash]byte)(EmptyRootHash)
			}
		}
		var valBuf [128]byte
		valLen := cell.accountForHashing(valBuf[:], storageRootHash)
		if bph.trace {
			fmt.Printf("accountLeafHashWithKey for [%x]=>[%x]\n", bph.hashAuxBuffer[:halfKeySize+1-depth], valBuf[:valLen])
		}
		return bph.accountLeafHashWithKey(buf, cell.downHashedKey[:halfKeySize+1-depth], rlp.RlpEncodedBytes(valBuf[:valLen]))
	}
	buf = append(buf, 0x80+32)
	if cell.extLen > 0 {
		// Extension
		if cell.hl > 0 {
			if bph.trace {
				fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.h[:cell.hl])
			}
			var hash [length.Hash]byte
			if hash, err = bph.extensionHash(cell.extension[:cell.extLen], cell.h[:cell.hl]); err != nil {
				return nil, err
			}
			buf = append(buf, hash[:]...)
		} else {
			return nil, fmt.Errorf("computeBinaryCellHash extension without hash")
		}
	} else if cell.hl > 0 {
		buf = append(buf, cell.h[:cell.hl]...)
	} else {
		buf = append(buf, EmptyRootHash...)
	}
	return buf, nil
}

func (bph *BinPatriciaHashed) needUnfolding(hashedKey []byte) int {
	var cell *BinaryCell
	var depth int
	if bph.activeRows == 0 {
		if bph.trace {
			fmt.Printf("needUnfolding root, rootChecked = %t\n", bph.rootChecked)
		}
		if bph.rootChecked && bph.root.downHashedLen == 0 && bph.root.hl == 0 {
			// Previously checked, empty root, no unfolding needed
			return 0
		}
		cell = &bph.root
		if cell.downHashedLen == 0 && cell.hl == 0 && !bph.rootChecked {
			// Need to attempt to unfold the root
			return 1
		}
	} else {
		col := int(hashedKey[bph.currentKeyLen])
		cell = &bph.grid[bph.activeRows-1][col]
		depth = bph.depths[bph.activeRows-1]
		if bph.trace {
			fmt.Printf("needUnfolding cell (%d, %x), currentKey=[%x], depth=%d, cell.h=[%x]\n", bph.activeRows-1, col, bph.currentKey[:bph.currentKeyLen], depth, cell.h[:cell.hl])
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
	if bph.trace {
		fmt.Printf("cpl=%d, cell.downHashedKey=[%x], depth=%d, hashedKey[depth:]=[%x]\n", cpl, cell.downHashedKey[:cell.downHashedLen], depth, hashedKey[depth:])
	}
	unfolding := cpl + 1
	if depth < halfKeySize && depth+unfolding > halfKeySize {
		// This is to make sure that unfolding always breaks at the level where storage subtrees start
		unfolding = halfKeySize - depth
		if bph.trace {
			fmt.Printf("adjusted unfolding=%d\n", unfolding)
		}
	}
	return unfolding
}

// unfoldBranchNode returns true if unfolding has been done
func (bph *BinPatriciaHashed) unfoldBranchNode(row int, deleted bool, depth int) (bool, error) {
	branchData, err := bph.branchFn(binToCompact(bph.currentKey[:bph.currentKeyLen]))
	if err != nil {
		return false, err
	}
	if !bph.rootChecked && bph.currentKeyLen == 0 && len(branchData) == 0 {
		// Special case - empty or deleted root
		bph.rootChecked = true
		return false, nil
	}
	if len(branchData) == 0 {
		log.Warn("got empty branch data during unfold", "row", row, "depth", depth, "deleted", deleted)
	}
	bph.branchBefore[row] = true
	bitmap := binary.BigEndian.Uint16(branchData[0:])
	pos := 2
	if deleted {
		// All cells come as deleted (touched but not present after)
		bph.afterMap[row] = 0
		bph.touchMap[row] = bitmap
	} else {
		bph.afterMap[row] = bitmap
		bph.touchMap[row] = 0
	}
	//fmt.Printf("unfoldBranchNode [%x], afterMap = [%016b], touchMap = [%016b]\n", branchData, bph.afterMap[row], bph.touchMap[row])
	// Loop iterating over the set bits of modMask
	for bitset, j := bitmap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &bph.grid[row][nibble]
		fieldBits := branchData[pos]
		pos++
		var err error
		if pos, err = cell.fillFromFields(branchData, pos, PartFlags(fieldBits)); err != nil {
			return false, fmt.Errorf("prefix [%x], branchData[%x]: %w", bph.currentKey[:bph.currentKeyLen], branchData, err)
		}
		if bph.trace {
			fmt.Printf("cell (%d, %x) depth=%d, hash=[%x], a=[%x], s=[%x], ex=[%x]\n", row, nibble, depth, cell.h[:cell.hl], cell.apk[:cell.apl], cell.spk[:cell.spl], cell.extension[:cell.extLen])
		}
		if cell.apl > 0 {
			bph.accountFn(cell.apk[:cell.apl], cell)
			if bph.trace {
				fmt.Printf("accountFn[%x] return balance=%d, nonce=%d code=%x\n", cell.apk[:cell.apl], &cell.Balance, cell.Nonce, cell.CodeHash[:])
			}
		}
		if cell.spl > 0 {
			bph.storageFn(cell.spk[:cell.spl], cell)
		}
		if err = cell.deriveHashedKeys(depth, bph.keccak, bph.accountKeyLen); err != nil {
			return false, err
		}
		bitset ^= bit
	}
	return true, nil
}

func (bph *BinPatriciaHashed) unfold(hashedKey []byte, unfolding int) error {
	if bph.trace {
		fmt.Printf("unfold %d: activeRows: %d\n", unfolding, bph.activeRows)
	}
	var upCell *BinaryCell
	var touched, present bool
	var col byte
	var upDepth, depth int
	if bph.activeRows == 0 {
		if bph.rootChecked && bph.root.hl == 0 && bph.root.downHashedLen == 0 {
			// No unfolding for empty root
			return nil
		}
		upCell = &bph.root
		touched = bph.rootTouched
		present = bph.rootPresent
		if bph.trace {
			fmt.Printf("unfold root, touched %t, present %t, column %d\n", touched, present, col)
		}
	} else {
		upDepth = bph.depths[bph.activeRows-1]
		col = hashedKey[upDepth-1]
		upCell = &bph.grid[bph.activeRows-1][col]
		touched = bph.touchMap[bph.activeRows-1]&(uint16(1)<<col) != 0
		present = bph.afterMap[bph.activeRows-1]&(uint16(1)<<col) != 0
		if bph.trace {
			fmt.Printf("upCell (%d, %x), touched %t, present %t\n", bph.activeRows-1, col, touched, present)
		}
		bph.currentKey[bph.currentKeyLen] = col
		bph.currentKeyLen++
	}
	row := bph.activeRows
	for i := 0; i < maxChild; i++ {
		bph.grid[row][i].fillEmpty()
	}
	bph.touchMap[row] = 0
	bph.afterMap[row] = 0
	bph.branchBefore[row] = false
	if upCell.downHashedLen == 0 {
		depth = upDepth + 1
		if unfolded, err := bph.unfoldBranchNode(row, touched && !present /* deleted */, depth); err != nil {
			return err
		} else if !unfolded {
			// Return here to prevent activeRow from being incremented
			return nil
		}
	} else if upCell.downHashedLen >= unfolding {
		depth = upDepth + unfolding
		nibble := upCell.downHashedKey[unfolding-1]
		if touched {
			bph.touchMap[row] = uint16(1) << nibble
		}
		if present {
			bph.afterMap[row] = uint16(1) << nibble
		}
		cell := &bph.grid[row][nibble]
		cell.fillFromUpperCell(upCell, depth, unfolding)
		if bph.trace {
			fmt.Printf("cell (%d, %x) depth=%d\n", row, nibble, depth)
		}
		if row >= halfKeySize {
			cell.apl = 0
		}
		if unfolding > 1 {
			copy(bph.currentKey[bph.currentKeyLen:], upCell.downHashedKey[:unfolding-1])
		}
		bph.currentKeyLen += unfolding - 1
	} else {
		// upCell.downHashedLen < unfolding
		depth = upDepth + upCell.downHashedLen
		nibble := upCell.downHashedKey[upCell.downHashedLen-1]
		if touched {
			bph.touchMap[row] = uint16(1) << nibble
		}
		if present {
			bph.afterMap[row] = uint16(1) << nibble
		}
		cell := &bph.grid[row][nibble]
		cell.fillFromUpperCell(upCell, depth, upCell.downHashedLen)
		if bph.trace {
			fmt.Printf("cell (%d, %x) depth=%d\n", row, nibble, depth)
		}
		if row >= halfKeySize {
			cell.apl = 0
		}
		if upCell.downHashedLen > 1 {
			copy(bph.currentKey[bph.currentKeyLen:], upCell.downHashedKey[:upCell.downHashedLen-1])
		}
		bph.currentKeyLen += upCell.downHashedLen - 1
	}
	bph.depths[bph.activeRows] = depth
	bph.activeRows++
	return nil
}

func (bph *BinPatriciaHashed) needFolding(hashedKey []byte) bool {
	return !bytes.HasPrefix(hashedKey, bph.currentKey[:bph.currentKeyLen])
}

// The purpose of fold is to reduce hph.currentKey[:hph.currentKeyLen]. It should be invoked
// until that current key becomes a prefix of hashedKey that we will proccess next
// (in other words until the needFolding function returns 0)
func (bph *BinPatriciaHashed) fold() (branchData BranchData, updateKey []byte, err error) {
	updateKeyLen := bph.currentKeyLen
	if bph.activeRows == 0 {
		return nil, nil, fmt.Errorf("cannot fold - no active rows")
	}
	if bph.trace {
		fmt.Printf("fold: activeRows: %d, currentKey: [%x], touchMap: %016b, afterMap: %016b\n", bph.activeRows, bph.currentKey[:bph.currentKeyLen], bph.touchMap[bph.activeRows-1], bph.afterMap[bph.activeRows-1])
	}
	// Move information to the row above
	row := bph.activeRows - 1
	var upBinaryCell *BinaryCell
	var col int
	var upDepth int
	if bph.activeRows == 1 {
		if bph.trace {
			fmt.Printf("upcell is root\n")
		}
		upBinaryCell = &bph.root
	} else {
		upDepth = bph.depths[bph.activeRows-2]
		col = int(bph.currentKey[upDepth-1])
		if bph.trace {
			fmt.Printf("upcell is (%d x %x), upDepth=%d\n", row-1, col, upDepth)
		}
		upBinaryCell = &bph.grid[row-1][col]
	}

	depth := bph.depths[bph.activeRows-1]
	updateKey = binToCompact(bph.currentKey[:updateKeyLen])
	partsCount := bits.OnesCount16(bph.afterMap[row])

	if bph.trace {
		fmt.Printf("touchMap[%d]=%016b, afterMap[%d]=%016b\n", row, bph.touchMap[row], row, bph.afterMap[row])
	}
	switch partsCount {
	case 0:
		// Everything deleted
		if bph.touchMap[row] != 0 {
			if row == 0 {
				// Root is deleted because the tree is empty
				bph.rootTouched = true
				bph.rootPresent = false
			} else if upDepth == halfKeySize {
				// Special case - all storage items of an account have been deleted, but it does not automatically delete the account, just makes it empty storage
				// Therefore we are not propagating deletion upwards, but turn it into a modification
				bph.touchMap[row-1] |= uint16(1) << col
			} else {
				// Deletion is propagated upwards
				bph.touchMap[row-1] |= uint16(1) << col
				bph.afterMap[row-1] &^= uint16(1) << col
			}
		}
		upBinaryCell.hl = 0
		upBinaryCell.apl = 0
		upBinaryCell.spl = 0
		upBinaryCell.extLen = 0
		upBinaryCell.downHashedLen = 0
		if bph.branchBefore[row] {
			branchData, _, err = EncodeBranch(0, bph.touchMap[row], 0, func(nibble int, skip bool) (*Cell, error) { return nil, nil })
			if err != nil {
				return nil, updateKey, fmt.Errorf("failed to encode leaf node update: %w", err)
			}
		}
		bph.activeRows--
		if upDepth > 0 {
			bph.currentKeyLen = upDepth - 1
		} else {
			bph.currentKeyLen = 0
		}
	case 1:
		// Leaf or extension node
		if bph.touchMap[row] != 0 {
			// any modifications
			if row == 0 {
				bph.rootTouched = true
			} else {
				// Modifiction is propagated upwards
				bph.touchMap[row-1] |= uint16(1) << col
			}
		}
		nibble := bits.TrailingZeros16(bph.afterMap[row])
		cell := &bph.grid[row][nibble]
		upBinaryCell.extLen = 0
		upBinaryCell.fillFromLowerBinaryCell(cell, depth, bph.currentKey[upDepth:bph.currentKeyLen], nibble)
		// Delete if it existed
		if bph.branchBefore[row] {
			//branchData, _, err = bph.EncodeBranchDirectAccess(0, row, depth)
			branchData, _, err = EncodeBranch(0, bph.touchMap[row], 0, func(nibble int, skip bool) (*Cell, error) { return nil, nil })
			if err != nil {
				return nil, updateKey, fmt.Errorf("failed to encode leaf node update: %w", err)
			}
		}
		bph.activeRows--
		if upDepth > 0 {
			bph.currentKeyLen = upDepth - 1
		} else {
			bph.currentKeyLen = 0
		}
	default:
		// Branch node
		if bph.touchMap[row] != 0 {
			// any modifications
			if row == 0 {
				bph.rootTouched = true
			} else {
				// Modifiction is propagated upwards
				bph.touchMap[row-1] |= uint16(1) << col
			}
		}
		bitmap := bph.touchMap[row] & bph.afterMap[row]
		if !bph.branchBefore[row] {
			// There was no branch node before, so we need to touch even the singular child that existed
			bph.touchMap[row] |= bph.afterMap[row]
			bitmap |= bph.afterMap[row]
		}
		// Calculate total length of all hashes
		totalBranchLen := 17 - partsCount // For every empty cell, one byte
		for bitset, j := bph.afterMap[row], 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			cell := &bph.grid[row][nibble]
			totalBranchLen += bph.computeBinaryCellHashLen(cell, depth)
			bitset ^= bit
		}

		bph.keccak2.Reset()
		pt := rlp.GenerateStructLen(bph.hashAuxBuffer[:], totalBranchLen)
		if _, err := bph.keccak2.Write(bph.hashAuxBuffer[:pt]); err != nil {
			return nil, nil, err
		}

		b := [...]byte{0x80}
		cellGetter := func(nibble int, skip bool) (*Cell, error) {
			if skip {
				if _, err := bph.keccak2.Write(b[:]); err != nil {
					return nil, fmt.Errorf("failed to write empty nibble to hash: %w", err)
				}
				if bph.trace {
					fmt.Printf("%x: empty(%d,%x)\n", nibble, row, nibble)
				}
				return nil, nil
			}
			cell := &bph.grid[row][nibble]
			cellHash, err := bph.computeBinaryCellHash(cell, depth, bph.hashAuxBuffer[:0])
			if err != nil {
				return nil, err
			}
			if bph.trace {
				fmt.Printf("%x: computeBinaryCellHash(%d,%x,depth=%d)=[%x]\n", nibble, row, nibble, depth, cellHash)
			}
			if _, err := bph.keccak2.Write(cellHash); err != nil {
				return nil, err
			}

			// TODO extension and downHashedKey should be encoded to hex format and vice versa, data loss due to array sizes
			return cell.unwrapToHexCell(), nil
		}

		var lastNibble int
		var err error
		_ = cellGetter

		//branchData, lastNibble, err = bph.EncodeBranchDirectAccess(bitmap, row, depth, branchData)
		branchData, lastNibble, err = EncodeBranch(bitmap, bph.touchMap[row], bph.afterMap[row], cellGetter)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode branch update: %w", err)
		}
		for i := lastNibble; i <= maxChild; i++ {
			if _, err := bph.keccak2.Write(b[:]); err != nil {
				return nil, nil, err
			}
			if bph.trace {
				fmt.Printf("%x: empty(%d,%x)\n", i, row, i)
			}
		}
		upBinaryCell.extLen = depth - upDepth - 1
		upBinaryCell.downHashedLen = upBinaryCell.extLen
		if upBinaryCell.extLen > 0 {
			copy(upBinaryCell.extension[:], bph.currentKey[upDepth:bph.currentKeyLen])
			copy(upBinaryCell.downHashedKey[:], bph.currentKey[upDepth:bph.currentKeyLen])
		}
		if depth < halfKeySize {
			upBinaryCell.apl = 0
		}
		upBinaryCell.spl = 0
		upBinaryCell.hl = 32
		if _, err := bph.keccak2.Read(upBinaryCell.h[:]); err != nil {
			return nil, nil, err
		}
		if bph.trace {
			fmt.Printf("} [%x]\n", upBinaryCell.h[:])
		}
		bph.activeRows--
		if upDepth > 0 {
			bph.currentKeyLen = upDepth - 1
		} else {
			bph.currentKeyLen = 0
		}
	}
	if branchData != nil {
		if bph.trace {
			fmt.Printf("fold: update key: %x, branchData: [%x]\n", CompactedKeyToHex(updateKey), branchData)
		}
	}
	return branchData, updateKey, nil
}

func (bph *BinPatriciaHashed) deleteBinaryCell(hashedKey []byte) {
	if bph.trace {
		fmt.Printf("deleteBinaryCell, activeRows = %d\n", bph.activeRows)
	}
	var cell *BinaryCell
	if bph.activeRows == 0 {
		// Remove the root
		cell = &bph.root
		bph.rootTouched = true
		bph.rootPresent = false
	} else {
		row := bph.activeRows - 1
		if bph.depths[row] < len(hashedKey) {
			if bph.trace {
				fmt.Printf("deleteBinaryCell skipping spurious delete depth=%d, len(hashedKey)=%d\n", bph.depths[row], len(hashedKey))
			}
			return
		}
		col := int(hashedKey[bph.currentKeyLen])
		cell = &bph.grid[row][col]
		if bph.afterMap[row]&(uint16(1)<<col) != 0 {
			// Prevent "spurios deletions", i.e. deletion of absent items
			bph.touchMap[row] |= uint16(1) << col
			bph.afterMap[row] &^= uint16(1) << col
			if bph.trace {
				fmt.Printf("deleteBinaryCell setting (%d, %x)\n", row, col)
			}
		} else {
			if bph.trace {
				fmt.Printf("deleteBinaryCell ignoring (%d, %x)\n", row, col)
			}
		}
	}
	cell.extLen = 0
	cell.Balance.Clear()
	copy(cell.CodeHash[:], EmptyCodeHash)
	cell.Nonce = 0
}

func (bph *BinPatriciaHashed) updateBinaryCell(plainKey, hashedKey []byte) *BinaryCell {
	var cell *BinaryCell
	var col, depth int
	if bph.activeRows == 0 {
		cell = &bph.root
		bph.rootTouched, bph.rootPresent = true, true
	} else {
		row := bph.activeRows - 1
		depth = bph.depths[row]
		col = int(hashedKey[bph.currentKeyLen])
		cell = &bph.grid[row][col]
		bph.touchMap[row] |= uint16(1) << col
		bph.afterMap[row] |= uint16(1) << col
		if bph.trace {
			fmt.Printf("updateBinaryCell setting (%d, %x), depth=%d\n", row, col, depth)
		}
	}
	if cell.downHashedLen == 0 {
		copy(cell.downHashedKey[:], hashedKey[depth:])
		cell.downHashedLen = len(hashedKey) - depth
		if bph.trace {
			fmt.Printf("set downHasheKey=[%x]\n", cell.downHashedKey[:cell.downHashedLen])
		}
	} else {
		if bph.trace {
			fmt.Printf("left downHasheKey=[%x]\n", cell.downHashedKey[:cell.downHashedLen])
		}
	}
	if len(hashedKey) == halfKeySize { // set account key
		cell.apl = len(plainKey)
		copy(cell.apk[:], plainKey)
	} else { // set storage key
		cell.spl = len(plainKey)
		copy(cell.spk[:], plainKey)
	}
	return cell
}

func (bph *BinPatriciaHashed) RootHash() ([]byte, error) {
	hash, err := bph.computeBinaryCellHash(&bph.root, 0, nil)
	if err != nil {
		return nil, err
	}
	return hash[1:], nil // first byte is 128+hash_len
}

func (bph *BinPatriciaHashed) ReviewKeys(plainKeys, hashedKeys [][]byte) (rootHash []byte, branchNodeUpdates map[string]BranchData, err error) {
	branchNodeUpdates = make(map[string]BranchData)

	stagedBinaryCell := new(BinaryCell)
	for i, hashedKey := range hashedKeys {
		plainKey := plainKeys[i]
		hashedKey = hexToBin(hashedKey)
		if bph.trace {
			fmt.Printf("plainKey=[%x], hashedKey=[%x], currentKey=[%x]\n", plainKey, hashedKey, bph.currentKey[:bph.currentKeyLen])
		}
		// Keep folding until the currentKey is the prefix of the key we modify
		for bph.needFolding(hashedKey) {
			if branchData, updateKey, err := bph.fold(); err != nil {
				return nil, nil, fmt.Errorf("fold: %w", err)
			} else if branchData != nil {
				branchNodeUpdates[string(updateKey)] = branchData
			}
		}
		// Now unfold until we step on an empty cell
		for unfolding := bph.needUnfolding(hashedKey); unfolding > 0; unfolding = bph.needUnfolding(hashedKey) {
			if err := bph.unfold(hashedKey, unfolding); err != nil {
				return nil, nil, fmt.Errorf("unfold: %w", err)
			}
		}

		// Update the cell
		stagedBinaryCell.fillEmpty()
		if len(plainKey) == bph.accountKeyLen {
			if err := bph.accountFn(plainKey, stagedBinaryCell); err != nil {
				return nil, nil, fmt.Errorf("accountFn for key %x failed: %w", plainKey, err)
			}
			if !stagedBinaryCell.Delete {
				cell := bph.updateBinaryCell(plainKey, hashedKey)
				cell.setAccountFields(stagedBinaryCell.CodeHash[:], &stagedBinaryCell.Balance, stagedBinaryCell.Nonce)

				if bph.trace {
					fmt.Printf("accountFn reading key %x => balance=%v nonce=%v codeHash=%x\n", cell.apk, cell.Balance.Uint64(), cell.Nonce, cell.CodeHash)
				}
			}
		} else {
			if err = bph.storageFn(plainKey, stagedBinaryCell); err != nil {
				return nil, nil, fmt.Errorf("storageFn for key %x failed: %w", plainKey, err)
			}
			if !stagedBinaryCell.Delete {
				bph.updateBinaryCell(plainKey, hashedKey).setStorage(stagedBinaryCell.Storage[:stagedBinaryCell.StorageLen])
				if bph.trace {
					fmt.Printf("storageFn reading key %x => %x\n", plainKey, stagedBinaryCell.Storage[:stagedBinaryCell.StorageLen])
				}
			}
		}

		if stagedBinaryCell.Delete {
			if bph.trace {
				fmt.Printf("delete cell %x hash %x\n", plainKey, hashedKey)
			}
			bph.deleteBinaryCell(hashedKey)
		}
	}
	// Folding everything up to the root
	for bph.activeRows > 0 {
		if branchData, updateKey, err := bph.fold(); err != nil {
			return nil, nil, fmt.Errorf("final fold: %w", err)
		} else if branchData != nil {
			branchNodeUpdates[string(updateKey)] = branchData
		}
	}

	rootHash, err = bph.RootHash()
	if err != nil {
		return nil, branchNodeUpdates, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	return rootHash, branchNodeUpdates, nil
}

func (bph *BinPatriciaHashed) SetTrace(trace bool) { bph.trace = trace }

func (bph *BinPatriciaHashed) Variant() TrieVariant { return VariantBinPatriciaTrie }

// Reset allows BinPatriciaHashed instance to be reused for the new commitment calculation
func (bph *BinPatriciaHashed) Reset() {
	bph.rootChecked = false
	bph.root.hl = 0
	bph.root.downHashedLen = 0
	bph.root.apl = 0
	bph.root.spl = 0
	bph.root.extLen = 0
	copy(bph.root.CodeHash[:], EmptyCodeHash)
	bph.root.StorageLen = 0
	bph.root.Balance.Clear()
	bph.root.Nonce = 0
	bph.rootTouched = false
	bph.rootPresent = true
}

func (bph *BinPatriciaHashed) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) error,
	storageFn func(plainKey []byte, cell *Cell) error,
) {
	bph.branchFn = branchFn
	bph.accountFn = wrapAccountStorageFn(accountFn)
	bph.storageFn = wrapAccountStorageFn(storageFn)
}

func (c *BinaryCell) bytes() []byte {
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

func (c *BinaryCell) decodeBytes(buf []byte) error {
	if len(buf) < 1 {
		return fmt.Errorf("invalid buffer size to contain BinaryCell (at least 1 byte expected)")
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
func (bph *BinPatriciaHashed) EncodeCurrentState(buf []byte) ([]byte, error) {
	s := binState{
		CurrentKeyLen: int16(bph.currentKeyLen),
		RootChecked:   bph.rootChecked,
		RootTouched:   bph.rootTouched,
		RootPresent:   bph.rootPresent,
		Root:          make([]byte, 0),
	}

	s.Root = bph.root.bytes()
	copy(s.CurrentKey[:], bph.currentKey[:])
	copy(s.Depths[:], bph.depths[:])
	copy(s.BranchBefore[:], bph.branchBefore[:])
	copy(s.TouchMap[:], bph.touchMap[:])
	copy(s.AfterMap[:], bph.afterMap[:])

	return s.Encode(buf)
}

// buf expected to be encoded hph state. Decode state and set up hph to that state.
func (bph *BinPatriciaHashed) SetState(buf []byte) error {
	if bph.activeRows != 0 {
		return fmt.Errorf("has active rows, could not reset state")
	}

	var s state
	if err := s.Decode(buf); err != nil {
		return err
	}

	bph.Reset()

	if err := bph.root.decodeBytes(s.Root); err != nil {
		return err
	}

	bph.currentKeyLen = int(s.CurrentKeyLen)
	bph.rootChecked = s.RootChecked
	bph.rootTouched = s.RootTouched
	bph.rootPresent = s.RootPresent

	copy(bph.currentKey[:], s.CurrentKey[:])
	copy(bph.depths[:], s.Depths[:])
	copy(bph.branchBefore[:], s.BranchBefore[:])
	copy(bph.touchMap[:], s.TouchMap[:])
	copy(bph.afterMap[:], s.AfterMap[:])

	return nil
}

func (bph *BinPatriciaHashed) ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (rootHash []byte, branchNodeUpdates map[string]BranchData, err error) {
	branchNodeUpdates = make(map[string]BranchData)

	for i, plainKey := range plainKeys {
		hashedKey := hashedKeys[i]
		if bph.trace {
			fmt.Printf("plainKey=[%x], hashedKey=[%x], currentKey=[%x]\n", plainKey, hashedKey, bph.currentKey[:bph.currentKeyLen])
		}
		// Keep folding until the currentKey is the prefix of the key we modify
		for bph.needFolding(hashedKey) {
			if branchData, updateKey, err := bph.fold(); err != nil {
				return nil, nil, fmt.Errorf("fold: %w", err)
			} else if branchData != nil {
				branchNodeUpdates[string(updateKey)] = branchData
			}
		}
		// Now unfold until we step on an empty cell
		for unfolding := bph.needUnfolding(hashedKey); unfolding > 0; unfolding = bph.needUnfolding(hashedKey) {
			if err := bph.unfold(hashedKey, unfolding); err != nil {
				return nil, nil, fmt.Errorf("unfold: %w", err)
			}
		}

		update := updates[i]
		// Update the cell
		if update.Flags == DeleteUpdate {
			bph.deleteBinaryCell(hashedKey)
			if bph.trace {
				fmt.Printf("key %x deleted\n", plainKey)
			}
		} else {
			cell := bph.updateBinaryCell(plainKey, hashedKey)
			if bph.trace {
				fmt.Printf("accountFn updated key %x =>", plainKey)
			}
			if update.Flags&BalanceUpdate != 0 {
				if bph.trace {
					fmt.Printf(" balance=%d", update.Balance.Uint64())
				}
				cell.Balance.Set(&update.Balance)
			}
			if update.Flags&NonceUpdate != 0 {
				if bph.trace {
					fmt.Printf(" nonce=%d", update.Nonce)
				}
				cell.Nonce = update.Nonce
			}
			if update.Flags&CodeUpdate != 0 {
				if bph.trace {
					fmt.Printf(" codeHash=%x", update.CodeHashOrStorage)
				}
				copy(cell.CodeHash[:], update.CodeHashOrStorage[:])
			}
			if bph.trace {
				fmt.Printf("\n")
			}
			if update.Flags&StorageUpdate != 0 {
				cell.setStorage(update.CodeHashOrStorage[:update.ValLength])
				if bph.trace {
					fmt.Printf("\rstorageFn filled key %x => %x\n", plainKey, update.CodeHashOrStorage[:update.ValLength])
				}
			}
		}
	}
	// Folding everything up to the root
	for bph.activeRows > 0 {
		if branchData, updateKey, err := bph.fold(); err != nil {
			return nil, nil, fmt.Errorf("final fold: %w", err)
		} else if branchData != nil {
			branchNodeUpdates[string(updateKey)] = branchData
		}
	}

	rootHash, err = bph.RootHash()
	if err != nil {
		return nil, branchNodeUpdates, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	return rootHash, branchNodeUpdates, nil
}

// Hashes provided key and expands resulting hash into nibbles (each byte split into two nibbles by 4 bits)
func (bph *BinPatriciaHashed) hashAndNibblizeKey2(key []byte) []byte { //nolint
	hashedKey := make([]byte, length.Hash)

	bph.keccak.Reset()
	bph.keccak.Write(key[:length.Addr])
	copy(hashedKey[:length.Hash], bph.keccak.Sum(nil))

	if len(key[length.Addr:]) > 0 {
		hashedKey = append(hashedKey, make([]byte, length.Hash)...)
		bph.keccak.Reset()
		bph.keccak.Write(key[length.Addr:])
		copy(hashedKey[length.Hash:], bph.keccak.Sum(nil))
	}

	nibblized := make([]byte, len(hashedKey)*2)
	for i, b := range hashedKey {
		nibblized[i*2] = (b >> 4) & 0xf
		nibblized[i*2+1] = b & 0xf
	}
	return nibblized
}

func binHashKey(keccak keccakState, plainKey []byte, dest []byte, hashedKeyOffset int) error {
	keccak.Reset()
	var hashBufBack [length.Hash]byte
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

func wrapAccountStorageFn(fn func([]byte, *Cell) error) func(pk []byte, bc *BinaryCell) error {
	return func(pk []byte, bc *BinaryCell) error {
		cl := bc.unwrapToHexCell()

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

		if cl.extLen > 0 {
			binExt := compactToBin(cl.extension[:cl.extLen])
			copy(bc.extension[:], binExt)
			bc.extLen = len(binExt)
		}
		if cl.downHashedLen > 0 {
			bindhk := compactToBin(cl.downHashedKey[:cl.downHashedLen])
			copy(bc.downHashedKey[:], bindhk)
			bc.downHashedLen = len(bindhk)
		}

		copy(bc.CodeHash[:], cl.CodeHash[:])
		copy(bc.Storage[:], cl.Storage[:])
		bc.Delete = cl.Delete
		return nil
	}
}

// represents state of the tree
type binState struct {
	TouchMap      [maxKeySize]uint16 // For each row, bitmap of cells that were either present before modification, or modified or deleted
	AfterMap      [maxKeySize]uint16 // For each row, bitmap of cells that were present after modification
	CurrentKeyLen int16
	Root          []byte // encoded root cell
	RootChecked   bool   // Set to false if it is not known whether the root is empty, set to true if it is checked
	RootTouched   bool
	RootPresent   bool
	BranchBefore  [maxKeySize]bool // For each row, whether there was a branch node in the database loaded in unfold
	CurrentKey    [maxKeySize]byte // For each row indicates which column is currently selected
	Depths        [maxKeySize]int  // For each row, the depth of cells in that row
}

func (s *binState) Encode(buf []byte) ([]byte, error) {
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
	for i := 0; i < halfKeySize; i++ {
		if s.BranchBefore[i] {
			before1 |= 1 << i
		}
	}
	for i, j := halfKeySize, 0; i < maxKeySize; i, j = i+1, j+1 {
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

func (s *binState) Decode(buf []byte) error {
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
	if n, err := aux.Read(s.CurrentKey[:]); err != nil || n != maxKeySize {
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

	// TODO invalid branch encode
	for i := 0; i < halfKeySize; i++ {
		if branch1&(1<<i) != 0 {
			s.BranchBefore[i] = true
		}
	}
	for i, j := halfKeySize, 0; i < maxKeySize; i, j = i+1, j+1 {
		if branch2&(1<<j) != 0 {
			s.BranchBefore[i] = true
		}
	}
	return nil
}
