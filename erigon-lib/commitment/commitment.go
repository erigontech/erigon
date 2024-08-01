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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"strings"

	"github.com/google/btree"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/cryptozerocopy"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/types"

	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/etl"
)

var (
	mxKeys                 = metrics.GetOrCreateCounter("domain_commitment_keys")
	mxBranchUpdatesApplied = metrics.GetOrCreateCounter("domain_commitment_updates_applied")
)

// Trie represents commitment variant.
type Trie interface {
	// RootHash produces root hash of the trie
	RootHash() (hash []byte, err error)

	// Makes trie more verbose
	SetTrace(bool)

	// Variant returns commitment trie variant
	Variant() TrieVariant

	// Reset Drops everything from the trie
	Reset()

	// Set context for state IO
	ResetContext(ctx PatriciaContext)

	ProcessTree(ctx context.Context, tree *Updates, logPrefix string) (rootHash []byte, err error)

	// Reads updates from storage
	ProcessKeys(ctx context.Context, pk [][]byte, logPrefix string) (rootHash []byte, err error)

	// Process already gathered updates
	ProcessUpdates(ctx context.Context, pk [][]byte, updates []Update) (rootHash []byte, err error)
}

type PatriciaContext interface {
	// GetBranch load branch node and fill up the cells
	// For each cell, it sets the cell type, clears the modified flag, fills the hash,
	// and for the extension, account, and leaf type, the `l` and `k`
	GetBranch(prefix []byte) ([]byte, uint64, error)
	// fetch account with given plain key
	GetAccount(plainKey []byte, cell *Cell) error
	// fetch storage with given plain key
	GetStorage(plainKey []byte, cell *Cell) error
	// store branch data
	PutBranch(prefix []byte, data []byte, prevData []byte, prevStep uint64) error
}

type TrieVariant string

const (
	// VariantHexPatriciaTrie used as default commitment approach
	VariantHexPatriciaTrie TrieVariant = "hex-patricia-hashed"
	// VariantBinPatriciaTrie - Experimental mode with binary key representation
	VariantBinPatriciaTrie TrieVariant = "bin-patricia-hashed"
)

func InitializeTrieAndUpdates(tv TrieVariant, mode Mode, tmpdir string) (Trie, *Updates) {
	switch tv {
	case VariantBinPatriciaTrie:
		trie := NewBinPatriciaHashed(length.Addr, nil, tmpdir)
		fn := func(key []byte) []byte { return hexToBin(key) }
		tree := NewUpdates(mode, tmpdir, fn)
		return trie, tree
	case VariantHexPatriciaTrie:
		fallthrough
	default:

		trie := NewHexPatriciaHashed(length.Addr, nil, tmpdir)
		tree := NewUpdates(mode, tmpdir, trie.hashAndNibblizeKey)
		return trie, tree
	}
}

type PartFlags uint8

const (
	HashedKeyPart    PartFlags = 1
	AccountPlainPart PartFlags = 2
	StoragePlainPart PartFlags = 4
	HashPart         PartFlags = 8
)

type BranchData []byte

func (branchData BranchData) String() string {
	if len(branchData) == 0 {
		return ""
	}
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4
	var sb strings.Builder
	var cell Cell
	fmt.Fprintf(&sb, "touchMap %016b, afterMap %016b\n", touchMap, afterMap)
	for bitset, j := touchMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		fmt.Fprintf(&sb, "   %x => ", nibble)
		if afterMap&bit == 0 {
			sb.WriteString("{DELETED}\n")
		} else {
			fieldBits := PartFlags(branchData[pos])
			pos++
			var err error
			if pos, err = cell.fillFromFields(branchData, pos, fieldBits); err != nil {
				// This is used for test output, so ok to panic
				panic(err)
			}
			sb.WriteString("{")
			var comma string
			if cell.downHashedLen > 0 {
				fmt.Fprintf(&sb, "hashedKey=[%x]", cell.downHashedKey[:cell.downHashedLen])
				comma = ","
			}
			if cell.accountPlainKeyLen > 0 {
				fmt.Fprintf(&sb, "%saccountPlainKey=[%x]", comma, cell.accountPlainKey[:cell.accountPlainKeyLen])
				comma = ","
			}
			if cell.storagePlainKeyLen > 0 {
				fmt.Fprintf(&sb, "%sstoragePlainKey=[%x]", comma, cell.storagePlainKey[:cell.storagePlainKeyLen])
				comma = ","
			}
			if cell.HashLen > 0 {
				fmt.Fprintf(&sb, "%shash=[%x]", comma, cell.hash[:cell.HashLen])
			}
			sb.WriteString("}\n")
		}
		bitset ^= bit
	}
	return sb.String()
}

type BranchEncoder struct {
	buf       *bytes.Buffer
	bitmapBuf [binary.MaxVarintLen64]byte
	merger    *BranchMerger
	updates   *etl.Collector
	tmpdir    string
}

func NewBranchEncoder(sz uint64, tmpdir string) *BranchEncoder {
	be := &BranchEncoder{
		buf:    bytes.NewBuffer(make([]byte, sz)),
		tmpdir: tmpdir,
		merger: NewHexBranchMerger(sz / 2),
	}
	//be.initCollector()
	return be
}

func (be *BranchEncoder) initCollector() {
	if be.updates != nil {
		be.updates.Close()
	}
	be.updates = etl.NewCollector("commitment.BranchEncoder", be.tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize/2), log.Root().New("branch-encoder"))
	be.updates.LogLvl(log.LvlDebug)
}

func (be *BranchEncoder) Load(pc PatriciaContext, args etl.TransformArgs) error {
	// do not collect them at least now. Write them at CollectUpdate into pc
	if be.updates == nil {
		return nil
	}

	if err := be.updates.Load(nil, "", func(prefix, update []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		stateValue, stateStep, err := pc.GetBranch(prefix)
		if err != nil {
			return err
		}

		cp, cu := common.Copy(prefix), common.Copy(update) // has to copy :(
		if err = pc.PutBranch(cp, cu, stateValue, stateStep); err != nil {
			return err
		}
		mxBranchUpdatesApplied.Inc()
		return nil
	}, args); err != nil {
		return err
	}
	be.initCollector()
	return nil
}

func (be *BranchEncoder) CollectUpdate(
	ctx PatriciaContext,
	prefix []byte,
	bitmap, touchMap, afterMap uint16,
	readCell func(nibble int, skip bool) (*Cell, error),
) (lastNibble int, err error) {

	var update []byte
	update, lastNibble, err = be.EncodeBranch(bitmap, touchMap, afterMap, readCell)
	if err != nil {
		return 0, err
	}

	prev, prevStep, err := ctx.GetBranch(prefix)
	_ = prevStep
	if err != nil {
		return 0, err
	}
	if len(prev) > 0 {
		if bytes.Equal(prev, update) {
			return lastNibble, nil // do not write the same data for prefix
		}
		update, err = be.merger.Merge(prev, update)
		if err != nil {
			return 0, err
		}
	}
	//fmt.Printf("collectBranchUpdate [%x] -> [%x]\n", prefix, update)
	// has to copy :(
	if err = ctx.PutBranch(common.Copy(prefix), common.Copy(update), prev, prevStep); err != nil {
		return 0, err
	}
	mxBranchUpdatesApplied.Inc()
	return lastNibble, nil
}

// Encoded result should be copied before next call to EncodeBranch, underlying slice is reused
func (be *BranchEncoder) EncodeBranch(bitmap, touchMap, afterMap uint16, readCell func(nibble int, skip bool) (*Cell, error)) (BranchData, int, error) {
	be.buf.Reset()

	if err := binary.Write(be.buf, binary.BigEndian, touchMap); err != nil {
		return nil, 0, err
	}
	if err := binary.Write(be.buf, binary.BigEndian, afterMap); err != nil {
		return nil, 0, err
	}

	putUvarAndVal := func(size uint64, val []byte) error {
		n := binary.PutUvarint(be.bitmapBuf[:], size)
		wn, err := be.buf.Write(be.bitmapBuf[:n])
		if err != nil {
			return err
		}
		if n != wn {
			return errors.New("n != wn size")
		}
		wn, err = be.buf.Write(val)
		if err != nil {
			return err
		}
		if len(val) != wn {
			return errors.New("wn != value size")
		}
		return nil
	}

	var lastNibble int
	for bitset, j := afterMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		for i := lastNibble; i < nibble; i++ {
			if _, err := readCell(i, true /* skip */); err != nil {
				return nil, 0, err
			} // only writes 0x80 into hasher
		}
		lastNibble = nibble + 1

		cell, err := readCell(nibble, false)
		if err != nil {
			return nil, 0, err
		}

		if bitmap&bit != 0 {
			var fieldBits PartFlags
			if cell.extLen > 0 && cell.storagePlainKeyLen == 0 {
				fieldBits |= HashedKeyPart
			}
			if cell.accountPlainKeyLen > 0 {
				fieldBits |= AccountPlainPart
			}
			if cell.storagePlainKeyLen > 0 {
				fieldBits |= StoragePlainPart
			}
			if cell.HashLen > 0 {
				fieldBits |= HashPart
			}
			if err := be.buf.WriteByte(byte(fieldBits)); err != nil {
				return nil, 0, err
			}
			if fieldBits&HashedKeyPart != 0 {
				if err := putUvarAndVal(uint64(cell.extLen), cell.extension[:cell.extLen]); err != nil {
					return nil, 0, err
				}
			}
			if fieldBits&AccountPlainPart != 0 {
				if err := putUvarAndVal(uint64(cell.accountPlainKeyLen), cell.accountPlainKey[:cell.accountPlainKeyLen]); err != nil {
					return nil, 0, err
				}
			}
			if fieldBits&StoragePlainPart != 0 {
				if err := putUvarAndVal(uint64(cell.storagePlainKeyLen), cell.storagePlainKey[:cell.storagePlainKeyLen]); err != nil {
					return nil, 0, err
				}
			}
			if fieldBits&HashPart != 0 {
				if err := putUvarAndVal(uint64(cell.HashLen), cell.hash[:cell.HashLen]); err != nil {
					return nil, 0, err
				}
			}
		}
		bitset ^= bit
	}
	//fmt.Printf("EncodeBranch [%x] size: %d\n", be.buf.Bytes(), be.buf.Len())
	return be.buf.Bytes(), lastNibble, nil
}

func RetrieveCellNoop(nibble int, skip bool) (*Cell, error) { return nil, nil }

// if fn returns nil, the original key will be copied from branchData
func (branchData BranchData) ReplacePlainKeys(newData []byte, fn func(key []byte, isStorage bool) (newKey []byte, err error)) (BranchData, error) {
	if len(branchData) < 4 {
		return branchData, nil
	}

	var numBuf [binary.MaxVarintLen64]byte
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	if touchMap&afterMap == 0 {
		return branchData, nil
	}
	pos := 4
	newData = append(newData[:0], branchData[:4]...)
	for bitset, j := touchMap&afterMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		fieldBits := PartFlags(branchData[pos])
		newData = append(newData, byte(fieldBits))
		pos++
		if fieldBits&HashedKeyPart != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for hashedKey len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for hashedKey len")
			}
			newData = append(newData, branchData[pos:pos+n]...)
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, errors.New("replacePlainKeys buffer too small for hashedKey")
			}
			if l > 0 {
				newData = append(newData, branchData[pos:pos+int(l)]...)
				pos += int(l)
			}
		}
		if fieldBits&AccountPlainPart != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for accountPlainKey len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for accountPlainKey len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, errors.New("replacePlainKeys buffer too small for accountPlainKey")
			}
			if l > 0 {
				pos += int(l)
			}
			newKey, err := fn(branchData[pos-int(l):pos], false)
			if err != nil {
				return nil, err
			}
			if newKey == nil {
				newData = append(newData, branchData[pos-int(l)-n:pos]...)
				if l != length.Addr {
					fmt.Printf("COPY %x LEN %d\n", []byte(branchData[pos-int(l):pos]), l)
				}
			} else {
				if len(newKey) > 8 && len(newKey) != length.Addr {
					fmt.Printf("SHORT %x LEN %d\n", newKey, len(newKey))
				}

				n = binary.PutUvarint(numBuf[:], uint64(len(newKey)))
				newData = append(newData, numBuf[:n]...)
				newData = append(newData, newKey...)
			}
		}
		if fieldBits&StoragePlainPart != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for storagePlainKey len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for storagePlainKey len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, errors.New("replacePlainKeys buffer too small for storagePlainKey")
			}
			if l > 0 {
				pos += int(l)
			}
			newKey, err := fn(branchData[pos-int(l):pos], true)
			if err != nil {
				return nil, err
			}
			if newKey == nil {
				newData = append(newData, branchData[pos-int(l)-n:pos]...) // -n to include length
				if l != length.Addr+length.Hash {
					fmt.Printf("COPY %x LEN %d\n", []byte(branchData[pos-int(l):pos]), l)
				}
			} else {
				if len(newKey) > 8 && len(newKey) != length.Addr+length.Hash {
					fmt.Printf("SHORT %x LEN %d\n", newKey, len(newKey))
				}

				n = binary.PutUvarint(numBuf[:], uint64(len(newKey)))
				newData = append(newData, numBuf[:n]...)
				newData = append(newData, newKey...)
			}
		}
		if fieldBits&HashPart != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for hash len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for hash len")
			}
			newData = append(newData, branchData[pos:pos+n]...)
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, errors.New("replacePlainKeys buffer too small for hash")
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

// IsComplete determines whether given branch data is complete, meaning that all information about all the children is present
// Each of 16 children of a branch node have two attributes
// touch - whether this child has been modified or deleted in this branchData (corresponding bit in touchMap is set)
// after - whether after this branchData application, the child is present in the tree or not (corresponding bit in afterMap is set)
func (branchData BranchData) IsComplete() bool {
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	return ^touchMap&afterMap == 0
}

// MergeHexBranches combines two branchData, number 2 coming after (and potentially shadowing) number 1
func (branchData BranchData) MergeHexBranches(branchData2 BranchData, newData []byte) (BranchData, error) {
	if branchData2 == nil {
		return branchData, nil
	}
	if branchData == nil {
		return branchData2, nil
	}

	touchMap1 := binary.BigEndian.Uint16(branchData[0:])
	afterMap1 := binary.BigEndian.Uint16(branchData[2:])
	bitmap1 := touchMap1 & afterMap1
	pos1 := 4
	touchMap2 := binary.BigEndian.Uint16(branchData2[0:])
	afterMap2 := binary.BigEndian.Uint16(branchData2[2:])
	bitmap2 := touchMap2 & afterMap2
	pos2 := 4
	var bitmapBuf [4]byte
	binary.BigEndian.PutUint16(bitmapBuf[0:], touchMap1|touchMap2)
	binary.BigEndian.PutUint16(bitmapBuf[2:], afterMap2)
	newData = append(newData[:0], bitmapBuf[:]...)
	for bitset, j := bitmap1|bitmap2, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		if bitmap2&bit != 0 {
			// Add fields from branchData2
			fieldBits := PartFlags(branchData2[pos2])
			newData = append(newData, byte(fieldBits))
			pos2++
			for i := 0; i < bits.OnesCount8(byte(fieldBits)); i++ {
				l, n := binary.Uvarint(branchData2[pos2:])
				if n == 0 {
					return nil, errors.New("MergeHexBranches buffer2 too small for field")
				} else if n < 0 {
					return nil, errors.New("MergeHexBranches value2 overflow for field")
				}
				newData = append(newData, branchData2[pos2:pos2+n]...)
				pos2 += n
				if len(branchData2) < pos2+int(l) {
					return nil, errors.New("MergeHexBranches buffer2 too small for field")
				}
				if l > 0 {
					newData = append(newData, branchData2[pos2:pos2+int(l)]...)
					pos2 += int(l)
				}
			}
		}
		if bitmap1&bit != 0 {
			add := (touchMap2&bit == 0) && (afterMap2&bit != 0) // Add fields from branchData1
			fieldBits := PartFlags(branchData[pos1])
			if add {
				newData = append(newData, byte(fieldBits))
			}
			pos1++
			for i := 0; i < bits.OnesCount8(byte(fieldBits)); i++ {
				l, n := binary.Uvarint(branchData[pos1:])
				if n == 0 {
					return nil, errors.New("MergeHexBranches buffer1 too small for field")
				} else if n < 0 {
					return nil, errors.New("MergeHexBranches value1 overflow for field")
				}
				if add {
					newData = append(newData, branchData[pos1:pos1+n]...)
				}
				pos1 += n
				if len(branchData) < pos1+int(l) {
					return nil, errors.New("MergeHexBranches buffer1 too small for field")
				}
				if l > 0 {
					if add {
						newData = append(newData, branchData[pos1:pos1+int(l)]...)
					}
					pos1 += int(l)
				}
			}
		}
		bitset ^= bit
	}
	return newData, nil
}

func (branchData BranchData) DecodeCells() (touchMap, afterMap uint16, row [16]*Cell, err error) {
	touchMap = binary.BigEndian.Uint16(branchData[0:])
	afterMap = binary.BigEndian.Uint16(branchData[2:])
	pos := 4
	for bitset, j := touchMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		if afterMap&bit != 0 {
			fieldBits := PartFlags(branchData[pos])
			pos++
			row[nibble] = new(Cell)
			if pos, err = row[nibble].fillFromFields(branchData, pos, fieldBits); err != nil {
				err = fmt.Errorf("failed to fill cell at nibble %x: %w", nibble, err)
				return
			}
		}
		bitset ^= bit
	}
	return
}

type BranchMerger struct {
	buf []byte
	num [4]byte
}

func NewHexBranchMerger(capacity uint64) *BranchMerger {
	return &BranchMerger{buf: make([]byte, capacity)}
}

// MergeHexBranches combines two branchData, number 2 coming after (and potentially shadowing) number 1
func (m *BranchMerger) Merge(branch1 BranchData, branch2 BranchData) (BranchData, error) {
	if len(branch2) == 0 {
		return branch1, nil
	}
	if len(branch1) == 0 {
		return branch2, nil
	}

	touchMap1 := binary.BigEndian.Uint16(branch1[0:])
	afterMap1 := binary.BigEndian.Uint16(branch1[2:])
	bitmap1 := touchMap1 & afterMap1
	pos1 := 4

	touchMap2 := binary.BigEndian.Uint16(branch2[0:])
	afterMap2 := binary.BigEndian.Uint16(branch2[2:])
	bitmap2 := touchMap2 & afterMap2
	pos2 := 4

	binary.BigEndian.PutUint16(m.num[0:], touchMap1|touchMap2)
	binary.BigEndian.PutUint16(m.num[2:], afterMap2)
	dataPos := 4

	m.buf = append(m.buf[:0], m.num[:]...)

	for bitset, j := bitmap1|bitmap2, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		if bitmap2&bit != 0 {
			// Add fields from branch2
			fieldBits := PartFlags(branch2[pos2])
			m.buf = append(m.buf, byte(fieldBits))
			pos2++

			for i := 0; i < bits.OnesCount8(byte(fieldBits)); i++ {
				l, n := binary.Uvarint(branch2[pos2:])
				if n == 0 {
					return nil, errors.New("MergeHexBranches branch2 is too small: expected node info size")
				} else if n < 0 {
					return nil, errors.New("MergeHexBranches branch2: size overflow for length")
				}

				m.buf = append(m.buf, branch2[pos2:pos2+n]...)
				pos2 += n
				dataPos += n
				if len(branch2) < pos2+int(l) {
					return nil, fmt.Errorf("MergeHexBranches branch2 is too small: expected at least %d got %d bytes", pos2+int(l), len(branch2))
				}
				if l > 0 {
					m.buf = append(m.buf, branch2[pos2:pos2+int(l)]...)
					pos2 += int(l)
					dataPos += int(l)
				}
			}
		}
		if bitmap1&bit != 0 {
			add := (touchMap2&bit == 0) && (afterMap2&bit != 0) // Add fields from branchData1
			fieldBits := PartFlags(branch1[pos1])
			if add {
				m.buf = append(m.buf, byte(fieldBits))
			}
			pos1++
			for i := 0; i < bits.OnesCount8(byte(fieldBits)); i++ {
				l, n := binary.Uvarint(branch1[pos1:])
				if n == 0 {
					return nil, errors.New("MergeHexBranches branch1 is too small: expected node info size")
				} else if n < 0 {
					return nil, errors.New("MergeHexBranches branch1: size overflow for length")
				}

				if add {
					m.buf = append(m.buf, branch1[pos1:pos1+n]...)
				}
				pos1 += n
				if len(branch1) < pos1+int(l) {
					fmt.Printf("b1: %x %v\n", branch1, branch1)
					fmt.Printf("b2: %x\n", branch2)
					return nil, fmt.Errorf("MergeHexBranches branch1 is too small: expected at least %d got %d bytes", pos1+int(l), len(branch1))
				}
				if l > 0 {
					if add {
						m.buf = append(m.buf, branch1[pos1:pos1+int(l)]...)
					}
					pos1 += int(l)
				}
			}
		}
		bitset ^= bit
	}
	return m.buf, nil
}

func ParseTrieVariant(s string) TrieVariant {
	var trieVariant TrieVariant
	switch s {
	case "bin":
		trieVariant = VariantBinPatriciaTrie
	case "hex":
		fallthrough
	default:
		trieVariant = VariantHexPatriciaTrie
	}
	return trieVariant
}

type BranchStat struct {
	KeySize     uint64
	ValSize     uint64
	MinCellSize uint64
	MaxCellSize uint64
	CellCount   uint64
	APKSize     uint64
	SPKSize     uint64
	ExtSize     uint64
	HashSize    uint64
	APKCount    uint64
	SPKCount    uint64
	HashCount   uint64
	ExtCount    uint64
	TAMapsSize  uint64
	IsRoot      bool
}

// do not add stat of root node to other branch stat
func (bs *BranchStat) Collect(other *BranchStat) {
	if other == nil {
		return
	}

	bs.KeySize += other.KeySize
	bs.ValSize += other.ValSize
	bs.MinCellSize = min(bs.MinCellSize, other.MinCellSize)
	bs.MaxCellSize = max(bs.MaxCellSize, other.MaxCellSize)
	bs.CellCount += other.CellCount
	bs.APKSize += other.APKSize
	bs.SPKSize += other.SPKSize
	bs.ExtSize += other.ExtSize
	bs.HashSize += other.HashSize
	bs.APKCount += other.APKCount
	bs.SPKCount += other.SPKCount
	bs.HashCount += other.HashCount
	bs.ExtCount += other.ExtCount
}

func DecodeBranchAndCollectStat(key, branch []byte, tv TrieVariant) *BranchStat {
	stat := &BranchStat{}
	if len(key) == 0 {
		return nil
	}

	stat.KeySize = uint64(len(key))
	stat.ValSize = uint64(len(branch))
	stat.IsRoot = true

	// if key is not "state" then we are interested in the branch data
	if !bytes.Equal(key, []byte("state")) {
		stat.IsRoot = false

		tm, am, cells, err := BranchData(branch).DecodeCells()
		if err != nil {
			return nil
		}
		stat.TAMapsSize = uint64(2 + 2) // touchMap + afterMap
		stat.CellCount = uint64(bits.OnesCount16(tm & am))
		for _, c := range cells {
			if c == nil {
				continue
			}
			enc := uint64(len(c.Encode()))
			stat.MinCellSize = min(stat.MinCellSize, enc)
			stat.MaxCellSize = max(stat.MaxCellSize, enc)
			switch {
			case c.accountPlainKeyLen > 0:
				stat.APKSize += uint64(c.accountPlainKeyLen)
				stat.APKCount++
			case c.storagePlainKeyLen > 0:
				stat.SPKSize += uint64(c.storagePlainKeyLen)
				stat.SPKCount++
			case c.HashLen > 0:
				stat.HashSize += uint64(c.HashLen)
				stat.HashCount++
			default:
				panic("no plain key" + fmt.Sprintf("#+%v", c))
				//case c.extLen > 0:
			}
			if c.extLen > 0 {
				switch tv {
				case VariantBinPatriciaTrie:
					stat.ExtSize += uint64(c.extLen)
				case VariantHexPatriciaTrie:
					stat.ExtSize += uint64(c.extLen)
				}
				stat.ExtCount++
			}
		}
	}
	return stat
}

// Defines how to evaluate commitments
type Mode uint

const (
	ModeDisabled Mode = 0
	ModeDirect   Mode = 1
	ModeUpdate   Mode = 2
)

func (m Mode) String() string {
	switch m {
	case ModeDisabled:
		return "disabled"
	case ModeDirect:
		return "direct"
	case ModeUpdate:
		return "update"
	default:
		return "unknown"
	}
}

func ParseCommitmentMode(s string) Mode {
	var mode Mode
	switch s {
	case "off":
		mode = ModeDisabled
	case "update":
		mode = ModeUpdate
	default:
		mode = ModeDirect
	}
	return mode
}

type Updates struct {
	keccak cryptozerocopy.KeccakState
	hasher keyHasher
	keys   map[string]struct{}
	etl    *etl.Collector
	tree   *btree.BTreeG[*KeyUpdate]
	mode   Mode
	tmpdir string
}

type keyHasher func(key []byte) []byte

func keyHasherNoop(key []byte) []byte { return key }

func NewUpdates(m Mode, tmpdir string, hasher keyHasher) *Updates {
	t := &Updates{
		keccak: sha3.NewLegacyKeccak256().(cryptozerocopy.KeccakState),
		hasher: hasher,
		tmpdir: tmpdir,
		mode:   m,
	}
	if t.mode == ModeDirect {
		t.keys = make(map[string]struct{})
		t.initCollector()
	} else if t.mode == ModeUpdate {
		t.tree = btree.NewG[*KeyUpdate](64, keyUpdateLessFn)
	}
	return t
}

func (t *Updates) Mode() Mode { return t.mode }

func (t *Updates) Size() (updates uint64) {
	switch t.mode {
	case ModeDirect:
		return uint64(len(t.keys))
	case ModeUpdate:
		return uint64(t.tree.Len())
	default:
		return 0
	}
}

func (t *Updates) initCollector() {
	if t.etl != nil {
		t.etl.Close()
		t.etl = nil
	}
	t.etl = etl.NewCollector("commitment", t.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize/2), log.Root().New("update-tree"))
	t.etl.LogLvl(log.LvlDebug)
	t.etl.SortAndFlushInBackground(true)
}

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (t *Updates) TouchPlainKey(key, val []byte, fn func(c *KeyUpdate, val []byte)) {
	switch t.mode {
	case ModeUpdate:
		pivot, updated := &KeyUpdate{plainKey: key}, false

		t.tree.DescendLessOrEqual(pivot, func(item *KeyUpdate) bool {
			if bytes.Equal(item.plainKey, pivot.plainKey) {
				fn(item, val)
				updated = true
			}
			return false
		})
		if !updated {
			pivot.update.plainKey = pivot.plainKey
			pivot.update.hashedKey = t.hasher(pivot.plainKey)
			fn(pivot, val)
			t.tree.ReplaceOrInsert(pivot)
		}
	case ModeDirect:
		if _, ok := t.keys[string(key)]; !ok {
			if err := t.etl.Collect(t.hasher(key), key); err != nil {
				log.Warn("failed to collect updated key", "key", key, "err", err)
			}
			t.keys[string(key)] = struct{}{}
		}
	default:
	}
}

func (t *Updates) TouchAccount(c *KeyUpdate, val []byte) {
	if len(val) == 0 {
		c.update.Flags = DeleteUpdate
		return
	}
	if c.update.Flags&DeleteUpdate != 0 {
		c.update.Flags ^= DeleteUpdate
	}
	nonce, balance, chash := types.DecodeAccountBytesV3(val)
	if c.update.Nonce != nonce {
		c.update.Nonce = nonce
		c.update.Flags |= NonceUpdate
	}
	if !c.update.Balance.Eq(balance) {
		c.update.Balance.Set(balance)
		c.update.Flags |= BalanceUpdate
	}
	if !bytes.Equal(chash, c.update.CodeHashOrStorage[:]) {
		if len(chash) == 0 {
			c.update.ValLength = length.Hash
			copy(c.update.CodeHashOrStorage[:], EmptyCodeHash)
		} else {
			copy(c.update.CodeHashOrStorage[:], chash)
			c.update.ValLength = length.Hash
			c.update.Flags |= CodeUpdate
		}
	}
}

func (t *Updates) TouchStorage(c *KeyUpdate, val []byte) {
	c.update.ValLength = len(val)
	if len(val) == 0 {
		c.update.Flags = DeleteUpdate
	} else {
		c.update.Flags |= StorageUpdate
		copy(c.update.CodeHashOrStorage[:], val)
	}
}

func (t *Updates) TouchCode(c *KeyUpdate, val []byte) {
	t.keccak.Reset()
	t.keccak.Write(val)
	t.keccak.Read(c.update.CodeHashOrStorage[:])
	if c.update.Flags == DeleteUpdate && len(val) == 0 {
		c.update.Flags = DeleteUpdate
		c.update.ValLength = 0
		return
	}
	c.update.ValLength = length.Hash
	if len(val) != 0 {
		c.update.Flags |= CodeUpdate
	}
}

func (t *Updates) Close() {
	if t.keys != nil {
		clear(t.keys)
	}
	if t.tree != nil {
		t.tree.Clear(true)
		t.tree = nil
	}
	if t.etl != nil {
		t.etl.Close()
	}
}

// HashSort sorts and applies fn to each key-value pair in the order of hashed keys.
func (t *Updates) HashSort(ctx context.Context, fn func(hk, pk []byte) error) error {
	switch t.mode {
	case ModeDirect:
		clear(t.keys)

		err := t.etl.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return fn(k, v)
		}, etl.TransformArgs{Quit: ctx.Done()})
		if err != nil {
			return err
		}

		t.initCollector()
	case ModeUpdate:
		t.tree.Ascend(func(item *KeyUpdate) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}

			if err := fn(item.update.hashedKey, item.plainKey); err != nil {
				return false
			}
			return true
		})
		t.tree.Clear(true)
	default:
		return nil
	}
	return nil
}

// Returns list of both plain and hashed keys. If .mode is ModeUpdate, updates also returned.
// No ordering guarantees is provided.
// TODO replace with Clear function. HashSort perfectly dumps all keys.
func (t *Updates) List(clear bool) ([][]byte, []Update) {
	switch t.mode {
	case ModeDirect:
		plainKeys := make([][]byte, 0, len(t.keys))
		err := t.HashSort(context.Background(), func(hk, pk []byte) error {
			plainKeys = append(plainKeys, common.Copy(pk))
			return nil
		})
		if err != nil {
			return nil, nil
		}
		return plainKeys, nil
	case ModeUpdate:
		plainKeys := make([][]byte, t.tree.Len())
		updates := make([]Update, t.tree.Len())
		i := 0
		t.tree.Ascend(func(item *KeyUpdate) bool {
			plainKeys[i], updates[i] = item.plainKey, item.update
			i++
			return true
		})
		if clear {
			t.tree.Clear(true)
		}
		return plainKeys, updates
	default:
		return nil, nil
	}
}

type KeyUpdate struct {
	plainKey []byte
	update   Update
}

func keyUpdateLessFn(i, j *KeyUpdate) bool {
	return bytes.Compare(i.plainKey, j.plainKey) < 0
}
