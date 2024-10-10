package commitment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"math/bits"
	"strings"

	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/common/length"
)

// Trie represents commitment variant.
type Trie interface {
	// RootHash produces root hash of the trie
	RootHash() (hash []byte, err error)

	// Variant returns commitment trie variant
	Variant() TrieVariant

	// Reset Drops everything from the trie
	Reset()

	ReviewKeys(pk, hk [][]byte) (rootHash []byte, branchNodeUpdates map[string]BranchData, err error)

	ProcessUpdates(pk, hk [][]byte, updates []Update) (rootHash []byte, branchNodeUpdates map[string]BranchData, err error)

	ResetFns(
		branchFn func(prefix []byte) ([]byte, error),
		accountFn func(plainKey []byte, cell *Cell) error,
		storageFn func(plainKey []byte, cell *Cell) error,
	)

	// Makes trie more verbose
	SetTrace(bool)
}

type TrieVariant string

const (
	// VariantHexPatriciaTrie used as default commitment approach
	VariantHexPatriciaTrie TrieVariant = "hex-patricia-hashed"
	// VariantBinPatriciaTrie - Experimental mode with binary key representation
	VariantBinPatriciaTrie TrieVariant = "bin-patricia-hashed"
)

func InitializeTrie(tv TrieVariant) Trie {
	switch tv {
	case VariantBinPatriciaTrie:
		return NewBinPatriciaHashed(length.Addr, nil, nil, nil)
	case VariantHexPatriciaTrie:
		fallthrough
	default:
		return NewHexPatriciaHashed(length.Addr, nil, nil, nil)
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
			}
			sb.WriteString("}\n")
		}
		bitset ^= bit
	}
	return sb.String()
}

func EncodeBranch(bitmap, touchMap, afterMap uint16, retriveCell func(nibble int, skip bool) (*Cell, error)) (branchData BranchData, lastNibble int, err error) {
	branchData = make(BranchData, 0, 32)
	var bitmapBuf [binary.MaxVarintLen64]byte

	binary.BigEndian.PutUint16(bitmapBuf[0:], touchMap)
	binary.BigEndian.PutUint16(bitmapBuf[2:], afterMap)

	branchData = append(branchData, bitmapBuf[:4]...)

	for bitset, j := afterMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		for i := lastNibble; i < nibble; i++ {
			if _, err := retriveCell(i, true /* skip */); err != nil {
				return nil, 0, err
			} // only writes 0x80 into hasher
		}
		lastNibble = nibble + 1

		cell, err := retriveCell(nibble, false)
		if err != nil {
			return nil, 0, err
		}

		if bitmap&bit != 0 {
			var fieldBits PartFlags
			if cell.extLen > 0 && cell.spl == 0 {
				fieldBits |= HashedKeyPart
			}
			if cell.apl > 0 {
				fieldBits |= AccountPlainPart
			}
			if cell.spl > 0 {
				fieldBits |= StoragePlainPart
			}
			if cell.hl > 0 {
				fieldBits |= HashPart
			}
			branchData = append(branchData, byte(fieldBits))
			if cell.extLen > 0 && cell.spl == 0 {
				n := binary.PutUvarint(bitmapBuf[:], uint64(cell.extLen))
				branchData = append(branchData, bitmapBuf[:n]...)
				branchData = append(branchData, cell.extension[:cell.extLen]...)
			}
			if cell.apl > 0 {
				n := binary.PutUvarint(bitmapBuf[:], uint64(cell.apl))
				branchData = append(branchData, bitmapBuf[:n]...)
				branchData = append(branchData, cell.apk[:cell.apl]...)
			}
			if cell.spl > 0 {
				n := binary.PutUvarint(bitmapBuf[:], uint64(cell.spl))
				branchData = append(branchData, bitmapBuf[:n]...)
				branchData = append(branchData, cell.spk[:cell.spl]...)
			}
			if cell.hl > 0 {
				n := binary.PutUvarint(bitmapBuf[:], uint64(cell.hl))
				branchData = append(branchData, bitmapBuf[:n]...)
				branchData = append(branchData, cell.h[:cell.hl]...)
			}
		}
		bitset ^= bit
	}
	return branchData, lastNibble, nil
}

// ExtractPlainKeys parses branchData and extract the plain keys for accounts and storage in the same order
// they appear witjin the branchData
func (branchData BranchData) ExtractPlainKeys() (accountPlainKeys [][]byte, storagePlainKeys [][]byte, err error) {
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4
	for bitset, j := touchMap&afterMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		fieldBits := PartFlags(branchData[pos])
		pos++
		if fieldBits&HashedKeyPart != 0 {
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
		if fieldBits&AccountPlainPart != 0 {
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
		if fieldBits&StoragePlainPart != 0 {
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
		if fieldBits&HashPart != 0 {
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

func (branchData BranchData) ReplacePlainKeys(accountPlainKeys [][]byte, storagePlainKeys [][]byte, newData []byte) (BranchData, error) {
	var numBuf [binary.MaxVarintLen64]byte
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4
	newData = append(newData, branchData[:4]...)
	var accountI, storageI int
	for bitset, j := touchMap&afterMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		fieldBits := PartFlags(branchData[pos])
		newData = append(newData, byte(fieldBits))
		pos++
		if fieldBits&HashedKeyPart != 0 {
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
		if fieldBits&AccountPlainPart != 0 {
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
			if l > 0 {
				pos += int(l)
			}
			n = binary.PutUvarint(numBuf[:], uint64(len(accountPlainKeys[accountI])))
			newData = append(newData, numBuf[:n]...)
			newData = append(newData, accountPlainKeys[accountI]...)
			accountI++
		}
		if fieldBits&StoragePlainPart != 0 {
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
			if l > 0 {
				pos += int(l)
			}
			n = binary.PutUvarint(numBuf[:], uint64(len(storagePlainKeys[storageI])))
			newData = append(newData, numBuf[:n]...)
			newData = append(newData, storagePlainKeys[storageI]...)
			storageI++
		}
		if fieldBits&HashPart != 0 {
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
	newData = append(newData, bitmapBuf[:]...)
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
					return nil, fmt.Errorf("MergeHexBranches buffer2 too small for field")
				} else if n < 0 {
					return nil, fmt.Errorf("MergeHexBranches value2 overflow for field")
				}
				newData = append(newData, branchData2[pos2:pos2+n]...)
				pos2 += n
				if len(branchData2) < pos2+int(l) {
					return nil, fmt.Errorf("MergeHexBranches buffer2 too small for field")
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
					return nil, fmt.Errorf("MergeHexBranches buffer1 too small for field")
				} else if n < 0 {
					return nil, fmt.Errorf("MergeHexBranches value1 overflow for field")
				}
				if add {
					newData = append(newData, branchData[pos1:pos1+n]...)
				}
				pos1 += n
				if len(branchData) < pos1+int(l) {
					return nil, fmt.Errorf("MergeHexBranches buffer1 too small for field")
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
				err = fmt.Errorf("faield to fill cell at nibble %x: %w", nibble, err)
				return
			}
		}
		bitset ^= bit
	}
	return
}

type BranchMerger struct {
	buf    *bytes.Buffer
	num    [4]byte
	keccak hash.Hash
}

func NewHexBranchMerger(capacity uint64) *BranchMerger {
	return &BranchMerger{buf: bytes.NewBuffer(make([]byte, capacity)), keccak: sha3.NewLegacyKeccak256()}
}

// MergeHexBranches combines two branchData, number 2 coming after (and potentially shadowing) number 1
func (m *BranchMerger) Merge(branch1 BranchData, branch2 BranchData) (BranchData, error) {
	if branch2 == nil {
		return branch1, nil
	}
	if branch1 == nil {
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

	m.buf.Reset()
	if _, err := m.buf.Write(m.num[:]); err != nil {
		return nil, err
	}

	for bitset, j := bitmap1|bitmap2, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		if bitmap2&bit != 0 {
			// Add fields from branch2
			fieldBits := PartFlags(branch2[pos2])
			if err := m.buf.WriteByte(byte(fieldBits)); err != nil {
				return nil, err
			}
			pos2++

			for i := 0; i < bits.OnesCount8(byte(fieldBits)); i++ {
				l, n := binary.Uvarint(branch2[pos2:])
				if n == 0 {
					return nil, fmt.Errorf("MergeHexBranches branch2 is too small: expected node info size")
				} else if n < 0 {
					return nil, fmt.Errorf("MergeHexBranches branch2: size overflow for length")
				}

				_, err := m.buf.Write(branch2[pos2 : pos2+n])
				if err != nil {
					return nil, err
				}
				pos2 += n
				dataPos += n
				if len(branch2) < pos2+int(l) {
					return nil, fmt.Errorf("MergeHexBranches branch2 is too small: expected at least %d got %d bytes", pos2+int(l), len(branch2))
				}
				if l > 0 {
					if _, err := m.buf.Write(branch2[pos2 : pos2+int(l)]); err != nil {
						return nil, err
					}
					pos2 += int(l)
					dataPos += int(l)
				}
			}
		}
		if bitmap1&bit != 0 {
			add := (touchMap2&bit == 0) && (afterMap2&bit != 0) // Add fields from branchData1
			fieldBits := PartFlags(branch1[pos1])
			if add {
				if err := m.buf.WriteByte(byte(fieldBits)); err != nil {
					return nil, err
				}
			}
			pos1++
			for i := 0; i < bits.OnesCount8(byte(fieldBits)); i++ {
				l, n := binary.Uvarint(branch1[pos1:])
				if n == 0 {
					return nil, fmt.Errorf("MergeHexBranches branch1 is too small: expected node info size")
				} else if n < 0 {
					return nil, fmt.Errorf("MergeHexBranches branch1: size overflow for length")
				}
				if add {
					if _, err := m.buf.Write(branch1[pos1 : pos1+n]); err != nil {
						return nil, err
					}
				}
				pos1 += n
				if len(branch1) < pos1+int(l) {
					return nil, fmt.Errorf("MergeHexBranches branch1 is too small: expected at least %d got %d bytes", pos1+int(l), len(branch1))
				}
				if l > 0 {
					if add {
						if _, err := m.buf.Write(branch1[pos1 : pos1+int(l)]); err != nil {
							return nil, err
						}
					}
					pos1 += int(l)
				}
			}
		}
		bitset ^= bit
	}
	target := make([]byte, m.buf.Len())
	copy(target, m.buf.Bytes())
	return target, nil
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
