// Copyright 2026 The Erigon Authors
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

package branch

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"strings"

	"github.com/erigontech/erigon/common/length"
)

type CellFields uint8

const (
	FieldExtension   CellFields = 1
	FieldAccountAddr CellFields = 2
	FieldStorageAddr CellFields = 4
	FieldHash        CellFields = 8
	FieldStateHash   CellFields = 16
)

func (p CellFields) String() string {
	var sb strings.Builder
	if p&FieldExtension != 0 {
		sb.WriteString("DownHash")
	}
	if p&FieldAccountAddr != 0 {
		sb.WriteString("+AccountPlain")
	}
	if p&FieldStorageAddr != 0 {
		sb.WriteString("+StoragePlain")
	}
	if p&FieldHash != 0 {
		sb.WriteString("+Hash")
	}
	if p&FieldStateHash != 0 {
		sb.WriteString("+LeafHash")
	}
	return sb.String()
}

type BranchData []byte

func (branchData BranchData) ChildCount() int {
	if len(branchData) < 4 {
		return 0
	}
	return bits.OnesCount16(binary.BigEndian.Uint16(branchData[2:4]))
}

func (branchData BranchData) IsTombstone() bool { return len(branchData) == 0 }

// A malformed branch reports true: treat as referenced, never under-report.
func (branchData BranchData) HasShortenedKeys() bool {
	_, _, shortened, err := branchData.CountPlainKeys()
	return shortened > 0 || err != nil
}

// CountPlainKeys tallies the branch's keys by kind. It walks every cell rather than stopping at the
// first shortened key, so a branch holding both kinds reports both truthfully. A non-nil err is a
// parse failure, and leaves the tally partial.
func (branchData BranchData) CountPlainKeys() (plainAccounts, plainStorages, shortened uint64, err error) {
	_, err = branchData.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		switch {
		case isStorage && len(key) == length.Addr+length.Hash:
			plainStorages++
		case !isStorage && len(key) == length.Addr:
			plainAccounts++
		default:
			shortened++
		}
		return nil, nil
	})
	return plainAccounts, plainStorages, shortened, err
}

// If fn returns nil, the original key is kept.
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
	anyChanged := false
	spanStart := 0
	for bitset, j := touchMap&afterMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		if pos >= len(branchData) {
			return nil, errors.New("replacePlainKeys buffer too small for cell fields")
		}
		fields := CellFields(branchData[pos])
		pos++
		if fields&FieldExtension != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for hashedKey len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for hashedKey len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hashedKey: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
		}
		if fields&FieldAccountAddr != 0 {
			keyFieldStart := pos
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for accountAddr len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for accountAddr len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for accountAddr: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
			newKey, err := fn(branchData[pos-int(l):pos], false)
			if err != nil {
				return nil, err
			}
			if newKey != nil {
				if !anyChanged {
					if cap(newData) < len(branchData) {
						newData = make([]byte, 0, len(branchData))
					} else {
						newData = newData[:0]
					}
					anyChanged = true
				}
				newData = append(newData, branchData[spanStart:keyFieldStart]...)
				n = binary.PutUvarint(numBuf[:], uint64(len(newKey)))
				newData = append(newData, numBuf[:n]...)
				newData = append(newData, newKey...)
				spanStart = pos
			}
		}
		if fields&FieldStorageAddr != 0 {
			keyFieldStart := pos
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for storageAddr len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for storageAddr len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for storageAddr: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
			newKey, err := fn(branchData[pos-int(l):pos], true)
			if err != nil {
				return nil, err
			}
			if newKey != nil {
				if !anyChanged {
					if cap(newData) < len(branchData) {
						newData = make([]byte, 0, len(branchData))
					} else {
						newData = newData[:0]
					}
					anyChanged = true
				}
				newData = append(newData, branchData[spanStart:keyFieldStart]...)
				n = binary.PutUvarint(numBuf[:], uint64(len(newKey)))
				newData = append(newData, numBuf[:n]...)
				newData = append(newData, newKey...)
				spanStart = pos
			}
		}
		if fields&FieldHash != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for hash len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for hash len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hash: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
		}
		if fields&FieldStateHash != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for acLeaf hash len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for acLeafhash len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for LeafHash: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
		}

		bitset ^= bit
	}

	if !anyChanged {
		return branchData, nil
	}
	newData = append(newData, branchData[spanStart:]...)
	return newData, nil
}

func (branchData BranchData) IsComplete() bool {
	if len(branchData) < 4 {
		return false
	}
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	return ^touchMap&afterMap == 0
}

// branch2 shadows branch1 where both touch the same cell.
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
			fields := CellFields(branchData2[pos2])
			newData = append(newData, byte(fields))
			pos2++
			for i := 0; i < bits.OnesCount8(byte(fields)); i++ {
				l, n := binary.Uvarint(branchData2[pos2:])
				if n == 0 {
					return nil, errors.New("MergeHexBranches buffer2 too small for field")
				} else if n < 0 {
					return nil, errors.New("MergeHexBranches value2 overflow for field")
				}
				newData = append(newData, branchData2[pos2:pos2+n]...)
				pos2 += n
				if len(branchData2) < pos2+int(l) {
					return nil, fmt.Errorf("MergeHexBranches buffer2 too small for %s : expected %d got %d", fields&CellFields(1<<i), pos2+int(l), len(branchData2))
				}
				if l > 0 {
					newData = append(newData, branchData2[pos2:pos2+int(l)]...)
					pos2 += int(l)
				}
			}
		}
		if bitmap1&bit != 0 {
			add := (touchMap2&bit == 0) && (afterMap2&bit != 0)
			fields := CellFields(branchData[pos1])
			if add {
				newData = append(newData, byte(fields))
			}
			pos1++
			for i := 0; i < bits.OnesCount8(byte(fields)); i++ {
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
					return nil, fmt.Errorf("MergeHexBranches buffer1 too small for %s : expected %d got %d", fields&CellFields(1<<i), pos1+int(l), len(branchData))
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

type BranchMerger struct {
	buf []byte
	num [4]byte
}

func NewHexBranchMerger(capacity uint64) *BranchMerger {
	return &BranchMerger{buf: make([]byte, capacity)}
}

// branch2 shadows branch1 where both touch the same cell.
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
			fields := CellFields(branch2[pos2])
			m.buf = append(m.buf, byte(fields))
			pos2++

			for i := 0; i < bits.OnesCount8(byte(fields)); i++ {
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
			add := (touchMap2&bit == 0) && (afterMap2&bit != 0)
			fields := CellFields(branch1[pos1])
			if add {
				m.buf = append(m.buf, byte(fields))
			}
			pos1++
			for i := 0; i < bits.OnesCount8(byte(fields)); i++ {
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

type BranchStat struct {
	KeySize       uint64
	ValSize       uint64
	MinCellSize   uint64
	MaxCellSize   uint64
	CellCount     uint64
	APKSize       uint64
	SPKSize       uint64
	ExtSize       uint64
	HashSize      uint64
	APKCount      uint64
	SPKCount      uint64
	HashCount     uint64
	ExtCount      uint64
	TAMapsSize    uint64
	LeafHashSize  uint64
	LeafHashCount uint64
	MedianAPK     uint64
	MedianSPK     uint64
	MedianHash    uint64
	MedianExt     uint64
	MedianLH      uint64
	IsRoot        bool
}

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

	setMedian := func(median *uint64, otherMedian uint64) {
		if *median == 0 {
			*median = otherMedian
		} else {
			*median = (*median + otherMedian) / 2
		}
	}
	setMedian(&bs.MedianExt, other.MedianExt)
	setMedian(&bs.MedianAPK, other.MedianAPK)
	setMedian(&bs.MedianSPK, other.MedianSPK)
	setMedian(&bs.MedianHash, other.MedianHash)
	setMedian(&bs.MedianLH, other.MedianLH)
	bs.MedianHash = (bs.MedianHash + other.MedianHash) / 2
	bs.MedianAPK = (bs.MedianAPK + other.MedianAPK) / 2
	bs.MedianSPK = (bs.MedianSPK + other.MedianSPK) / 2
	bs.MedianLH = (bs.MedianLH + other.MedianLH) / 2
	bs.TAMapsSize += other.TAMapsSize
	bs.LeafHashSize += other.LeafHashSize
	bs.LeafHashCount += other.LeafHashCount
}
