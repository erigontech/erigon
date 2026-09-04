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

package seg

import "fmt"

type FileCompression uint8

const (
	CompressNone FileCompression = 0b1
	CompressKeys FileCompression = 0b10
	CompressVals FileCompression = 0b100
)

const (
	FileCompressionFormatV0 = uint8(0)
	FileCompressionFormatV1 = uint8(1)
	FileCompressionFormatV2 = uint8(2)
)

type FeatureFlag uint8

const (
	PageLevelCompressionEnabled    FeatureFlag = 0b1
	WordLevelKeyCompressionEnabled FeatureFlag = 0b10
	WordLevelValCompressionEnabled FeatureFlag = 0b100
)

type FeatureFlagBitmask uint8

func (m FeatureFlagBitmask) Has(flag FeatureFlag) bool {
	return m&FeatureFlagBitmask(flag) == FeatureFlagBitmask(flag)
}

func (m *FeatureFlagBitmask) Set(flag FeatureFlag) {
	*m |= FeatureFlagBitmask(flag)
}

func ParseFileCompression(s string) (FileCompression, error) {
	switch s {
	case "", "none":
		return CompressNone, nil
	case "k", "keys":
		return CompressKeys, nil
	case "v", "vals":
		return CompressVals, nil
	case "kv", "keys+vals":
		return CompressKeys | CompressVals, nil
	default:
		return CompressNone, fmt.Errorf("unknown file compression %q (want: none, k|keys, v|vals, kv|keys+vals)", s)
	}
}

func (c FileCompression) Has(flag FileCompression) bool {
	return c&flag != 0
}

func (c FileCompression) String() string {
	if c.Has(CompressKeys) && c.Has(CompressVals) {
		return "keys+vals"
	}
	if c.Has(CompressKeys) {
		return "keys"
	}
	if c.Has(CompressVals) {
		return "vals"
	}
	return "none"
}

type ReaderI interface {
	Next(buf []byte) ([]byte, uint64)
	Size() int
	Count() int
	Reset(offset uint64)
	HasNext() bool
	Skip() (uint64, int)
	FileName() string
	BinarySearch(seek []byte, count int, getOffset func(i uint64) (offset uint64)) (foundOffset uint64, ok bool)
	GetMetadata() []byte
	MadvNormal() MadvDisabler
	DisableReadAhead()
}

type MadvDisabler interface {
	DisableReadAhead()
}
