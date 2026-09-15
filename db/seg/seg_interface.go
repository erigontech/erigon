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
)

type FeatureFlag uint8

const (
	PageLevelCompressionEnabled FeatureFlag = 1 << iota // 0b001
)

type FeatureFlagBitmask uint8

func (m FeatureFlagBitmask) Has(flag FeatureFlag) bool {
	return m&FeatureFlagBitmask(flag) == FeatureFlagBitmask(flag)
}

func (m *FeatureFlagBitmask) Set(flag FeatureFlag) {
	*m |= FeatureFlagBitmask(flag)
}

// FromString accepts both the short form used in file metadata ("k", "v",
// "kv") and the long form CLI flags take ("keys", "values", "all").
func (c *FileCompression) FromString(s string) error {
	switch s {
	case "none", "":
		*c = CompressNone
	case "k", "keys":
		*c = CompressKeys
	case "v", "values":
		*c = CompressVals
	case "kv", "all":
		*c = CompressKeys | CompressVals
	default:
		return fmt.Errorf("invalid file compression type: %s", s)
	}
	return nil
}

func ParseFileCompression(s string) (FileCompression, error) {
	var c FileCompression
	err := c.FromString(s)
	return c, err
}

func (c FileCompression) Has(flag FileCompression) bool {
	return c&flag != 0
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
	CompressedPageValuesCount() int
}

type MadvDisabler interface {
	DisableReadAhead()
}
