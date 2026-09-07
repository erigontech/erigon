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

func ParseFileCompression(s string) (FileCompression, error) {
	switch s {
	case "none", "":
		return CompressNone, nil
	case "k":
		return CompressKeys, nil
	case "v":
		return CompressVals, nil
	case "kv":
		return CompressKeys | CompressVals, nil
	default:
		return CompressNone, fmt.Errorf("invalid file compression type: %s", s)
	}
}

func (c FileCompression) Has(flag FileCompression) bool {
	return c&flag != 0
}

type MadvDisabler interface {
	DisableReadAhead()
}
