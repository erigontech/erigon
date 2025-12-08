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

type FileCompression uint8

const (
	CompressNone FileCompression = 0b1
	CompressKeys FileCompression = 0b10
	CompressVals FileCompression = 0b100
)

func ParseFileCompression(s string) (FileCompression, error) {
	// Implementation would be here
	return CompressNone, nil
}

func (c FileCompression) Has(flag FileCompression) bool {
	return c&flag != 0
}

func (c FileCompression) String() string {
	return "none" // Simplified implementation
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
	MadvNormal() MadvDisabler
	DisableReadAhead()
}

type MadvDisabler interface {
	DisableReadAhead()
}
