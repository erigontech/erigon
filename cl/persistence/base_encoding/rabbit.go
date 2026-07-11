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

package base_encoding

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/klauspost/compress/zstd"
)

func WriteRabbits(in []uint64, w io.Writer) error {
	// Retrieve compressor first.
	compressor := compressorPool.Get().(*zstd.Encoder)
	defer putComp(compressor)
	compressor.Reset(w)

	var buf [8]byte
	writeNum := func(v uint64) error {
		binary.LittleEndian.PutUint64(buf[:], v)
		n, err := compressor.Write(buf[:])
		if err != nil {
			return err
		}
		if n != len(buf) {
			return io.ErrShortWrite
		}
		return nil
	}

	expectedNum := uint64(0)
	count := 0
	// write length
	if err := writeNum(uint64(len(in))); err != nil {
		return err
	}
	for _, element := range in {
		if expectedNum != element {
			// [1,2,5,6]
			// write contiguous sequence
			if err := writeNum(uint64(count)); err != nil {
				return err
			}
			// write non-contiguous element
			if err := writeNum(element - expectedNum); err != nil {
				return err
			}
			count = 0
		}
		count++
		expectedNum = element + 1

	}
	// write last contiguous sequence
	if err := writeNum(uint64(count)); err != nil {
		return err
	}
	return compressor.Close()
}

func ReadRabbits(out []uint64, r io.Reader) ([]uint64, error) {
	// Retrieve compressor first
	decompressor, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	var buf [8]byte
	readNum := func() (uint64, error) {
		if _, err := io.ReadFull(decompressor, buf[:]); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(buf[:]), nil
	}

	length, err := readNum()
	if err != nil {
		return nil, err
	}

	if cap(out) < int(length) {
		out = make([]uint64, 0, length)
	}
	out = out[:0]
	var current uint64
	active := true
	for {
		count, err := readNum()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if active {
			for i := current; i < current+count; i++ {
				out = append(out, i)
			}
		}
		current += count
		active = !active
	}
	return out, nil
}
