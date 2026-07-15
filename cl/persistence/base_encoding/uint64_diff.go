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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// make a sync.pool of compressors (zstd)
var compressorPool = sync.Pool{
	New: func() any {
		compressor, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			panic(err)
		}
		return compressor
	},
}

func putComp(v *zstd.Encoder) {
	v.Reset(nil)
	compressorPool.Put(v)
}

var repeatedPatternBufferPool = sync.Pool{
	New: func() any {
		b := make([]repeatedPatternEntry, 1028)
		return &b
	},
}

type repeatedPatternEntry struct {
	val   uint64
	count uint32
}

func ComputeCompressedSerializedUint64ListDiff(w io.Writer, old, new []byte) error {
	if len(old) > len(new) {
		return errors.New("old list is longer than new list")
	}

	compressor := compressorPool.Get().(*zstd.Encoder)
	defer putComp(compressor)
	compressor.Reset(w)

	// Get one repeated pattern buffer from the pool
	repeatedPatternPtr := repeatedPatternBufferPool.Get().(*[]repeatedPatternEntry)
	defer repeatedPatternBufferPool.Put(repeatedPatternPtr)
	repeatedPattern := *repeatedPatternPtr
	repeatedPattern = repeatedPattern[:0]

	delta := func(i int) uint64 {
		if i+8 > len(old) {
			return binary.LittleEndian.Uint64(new[i:])
		}
		return binary.LittleEndian.Uint64(new[i:i+8]) - binary.LittleEndian.Uint64(old[i:i+8])
	}
	// Run-length encode the deltas in a single pass
	prevVal := delta(0)
	count := uint32(1)
	for i := 8; i < len(new); i += 8 {
		if d := delta(i); d == prevVal {
			count++
		} else {
			repeatedPattern = append(repeatedPattern, repeatedPatternEntry{prevVal, count})
			prevVal = d
			count = 1
		}
	}
	repeatedPattern = append(repeatedPattern, repeatedPatternEntry{prevVal, count})
	var temp [12]byte
	binary.BigEndian.PutUint32(temp[:4], uint32(len(repeatedPattern)))
	if _, err := w.Write(temp[:4]); err != nil {
		return err
	}

	// Write the repeated pattern
	for _, entry := range repeatedPattern {
		binary.BigEndian.PutUint32(temp[:4], entry.count)
		binary.BigEndian.PutUint64(temp[4:], entry.val)
		if _, err := compressor.Write(temp[:]); err != nil {
			return err
		}
	}
	*repeatedPatternPtr = repeatedPattern[:0]

	return compressor.Close()
}

const (
	validatorSSZSize       = 121
	effectiveBalanceOffset = 80
)

// AppendEffectiveBalances appends each validator's 8-byte effective balance, tightly packed, to dst.
func AppendEffectiveBalances(dst, validatorSetSSZ []byte) []byte {
	validators := len(validatorSetSSZ) / validatorSSZSize
	dst = slices.Grow(dst, validators*8)
	for i := 0; i < validators; i++ {
		off := i*validatorSSZSize + effectiveBalanceOffset
		dst = append(dst, validatorSetSSZ[off:off+8]...)
	}
	return dst
}

func ApplyCompressedSerializedUint64ListDiff(in, out []byte, diff []byte, reverse bool) ([]byte, error) {
	out = out[:0]

	reader := bytes.NewReader(diff)

	var length uint32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	var entry repeatedPatternEntry

	decompressor, err := zstd.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	temp := make([]byte, 8)
	currIndex := 0
	for i := 0; i < int(length); i++ {
		n, err := io.ReadFull(decompressor, temp[:4])
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n != 4 {
			return nil, io.EOF
		}
		entry.count = binary.BigEndian.Uint32(temp[:4])

		n, err = io.ReadFull(decompressor, temp)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n != 8 {
			return nil, io.EOF
		}
		entry.val = binary.BigEndian.Uint64(temp)
		for j := 0; j < int(entry.count); j++ {
			if currIndex+8 > len(in) {
				// Append the remaining new bytes that were not in the old slice
				out = binary.LittleEndian.AppendUint64(out, entry.val)
				currIndex += 8
				continue
			}
			if !reverse {
				out = binary.LittleEndian.AppendUint64(out, binary.LittleEndian.Uint64(in[currIndex:currIndex+8])+entry.val)
			} else {
				out = binary.LittleEndian.AppendUint64(out, binary.LittleEndian.Uint64(in[currIndex:currIndex+8])-entry.val)
			}
			currIndex += 8
		}
	}

	return out, nil
}

func ComputeCompressedSerializedValidatorSetListDiff(w io.Writer, old, new []byte) error {
	if len(old) > len(new) {
		return errors.New("old list is longer than new list")
	}

	validatorLength := validatorSSZSize
	if len(old)%validatorLength != 0 {
		return fmt.Errorf("old list is not a multiple of validator length got %d", len(old))
	}
	if len(new)%validatorLength != 0 {
		return fmt.Errorf("new list is not a multiple of validator length got %d", len(new))
	}
	for i := 0; i < len(old); i += validatorLength {
		if !bytes.Equal(old[i:i+validatorLength], new[i:i+validatorLength]) {
			if err := binary.Write(w, binary.BigEndian, uint32(i/validatorLength)); err != nil {
				return err
			}
			if _, err := w.Write(new[i : i+validatorLength]); err != nil {
				return err
			}
		}
	}
	if err := binary.Write(w, binary.BigEndian, uint32(1<<31)); err != nil {
		return err
	}

	if _, err := w.Write(new[len(old):]); err != nil {
		return err
	}

	return nil
}

func ApplyCompressedSerializedValidatorListDiff(in, out []byte, diff []byte, reverse bool) ([]byte, error) {
	out = out[:0]
	if cap(out) < len(in) {
		out = make([]byte, len(in))
	}
	out = out[:len(in)]

	reader := bytes.NewReader(diff)

	currValidator := make([]byte, validatorSSZSize)

	for {
		var index uint32
		if err := binary.Read(reader, binary.BigEndian, &index); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if index == 1<<31 {
			break
		}
		n, err := io.ReadFull(reader, currValidator)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n == 0 {
			break
		}
		if n != validatorSSZSize {
			return nil, fmt.Errorf("read %d bytes, expected %d", n, validatorSSZSize)
		}
		// overwrite the validator
		copy(out[index*validatorSSZSize:], currValidator)
	}
	for {
		n, err := io.ReadFull(reader, currValidator)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n == 0 {
			break
		}
		if n != validatorSSZSize {
			return nil, fmt.Errorf("read %d bytes, expected %d", n, validatorSSZSize)
		}
		out = append(out, currValidator...)
	}

	return out, nil
}
