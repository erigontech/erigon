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
	"sync"

	"github.com/klauspost/compress/zstd"
)

// make a sync.pool of compressors (zstd)
var compressorPool = sync.Pool{
	New: func() interface{} {
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

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

var plainUint64BufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]uint64, 1028)
		return &b
	},
}

var plainBytesBufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 1028)
		return &b
	},
}

var repeatedPatternBufferPool = sync.Pool{
	New: func() interface{} {
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

	// Get one plain buffer from the pool
	plainBufferPtr := plainUint64BufferPool.Get().(*[]uint64)
	defer plainUint64BufferPool.Put(plainBufferPtr)
	plainBuffer := *plainBufferPtr
	plainBuffer = plainBuffer[:0]

	// Get one repeated pattern buffer from the pool
	repeatedPatternPtr := repeatedPatternBufferPool.Get().(*[]repeatedPatternEntry)
	defer repeatedPatternBufferPool.Put(repeatedPatternPtr)
	repeatedPattern := *repeatedPatternPtr
	repeatedPattern = repeatedPattern[:0]

	for i := 0; i < len(new); i += 8 {
		if i+8 > len(old) {
			// Append the remaining new bytes that were not in the old slice
			plainBuffer = append(plainBuffer, binary.LittleEndian.Uint64(new[i:]))
			continue
		}
		plainBuffer = append(plainBuffer, binary.LittleEndian.Uint64(new[i:i+8])-binary.LittleEndian.Uint64(old[i:i+8]))
	}
	// Find the repeated pattern
	prevVal := plainBuffer[0]
	count := uint32(1)
	for i := 1; i < len(plainBuffer); i++ {
		if plainBuffer[i] == prevVal {
			count++
			continue
		}
		repeatedPattern = append(repeatedPattern, repeatedPatternEntry{prevVal, count})
		prevVal = plainBuffer[i]
		count = 1
	}
	repeatedPattern = append(repeatedPattern, repeatedPatternEntry{prevVal, count})
	if err := binary.Write(w, binary.BigEndian, uint32(len(repeatedPattern))); err != nil {
		return err
	}
	temp := make([]byte, 8)

	// Write the repeated pattern
	for _, entry := range repeatedPattern {
		binary.BigEndian.PutUint32(temp[:4], entry.count)
		if _, err := compressor.Write(temp[:4]); err != nil {
			return err
		}
		binary.BigEndian.PutUint64(temp, entry.val)
		if _, err := compressor.Write(temp); err != nil {
			return err
		}
	}
	*repeatedPatternPtr = repeatedPattern[:0]
	*plainBufferPtr = plainBuffer[:0]

	return compressor.Close()
}

func ComputeCompressedSerializedEffectiveBalancesDiff(w io.Writer, old, new []byte) error {
	if len(old) > len(new) {
		return errors.New("old list is longer than new list")
	}

	compressor := compressorPool.Get().(*zstd.Encoder)
	defer putComp(compressor)
	compressor.Reset(w)

	// Get one plain buffer from the pool
	plainBufferPtr := plainUint64BufferPool.Get().(*[]uint64)
	defer plainUint64BufferPool.Put(plainBufferPtr)
	plainBuffer := *plainBufferPtr
	plainBuffer = plainBuffer[:0]

	// Get one repeated pattern buffer from the pool
	repeatedPatternPtr := repeatedPatternBufferPool.Get().(*[]repeatedPatternEntry)
	defer repeatedPatternBufferPool.Put(repeatedPatternPtr)
	repeatedPattern := *repeatedPatternPtr
	repeatedPattern = repeatedPattern[:0]

	validatorSize := 121
	for i := 0; i < len(new); i += validatorSize {
		// 80:88
		if i+88 > len(old) {
			// Append the remaining new bytes that were not in the old slice
			plainBuffer = append(plainBuffer, binary.LittleEndian.Uint64(new[i+80:i+88]))
			continue
		}
		plainBuffer = append(plainBuffer, binary.LittleEndian.Uint64(new[i+80:i+88])-binary.LittleEndian.Uint64(old[i+80:i+88]))
	}
	// Find the repeated pattern
	prevVal := plainBuffer[0]
	count := uint32(1)
	for i := 1; i < len(plainBuffer); i++ {
		if plainBuffer[i] == prevVal {
			count++
			continue
		}
		repeatedPattern = append(repeatedPattern, repeatedPatternEntry{prevVal, count})
		prevVal = plainBuffer[i]
		count = 1
	}
	repeatedPattern = append(repeatedPattern, repeatedPatternEntry{prevVal, count})
	if err := binary.Write(w, binary.BigEndian, uint32(len(repeatedPattern))); err != nil {
		return err
	}
	temp := make([]byte, 8)

	// Write the repeated pattern
	for _, entry := range repeatedPattern {
		binary.BigEndian.PutUint32(temp[:4], entry.count)
		if _, err := compressor.Write(temp[:4]); err != nil {
			return err
		}
		binary.BigEndian.PutUint64(temp, entry.val)
		if _, err := compressor.Write(temp); err != nil {
			return err
		}
	}
	*repeatedPatternPtr = repeatedPattern[:0]
	*plainBufferPtr = plainBuffer[:0]

	return compressor.Close()
}

func ApplyCompressedSerializedUint64ListDiff(in, out []byte, diff []byte, reverse bool) ([]byte, error) {
	out = out[:0]

	buffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buffer)
	buffer.Reset()

	if _, err := buffer.Write(diff); err != nil {
		return nil, err
	}

	var length uint32
	if err := binary.Read(buffer, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	var entry repeatedPatternEntry

	decompressor, err := zstd.NewReader(buffer)
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

	validatorLength := 121
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

	buffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buffer)
	buffer.Reset()

	if _, err := buffer.Write(diff); err != nil {
		return nil, err
	}

	currValidator := make([]byte, 121)

	for {
		var index uint32
		if err := binary.Read(buffer, binary.BigEndian, &index); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if index == 1<<31 {
			break
		}
		n, err := io.ReadFull(buffer, currValidator)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n == 0 {
			break
		}
		if n != 121 {
			return nil, fmt.Errorf("read %d bytes, expected 121", n)
		}
		// overwrite the validator
		copy(out[index*121:], currValidator)
	}
	for {
		n, err := io.ReadFull(buffer, currValidator)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n == 0 {
			break
		}
		if n != 121 {
			return nil, fmt.Errorf("read %d bytes, expected 121", n)
		}
		out = append(out, currValidator...)
	}

	return out, nil
}
