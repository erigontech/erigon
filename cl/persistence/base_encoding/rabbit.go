package base_encoding

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/klauspost/compress/zstd"
)

func WriteRabbits(in []uint64, w io.Writer) error {
	// Retrieve compressor first
	compressor := compressorPool.Get().(*zstd.Encoder)
	defer compressorPool.Put(compressor)
	compressor.Reset(w)

	expectedNum := uint64(0)
	count := 0
	// write length
	if err := binary.Write(compressor, binary.LittleEndian, uint64(len(in))); err != nil {
		return err
	}
	for _, element := range in {
		if expectedNum != element {
			// [1,2,5,6]
			// write contiguous sequence
			if err := binary.Write(compressor, binary.LittleEndian, uint64(count)); err != nil {
				return err
			}
			// write non-contiguous element
			if err := binary.Write(compressor, binary.LittleEndian, element-expectedNum); err != nil {
				return err
			}
			count = 0
		}
		count++
		expectedNum = element + 1

	}
	// write last contiguous sequence
	if err := binary.Write(compressor, binary.LittleEndian, uint64(count)); err != nil {
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

	var length uint64
	if err := binary.Read(decompressor, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	if cap(out) < int(length) {
		out = make([]uint64, 0, length)
	}
	out = out[:0]
	var count uint64
	var current uint64
	active := true
	for err != io.EOF {
		err = binary.Read(decompressor, binary.LittleEndian, &count)
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
			current += count
		} else {
			current += count
		}
		active = !active
	}
	return out, nil
}
