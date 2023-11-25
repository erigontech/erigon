package base_encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

// make a sync.pool of compressors (zstd)
var compressorPool = sync.Pool{
	New: func() interface{} {
		compressor, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(8))
		if err != nil {
			panic(err)
		}
		return compressor
	},
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func ComputeCompressedSerializedUint64ListDiff(w io.Writer, old, new []byte) error {
	if len(old) > len(new) {
		return fmt.Errorf("old list is longer than new list")
	}

	compressor := compressorPool.Get().(*zstd.Encoder)
	defer compressorPool.Put(compressor)
	compressor.Reset(w)

	if err := binary.Write(w, binary.BigEndian, uint32(len(new))); err != nil {
		return err
	}
	temp := make([]byte, 8)
	for i := 0; i < len(old); i += 8 {
		binary.LittleEndian.PutUint64(temp, binary.LittleEndian.Uint64(new[i:i+8])-binary.LittleEndian.Uint64(old[i:i+8]))
		if _, err := compressor.Write(temp); err != nil {
			return err
		}
	}
	// dump the remaining bytes
	if _, err := compressor.Write(new[len(old):]); err != nil {
		return err
	}
	return compressor.Close()
}

func ComputeCompressedSerializedEffectiveBalancesDiff(w io.Writer, old, new []byte) error {
	if len(old) > len(new) {
		return fmt.Errorf("old list is longer than new list")
	}

	compressor := compressorPool.Get().(*zstd.Encoder)
	defer compressorPool.Put(compressor)
	compressor.Reset(w)

	if err := binary.Write(w, binary.BigEndian, uint32(len(new))); err != nil {
		return err
	}
	temp := make([]byte, 8)
	validatorSetSize := 121
	for i := 0; i < len(old)/validatorSetSize; i++ {
		// 80:88
		binary.LittleEndian.PutUint64(temp, binary.LittleEndian.Uint64(new[i*validatorSetSize+80:i*validatorSetSize+88])-binary.LittleEndian.Uint64(old[i*validatorSetSize+80:i*validatorSetSize+88]))
		if _, err := compressor.Write(temp); err != nil {
			return err
		}
	}
	// dump the remaining bytes
	if _, err := compressor.Write(new[len(old):]); err != nil {
		return err
	}
	if err := compressor.Close(); err != nil {
		return err
	}
	return nil
}

func ApplyCompressedSerializedUint64ListDiff(old, out []byte, diff []byte) ([]byte, error) {
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

	decompressor, err := zstd.NewReader(buffer)
	if err != nil {
		return nil, err
	}

	temp := make([]byte, 8)
	for i := 0; i < len(old); i += 8 {
		n, err := decompressor.Read(temp)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		fmt.Println(temp, err, n)
		if n != 8 {
			return nil, io.EOF
		}
		binary.LittleEndian.PutUint64(temp, binary.LittleEndian.Uint64(old[i:i+8])+binary.LittleEndian.Uint64(temp))
		out = append(out, temp...)
	}

	// Append the remaining new bytes that were not in the old slice
	remainingBytes := make([]byte, int(length)-len(old))
	var n int
	n, err = decompressor.Read(remainingBytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if n != len(remainingBytes) {
		return nil, io.EOF
	}
	out = append(out, remainingBytes...)

	return out, nil
}

func ComputeCompressedSerializedByteListDiff(w io.Writer, old, new []byte) error {
	if len(old) > len(new) {
		return fmt.Errorf("old list is longer than new list")
	}

	compressor := compressorPool.Get().(*zstd.Encoder)
	defer compressorPool.Put(compressor)
	compressor.Reset(w)

	if err := binary.Write(w, binary.BigEndian, uint32(len(new))); err != nil {
		return err
	}

	bytesClock := 64
	blockXorLen := len(old) / bytesClock
	tmp := make([]byte, bytesClock)
	for i := 0; i < blockXorLen; i++ {
		blockXOR(tmp, old[i*bytesClock:(i+1)*bytesClock], new[i*bytesClock:(i+1)*bytesClock])
		if _, err := compressor.Write(tmp); err != nil {
			return err
		}
	}
	// Take full advantage of the blockXOR
	for i := blockXorLen * bytesClock; i < len(old); i++ {
		if _, err := compressor.Write([]byte{new[i] - old[i]}); err != nil {
			return err
		}
	}
	// dump the remaining bytes
	if _, err := compressor.Write(new[len(old):]); err != nil {
		return err
	}

	if err := compressor.Close(); err != nil {
		return err
	}

	return nil
}

func ApplyCompressedSerializedByteListDiff(old, out []byte, diff []byte) ([]byte, error) {
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

	decompressor, err := zstd.NewReader(buffer)
	if err != nil {
		return nil, err
	}

	bytesClock := 64

	tmp := make([]byte, bytesClock)
	blockXorLen := len(old) / bytesClock
	block := make([]byte, bytesClock)
	for i := 0; i < blockXorLen; i++ {
		n, err := decompressor.Read(tmp)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}
		if n != bytesClock {
			return nil, io.EOF
		}

		blockXOR(block, old[i*bytesClock:(i+1)*bytesClock], tmp)
		out = append(out, block...)
	}

	readByte := make([]byte, 1)
	// Handle the remaining bytes with XOR
	for i := blockXorLen * bytesClock; i < len(old); i++ {
		n, err := decompressor.Read(readByte)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n != 1 {
			return nil, io.EOF
		}
		out = append(out, old[i]+readByte[0])
	}

	// Append the remaining new bytes that were not in the old slice
	remainingBytes := make([]byte, int(length)-len(old))
	var n int
	n, err = decompressor.Read(remainingBytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if n != len(remainingBytes) {
		return nil, io.EOF
	}
	out = append(out, remainingBytes...)

	return out, nil
}

// Block stores (a xor b) in dst, where a, b, and dst all have length 16.
func blockXOR(dst, a, b []byte) {
	dw := (*[8]uintptr)(unsafe.Pointer(&dst[0]))
	aw := (*[8]uintptr)(unsafe.Pointer(&a[0]))
	bw := (*[8]uintptr)(unsafe.Pointer(&b[0]))
	dw[0] = aw[0] ^ bw[0]
	dw[1] = aw[1] ^ bw[1]
	dw[2] = aw[2] ^ bw[2]
	dw[3] = aw[3] ^ bw[3]
	dw[4] = aw[4] ^ bw[4]
	dw[5] = aw[5] ^ bw[5]
	dw[6] = aw[6] ^ bw[6]
	dw[7] = aw[7] ^ bw[7]
}
