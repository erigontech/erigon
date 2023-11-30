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
		compressor, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
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
		return fmt.Errorf("old list is longer than new list")
	}

	compressor := compressorPool.Get().(*zstd.Encoder)
	defer compressorPool.Put(compressor)
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
		return fmt.Errorf("old list is longer than new list")
	}

	compressor := compressorPool.Get().(*zstd.Encoder)
	defer compressorPool.Put(compressor)
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
	var entry repeatedPatternEntry

	decompressor, err := zstd.NewReader(buffer)
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	temp := make([]byte, 8)
	currIndex := 0
	for i := 0; i < int(length); i++ {
		n, err := decompressor.Read(temp[:4])
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n != 4 {
			return nil, io.EOF
		}
		entry.count = binary.BigEndian.Uint32(temp[:4])
		n, err = decompressor.Read(temp)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n != 8 {
			return nil, io.EOF
		}
		entry.val = binary.BigEndian.Uint64(temp)
		for j := 0; j < int(entry.count); j++ {
			if currIndex+8 > len(old) {
				// Append the remaining new bytes that were not in the old slice
				out = binary.LittleEndian.AppendUint64(out, entry.val)
				currIndex += 8
				continue
			}
			out = binary.LittleEndian.AppendUint64(out, binary.LittleEndian.Uint64(old[currIndex:currIndex+8])+entry.val)
			currIndex += 8
		}
	}

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
	defer decompressor.Close()

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

// func PartecipationBitlistDiff(w io.Writer, old, new []byte) error {
// 	if len(old) > len(new) {
// 		return fmt.Errorf("old list is longer than new list")
// 	}

// 	// ger bytes buffer from pool
// 	tempBufPtr := plainBytesBufferPool.Get().(*[]byte)
// 	defer plainBytesBufferPool.Put(tempBufPtr)
// 	tempBuf := *tempBufPtr
// 	tempBuf = tempBuf[:0]

// 	zeroCount := 0
// 	for i := 0; i < len(new); i++ {
// 		xored := new[i] ^ old[i]
// 		if xored == 0 {
// 			zeroCount++
// 			if zeroCount == math.MaxUint8 {
// 				tempBuf = append(tempBuf, 0x0)
// 				tempBuf = append(tempBuf, byte(zeroCount))
// 				zeroCount = 0
// 			}
// 			continue
// 		}
// 		if zeroCount > 0 {
// 			tempBuf = append(tempBuf, 0x0)
// 			tempBuf = append(tempBuf, byte(zeroCount))
// 			zeroCount = 0
// 		}
// 		tempBuf = append(tempBuf, xored)
// 	}
// 	if zeroCount > 0 {
// 		tempBuf = append(tempBuf, 0x0)
// 		tempBuf = append(tempBuf, byte(zeroCount))
// 	}

// 	if err := binary.Write(w, binary.BigEndian, uint32(len(tempBuf))); err != nil {
// 		return err
// 	}

// 	compressor := compressorPool.Get().(*zstd.Encoder)
// 	defer compressorPool.Put(compressor)
// 	compressor.Reset(w)

// 	if _, err := compressor.Write(tempBuf); err != nil {
// 		return err
// 	}

// 	return compressor.Close()
// }
