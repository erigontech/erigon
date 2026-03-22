package etl

import (
	"encoding/binary"
	"testing"
	"unsafe"
)

func BenchmarkLengthEncoding(b *testing.B) {
	var buf [binary.MaxVarintLen64]byte
	lengths := []int{0, 32, 128, 256, 1024, 65535}

	for _, length := range lengths {
		b.Run("PutVarint/"+itoa(length), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				binary.PutVarint(buf[:], int64(length))
			}
		})

		b.Run("BigEndian64/"+itoa(length), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				binary.BigEndian.PutUint64(buf[:8], uint64(length))
			}
		})

		b.Run("NativeEndian64/"+itoa(length), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				*(*uint64)(unsafe.Pointer(&buf[0])) = uint64(length)
			}
		})

		b.Run("NativeEndian32/"+itoa(length), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				*(*uint32)(unsafe.Pointer(&buf[0])) = uint32(length)
			}
		})
	}

	// Also bench the read side (decoding)
	for _, length := range lengths {
		var encoded [binary.MaxVarintLen64]byte

		n := binary.PutVarint(encoded[:], int64(length))
		b.Run("ReadVarint/"+itoa(length), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				binary.Varint(encoded[:n])
			}
		})

		binary.BigEndian.PutUint64(encoded[:8], uint64(length))
		b.Run("ReadBigEndian64/"+itoa(length), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				binary.BigEndian.Uint64(encoded[:8])
			}
		})

		*(*uint64)(unsafe.Pointer(&encoded[0])) = uint64(length)
		b.Run("ReadNativeEndian64/"+itoa(length), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = *(*uint64)(unsafe.Pointer(&encoded[0]))
			}
		})

		*(*uint32)(unsafe.Pointer(&encoded[0])) = uint32(length)
		b.Run("ReadNativeEndian32/"+itoa(length), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = *(*uint32)(unsafe.Pointer(&encoded[0]))
			}
		})
	}
}

func itoa(n int) string {
	return string(binary.AppendUvarint(nil, uint64(n)))
}
