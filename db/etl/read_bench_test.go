package etl

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"os"
	"testing"
	"unsafe"

	"github.com/erigontech/erigon/common/mmap"
)

// readFieldU16 reads a uint16-prefixed field from mmap data (zero-copy, bench only).
func readFieldU16(m *mmapBytesReader) ([]byte, error) {
	if m.pos+2 > len(m.data) {
		return nil, io.EOF
	}
	n := int(*(*uint16)(unsafe.Pointer(&m.data[m.pos])))
	m.pos += 2
	if n == math.MaxUint16 {
		return nil, nil
	}
	if m.pos+n > len(m.data) {
		return nil, io.EOF
	}
	s := m.data[m.pos : m.pos+n]
	m.pos += n
	return s, nil
}

// BenchmarkSequentialRead compares mmap vs bufio for sequential reads of length-prefixed records
// from a 128MB file. Compares native16 (current format) vs native32 length encoding.
func BenchmarkSequentialRead(b *testing.B) {
	const fileSize = 128 * 1024 * 1024 // 128MB

	for _, valSize := range []int{32, 128, 1024} {
		name := fmt.Sprintf("val%d", valSize)
		tmpdir := b.TempDir()

		fnameU16 := createTestFileU16(b, tmpdir, 32, valSize, fileSize)
		fnameU32 := createTestFileU32(b, tmpdir, 32, valSize, fileSize)

		b.Run(name+"/mmap_u16", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchMmapU16(b, fnameU16)
			}
		})

		b.Run(name+"/mmap_u32", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchMmapU32(b, fnameU32)
			}
		})

		b.Run(name+"/bufio_u16", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchBufioU16(b, fnameU16, BufIOSize)
			}
		})

		b.Run(name+"/bufio_u32", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchBufioU32(b, fnameU32, BufIOSize)
			}
		})
	}
}

func createTestFileU16(b *testing.B, tmpdir string, keySize, valSize, fileSize int) string {
	b.Helper()
	f, err := os.CreateTemp(tmpdir, "bench-u16-")
	if err != nil {
		b.Fatal(err)
	}
	w := bufio.NewWriterSize(f, BufIOSize)

	key := make([]byte, keySize)
	val := make([]byte, valSize)
	rand.Read(key)
	rand.Read(val)

	var lenBuf [2]byte
	written := 0
	for written < fileSize {
		*(*uint16)(unsafe.Pointer(&lenBuf[0])) = uint16(keySize)
		w.Write(lenBuf[:])
		w.Write(key)
		*(*uint16)(unsafe.Pointer(&lenBuf[0])) = uint16(valSize)
		w.Write(lenBuf[:])
		w.Write(val)
		written += 2 + keySize + 2 + valSize
	}
	w.Flush()
	f.Close()
	return f.Name()
}

func createTestFileU32(b *testing.B, tmpdir string, keySize, valSize, fileSize int) string {
	b.Helper()
	f, err := os.CreateTemp(tmpdir, "bench-u32-")
	if err != nil {
		b.Fatal(err)
	}
	w := bufio.NewWriterSize(f, BufIOSize)

	key := make([]byte, keySize)
	val := make([]byte, valSize)
	rand.Read(key)
	rand.Read(val)

	var lenBuf [4]byte
	written := 0
	for written < fileSize {
		*(*uint32)(unsafe.Pointer(&lenBuf[0])) = uint32(keySize)
		w.Write(lenBuf[:])
		w.Write(key)
		*(*uint32)(unsafe.Pointer(&lenBuf[0])) = uint32(valSize)
		w.Write(lenBuf[:])
		w.Write(val)
		written += 4 + keySize + 4 + valSize
	}
	w.Flush()
	f.Close()
	return f.Name()
}

func openMmap(b *testing.B, fname string) ([]byte, *os.File) {
	b.Helper()
	f, err := os.Open(fname)
	if err != nil {
		b.Fatal(err)
	}
	fi, err := f.Stat()
	if err != nil {
		b.Fatal(err)
	}
	data, _, err := mmap.Mmap(f, int(fi.Size()))
	if err != nil {
		b.Fatal(err)
	}
	_ = mmap.MadviseSequential(data)
	return data, f
}

func benchMmapU16(b *testing.B, fname string) {
	b.Helper()
	data, f := openMmap(b, fname)
	defer f.Close()

	m := &mmapBytesReader{data: data, pos: 0}
	for {
		if _, err := readFieldU16(m); err != nil {
			break
		}
		if _, err := readFieldU16(m); err != nil {
			break
		}
	}
}

func benchMmapU32(b *testing.B, fname string) {
	b.Helper()
	data, f := openMmap(b, fname)
	defer f.Close()

	pos := 0
	for pos+4 <= len(data) {
		kLen := int(*(*uint32)(unsafe.Pointer(&data[pos])))
		pos += 4
		if pos+kLen > len(data) {
			break
		}
		_ = data[pos : pos+kLen]
		pos += kLen

		if pos+4 > len(data) {
			break
		}
		vLen := int(*(*uint32)(unsafe.Pointer(&data[pos])))
		pos += 4
		if pos+vLen > len(data) {
			break
		}
		_ = data[pos : pos+vLen]
		pos += vLen
	}
}

func benchBufioU16(b *testing.B, fname string, bufSize int) {
	b.Helper()
	f, err := os.Open(fname)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, bufSize)
	var buf []byte
	for {
		if buf, err = readFieldBufioU16(r, buf); err != nil {
			break
		}
		if buf, err = readFieldBufioU16(r, buf); err != nil {
			break
		}
	}
}

func benchBufioU32(b *testing.B, fname string, bufSize int) {
	b.Helper()
	f, err := os.Open(fname)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, bufSize)
	var buf []byte
	for {
		if buf, err = readFieldBufioU32(r, buf); err != nil {
			break
		}
		if buf, err = readFieldBufioU32(r, buf); err != nil {
			break
		}
	}
}

func readFieldBufioU16(r *bufio.Reader, buf []byte) ([]byte, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return buf, err
	}
	n := int(*(*uint16)(unsafe.Pointer(&lenBuf[0])))
	if n == math.MaxUint16 {
		return buf, nil
	}
	if cap(buf) >= n {
		buf = buf[:n]
	} else {
		buf = make([]byte, n)
	}
	if _, err := io.ReadFull(r, buf); err != nil {
		return buf, err
	}
	return buf, nil
}

func readFieldBufioU32(r *bufio.Reader, buf []byte) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return buf, err
	}
	n := int(*(*uint32)(unsafe.Pointer(&lenBuf[0])))
	if cap(buf) >= n {
		buf = buf[:n]
	} else {
		buf = make([]byte, n)
	}
	if _, err := io.ReadFull(r, buf); err != nil {
		return buf, err
	}
	return buf, nil
}
