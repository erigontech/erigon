package trie

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/stretchr/testify/assert"
)

func TestCompressNibbles(t *testing.T) {
	cases := []struct {
		in     string
		expect string
	}{
		{in: "0000", expect: "00"},
		{in: "0102", expect: "12"},
		{in: "0102030405060708090f", expect: "123456789f"},
		{in: "0f000101", expect: "f011"},
		{in: "", expect: ""},
	}

	compressBuf := &bytes.Buffer{}
	decompressBuf := &bytes.Buffer{}
	for _, tc := range cases {
		in := common.Hex2Bytes(tc.in)
		err := CompressNibbles(in, compressBuf)
		compressed := compressBuf.Bytes()
		assert.Nil(t, err)
		msg := "On: " + tc.in + " Len: " + strconv.Itoa(len(compressed))
		assert.Equal(t, tc.expect, fmt.Sprintf("%x", compressed), msg)
		compressBuf.Reset()

		err = DecompressNibbles(compressed, decompressBuf)
		assert.Nil(t, err)
		decompressed := decompressBuf.Bytes()
		assert.Equal(t, tc.in, fmt.Sprintf("%x", decompressed), msg)
		decompressBuf.Reset()
	}
}

/*
- Fastest version: 18ns

Compress4(nibbles []byte, out *[]byte) {
	*out = (*out)[:0]
	...
	*out = append(*out, b)

BenchmarkCompImplOnly/buf,_io.ByteWriter-12         	22350472	        54.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompImplOnly/[]byte-12                     	64453431	        18.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompImplOnly/*[]byte-12                    	59190687	        20.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompImplOnly/[]byte_+_append()-12          	74415922	        15.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompImplOnly/*[]byte_+_append()-12         	65587525	        15.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompImplOnly/var_[64]byte-12               	65292729	        18.0 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkCompImplOnly(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")

	b.Run("buf, io.ByteWriter", func(b *testing.B) {
		buf := pool.GetBuffer(64)
		defer pool.PutBuffer(buf)
		for i := 0; i < b.N; i++ {
			buf.B = buf.B[:0]
			if err := Compress2(in, buf); err != nil {
				panic(err)
			}
			_ = buf.B
		}
	})
	b.Run("[]byte", func(b *testing.B) {
		buf := pool.GetBuffer(64)
		defer pool.PutBuffer(buf)
		for i := 0; i < b.N; i++ {
			l := Compress3(in, buf.B)
			k := buf.B[:l]
			_ = k
		}
	})

	b.Run("*[]byte", func(b *testing.B) {
		buf := pool.GetBuffer(64)
		defer pool.PutBuffer(buf)
		for i := 0; i < b.N; i++ {
			Compress3a(in, &buf.B)
		}
	})

	b.Run("[]byte + append()", func(b *testing.B) {
		buf := pool.GetBuffer(64)
		defer pool.PutBuffer(buf)
		for i := 0; i < b.N; i++ {
			l := Compress4a(in, buf.B)
			k := buf.B[:l]
			_ = k
		}
	})

	b.Run("*[]byte + append()", func(b *testing.B) {
		buf := pool.GetBuffer(64)
		defer pool.PutBuffer(buf)
		for i := 0; i < b.N; i++ {
			Compress4(in, &buf.B)
		}
	})
	b.Run("var [64]byte", func(b *testing.B) {
		var out [64]byte
		for i := 0; i < b.N; i++ {
			l := Compress5(in, &out)
			k := out[:l]
			_ = k
		}
	})
}

/*
- Fastest version: 25ns, doesn’t produce garbage
Only this version and versions with Pool - don't produce garbage

var out [64]byte
l := CompressNibbles5(in, &out)
k := out[:l]

func CompressNibbles5(nibbles []byte, out *[64]byte) (outLength int)

BenchmarkCompWithAlloc/Buf_Pool_as_bytes-12         	30272492	        37.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompWithAlloc/make_outside-12              	33918350	        32.6 ns/op	      16 B/op	       1 allocs/op
BenchmarkCompWithAlloc/var_[64]byte-12              	65102901	        19.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompWithAlloc/make_inside_func-12          	33064528	        37.2 ns/op	      16 B/op	       1 allocs/op
*/
func BenchmarkCompWithAlloc(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")

	b.Run("Buf Pool as bytes", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(64)
			Compress4(in, &buf.B)
			pool.PutBuffer(buf)
		}
	})

	b.Run("make outside", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			k := make([]byte, len(in)/2)
			Compress4(in, &k)
		}
	})

	b.Run("var [64]byte", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var out [64]byte
			l := Compress5(in, &out)
			k := out[:l]
			_ = k
		}
	})
	b.Run("make inside func", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			k := Compress6(in)
			_ = k
		}
	})
}

func Compress2(nibbles []byte, out io.ByteWriter) error {
	for i := 0; i < len(nibbles); i += 2 {
		if err := out.WriteByte(nibbles[i]<<4 | nibbles[i+1]); err != nil {
			return err
		}
	}

	return nil
}

func Compress3(nibbles []byte, out []byte) int {
	for i := 0; i < len(nibbles); i += 2 {
		out[i/2] = nibbles[i]<<4 | nibbles[i+1]
	}
	return len(nibbles) / 2
}

func Compress3a(nibbles []byte, out *[]byte) {
	*out = (*out)[:len(nibbles)/2]
	for i := 0; i < len(nibbles); i += 2 {
		(*out)[i/2] = nibbles[i]<<4 | nibbles[i+1]
	}
}

func Compress4(nibbles []byte, out *[]byte) {
	k := (*out)[:0]
	for i := 0; i < len(nibbles); i += 2 {
		k = append(k, nibbles[i]<<4|nibbles[i+1])
	}
	*out = k
}

func Compress4a(nibbles []byte, out []byte) int {
	out = out[:0]
	for i := 0; i < len(nibbles); i += 2 {
		out = append(out, nibbles[i]<<4|nibbles[i+1])
	}
	return len(nibbles) / 2
}

func Compress5(nibbles []byte, out *[64]byte) (outLength int) {
	for i := 0; i < len(nibbles); i += 2 {
		out[i/2] = (nibbles[i] << 4) | nibbles[i+1]
	}
	return len(nibbles) / 2
}

func Compress6(nibbles []byte) []byte {
	out := make([]byte, len(nibbles)/2)
	for i := 0; i < len(nibbles); i += 2 {
		out[i/2] = nibbles[i]<<4 | nibbles[i+1]
	}
	return out
}
