package trie

import (
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

	compressBuf := pool.GetBuffer(64)
	defer pool.PutBuffer(compressBuf)

	decompressBuf := pool.GetBuffer(64)
	defer pool.PutBuffer(decompressBuf)

	for _, tc := range cases {
		compressBuf.Reset()
		decompressBuf.Reset()

		in := common.Hex2Bytes(tc.in)
		err := CompressNibbles(in, &compressBuf.B)
		compressed := compressBuf.Bytes()
		assert.Nil(t, err)
		msg := "On: " + tc.in + " Len: " + strconv.Itoa(len(compressed))
		assert.Equal(t, tc.expect, fmt.Sprintf("%x", compressed), msg)
		err = DecompressNibbles(compressed, &decompressBuf.B)
		assert.Nil(t, err)
		decompressed := decompressBuf.Bytes()
		assert.Equal(t, tc.in, fmt.Sprintf("%x", decompressed), msg)
	}
}

/*
BenchmarkCompImplOnly/buf,_io.ByteWriter-12         	22753586	        55.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompImplOnly/[]byte-12                     	82462410	        17.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompImplOnly/*[]byte-12                    	64536444	        16.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompImplOnly/*[]byte_+_append()-12         	76377957	        15.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompImplOnly/var_[64]byte-12               	72647452	        15.9 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkCompImplOnly(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")
	pool.PutBuffer(pool.GetBuffer(64))

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
			buf.B = buf.B[:0]
			l := Compress3(in, buf.B)
			k := buf.B[:l]
			_ = k
		}
	})

	b.Run("*[]byte", func(b *testing.B) {
		buf := pool.GetBuffer(64)
		defer pool.PutBuffer(buf)
		for i := 0; i < b.N; i++ {
			buf.B = buf.B[:0]
			Compress3a(in, &buf.B)
		}
	})

	//b.Run("[]byte + append()", func(b *testing.B) {
	//	buf := pool.GetBuffer(64)
	//	defer pool.PutBuffer(buf)
	//	for i := 0; i < b.N; i++ {
	//		l := Compress4a(in, buf.B)
	//		k := buf.B[:l]
	//		_ = k
	//	}
	//})

	b.Run("*[]byte + append()", func(b *testing.B) {
		buf := pool.GetBuffer(64)
		defer pool.PutBuffer(buf)
		for i := 0; i < b.N; i++ {
			buf.B = buf.B[:0]
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
BenchmarkCompWithAlloc/Buf_Pool_as_io.ByteWriter-12 	12046102	       105 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompWithAlloc/Buf_Pool_as_bytes-12         	30445060	        38.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompWithAlloc/make_outside-12              	35515688	        33.0 ns/op	      16 B/op	       1 allocs/op
BenchmarkCompWithAlloc/var_[64]byte-12              	77704927	        17.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkCompWithAlloc/make_inside_func-12          	35306130	        32.6 ns/op	      16 B/op	       1 allocs/op
*/
func BenchmarkCompWithAlloc(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")
	pool.PutBuffer(pool.GetBuffer(64))

	b.Run("Buf Pool as io.ByteWriter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(64)
			buf.Reset()
			if err := Compress2(in, buf); err != nil {
				panic(err)
			}
			pool.PutBuffer(buf)
		}
	})

	b.Run("Buf Pool as bytes", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := pool.GetBuffer(64)
			buf.Reset()
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
	var bi int
	out = out[:len(nibbles)/2]
	for i := 0; i < len(nibbles); bi, i = bi+1, i+2 {
		out[bi] = nibbles[i]<<4 | nibbles[i+1]
	}
	return bi
}

func Compress3a(nibbles []byte, out *[]byte) {
	k := (*out)[:len(nibbles)/2]
	var bi int
	for i := 0; i < len(nibbles); bi, i = bi+1, i+2 {
		k[bi] = nibbles[i]<<4 | nibbles[i+1]
	}
	*out = k
}

func Compress4(nibbles []byte, out *[]byte) {
	k := (*out)[:0]
	for j, i := 0, 0; i < len(nibbles); j, i = i+1, i+2 {
		k = append(k, nibbles[i]<<4|nibbles[j])
	}
	*out = k
}

// this version disqualified because linter doesn't like it
//func Compress4a(nibbles []byte, out []byte) int {
//	out = out[:0]
//	for i := 0; i < len(nibbles); i += 2 {
//		out = append(out, nibbles[i]<<4|nibbles[i+1])
//	}
//	return len(nibbles) / 2
//}

func Compress5(nibbles []byte, out *[64]byte) (outLength int) {
	var bi int
	for i := 0; i < len(nibbles); bi, i = bi+1, i+2 {
		out[bi] = (nibbles[i] << 4) | nibbles[i+1]
	}
	return bi
}

func Compress6(nibbles []byte) []byte {
	out := make([]byte, len(nibbles)/2)
	for bi, i := 0, 0; i < len(nibbles); bi, i = bi+1, i+2 {
		out[bi] = nibbles[i]<<4 | nibbles[i+1]
	}
	return out
}
