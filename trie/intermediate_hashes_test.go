package trie

import (
	"bytes"
	"errors"
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
- Fastest version: 25ns, doesn’t produce garbage
Only this version and versions with Pool - don't produce garbage

var out [64]byte
l := CompressNibbles5(in, &out)
k := out[:l]

func CompressNibbles5(nibbles []byte, out *[64]byte) (outLength int)


BenchmarkComp2Buf-12            	14348829	        79.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkComp3Buf-12            	26500005	        45.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkComp3MakeOutside-12    	31024833	        36.3 ns/op	      16 B/op	       1 allocs/op
BenchmarkComp4BufAppend-12      	23995503	        48.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkComp5-12               	51967686	        22.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkComp6-12               	32402132	        35.8 ns/op	      16 B/op	       1 allocs/op
*/
func BenchmarkComp2Buf(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.GetBuffer(64)
		buf.Reset()
		Compress2(in, buf)
		k := buf.Bytes()
		_ = k
		pool.PutBuffer(buf)
	}
}

func BenchmarkComp3Buf(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.GetBuffer(64)
		l := Compress3(in, buf.B)
		k := buf.B[:l]
		_ = k
		pool.PutBuffer(buf)
	}
}

func BenchmarkComp3MakeOutside(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := make([]byte, len(in)/2)
		Compress3(in, k)
	}
}

func BenchmarkComp4BufAppend(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.GetBuffer(64)
		k := buf.B[:0]
		Compress4(in, &k)
		pool.PutBuffer(buf)
	}
}

func BenchmarkComp5(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out [64]byte
		l := Compress5(in, &out)
		k := out[:l]
		_ = k
	}
}

func BenchmarkComp6(b *testing.B) {
	in := common.Hex2Bytes("0102030405060708090f0102030405060708090f0102030405060708090f")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := Compress6(in)
		_ = k
	}
}

func Compress2(nibbles []byte, out io.ByteWriter) error {
	if len(nibbles)%2 != 0 {
		return errors.New("this method supports only arrays of even nibbles")
	}

	var b byte
	for i := 0; i < len(nibbles); i++ {
		b = (nibbles[i] << 4) & 0xF0
		i++
		b |= nibbles[i] & 0x0F

		if err := out.WriteByte(b); err != nil {
			return err
		}
	}

	return nil
}

func Compress3(nibbles []byte, out []byte) int {
	if len(nibbles)%2 != 0 {
		panic(errors.New("this method supports only arrays of even nibbles"))
	}

	for i := 0; i < len(nibbles); i++ {
		k := i / 2
		out[k] = (nibbles[i] << 4) & 0xF0
		i++
		out[k] |= nibbles[i] & 0x0F
	}
	return len(nibbles) / 2
}

func Compress4(nibbles []byte, out *[]byte) {
	if len(nibbles)%2 != 0 {
		panic(errors.New("this method supports only arrays of even nibbles"))
	}

	var b byte
	for i := 0; i < len(nibbles); i++ {
		b = (nibbles[i] << 4) & 0xF0
		i++
		b |= nibbles[i] & 0x0F
		*out = append(*out, b)
	}
}

func Compress5(nibbles []byte, out *[64]byte) (outLength int) {
	if len(nibbles)%2 != 0 {
		panic(errors.New("this method supports only arrays of even nibbles"))
	}

	for i := 0; i < len(nibbles); i++ {
		k := i / 2
		out[k] = (nibbles[i] << 4) & 0xF0
		i++
		out[k] |= nibbles[i] & 0x0F
	}
	return len(nibbles) / 2
}

func Compress6(nibbles []byte) []byte {
	if len(nibbles)%2 != 0 {
		panic(errors.New("this method supports only arrays of even nibbles"))
	}
	out := make([]byte, len(nibbles)/2)
	for i := 0; i < len(nibbles); i++ {
		k := i / 2
		out[k] = (nibbles[i] << 4) & 0xF0
		i++
		out[k] |= nibbles[i] & 0x0F
	}
	return out
}
