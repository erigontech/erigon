// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package remote

import (
	"bytes"
	"strconv"
	"testing"
)

func BenchmarkSerialize(b *testing.B) {
	k := []byte(strconv.Itoa(1))
	v := []byte(strconv.Itoa(1))
	var r, w bytes.Buffer

	encoder := newEncoder(&w)
	defer returnEncoderToPool(encoder)
	// output buffer to receive the result of the command
	decoder := newDecoder(&r)
	defer returnDecoderToPool(decoder)

	b.ResetTimer()
	b.Run("encodeKeyValue()", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = encoder.Encode(ResponseOk)
			_ = encodeKeyValue(encoder, &k, &v)
		}
	})
	b.Run("encoder.Encode(&k)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = encoder.Encode(ResponseOk)
			_ = encoder.Encode(&k)
			_ = encoder.Encode(&v)
		}
	})
	b.Run("encoder.Encode(k)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = encoder.Encode(ResponseOk)
			_ = encoder.Encode(k)
			_ = encoder.Encode(v)
		}
	})
	b.Run("encoder.MustEncode(&k)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoder.MustEncode(ResponseOk)
			encoder.MustEncode(&k)
			encoder.MustEncode(&v)
		}
	})
	b.Run("encoder.MustEncode(k)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoder.MustEncode(ResponseOk)
			encoder.MustEncode(k)
			encoder.MustEncode(v)
		}
	})
	b.Run("Encode(struct)", func(b *testing.B) {
		type kv struct {
			K []byte
			V []byte
		}
		type a struct {
			C ResponseCode
			kv
		}
		x := &a{}

		for i := 0; i < b.N; i++ {
			x.C = ResponseOk
			x.K = k
			x.V = v
			encoder.MustEncode(x)
		}
	})

	b.Run("10K Encode(&k, &v)", func(b *testing.B) {
		M := 10 * 1000
		for i := 0; i < b.N; i++ {
			encoder.MustEncode(ResponseOk)
			for i := 0; i < M; i++ {
				encoder.MustEncode(&k)
				encoder.MustEncode(&v)
			}
		}
	})
	b.Run("Encode([10K]k, [10K]v)", func(b *testing.B) {
		M := 10 * 1000
		for i := 0; i < b.N; i++ {
			keys := make([][]byte, M)
			values := make([][]byte, M)
			for i := 0; i < M; i++ {
				keys[i] = k
				values[i] = v
			}
			encoder.MustEncode(ResponseOk)
			encoder.MustEncode(keys)
			encoder.MustEncode(values)
		}
	})
}
