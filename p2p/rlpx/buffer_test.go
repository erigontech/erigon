// Copyright 2021 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package rlpx

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadBufferReset(t *testing.T) {
	reader := bytes.NewReader(hexutil.MustDecode("0x010202030303040505"))
	var b readBuffer

	s1, _ := b.read(reader, 1)
	s2, _ := b.read(reader, 2)
	s3, _ := b.read(reader, 3)

	assert.Equal(t, []byte{1}, s1)
	assert.Equal(t, []byte{2, 2}, s2)
	assert.Equal(t, []byte{3, 3, 3}, s3)

	b.reset()

	s4, _ := b.read(reader, 1)
	s5, _ := b.read(reader, 2)

	assert.Equal(t, []byte{4}, s4)
	assert.Equal(t, []byte{5, 5}, s5)

	s6, err := b.read(reader, 2)

	assert.EqualError(t, err, "EOF")
	assert.Nil(t, s6)
}

func TestReadBufferTruncated(t *testing.T) {
	for _, size := range []int{0, 1, 4096, 8192} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var b readBuffer
			data, err := b.read(bytes.NewReader(make([]byte, size)), 1<<20)
			wantErr := io.ErrUnexpectedEOF
			if size == 0 {
				wantErr = io.EOF
			}
			assert.ErrorIs(t, err, wantErr)
			assert.Nil(t, data)
		})
	}
}

func TestReadBufferGrowsWithInput(t *testing.T) {
	input := bytes.Repeat([]byte{1, 2, 3, 4}, 256*1024)
	r := bytes.NewReader(input)
	var b readBuffer
	data, err := b.read(readerFunc(func(p []byte) (int, error) {
		received := len(input) - r.Len()
		require.LessOrEqual(t, cap(b.data), 4*max(4096, received))
		return r.Read(p[:min(len(p), 127)])
	}), len(input))
	assert.NoError(t, err)
	assert.Equal(t, input, data)
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
