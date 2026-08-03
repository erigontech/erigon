// Copyright 2024 The Erigon Authors
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

package base_encoding

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func BenchmarkWriteRabbits(b *testing.B) {
	// sparse list with many non-contiguous runs -> many scalar writes
	list := make([]uint64, 0, 100000)
	for i := range uint64(100000) {
		list = append(list, i*3)
	}
	var w bytes.Buffer
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.Reset()
		if err := WriteRabbits(list, &w); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRabbit(t *testing.T) {
	list := []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17, 23, 90}
	var w bytes.Buffer
	if err := WriteRabbits(list, &w); err != nil {
		t.Fatal(err)
	}
	var out []uint64
	out, err := ReadRabbits(out, &w)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, list, out)
}

// [0,1,2,5] encodes as: element count 4, active run of 3 (0..2), gap of 2, active run of 1 (5).
// Hand-computed from the format rather than from either codec, so it pins the wire bytes
// independently of a writer/reader round trip.
var (
	goldenRabbitList    = []uint64{0, 1, 2, 5}
	goldenRabbitScalars = []uint64{4, 3, 2, 1}
)

func TestRabbitWriteWireFormat(t *testing.T) {
	var w bytes.Buffer
	require.NoError(t, WriteRabbits(goldenRabbitList, &w))
	require.Equal(t, leScalars(goldenRabbitScalars...), zstdDecompress(t, w.Bytes()))
}

func TestRabbitReadWireFormat(t *testing.T) {
	out, err := ReadRabbits(nil, zstdCompress(t, leScalars(goldenRabbitScalars...)))
	require.NoError(t, err)
	require.Equal(t, goldenRabbitList, out)
}

func TestRabbitEmpty(t *testing.T) {
	var w bytes.Buffer
	require.NoError(t, WriteRabbits(nil, &w))
	out, err := ReadRabbits(nil, &w)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestRabbitReadMalformed(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		wantErrIs error
		wantErr   string
	}{
		{
			name:      "header scalar truncated",
			payload:   leScalars(4)[:4],
			wantErrIs: io.ErrUnexpectedEOF,
		},
		{
			name:      "run scalar truncated",
			payload:   leScalars(4, 3)[:12],
			wantErrIs: io.ErrUnexpectedEOF,
		},
		{
			name:    "header length overflows int",
			payload: leScalars(math.MaxUint64),
			wantErr: "overflows int",
		},
		{
			name:    "fewer elements decoded than header declares",
			payload: leScalars(4, 3),
			wantErr: "decoded 3 elements, header declared 4",
		},
		{
			name:    "active run exceeds remaining declared elements",
			payload: leScalars(2, 5),
			wantErr: "exceeds remaining",
		},
		{
			name:    "gap overflows the index space",
			payload: leScalars(3, 2, math.MaxUint64),
			wantErr: "index overflow",
		},
		{
			name:    "stream ends after the header",
			payload: leScalars(0),
			wantErr: "trailing contiguous-run count",
		},
		{
			name:    "stream ends on a gap run",
			payload: leScalars(4, 3, 2, 1, 7),
			wantErr: "trailing data",
		},
		{
			name:    "trailing zero-length runs after the final contiguous run",
			payload: leScalars(4, 3, 2, 1, 0, 0),
			wantErr: "trailing data",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ReadRabbits(nil, zstdCompress(t, tt.payload))
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func leScalars(vs ...uint64) []byte {
	b := make([]byte, 0, len(vs)*8)
	for _, v := range vs {
		b = binary.LittleEndian.AppendUint64(b, v)
	}
	return b
}

func zstdCompress(t *testing.T, payload []byte) io.Reader {
	t.Helper()
	compressor := compressorPool.Get().(*zstd.Encoder)
	defer putComp(compressor)
	var b bytes.Buffer
	compressor.Reset(&b)
	_, err := compressor.Write(payload)
	require.NoError(t, err)
	require.NoError(t, compressor.Close())
	return &b
}

func zstdDecompress(t *testing.T, frame []byte) []byte {
	t.Helper()
	dec, err := zstd.NewReader(bytes.NewReader(frame))
	require.NoError(t, err)
	defer dec.Close()
	payload, err := io.ReadAll(dec)
	require.NoError(t, err)
	return payload
}
