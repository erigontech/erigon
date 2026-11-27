// Copyright 2026 The Erigon Authors
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

package raw

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common/clonable"
)

// fakeField is a dynamic SSZ object whose declared size can disagree with its data.
type fakeField struct {
	data     []byte
	declared int
}

func newFakeField(data ...byte) *fakeField { return &fakeField{data: data, declared: len(data)} }

func (f *fakeField) EncodeSSZ(dst []byte) ([]byte, error) { return append(dst, f.data...), nil }
func (f *fakeField) EncodingSizeSSZ() int                 { return f.declared }
func (f *fakeField) DecodeSSZ(buf []byte, _ int) error    { f.data = bytes.Clone(buf); return nil }
func (f *fakeField) Static() bool                         { return false }
func (f *fakeField) Clone() clonable.Clonable             { return &fakeField{} }

// fakeStream is a dynamic SSZ object that reads and writes through the stream interface.
type fakeStream struct {
	fakeField
	writeExtra int
	writeLess  int
	readLess   int
	readExtra  int
}

func (f *fakeStream) EncodeSSZTo(w io.Writer) error {
	out := append(bytes.Clone(f.data), make([]byte, f.writeExtra)...)
	_, err := w.Write(out[:len(out)-f.writeLess])
	return err
}

func (f *fakeStream) DecodeSSZFrom(r io.Reader, n int) error {
	f.data = make([]byte, n-f.readLess+f.readExtra)
	_, err := io.ReadFull(r, f.data)
	return err
}

func encodeFake(t *testing.T, schema ...any) []byte {
	t.Helper()
	out, err := ssz2.MarshalSSZ(nil, schema...)
	require.NoError(t, err)
	return out
}

func TestMarshalSSZToMatchesMarshalSSZ(t *testing.T) {
	counter := uint64(0x0102030405060708)
	fixed := []byte{9, 8, 7}
	schema := []any{&counter, fixed, newFakeField(1, 2, 3), newFakeField(), &fakeStream{fakeField: *newFakeField(4, 5)}}
	want := encodeFake(t, schema...)

	var got bytes.Buffer
	n, err := marshalSSZTo(&got, schema...)
	require.NoError(t, err)
	require.Equal(t, want, got.Bytes())
	require.Equal(t, len(want), n)
}

func TestMarshalSSZToRejectsSizeMismatch(t *testing.T) {
	for name, field := range map[string]any{
		"declared size larger":   &fakeField{data: []byte{1, 2, 3}, declared: 4},
		"declared size smaller":  &fakeField{data: []byte{1, 2, 3}, declared: 2},
		"stream writes more":     &fakeStream{fakeField: *newFakeField(1, 2, 3), writeExtra: 1},
		"stream writes less":     &fakeStream{fakeField: *newFakeField(1, 2, 3), writeLess: 1},
		"stream writes nothing":  &fakeStream{fakeField: *newFakeField(1, 2, 3), writeLess: 3},
		"stream writes too much": &fakeStream{fakeField: *newFakeField(1), writeExtra: 100},
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			_, err := marshalSSZTo(&out, newFakeField(7), field, newFakeField(8))
			require.Error(t, err)
		})
	}
}

type errWriter struct {
	okBytes int
	err     error
}

func (w *errWriter) Write(p []byte) (int, error) {
	if len(p) > w.okBytes {
		return 0, w.err
	}
	w.okBytes -= len(p)
	return len(p), nil
}

func TestMarshalSSZToReturnsWriterError(t *testing.T) {
	sentinel := errors.New("disk full")
	schema := []any{newFakeField(1, 2, 3), &fakeStream{fakeField: *newFakeField(4, 5)}}
	for _, okBytes := range []int{0, 8, 11} {
		_, err := marshalSSZTo(&errWriter{okBytes: okBytes, err: sentinel}, schema...)
		require.ErrorIs(t, err, sentinel, "okBytes=%d", okBytes)
	}
}

type panicEncoder struct{ fakeField }

func (*panicEncoder) EncodeSSZ([]byte) ([]byte, error) { panic("encoder boom") }

type panicWriter struct{}

func (panicWriter) Write([]byte) (int, error) { panic("writer boom") }

func TestMarshalSSZToTurnsEncoderPanicIntoError(t *testing.T) {
	field := &panicEncoder{fakeField{data: []byte{1}, declared: 1}}
	_, err := marshalSSZTo(io.Discard, newFakeField(1), field)
	require.ErrorContains(t, err, "encoder boom")
}

func TestMarshalSSZToPropagatesWriterPanic(t *testing.T) {
	require.PanicsWithValue(t, "writer boom", func() {
		_, _ = marshalSSZTo(panicWriter{}, newFakeField(1), &fakeStream{fakeField: *newFakeField(2)})
	})
}

func TestMarshalSSZToRejectsUnknownSchemaKind(t *testing.T) {
	_, err := marshalSSZTo(io.Discard, uint32(1))
	require.Error(t, err)
}

func decodeFake(buf []byte, length int, schema ...any) error {
	return unmarshalSSZFrom(bytes.NewReader(buf), length, 0, schema...)
}

func TestUnmarshalSSZFromRoundTrip(t *testing.T) {
	counter := uint64(0x0102030405060708)
	fixed := []byte{9, 8, 7}
	a, b, c := newFakeField(1, 2, 3), newFakeField(), &fakeStream{fakeField: *newFakeField(4, 5)}
	buf := encodeFake(t, &counter, fixed, a, b, c)

	var gotCounter uint64
	gotFixed := make([]byte, 3)
	ga, gb, gc := newFakeField(), newFakeField(), &fakeStream{}
	require.NoError(t, decodeFake(buf, len(buf), &gotCounter, gotFixed, ga, gb, gc))
	require.Equal(t, counter, gotCounter)
	require.Equal(t, fixed, gotFixed)
	require.Equal(t, a.data, ga.data)
	require.Empty(t, gb.data)
	require.Equal(t, c.data, gc.data)
}

func TestUnmarshalSSZFromRejectsMalformedInput(t *testing.T) {
	newSchema := func() []any {
		return []any{new(uint64), newFakeField(), newFakeField(), &fakeStream{}}
	}
	one, two, three := uint64(1), newFakeField(1, 2), newFakeField(3)
	valid := encodeFake(t, &one, two, three, &fakeStream{fakeField: *newFakeField(4, 5, 6)})
	fixedSize := 8 + 3*4
	setOffset := func(buf []byte, i int, v uint32) []byte {
		buf = bytes.Clone(buf)
		binary.LittleEndian.PutUint32(buf[8+4*i:], v)
		return buf
	}
	require.NoError(t, decodeFake(valid, len(valid), newSchema()...))

	cases := map[string]struct {
		buf    []byte
		length int
	}{
		"truncated body":          {valid[:len(valid)-1], len(valid)},
		"truncated fixed part":    {valid[:fixedSize-1], len(valid)},
		"length below fixed part": {valid[:fixedSize-1], fixedSize - 1},
		"gap after fixed part":    {setOffset(valid, 0, uint32(fixedSize+1)), len(valid)},
		"first offset in fixed":   {setOffset(valid, 0, uint32(fixedSize-1)), len(valid)},
		"decreasing offsets":      {setOffset(valid, 2, uint32(fixedSize)), len(valid)},
		"offset past length":      {setOffset(valid, 2, uint32(len(valid)+1)), len(valid)},
		"huge offset":             {setOffset(valid, 1, 0xffffffff), len(valid)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, decodeFake(tc.buf, tc.length, newSchema()...))
		})
	}
}

func TestUnmarshalSSZFromRejectsFixedOnlyTrailingBytes(t *testing.T) {
	buf := append(encodeFake(t, new(uint64)), 0)
	require.Error(t, decodeFake(buf, len(buf), new(uint64)))
}

func TestUnmarshalSSZFromRequiresExactFieldConsumption(t *testing.T) {
	counter := uint64(1)
	for name, tc := range map[string]struct {
		field   *fakeStream
		wantErr error
	}{
		"under-read": {&fakeStream{fakeField: *newFakeField(1, 2, 3, 4), readLess: 1}, nil},
		"over-read":  {&fakeStream{fakeField: *newFakeField(1, 2, 3, 4), readExtra: 1}, io.ErrUnexpectedEOF},
	} {
		t.Run(name, func(t *testing.T) {
			buf := encodeFake(t, &counter, &fakeStream{fakeField: *newFakeField(1, 2, 3, 4)}, newFakeField(5))
			err := decodeFake(buf, len(buf), new(uint64), tc.field, newFakeField())
			require.Error(t, err)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			}
		})
	}
}

func TestUnmarshalSSZFromLeavesNoTrailingInput(t *testing.T) {
	counter := uint64(1)
	buf := encodeFake(t, &counter, newFakeField(1, 2))
	r := bytes.NewReader(append(bytes.Clone(buf), 0xaa))
	require.NoError(t, unmarshalSSZFrom(r, len(buf), 0, new(uint64), newFakeField()))
	require.Equal(t, 1, r.Len())
}

func TestBeaconStateStreamMatchesSSZ(t *testing.T) {
	for version := clparams.Phase0Version; version <= clparams.GloasVersion; version++ {
		t.Run(version.String(), func(t *testing.T) {
			state := newTestState(version)
			want, err := state.EncodeSSZ(nil)
			require.NoError(t, err)

			var got bytes.Buffer
			require.NoError(t, state.EncodeSSZTo(&got))
			require.Equal(t, want, got.Bytes())

			decoded := New(&clparams.MainnetBeaconConfig)
			require.NoError(t, decoded.DecodeCanonicalSSZFrom(bytes.NewReader(want), len(want), int(version)))
			again, err := decoded.EncodeSSZ(nil)
			require.NoError(t, err)
			require.Equal(t, want, again)
			wantRoot, err := state.HashSSZ()
			require.NoError(t, err)
			gotRoot, err := decoded.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, wantRoot, gotRoot)

			require.Error(t, New(&clparams.MainnetBeaconConfig).DecodeCanonicalSSZFrom(bytes.NewReader(want[:len(want)-1]), len(want), int(version)))
		})
	}
}
