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

package ssz_snappy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

var snappyStreamIdentifier = []byte{0xff, 0x06, 0x00, 0x00, 's', 'N', 'a', 'P', 'p', 'Y'}

type countingReader struct {
	r     *bytes.Reader
	bytes int
}

type terminalErrorReader struct {
	r   *bytes.Reader
	err error
}

func (r *terminalErrorReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n == 0 {
		return 0, r.err
	}
	return n, err
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.bytes += n
	return n, err
}

func TestDecodeAndReadNoForkDigestMaxPayloadSize(t *testing.T) {
	for _, tc := range []struct {
		name      string
		size      uint64
		wantError bool
	}{
		{name: "at limit", size: clparams.MaxPayloadSize},
		{name: "over limit", size: clparams.MaxPayloadSize + 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payloadBytes := make([]byte, tc.size)
			if !tc.wantError {
				_, err := rand.New(rand.NewSource(1)).Read(payloadBytes)
				require.NoError(t, err)
			}
			payload := solid.NewByteListSSZ(tc.size)
			require.NoError(t, payload.SetBytes(payloadBytes))
			var encoded bytes.Buffer
			require.NoError(t, EncodeAndWrite(&encoded, payload))

			decoded := solid.NewByteListSSZ(tc.size)
			err := DecodeAndReadNoForkDigest(bytes.NewReader(encoded.Bytes()), decoded, clparams.Phase0Version)
			if tc.wantError {
				require.ErrorContains(t, err, "payload too big")
				return
			}
			require.NoError(t, err)
			require.Equal(t, int(tc.size), decoded.Len())
			require.Equal(t, payloadBytes, decoded.Bytes())
		})
	}
}

func TestDecodeAndReadNoForkDigestExactRejectsTrailingFrames(t *testing.T) {
	for _, test := range []struct {
		name  string
		frame []byte
	}{
		{name: "stream identifier", frame: snappyStreamIdentifier},
		{name: "skippable frame", frame: []byte{0x80, 0x01, 0x00, 0x00, 0x00}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var encoded bytes.Buffer
			require.NoError(t, EncodeAndWrite(&encoded, &cltypes.Ping{Id: 1}))
			encoded.Write(test.frame)

			err := DecodeAndReadNoForkDigestExact(bytes.NewReader(encoded.Bytes()), &cltypes.Ping{}, clparams.Phase0Version, 8)
			require.Error(t, err)
		})
	}
}

func TestDecodeAndReadNoForkDigestExactBoundsCompressedInput(t *testing.T) {
	var encoded bytes.Buffer
	require.NoError(t, EncodeAndWrite(&encoded, &cltypes.Ping{Id: 1}))
	for range 10 {
		encoded.Write(snappyStreamIdentifier)
	}

	reader := &countingReader{r: bytes.NewReader(encoded.Bytes())}
	err := DecodeAndReadNoForkDigestExact(reader, &cltypes.Ping{}, clparams.Phase0Version, 8)
	require.Error(t, err)
	require.LessOrEqual(t, reader.bytes, 1+32+8+8/6)
}

func TestDecodeAndReadNoForkDigestBoundsCompressedInput(t *testing.T) {
	const payloadSize = 8
	maxCompressedSize := snappy.MaxEncodedLen(payloadSize)

	var encoded bytes.Buffer
	require.NoError(t, EncodeAndWrite(&encoded, &cltypes.Ping{Id: 1}))
	prefix, body := encoded.Bytes()[:1], encoded.Bytes()[1:]
	trailer := []byte{0xaa, 0xbb}
	for _, tc := range []struct {
		name      string
		extraByte int
		wantError bool
	}{
		{name: "at limit"},
		{name: "over limit", extraByte: 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			padding := make([]byte, maxCompressedSize-len(body)+tc.extraByte)
			padding[0] = 0xfe
			padding[1] = byte(len(padding) - 4)
			framed := append(append(append(append([]byte{}, prefix...), body[:len(snappyStreamIdentifier)]...), padding...), body[len(snappyStreamIdentifier):]...)
			require.Len(t, framed, len(prefix)+maxCompressedSize+tc.extraByte)
			framed = append(framed, trailer...)

			reader := &countingReader{r: bytes.NewReader(framed)}
			var ping cltypes.Ping
			err := DecodeAndReadNoForkDigest(reader, &ping, clparams.Phase0Version)
			if tc.wantError {
				require.ErrorIs(t, err, errCompressedPayloadLimit)
				require.LessOrEqual(t, reader.bytes, 1+maxCompressedSize)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(1), ping.Id)
			require.Equal(t, len(prefix)+maxCompressedSize, reader.bytes)
			require.Equal(t, len(trailer), reader.r.Len())
		})
	}
}

func TestDecodeAndReadNoForkDigestExactProbesEndAtCompressedLimit(t *testing.T) {
	const payloadSize = 8
	const maxCompressedSize = 32 + payloadSize + payloadSize/6

	var encoded bytes.Buffer
	require.NoError(t, EncodeAndWrite(&encoded, &cltypes.Ping{Id: 1}))
	prefix, body := encoded.Bytes()[:1], encoded.Bytes()[1:]
	require.Less(t, len(body), maxCompressedSize)
	var ping cltypes.Ping
	require.NoError(t, DecodeAndReadNoForkDigestExact(bytes.NewReader(encoded.Bytes()), &ping, clparams.Phase0Version, payloadSize))
	require.Equal(t, uint64(1), ping.Id)

	padding := make([]byte, maxCompressedSize-len(body))
	padding[0] = 0x80
	padding[1] = byte(len(padding) - 4)
	exactMax := append(append(append(append([]byte{}, prefix...), body[:len(snappyStreamIdentifier)]...), padding...), body[len(snappyStreamIdentifier):]...)
	require.Len(t, exactMax, 1+maxCompressedSize)

	exactMaxReader := &countingReader{r: bytes.NewReader(exactMax)}
	ping = cltypes.Ping{}
	require.NoError(t, DecodeAndReadNoForkDigestExact(exactMaxReader, &ping, clparams.Phase0Version, payloadSize))
	require.Equal(t, uint64(1), ping.Id)
	require.Equal(t, 1+maxCompressedSize, exactMaxReader.bytes)

	overMax := append(append([]byte{}, exactMax...), 0, 0)
	reader := &countingReader{r: bytes.NewReader(overMax)}
	require.ErrorIs(t, DecodeAndReadNoForkDigestExact(reader, &cltypes.Ping{}, clparams.Phase0Version, payloadSize), ErrTrailingData)
	require.Equal(t, 1+maxCompressedSize+1, reader.bytes)
}

func TestDecodeAndReadNoForkDigestMaxAcceptsPayloadAtCompressedLimit(t *testing.T) {
	const payloadSize = length.Hash
	maxCompressedSize := snappy.MaxEncodedLen(payloadSize)

	roots := solid.NewHashList(16)
	roots.Append(common.Hash{1})
	var encoded bytes.Buffer
	require.NoError(t, EncodeAndWrite(&encoded, roots))
	prefix, body := encoded.Bytes()[:1], encoded.Bytes()[1:]
	padding := make([]byte, maxCompressedSize-len(body))
	padding[0] = 0xfe
	padding[1] = byte(len(padding) - 4)
	exactMax := append(append(append(append([]byte{}, prefix...), body[:len(snappyStreamIdentifier)]...), padding...), body[len(snappyStreamIdentifier):]...)
	require.Len(t, exactMax, 1+maxCompressedSize)

	decoded := solid.NewHashList(16)
	require.NoError(t, DecodeAndReadNoForkDigestMax(bytes.NewReader(exactMax), decoded, clparams.Phase0Version, 16*length.Hash))
	require.Equal(t, 1, decoded.Length())
	require.Equal(t, common.Hash{1}, decoded.Get(0))

	withTrailer := append(append([]byte{}, exactMax...), 0x00)
	err := DecodeAndReadNoForkDigestMax(bytes.NewReader(withTrailer), solid.NewHashList(16), clparams.Phase0Version, 16*length.Hash)
	require.ErrorIs(t, err, ErrTrailingData)
}

func TestDecodeAndReadNoForkDigestExactPreservesTerminalReadError(t *testing.T) {
	var encoded bytes.Buffer
	require.NoError(t, EncodeAndWrite(&encoded, &cltypes.Ping{Id: 1}))

	terminalErr := errors.New("terminal read error")
	reader := &terminalErrorReader{r: bytes.NewReader(encoded.Bytes()), err: terminalErr}
	err := DecodeAndReadNoForkDigestExact(reader, &cltypes.Ping{}, clparams.Phase0Version, 8)
	require.ErrorIs(t, err, terminalErr)
}

func TestDecodeAndReadNoForkDigestExactRejectsWrappedEOF(t *testing.T) {
	var encoded bytes.Buffer
	require.NoError(t, EncodeAndWrite(&encoded, &cltypes.Ping{Id: 1}))

	terminalErr := fmt.Errorf("transport failed: %w", io.EOF)
	reader := &terminalErrorReader{r: bytes.NewReader(encoded.Bytes()), err: terminalErr}
	err := DecodeAndReadNoForkDigestExact(reader, &cltypes.Ping{}, clparams.Phase0Version, 8)
	require.ErrorIs(t, err, terminalErr)
}

func TestDecodeAndReadNoForkDigestDoesNotPreallocateDeclaredLength(t *testing.T) {
	header := binary.AppendUvarint(nil, clparams.MaxPayloadSize)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := DecodeAndReadNoForkDigest(bytes.NewReader(header), &cltypes.BeaconBlocksByRangeRequest{}, clparams.Phase0Version)
	runtime.ReadMemStats(&after)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(datasize.MB))
}

func encodeHashList(t *testing.T, count int) []byte {
	t.Helper()
	request := solid.NewHashList(count)
	for i := range count {
		request.Append(common.Hash{byte(i + 1)})
	}
	var encoded bytes.Buffer
	require.NoError(t, EncodeAndWrite(&encoded, request))
	return encoded.Bytes()
}

func TestDecodeAndReadNoForkDigestMaxRejectsTrailingData(t *testing.T) {
	// Two roots (64 SSZ bytes) with the length prefix rewritten to declare one root.
	wire := encodeHashList(t, 2)
	require.Equal(t, byte(2*length.Hash), wire[0])
	wire[0] = length.Hash

	decoded := solid.NewHashList(16)
	err := DecodeAndReadNoForkDigestMax(bytes.NewReader(wire), decoded, clparams.Phase0Version, 16*length.Hash)
	require.ErrorIs(t, err, ErrTrailingData)

	err = DecodeAndReadNoForkDigestExact(bytes.NewReader(wire), solid.NewHashList(16), clparams.Phase0Version, length.Hash)
	require.ErrorIs(t, err, ErrTrailingData)
}

func TestDecodeAndReadNoForkDigestMaxRejectsTrailingFrames(t *testing.T) {
	wire := append(encodeHashList(t, 1), snappyStreamIdentifier...)

	err := DecodeAndReadNoForkDigestMax(bytes.NewReader(wire), solid.NewHashList(16), clparams.Phase0Version, 16*length.Hash)
	require.ErrorIs(t, err, ErrTrailingData)
}

func TestDecodeAndReadNoForkDigestMaxRejectsMalformedTrailingBytes(t *testing.T) {
	const maxSize = 16 * length.Hash
	for _, test := range []struct {
		name    string
		trailer []byte
	}{
		{name: "zero byte", trailer: []byte{0x00}},
		{name: "one byte", trailer: []byte{0x01}},
		{name: "three garbage bytes", trailer: []byte{0xab, 0xab, 0xab}},
		{name: "padding past compressed limit", trailer: append([]byte{0xfe, 0xff, 0xff, 0x00}, make([]byte, snappy.MaxEncodedLen(maxSize))...)},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire := append(encodeHashList(t, 1), test.trailer...)

			err := DecodeAndReadNoForkDigestMax(bytes.NewReader(wire), solid.NewHashList(16), clparams.Phase0Version, maxSize)
			require.ErrorIs(t, err, ErrTrailingData)
		})
	}
}

func TestDecodeAndReadNoForkDigestMaxAcceptsCompletePayload(t *testing.T) {
	for _, count := range []int{1, 2, 16} {
		wire := encodeHashList(t, count)

		decoded := solid.NewHashList(16)
		require.NoError(t, DecodeAndReadNoForkDigestMax(bytes.NewReader(wire), decoded, clparams.Phase0Version, 16*length.Hash))
		require.Equal(t, count, decoded.Length())
		for i := range count {
			require.Equal(t, common.Hash{byte(i + 1)}, decoded.Get(i))
		}
	}
}

func TestDecodeAndReadNoForkDigestIgnoresTrailingData(t *testing.T) {
	// The unbounded decoder is also used for stored files and responses, so it keeps its behaviour.
	wire := encodeHashList(t, 2)
	wire[0] = length.Hash

	decoded := solid.NewHashList(16)
	require.NoError(t, DecodeAndReadNoForkDigest(bytes.NewReader(wire), decoded, clparams.Phase0Version))
	require.Equal(t, 1, decoded.Length())
}
