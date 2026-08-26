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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

var snappyStreamIdentifier = []byte{0xff, 0x06, 0x00, 0x00, 's', 'N', 'a', 'P', 'p', 'Y'}

type countingReader struct {
	r     *bytes.Reader
	bytes int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.bytes += n
	return n, err
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

func TestDecodeAndReadNoForkDigestExactCompressedLimitIsInclusive(t *testing.T) {
	const payloadSize = 8
	const maxCompressedSize = 32 + payloadSize + payloadSize/6

	var encoded bytes.Buffer
	require.NoError(t, EncodeAndWrite(&encoded, &cltypes.Ping{Id: 1}))
	prefix, body := encoded.Bytes()[:1], encoded.Bytes()[1:]
	require.Less(t, len(body), maxCompressedSize)

	padding := make([]byte, maxCompressedSize-len(body))
	padding[0] = 0x80
	padding[1] = byte(len(padding) - 4)
	exactMax := append(append(append(append([]byte{}, prefix...), body[:len(snappyStreamIdentifier)]...), padding...), body[len(snappyStreamIdentifier):]...)
	require.Len(t, exactMax, 1+maxCompressedSize)

	exactMaxReader := &countingReader{r: bytes.NewReader(exactMax)}
	var ping cltypes.Ping
	require.NoError(t, DecodeAndReadNoForkDigestExact(exactMaxReader, &ping, clparams.Phase0Version, payloadSize))
	require.Equal(t, uint64(1), ping.Id)
	require.LessOrEqual(t, exactMaxReader.bytes, 1+maxCompressedSize)

	overMax := append(append([]byte{}, exactMax...), 0)
	reader := &countingReader{r: bytes.NewReader(overMax)}
	require.NoError(t, DecodeAndReadNoForkDigestExact(reader, &cltypes.Ping{}, clparams.Phase0Version, payloadSize))
	require.LessOrEqual(t, reader.bytes, 1+maxCompressedSize)
}
