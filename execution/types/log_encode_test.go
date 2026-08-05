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

package types

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

func logEncodingSamples() map[string]*Log {
	return map[string]*Log{
		"no topics, no data": {Address: common.HexToAddress("0x1")},
		"one topic": {
			Address: common.HexToAddress("0xecf8f87f810ecf450940c9f60066b4a7a501d6a7"),
			Topics:  []common.Hash{common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")},
			Data:    []byte{0x7f},
		},
		"single byte data below threshold": {
			Address: common.HexToAddress("0x2"),
			Data:    []byte{0x01},
		},
		"single byte data above threshold": {
			Address: common.HexToAddress("0x2"),
			Data:    []byte{0xff},
		},
		"four topics, long data": {
			Address: common.HexToAddress("0x3"),
			Topics: []common.Hash{
				common.HexToHash("0x01"), common.HexToHash("0x02"),
				common.HexToHash("0x03"), common.HexToHash("0x04"),
			},
			Data: bytes.Repeat([]byte{0xab}, 1024),
		},
		"data exactly 55 bytes": {
			Address: common.HexToAddress("0x4"),
			Data:    bytes.Repeat([]byte{0xcd}, 55),
		},
		"data exactly 56 bytes": {
			Address: common.HexToAddress("0x5"),
			Data:    bytes.Repeat([]byte{0xef}, 56),
		},
	}
}

func TestLogEncodeRLPBufMatchesEncodeRLP(t *testing.T) {
	t.Parallel()
	for name, l := range logEncodingSamples() {
		t.Run(name, func(t *testing.T) {
			want, err := rlp.EncodeToBytes(l)
			require.NoError(t, err)

			var got bytes.Buffer
			buf := rlp.NewEncodingBuf()
			defer buf.Release()
			require.NoError(t, l.encodeRLP(&got, buf[:]))

			require.Equal(t, want, got.Bytes())
			require.Equal(t, len(want), l.encodingSize())
		})
	}
}

func TestLogsRlpHashMatchesReflectionHash(t *testing.T) {
	t.Parallel()
	samples := logEncodingSamples()

	var flat Logs
	var byTx []Logs
	for _, l := range samples {
		flat = append(flat, l)
		byTx = append(byTx, Logs{l})
	}
	byTx = append(byTx, nil, Logs{})

	require.Equal(t, RlpHash(flat), RlpHashLogs(byTx))
	require.Equal(t, RlpHash(Logs(nil)), RlpHashLogs(nil))
}
