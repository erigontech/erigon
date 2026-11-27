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
	"encoding/json"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func TestFrameJSONMatchesMarshal(t *testing.T) {
	t.Parallel()
	target := common.Address{1}
	maxValue := *new(uint256.Int).SetAllOne()
	limits := mdgas.MdGas{Execution: math.MaxUint64, State: math.MaxUint64}
	timestamp := hexutil.Uint64(42)
	for name, value := range map[string]jsonstream.Marshaler{
		"nil keys":         FrameNonceKeys(nil),
		"empty keys":       FrameNonceKeys{},
		"keys":             FrameNonceKeys{{}, {1}, maxValue},
		"nil frames":       Frames(nil),
		"empty frames":     Frames{},
		"frames":           Frames{{}, {Mode: FrameSender, Flags: 4, Target: &target, Limits: limits, Value: maxValue, Data: []byte{0, 0xff}}},
		"nil signatures":   FrameSignatures(nil),
		"empty signatures": FrameSignatures{},
		"signatures":       FrameSignatures{{}, {Scheme: FrameSignatureP256, Signer: []byte{1}, Message: []byte{2}, Signature: []byte{0, 0xff}}},
		"nil receipts":     FrameReceipts(nil),
		"empty receipts":   FrameReceipts{},
		"receipts": FrameReceipts{{}, {Logs: Logs{}}, {
			Status: ReceiptStatusSuccessful, GasUsed: limits,
			Logs: Logs{nil, {
				Address: target, Topics: []common.Hash{{2}}, Data: []byte{0, 0xff},
				BlockNumber: 3, TxHash: common.Hash{4}, TxIndex: 5, BlockHash: common.Hash{6},
				Index: 7, Removed: true, BlockTimestamp: &timestamp,
			}},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal(value)
			require.NoError(t, err)
			got, err := jsonstream.Marshal(value)
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
}

func TestFrameJSONStreamsLargeArrays(t *testing.T) {
	t.Parallel()
	frames := make(Frames, 64)
	for i := range frames {
		frames[i].Data = make([]byte, 8192)
	}
	logs := make(Logs, 2048)
	for i := range logs {
		logs[i] = &Log{Data: make([]byte, 256)}
	}
	for name, value := range map[string]jsonstream.Marshaler{
		"frames":   frames,
		"receipts": FrameReceipts{{Status: ReceiptStatusSuccessful, Logs: logs}},
	} {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal(value)
			require.NoError(t, err)
			require.Greater(t, len(want), 2*jsonstream.FlushThreshold)
			var out frameJSONWriter
			require.NoError(t, jsonstream.MarshalTo(&out, value))
			require.Equal(t, string(want), out.String())
			require.Less(t, out.largest, 2*jsonstream.FlushThreshold, "large arrays must be written in chunks")
		})
	}
}

type frameJSONWriter struct {
	bytes.Buffer
	largest int
}

func (w *frameJSONWriter) Write(p []byte) (int, error) {
	w.largest = max(w.largest, len(p))
	return w.Buffer.Write(p)
}
