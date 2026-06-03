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

package rpc

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/rpc/jsonstream"
)

// TestWriteToMatchesJSONMarshal asserts writeTo produces output byte-identical to json.Marshal of
// the same message — for both the hand-written success fast path and the json.Marshal fallback
// (errors, nil result, and other non-plain shapes).
func TestWriteToMatchesJSONMarshal(t *testing.T) {
	results := []string{
		`{"a":1,"b":[1,2,3],"c":"0xdeadbeef"}`,
		`"0x1234"`,
		`null`,
		`[{"blob":"0x00","proofs":["0x01","0x02"]}]`,
		`[]`,
		`true`,
		`12345`,
	}
	ids := []json.RawMessage{json.RawMessage("1"), json.RawMessage(`"abc-123"`), json.RawMessage("9007199254740991")}

	var msgs []*jsonrpcMessage
	for _, r := range results {
		for _, id := range ids {
			msgs = append(msgs, &jsonrpcMessage{Version: vsn, ID: id, Result: json.RawMessage(r)})
		}
	}
	// non-plain messages take the json.Marshal fallback inside writeTo
	msgs = append(msgs,
		&jsonrpcMessage{Version: vsn, ID: json.RawMessage("1"), Error: &jsonError{Code: -32000, Message: "boom"}},
		&jsonrpcMessage{Version: vsn, ID: json.RawMessage("2"), Error: &jsonError{Code: -1, Message: "with data", Data: "0xdeadbeef"}},
		&jsonrpcMessage{Version: vsn, ID: json.RawMessage("3")},
	)

	for _, msg := range msgs {
		want, err := json.Marshal(msg)
		require.NoError(t, err)

		var buf bytes.Buffer
		stream := jsonstream.New(&buf)
		msg.writeTo(stream)
		require.NoError(t, stream.Flush())

		require.Equal(t, string(want), buf.String(), "want=%s", want)
	}
}
