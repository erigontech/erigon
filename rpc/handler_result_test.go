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

// TestWriteResultToMatchesJSONMarshal asserts the streamed result-response path produces output
// byte-identical to json.Marshal of the same message, across id and result shapes.
func TestWriteResultToMatchesJSONMarshal(t *testing.T) {
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
	for _, r := range results {
		for _, id := range ids {
			msg := &jsonrpcMessage{Version: vsn, ID: id, Result: json.RawMessage(r)}
			require.True(t, msg.isPlainResult())

			want, err := json.Marshal(msg)
			require.NoError(t, err)

			var buf bytes.Buffer
			stream := jsonstream.New(&buf)
			msg.writeResultTo(stream)
			require.NoError(t, stream.Flush())

			require.Equal(t, string(want), buf.String(), "result=%s id=%s", r, id)
		}
	}
}

// TestIsPlainResult guards the fast-path predicate: only ordinary success responses qualify;
// errors, requests and notifications fall back to json.Marshal.
func TestIsPlainResult(t *testing.T) {
	require.True(t, (&jsonrpcMessage{Version: vsn, ID: json.RawMessage("1"), Result: json.RawMessage("1")}).isPlainResult())
	require.False(t, (&jsonrpcMessage{Version: vsn, ID: json.RawMessage("1"), Error: &jsonError{Code: -1, Message: "boom"}}).isPlainResult())
	require.False(t, (&jsonrpcMessage{Version: vsn, ID: json.RawMessage("1"), Method: "eth_subscription", Params: json.RawMessage("{}")}).isPlainResult())
	require.False(t, (&jsonrpcMessage{Version: vsn, Result: json.RawMessage("1")}).isPlainResult()) // no id
}
