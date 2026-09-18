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

// TestWriteResponseMatchesJSONMarshal asserts writeResponse produces output byte-identical to
// json.Marshal of the same response message.
func TestWriteResponseMatchesJSONMarshal(t *testing.T) {
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
			want, err := json.Marshal(&jsonrpcMessage{Version: vsn, ID: id, Result: json.RawMessage(r)})
			require.NoError(t, err)

			var buf bytes.Buffer
			stream := jsonstream.New(&buf)
			_ = (&jsonrpcMessage{Version: vsn, ID: id}).writeResponse(stream, json.RawMessage(r))
			require.NoError(t, stream.Flush())

			require.Equal(t, string(want), buf.String(), "want=%s", want)
		}
	}
}

// TestWriteResponseSkipsIDHTMLEscaping documents the one accepted divergence from json.Marshal:
// the id is copied verbatim, so '<', '>', '&' (and U+2028/2029) in it are left unescaped. The
// JSON is still valid and decodes to the same value.
func TestWriteResponseSkipsIDHTMLEscaping(t *testing.T) {
	id := json.RawMessage(`"a<b>&c"`)

	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	_ = (&jsonrpcMessage{Version: vsn, ID: id}).writeResponse(stream, 1)
	require.NoError(t, stream.Flush())
	require.Equal(t, `{"jsonrpc":"2.0","id":"a<b>&c","result":1}`, buf.String())

	marshaled, err := json.Marshal(&jsonrpcMessage{Version: vsn, ID: id, Result: json.RawMessage("1")})
	require.NoError(t, err)
	require.NotContains(t, string(marshaled), "<") // stdlib HTML-escapes '<' where writeResponse does not
	require.NotEqual(t, buf.String(), string(marshaled))
}
