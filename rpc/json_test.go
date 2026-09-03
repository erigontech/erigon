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
	"github.com/erigontech/erigon/rpc/jsonstream"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePositionalArgumentsRejectsNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rawArgs string
		types   []reflect.Type
		wantErr string
	}{
		{
			name:    "null for required int",
			rawArgs: `[null]`,
			types:   []reflect.Type{reflect.TypeFor[int]()},
			wantErr: "missing value for required argument 0",
		},
		{
			name:    "null for required struct",
			rawArgs: `[null]`,
			types:   []reflect.Type{reflect.TypeFor[echoArgs]()},
			wantErr: "missing value for required argument 0",
		},
		{
			name:    "null for second required argument",
			rawArgs: `["hi", null]`,
			types:   []reflect.Type{reflect.TypeFor[string](), reflect.TypeFor[int]()},
			wantErr: "missing value for required argument 1",
		},
		{
			name:    "null for required slice",
			rawArgs: `[null]`,
			types:   []reflect.Type{reflect.TypeFor[[]int]()},
			wantErr: "missing value for required argument 0",
		},
		{
			name:    "padded null still rejected",
			rawArgs: "[ \n null ]",
			types:   []reflect.Type{reflect.TypeFor[int]()},
			wantErr: "missing value for required argument 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parsePositionalArguments(json.RawMessage(tt.rawArgs), tt.types)
			require.Error(t, err)
			assert.Equal(t, tt.wantErr, err.Error())
		})
	}
}

func TestParsePositionalArgumentsAcceptsNullForOptional(t *testing.T) {
	t.Parallel()

	types := []reflect.Type{reflect.TypeFor[string](), reflect.TypeFor[*echoArgs]()}

	args, err := parsePositionalArguments(json.RawMessage(`["hi", null]`), types)
	require.NoError(t, err)
	require.Len(t, args, 2)
	assert.Equal(t, "hi", args[0].String())
	assert.True(t, args[1].IsNil())
}

func TestParsePositionalArgumentsValues(t *testing.T) {
	t.Parallel()

	types := []reflect.Type{reflect.TypeFor[string](), reflect.TypeFor[int](), reflect.TypeFor[*echoArgs]()}

	args, err := parsePositionalArguments(json.RawMessage(`["hi", 7, {"S": "there"}]`), types)
	require.NoError(t, err)
	require.Len(t, args, 3)
	assert.Equal(t, "hi", args[0].String())
	assert.EqualValues(t, 7, args[1].Int())
	assert.Equal(t, "there", args[2].Interface().(*echoArgs).S)
}

func TestParsePositionalArgumentsInvalidArgument(t *testing.T) {
	t.Parallel()

	_, err := parsePositionalArguments(json.RawMessage(`["hi"]`), []reflect.Type{reflect.TypeFor[int]()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid argument 0")
}

func blockResultFixture(n int) map[string]any {
	txs := make([]any, n)
	for i := range txs {
		txs[i] = map[string]any{
			"blockHash": "0x1122334455667788990011223344556677889900112233445566778899001122",
			"from":      "0xdAC17F958D2ee523a2206206994597C13D831ec7",
			"gas":       "0x5208", "gasPrice": "0x3b9aca00", "nonce": "0x1",
			"to": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "value": "0xde0b6b3a7640000",
			"input": "0xa9059cbb0000000000000000000000001111111111111111111111111111111111111111",
			"v":     "0x1", "r": "0x2", "s": "0x3", "type": "0x2",
		}
	}
	return map[string]any{
		"number": "0x18ae5c0", "hash": "0xaabb", "parentHash": "0xccdd",
		"gasLimit": "0x1c9c380", "gasUsed": "0xd59f80", "timestamp": "0x65000000",
		"transactions": txs,
	}
}

// BenchmarkResponsePreMarshal is the path this change replaces: the result is
// marshalled to a standalone []byte, which writeTo then copies into the stream.
func BenchmarkResponsePreMarshal(b *testing.B) {
	res, id := blockResultFixture(150), json.RawMessage(`1`)
	b.ReportAllocs()
	for b.Loop() {
		enc, err := json.Marshal(res)
		if err != nil {
			b.Fatal(err)
		}
		s := jsonstream.Get(&bytes.Buffer{})
		(&jsonrpcMessage{Version: vsn, ID: id, Result: enc}).writeTo(s)
		_ = s.Flush()
		jsonstream.Put(s)
	}
}

// BenchmarkResponseDeferred encodes the value straight into the pooled stream.
func BenchmarkResponseDeferred(b *testing.B) {
	res, id := blockResultFixture(150), json.RawMessage(`1`)
	b.ReportAllocs()
	for b.Loop() {
		s := jsonstream.Get(&bytes.Buffer{})
		(&jsonrpcMessage{Version: vsn, ID: id, resultValue: res}).writeTo(s)
		_ = s.Flush()
		jsonstream.Put(s)
	}
}

// the two paths must be byte-identical
func TestResponsePathsIdentical(t *testing.T) {
	for _, n := range []int{0, 1, 150} {
		res := blockResultFixture(n)
		id := json.RawMessage(`1`)

		enc, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		var oldBuf bytes.Buffer
		s1 := jsonstream.Get(&oldBuf)
		(&jsonrpcMessage{Version: vsn, ID: id, Result: enc}).writeTo(s1)
		_ = s1.Flush()

		var newBuf bytes.Buffer
		s2 := jsonstream.Get(&newBuf)
		(&jsonrpcMessage{Version: vsn, ID: id, resultValue: res}).writeTo(s2)
		_ = s2.Flush()

		if oldBuf.String() != newBuf.String() {
			t.Fatalf("n=%d differ:\n old: %.200s\n new: %.200s", n, oldBuf.String(), newBuf.String())
		}
	}
	t.Log("byte-identical across 0/1/150 transactions")
}

// A result that cannot be encoded must come back as a JSON-RPC error carrying the
// request id, the same as before the result was encoded lazily -- never as a
// success with a null result, and never as a dropped reply.
func TestResponseUnmarshalableResultBecomesError(t *testing.T) {
	var out bytes.Buffer
	s := jsonstream.Get(&out)
	defer jsonstream.Put(s)

	// a channel has no JSON representation
	msg := &jsonrpcMessage{Version: vsn, ID: json.RawMessage(`7`), resultValue: make(chan int)}
	msg.writeTo(s)
	require.NoError(t, s.Flush())

	var got jsonrpcMessage
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	require.NotNil(t, got.Error, "must be an error response, got %s", out.String())
	require.Equal(t, `7`, string(got.ID), "the error must carry the request id")
	require.Nil(t, got.Result, "must not also claim a result")
}

// A nil result is still a success carrying an explicit null, as JSON-RPC requires.
func TestResponseNilResultEmitsNull(t *testing.T) {
	var out bytes.Buffer
	s := jsonstream.Get(&out)
	defer jsonstream.Put(s)

	req := &jsonrpcMessage{Version: vsn, ID: json.RawMessage(`7`)}
	req.response(nil).writeTo(s)
	require.NoError(t, s.Flush())
	require.Equal(t, `{"jsonrpc":"2.0","id":7,"result":null}`, out.String())
}

// The three transports differ in how writeTo's output reaches the client, and an
// encode failure has to produce a proper error response on all of them. The
// WS/IPC path is the one that matters most: it reads the bytes back out of
// Buffer with no writer at all, so a failure signalled only through Flush is
// invisible there.
func TestResponseEncodeFailureAcrossTransports(t *testing.T) {
	bad := func() *jsonrpcMessage {
		return &jsonrpcMessage{Version: vsn, ID: json.RawMessage(`7`), resultValue: make(chan int)}
	}
	assertErrorResponse := func(t *testing.T, raw []byte) {
		t.Helper()
		var got jsonrpcMessage
		require.NoError(t, json.Unmarshal(raw, &got), "must be valid JSON: %s", raw)
		require.NotNil(t, got.Error, "must be an error response, got %s", raw)
		require.Equal(t, `7`, string(got.ID))
	}

	t.Run("ws-ipc", func(t *testing.T) {
		// answerBuffered: no writer, the caller reads Buffer() directly
		s := jsonstream.Get(nil)
		defer jsonstream.Put(s)
		bad().writeTo(s)
		require.NotEmpty(t, s.Buffer(), "a dropped reply leaves the client waiting forever")
		assertErrorResponse(t, s.Buffer())
	})

	t.Run("batch-item", func(t *testing.T) {
		// one stream per batch entry; an empty buffer means the entry is dropped
		var buf bytes.Buffer
		s := jsonstream.Get(&buf)
		defer jsonstream.Put(s)
		bad().writeTo(s)
		require.NoError(t, s.Flush())
		require.NotZero(t, buf.Len(), "an empty buffer drops this entry from the batch array")
		assertErrorResponse(t, buf.Bytes())
	})

	t.Run("http-streaming", func(t *testing.T) {
		var out bytes.Buffer
		s := jsonstream.Get(&out)
		defer jsonstream.Put(s)
		bad().writeTo(s)
		require.NoError(t, s.Flush())
		assertErrorResponse(t, out.Bytes())
	})
}
