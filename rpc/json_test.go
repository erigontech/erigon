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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/rpc/jsonstream"
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

// messageCorpus seeds the fuzz targets below. Every entry is valid JSON, which
// is the state of a message by the time parseMessage sees it.
var messageCorpus = []string{
	`{}`,
	`{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`,
	`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x00"},"latest"]}`,
	`{"id":null}`,
	`{"result":null}`,
	`{"params":null}`,
	`{"jsonrpc":"2.0","id":1,"result":null}`,
	`{"id":"str-id","method":"m"}`,
	`{"id":1.5e3,"method":"m"}`,
	`{"id":true,"method":"m"}`,
	// escapes in the string fields
	`{"method":"a\"b","id":1}`,
	`{"method":"a\\b","id":1}`,
	`{"method":"tab\there","id":1}`,
	`{"method":"back\\\\slash","id":1}`,
	// structural bytes inside strings must not confuse the scan
	`{"method":"m","params":["{[,:}]"],"id":1}`,
	`{"method":"m","params":["a\"},{\"b"],"id":1}`,
	`{"method":"m","params":["ends with backslash\\"],"id":1}`,
	// whitespace
	"{ \"method\" : \"m\" , \"id\" : 1 }",
	"\n\t{\"method\":\"m\",\"id\":2}\r\n",
	`{"params":  [  1  ,  2  ]  ,"id":3}`,
	// duplicate keys, last one wins
	`{"method":"first","method":"second","id":1}`,
	`{"id":1,"id":2}`,
	// keys in other spellings are unknown fields, only the exact names match
	`{"METHOD":"m","ID":1}`,
	`{"Method":"m","Id":1,"Jsonrpc":"2.0"}`,
	// unicode escapes in keys are unescaped before matching (method == method)
	"{\"metho\\u0064\":\"m\",\"i\\u0064\":7}",
	// a double-escaped key stays literal method and is not the method field
	`{"metho\\u0064":"not the method key"}`,
	// unknown fields are ignored
	`{"method":"m","id":1,"extra":{"a":[1,2,3]},"more":"x"}`,
	// nested params
	`{"method":"m","id":1,"params":[[[[1]]]],"x":1}`,
	`{"method":"m","id":1,"params":[{"a":{"b":{"c":[]}}}]}`,
	// empty and odd values
	`{"method":"","id":1}`,
	`{"":1,"method":"m"}`,
	// not an object at all
	`1`,
	`"str"`,
	`null`,
	`true`,
	// batches
	`[]`,
	`[{"method":"a","id":1}]`,
	`[{"method":"a","id":1},{"method":"b","id":2}]`,
	`[null]`,
	`[{"method":"a","id":1},null,{"method":"b","id":2}]`,
	`[1,2,3]`,
	`[[1],[2]]`,
	`[ { "method" : "a" , "id" : 1 } , null ]`,
	`[{"method":"m","params":["},{"]}]`,
}

func testMessage(version, method, id, params string) *jsonrpcMessage {
	m := &jsonrpcMessage{Version: version, Method: method}
	if id != "" {
		m.ID = json.RawMessage(id)
	}
	if params != "" {
		m.Params = json.RawMessage(params)
	}
	return m
}

// TestParseMessage covers the inputs where the envelope split has to decide.
func TestParseMessage(t *testing.T) {
	t.Parallel()

	zero := func() *jsonrpcMessage { return testMessage("", "", "", "") }
	tests := []struct {
		name  string
		input string
		batch bool
		want  []*jsonrpcMessage
	}{
		{"empty object", `{}`, false, []*jsonrpcMessage{zero()}},
		{"call", `{"jsonrpc":"2.0","id":1,"method":"m","params":[1,2]}`, false,
			[]*jsonrpcMessage{testMessage("2.0", "m", "1", "[1,2]")}},
		{"null message", `null`, false, []*jsonrpcMessage{nil}},
		{"not an object", `1`, false, []*jsonrpcMessage{zero()}},
		{"string", `"str"`, false, []*jsonrpcMessage{zero()}},
		{"empty batch", `[]`, true, nil},
		{"batch", `[{"method":"a","id":1},{"method":"b","id":2}]`, true,
			[]*jsonrpcMessage{testMessage("", "a", "1", ""), testMessage("", "b", "2", "")}},
		{"batch with null", `[{"method":"a","id":1},null]`, true,
			[]*jsonrpcMessage{testMessage("", "a", "1", ""), nil}},
		{"duplicate key, last wins", `{"method":"first","method":"second","id":1}`, false,
			[]*jsonrpcMessage{testMessage("", "second", "1", "")}},

		// field names have one spelling in the spec; any other spelling is an
		// unknown key, but unicode escapes are unescaped first so they match the
		// same way encoding/json map keys do
		{"cased keys ignored", `{"Method":"m","ID":1,"Params":[1]}`, false, []*jsonrpcMessage{zero()}},
		{"unicode-escaped method key", "{\"metho\\u0064\":\"m\",\"i\\u0064\":7}", false,
			[]*jsonrpcMessage{testMessage("", "m", "7", "")}},
		{"double-escaped key is not method", `{"metho\\u0064":"x"}`, false, []*jsonrpcMessage{zero()}},

		// a string holding structural bytes must not end the value early
		{"structural bytes in a string", `{"method":"m","params":["a\"},{\"b"],"id":1}`, false,
			[]*jsonrpcMessage{testMessage("", "m", "1", `["a\"},{\"b"]`)}},
		{"batch element with a brace in a string", `[{"method":"m","params":["},{"]}]`, true,
			[]*jsonrpcMessage{testMessage("", "m", "", `["},{"]`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, batch, err := parseMessage(json.RawMessage(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.batch, batch)
			require.Len(t, got, len(tt.want))
			for i := range tt.want {
				if tt.want[i] == nil {
					require.Nil(t, got[i], "message %d", i)
					continue
				}
				require.NoError(t, sameMessage(got[i], tt.want[i]), "message %d", i)
			}
		})
	}
}

func sameMessage(got, want *jsonrpcMessage) error {
	if got.Version != want.Version {
		return fmt.Errorf("Version = %q, want %q", got.Version, want.Version)
	}
	if got.Method != want.Method {
		return fmt.Errorf("Method = %q, want %q", got.Method, want.Method)
	}
	for _, f := range []struct {
		name      string
		got, want json.RawMessage
	}{
		{"ID", got.ID, want.ID},
		{"Params", got.Params, want.Params},
		{"Result", got.Result, want.Result},
	} {
		if (f.got == nil) != (f.want == nil) {
			return fmt.Errorf("%s nil = %v, want %v (got %q want %q)", f.name, f.got == nil, f.want == nil, f.got, f.want)
		}
		// A raw field is a slice of the input, so it should come back byte for byte.
		if !bytes.Equal(f.got, f.want) {
			return fmt.Errorf("%s = %q, want %q", f.name, f.got, f.want)
		}
	}
	if (got.Error == nil) != (want.Error == nil) {
		return fmt.Errorf("Error nil = %v, want %v", got.Error == nil, want.Error == nil)
	}
	// jsonError.Data holds a decoded value, so it can be a slice or a map that == would panic on.
	if got.Error != nil && !reflect.DeepEqual(*got.Error, *want.Error) {
		return fmt.Errorf("Error = %+v, want %+v", *got.Error, *want.Error)
	}
	return nil
}

// selfDecoding stands in for an argument type that unmarshals itself, which is
// the shape an engine API payload has.
type selfDecoding struct {
	Text string
}

func (s *selfDecoding) UnmarshalJSON(input []byte) error {
	if len(input) < 2 || input[0] != '"' {
		return fmt.Errorf("selfDecoding: not a string: %s", input)
	}
	s.Text = string(input[1 : len(input)-1])
	return nil
}

// TestParsePositionalArgumentsScan covers how an argument array is cut up.
func TestParsePositionalArgumentsScan(t *testing.T) {
	t.Parallel()

	var (
		tInt  = reflect.TypeFor[int]()
		tPtr  = reflect.TypeFor[*int]()
		tStr  = reflect.TypeFor[string]()
		tSelf = reflect.TypeFor[selfDecoding]()
	)
	tests := []struct {
		name    string
		args    string
		types   []reflect.Type
		want    []any
		wantErr string
	}{
		{"no arguments", `[]`, nil, nil, ""},
		{"two ints", `[1,2]`, []reflect.Type{tInt, tInt}, []any{1, 2}, ""},
		{"whitespace", `[  1  ,  2  ]`, []reflect.Type{tInt, tInt}, []any{1, 2}, ""},
		{"missing optional", `[1]`, []reflect.Type{tInt, tPtr}, []any{1, (*int)(nil)}, ""},
		{"string with structural bytes", `["},{"]`, []reflect.Type{tStr}, []any{"},{"}, ""},
		{"self-decoding argument", `["abc"]`, []reflect.Type{tSelf}, []any{selfDecoding{Text: "abc"}}, ""},
		{"empty params", ``, nil, nil, ""},
		{"null params", `null`, nil, nil, ""},
		{"too many arguments", `[1,2]`, []reflect.Type{tInt}, nil, "too many arguments"},
		{"non-array args", `{"a":1}`, []reflect.Type{tInt}, nil, "non-array args"},
		{"bad argument", `["x"]`, []reflect.Type{tInt}, nil, "invalid argument 0"},
		{"missing required", `[]`, []reflect.Type{tInt}, nil, "missing value for required argument 0"},
		{"null for required", `[null]`, []reflect.Type{tInt}, nil, "missing value for required argument 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parsePositionalArguments(json.RawMessage(tt.args), tt.types)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.want))
			for i := range tt.want {
				assert.Equal(t, tt.want[i], got[i].Interface(), "argument %d", i)
			}
		})
	}
}

// FuzzJSONScanFields checks that the field scan agrees with encoding/json on any
// valid JSON object.
func FuzzJSONScanFields(f *testing.F) {
	for _, s := range messageCorpus {
		f.Add(s)
	}
	f.Add(`{"a":"😀","b":[1,{"c":null}]}`)
	f.Fuzz(func(t *testing.T, input string) {
		data := []byte(input)
		if !json.Valid(data) {
			return
		}
		var want map[string]json.RawMessage
		if err := json.Unmarshal(data, &want); err != nil {
			return // not an object
		}
		got := make(map[string]json.RawMessage)
		forEachJSONField(data, func(key, value []byte) {
			// The scan hands back the key still escaped, so unescape it the same
			// way the map decode did before comparing.
			var k string
			if err := json.Unmarshal(append(append([]byte{'"'}, key...), '"'), &k); err != nil {
				t.Fatalf("key %q does not unescape: %v", key, err)
			}
			got[k] = value
		})
		require.Len(t, got, len(want), "input %s", input)
		for k, wv := range want {
			gv, ok := got[k]
			require.True(t, ok, "missing field %q (input %s)", k, input)
			require.Equal(t, string(bytes.TrimSpace(wv)), string(bytes.TrimSpace(gv)), "field %q (input %s)", k, input)
		}
	})
}

// FuzzJSONScanElements checks that the element scan agrees with encoding/json on
// any valid JSON array.
// TestJSONScanMalformedTerminates pins that both walkers make progress on input
// json.Valid rejects, so a custom transport that skips that gate cannot hang.
func TestJSONScanMalformedTerminates(t *testing.T) {
	const maxCalls = 1000
	for _, input := range []string{"[}", "[}]", "[,}", "{\"a\":}", "{\"a\":,}", "{}}", "[[}", "[:", "{\"a\"}"} {
		calls := 0
		count := func() {
			calls++
			if calls > maxCalls {
				t.Fatalf("no progress on %q", input)
			}
		}
		forEachJSONElement([]byte(input), func([]byte) { count() })
		calls = 0
		forEachJSONField([]byte(input), func(_, _ []byte) { count() })
	}
}

func FuzzJSONScanElements(f *testing.F) {
	for _, s := range messageCorpus {
		f.Add(s)
	}
	f.Add(`[1,"two",{"three":3},[4],null,true]`)
	f.Fuzz(func(t *testing.T, input string) {
		data := []byte(input)
		if !json.Valid(data) {
			return
		}
		var want []json.RawMessage
		if err := json.Unmarshal(data, &want); err != nil {
			return // not an array
		}
		var got []json.RawMessage
		forEachJSONElement(data, func(value []byte) {
			got = append(got, value)
		})
		require.Len(t, got, len(want), "input %s", input)
		for i := range want {
			require.Equal(t, string(bytes.TrimSpace(want[i])), string(bytes.TrimSpace(got[i])), "element %d (input %s)", i, input)
		}
	})
}

// FuzzFillMessage checks the envelope split against encoding/json field by
// field. Field names have one spelling, so the reference picks each one out of
// a decoded map by its exact name.
func FuzzFillMessage(f *testing.F) {
	for _, s := range messageCorpus {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		data := []byte(input)
		if !json.Valid(data) {
			return
		}
		var want jsonrpcMessage
		// encoding/json splits the object; the per-field rules mirror fillMessage.
		// A map would collapse repeated keys, which both of them merge.
		dec := json.NewDecoder(bytes.NewReader(data))
		if tok, err := dec.Token(); err == nil && tok == json.Delim('{') {
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					break
				}
				key, _ := keyTok.(string)
				var v json.RawMessage
				if err := dec.Decode(&v); err != nil {
					break
				}
				switch key {
				case "jsonrpc":
					if json.Unmarshal(v, &want.Version) != nil {
						want.Version = ""
					}
				case "id":
					want.ID = v
				case "method":
					if json.Unmarshal(v, &want.Method) != nil {
						want.Method = ""
					}
				case "params":
					want.Params = v
				case "error":
					if json.Unmarshal(v, &want.Error) != nil {
						want.Error = nil
					}
				case "result":
					want.Result = v
				}
			}
		}
		got := new(jsonrpcMessage)
		fillMessage(data, got)
		require.NoError(t, sameMessage(got, &want), "input %s", input)
	})
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
		(&jsonrpcMessage{Version: vsn, ID: id}).response(res).writeTo(s2)
		_ = s2.Flush()

		if oldBuf.String() != newBuf.String() {
			t.Fatalf("n=%d differ:\n old: %.200s\n new: %.200s", n, oldBuf.String(), newBuf.String())
		}
	}
	t.Log("byte-identical across 0/1/150 transactions")
}

// An unencodable result must come back as an error carrying the request id,
// never as a success with a null result and never as a dropped reply.
func TestResponseUnmarshalableResultBecomesError(t *testing.T) {
	var out bytes.Buffer
	s := jsonstream.Get(&out)
	defer jsonstream.Put(s)

	// a channel has no JSON representation
	msg := (&jsonrpcMessage{Version: vsn, ID: json.RawMessage(`7`)}).response(make(chan int))
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

// WS/IPC reads the bytes back out of Buffer with no writer at all, so a failure
// signalled only through Flush would be invisible there.
func TestResponseEncodeFailureAcrossTransports(t *testing.T) {
	bad := func() *jsonrpcMessage {
		return (&jsonrpcMessage{Version: vsn, ID: json.RawMessage(`7`)}).response(make(chan int))
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

// A result too large to buffer must still reach the client byte-for-byte,
// without growing the stream buffer past what the stream pool keeps.
func TestLargeResultStreamsAndStaysPoolable(t *testing.T) {
	res := blockResultFixture(4000)
	enc, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) <= jsonstream.FlushThreshold {
		t.Fatalf("fixture is only %d bytes, too small to exercise the bound", len(enc))
	}
	id := json.RawMessage(`1`)

	var want, got bytes.Buffer
	s1 := jsonstream.Get(&want)
	(&jsonrpcMessage{Version: vsn, ID: id, Result: enc}).writeTo(s1)
	_ = s1.Flush()

	s2 := jsonstream.Get(&got)
	(&jsonrpcMessage{Version: vsn, ID: id}).response(res).writeTo(s2)
	require.LessOrEqual(t, cap(s2.Buffer()), 16*jsonstream.FlushThreshold,
		"the result grew the stream buffer past the pool limit")
	_ = s2.Flush()

	require.Equal(t, want.String(), got.String())
	jsonstream.Put(s1)
	jsonstream.Put(s2)
}

// The result is encoded before any of the envelope, so an id of any size is
// safe. Sizes straddle prefix+id == FlushThreshold.
func TestHugeRequestIDStillProducesValidJSON(t *testing.T) {
	const prefix = len(`{"jsonrpc":"2.0","id":`)
	for _, n := range []int{jsonstream.FlushThreshold - prefix - 1, jsonstream.FlushThreshold - prefix, jsonstream.FlushThreshold} {
		id := json.RawMessage(`"` + strings.Repeat("i", n-2) + `"`)

		var out bytes.Buffer
		s := jsonstream.Get(&out)
		(&jsonrpcMessage{Version: vsn, ID: id}).response(map[string]int{"n": 1}).writeTo(s)
		_ = s.Flush()
		jsonstream.Put(s)

		var back map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(out.Bytes(), &back), "id of %d bytes produced invalid JSON", n)
		require.Equal(t, string(id), string(back["id"]))
		require.Equal(t, `{"n":1}`, string(back["result"]))

		// The same id with a result that cannot encode must yield one error object.
		out.Reset()
		s = jsonstream.Get(&out)
		(&jsonrpcMessage{Version: vsn, ID: id}).response(make(chan int)).writeTo(s)
		_ = s.Flush()
		jsonstream.Put(s)

		back = nil
		require.NoError(t, json.Unmarshal(out.Bytes(), &back), "id of %d bytes produced invalid JSON on failure", n)
		require.Contains(t, back, "error")
		require.NotContains(t, back, "result")
	}

}
