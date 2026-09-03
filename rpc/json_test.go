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
	if got.Error != nil && *got.Error != *want.Error {
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
		tInt  = reflect.TypeOf(int(0))
		tPtr  = reflect.TypeOf(new(int))
		tStr  = reflect.TypeOf("")
		tSelf = reflect.TypeOf(selfDecoding{})
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
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(data, &obj); err == nil {
			if v, ok := obj["jsonrpc"]; ok {
				_ = json.Unmarshal(v, &want.Version)
			}
			if v, ok := obj["id"]; ok {
				want.ID = v
			}
			if v, ok := obj["method"]; ok {
				_ = json.Unmarshal(v, &want.Method)
			}
			if v, ok := obj["params"]; ok {
				want.Params = v
			}
			if v, ok := obj["error"]; ok {
				_ = json.Unmarshal(v, &want.Error)
			}
			if v, ok := obj["result"]; ok {
				want.Result = v
			}
		}
		got := new(jsonrpcMessage)
		fillMessage(data, got)
		require.NoError(t, sameMessage(got, &want), "input %s", input)
	})
}
