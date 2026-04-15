// Copyright 2024 The Erigon Authors
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
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func TestHandlerDoesNotDoubleWriteNull(t *testing.T) {

	tests := map[string]struct {
		params   []byte
		expected string
	}{
		"error_with_stream_write": {
			params:   []byte("[1]"),
			expected: `{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-32000,"message":"id 1"}}`,
		},
		"error_without_stream_write": {
			params:   []byte("[2]"),
			expected: `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"id 2"}}`,
		},
		"no_error": {
			params:   []byte("[3]"),
			expected: `{"jsonrpc":"2.0","id":1,"result":{}}`,
		},
		"err_with_valid_json_empty_array": {
			params:   []byte("[4]"),
			expected: `{"jsonrpc":"2.0","id":1,"result":{"structLogs":[]},"error":{"code":-32000,"message":"id 4"}}`,
		},
		"err_with_valid_json_empty_object": {
			params:   []byte("[5]"),
			expected: `{"jsonrpc":"2.0","id":1,"result":{"structLogs":{}},"error":{"code":-32000,"message":"id 4"}}`,
		},
	}

	for name, testParams := range tests {
		t.Run(name, func(t *testing.T) {
			msg := jsonrpcMessage{
				Version: "2.0",
				ID:      []byte{49},
				Method:  "test_test",
				Params:  testParams.params,
				Error:   nil,
				Result:  nil,
			}

			dummyFunc := func(id int, stream jsonstream.Stream) error {
				if id == 1 {
					stream.WriteNil()
					return errors.New("id 1")
				}
				if id == 2 {
					return errors.New("id 2")
				}
				if id == 3 {
					stream.WriteEmptyObject()
					return nil
				}
				if id == 4 {
					stream.WriteObjectStart()
					stream.WriteObjectField("structLogs")
					stream.WriteEmptyArray()
					stream.WriteObjectEnd()
					return errors.New("id 4")
				}
				if id == 5 {
					stream.WriteObjectStart()
					stream.WriteObjectField("structLogs")
					stream.WriteEmptyObject()
					stream.WriteObjectEnd()
					return errors.New("id 4")
				}
				return nil
			}

			cb := &callback{
				fn:          reflect.ValueOf(dummyFunc),
				rcvr:        reflect.Value{},
				argTypes:    []reflect.Type{reflect.TypeFor[int]()},
				hasCtx:      false,
				errPos:      0,
				isSubscribe: false,
				streamable:  true,
			}

			args, err := parsePositionalArguments((msg).Params, cb.argTypes)
			if err != nil {
				t.Fatal(err)
			}

			var buf bytes.Buffer
			stream := jsonstream.New(jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096))

			h := handler{}
			h.runMethod(context.Background(), &msg, cb, args, stream)

			stream.Flush()

			output := buf.String()
			assert.Equal(t, testParams.expected, output, "expected output should match")
		})
	}

}

// --- RegisterMethod ---

func TestRegisterMethod_InvalidName(t *testing.T) {
	s := NewServer(1, false, false, true, log.New(), 0)
	defer s.Stop()
	for _, bad := range []string{"noUnderscore", "_method", "ns_", ""} {
		err := RegisterMethod(s, bad, func(_ context.Context, _ struct{}) (uint64, error) { return 0, nil })
		require.Errorf(t, err, "expected error for name %q", bad)
	}
}

func TestRegisterMethod_ValidName(t *testing.T) {
	s := NewServer(1, false, false, true, log.New(), 0)
	defer s.Stop()
	require.NoError(t, RegisterMethod(s, "eth_blockNumber", func(_ context.Context, _ struct{}) (uint64, error) { return 0, nil }))
}

// --- Typed dispatch end-to-end ---

type typedPriorityService struct{}

func (typedPriorityService) TypedPriority(_ context.Context) (string, error) { return "reflect", nil }

func newTypedDispatchServer(t *testing.T) (*Server, *Client) {
	t.Helper()
	logger := log.New()
	s := NewServer(50, false, false, true, logger, time.Duration(0))
	require.NoError(t, RegisterMethod(s, "eth_blockNumber", func(_ context.Context, _ struct{}) (uint64, error) { return 99, nil }))
	require.NoError(t, RegisterMethod(s, "eth_fail", func(_ context.Context, _ struct{}) (string, error) { return "", errors.New("boom") }))
	require.NoError(t, s.RegisterName("typed", typedPriorityService{}))
	require.NoError(t, RegisterMethod(s, "typed_typedPriority", func(_ context.Context, _ struct{}) (string, error) { return "typed", nil }))
	ts := httptest.NewServer(s)
	c, err := DialHTTP(ts.URL, logger)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close(); ts.Close(); s.Stop() })
	return s, c
}

func TestTypedDispatch_NoParams(t *testing.T) {
	_, c := newTypedDispatchServer(t)
	var result uint64
	require.NoError(t, c.Call(&result, "eth_blockNumber"))
	assert.Equal(t, uint64(99), result)
}

func TestTypedDispatch_WithParams(t *testing.T) {
	type addParams struct {
		A int `json:"a"`
		B int `json:"b"`
	}
	logger := log.New()
	s := NewServer(1, false, false, true, logger, 0)
	defer s.Stop()
	require.NoError(t, RegisterMethod(s, "math_add", func(_ context.Context, p addParams) (int, error) { return p.A + p.B, nil }))
	ts := httptest.NewServer(s)
	defer ts.Close()
	c, err := DialHTTP(ts.URL, logger)
	require.NoError(t, err)
	defer c.Close()

	var result int
	require.NoError(t, c.Call(&result, "math_add", addParams{A: 3, B: 4}))
	assert.Equal(t, 7, result)
}

func TestTypedDispatch_ErrorPropagation(t *testing.T) {
	_, c := newTypedDispatchServer(t)
	var result string
	err := c.Call(&result, "eth_fail")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestTypedDispatch_TakesPriorityOverReflect(t *testing.T) {
	_, c := newTypedDispatchServer(t)
	var result string
	require.NoError(t, c.Call(&result, "typed_typedPriority"))
	assert.Equal(t, "typed", result)
}

// --- Method.invoke params handling ---

func TestMethodInvoke_Params(t *testing.T) {
	type params struct {
		X int `json:"x"`
	}
	inv := Method[params, int]{fn: func(_ context.Context, p params) (int, error) { return p.X, nil }}

	cases := []struct {
		name string
		raw  string
		want int
	}{
		{"nil", ``, 0},
		{"null", `null`, 0},
		{"empty array", `[]`, 0},
		{"named object", `{"x":42}`, 42},
		{"single-element array", `[{"x":7}]`, 7},
		{"whitespace-padded null", "  null  ", 0},
		{"whitespace-padded object", `  {"x":5}  `, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := inv.invoke(t.Context(), json.RawMessage(tc.raw))
			require.NoError(t, err)
			assert.Equal(t, tc.want, result)
		})
	}
}

func TestMethodInvoke_MultiArgSliceParams(t *testing.T) {
	inv := Method[[]int, int]{fn: func(_ context.Context, p []int) (int, error) {
		sum := 0
		for _, v := range p {
			sum += v
		}
		return sum, nil
	}}
	result, err := inv.invoke(t.Context(), json.RawMessage(`[1,2,3]`))
	require.NoError(t, err)
	assert.Equal(t, 6, result)
}

func TestMethodInvoke_InvalidJSON(t *testing.T) {
	inv := Method[struct{ X int }, int]{fn: func(_ context.Context, p struct{ X int }) (int, error) { return p.X, nil }}
	_, err := inv.invoke(t.Context(), json.RawMessage(`{bad json}`))
	require.Error(t, err)
	var ipErr *InvalidParamsError
	assert.True(t, errors.As(err, &ipErr))
}
