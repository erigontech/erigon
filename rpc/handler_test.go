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
	"fmt"
	"reflect"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
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
			expected: `{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-32000,"message":"id 2"}}`,
		},
		"no_error": {
			params:   []byte("[3]"),
			expected: `{"jsonrpc":"2.0","id":1,"result":{}}`,
		},
		"err_with_valid_json": {
			params:   []byte("[4]"),
			expected: `{"jsonrpc":"2.0","id":1,"result":{"structLogs":[]},"error":{"code":-32000,"message":"id 4"}}`,
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

			dummyFunc := func(id int, stream *jsoniter.Stream) error {
				if id == 1 {
					stream.WriteNil()
					return fmt.Errorf("id 1")
				}
				if id == 2 {
					return fmt.Errorf("id 2")
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
					return fmt.Errorf("id 4")
				}
				return nil
			}

			var arg1 int
			cb := &callback{
				fn:          reflect.ValueOf(dummyFunc),
				rcvr:        reflect.Value{},
				argTypes:    []reflect.Type{reflect.TypeOf(arg1)},
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
			stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)

			h := handler{}
			h.runMethod(context.Background(), &msg, cb, args, stream)

			output := buf.String()
			assert.Equal(t, testParams.expected, output, "expected output should match")
		})
	}

}
