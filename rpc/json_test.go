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
	"encoding/json"
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
