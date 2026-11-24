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

	"github.com/erigontech/erigon/common"
)

func BenchmarkParsePositionalArguments(b *testing.B) {
	for _, bc := range []struct {
		name  string
		raw   string
		types []reflect.Type
	}{
		{"self-decoding", `["abc"]`, []reflect.Type{reflect.TypeFor[selfDecoding]()}},
		{"optional self-decoding", `["abc"]`, []reflect.Type{reflect.TypeFor[*selfDecoding]()}},
		{
			"address and optional block", `["0x43d8c21431c86df8698e546b7e05f5d5d078828e","latest"]`,
			[]reflect.Type{reflect.TypeFor[common.Address](), reflect.TypeFor[*BlockNumberOrHash]()},
		},
	} {
		b.Run(bc.name, func(b *testing.B) {
			raw := json.RawMessage(bc.raw)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := parsePositionalArguments(raw, bc.types); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
