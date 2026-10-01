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

package rpc_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// BenchmarkParseCallArguments decodes eth_call params with the argument types of APIImpl.Call.
func BenchmarkParseCallArguments(b *testing.B) {
	types := []reflect.Type{
		reflect.TypeFor[ethapi.CallArgs](),
		reflect.TypeFor[*rpc.BlockNumberOrHash](),
		reflect.TypeFor[*ethapi.StateOverrides](),
		reflect.TypeFor[*ethapi.BlockOverrides](),
	}
	for _, size := range []int{68, 13 * 1024, 67 * 1024} {
		raw := fmt.Appendf(nil, `[{"from":"0x9b78abe4c4d18c675c892c324445724cb8dd0ffc","to":"0x77eb52e44db06777bce478159f25c0d55a295314","gas":"0x7a1200","value":"0x0","data":"0x%s"},"latest"]`, strings.Repeat("ab", size))
		b.Run(fmt.Sprintf("calldata=%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := rpc.ParsePositionalArguments(raw, types); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
