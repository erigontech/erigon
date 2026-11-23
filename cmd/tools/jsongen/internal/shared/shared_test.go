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

package shared

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjsontest"
)

func ptr[T any](v T) *T { return &v }

func allSet(n uint64) *Item {
	return &Item{
		Position: Position{Number: hexutil.Uint64(n)},
		Hash:     common.Hash{byte(n), 0xff},
		Index:    n,
		Flag:     true,
		Stamp:    ptr(hexutil.Uint64(^uint64(0))),
		Null:     ptr(^uint64(0)),
		Payload:  []byte{byte(n)},
		OptStart: ptr(^uint64(0)),
		OptNum:   hexutil.Uint(^uint(0)),
	}
}

// A group is reused only while the next element holds the same values, so each shared field gets
// a case where it alone changes between elements, and one keeps equal values behind new pointers.
func TestItemsMatchItsTags(t *testing.T) {
	allOmitted := &Item{Payload: []byte{}}
	cases := map[string]Items{
		"nil":                 nil,
		"empty":               {},
		"all set":             {allSet(1)},
		"all omitted":         {allOmitted},
		"nil element":         {allSet(1), nil, allSet(1)},
		"set then omitted":    {allSet(1), allOmitted, allSet(1), allOmitted},
		"only a later member": {allOmitted, {OptNum: 1}, allOmitted},
	}
	for name, change := range map[string]func(*Item){
		"same values":     func(x *Item) { x.Stamp, x.Null, x.OptStart = ptr(*x.Stamp), ptr(*x.Null), ptr(*x.OptStart) },
		"number changes":  func(x *Item) { x.Number = 2 },
		"hash changes":    func(x *Item) { x.Hash[1] = 0 },
		"flag changes":    func(x *Item) { x.Flag = false },
		"stamp changes":   func(x *Item) { x.Stamp = ptr(hexutil.Uint64(0)) },
		"stamp omitted":   func(x *Item) { x.Stamp = nil },
		"null changes":    func(x *Item) { x.Null = ptr(uint64(5)) },
		"null is null":    func(x *Item) { x.Null = nil },
		"optStart change": func(x *Item) { x.OptStart = ptr(uint64(0)) },
		"optNum changes":  func(x *Item) { x.OptNum = 7 },
	} {
		changed := allSet(1)
		change(changed)
		cases[name] = Items{allSet(1), changed, changed, allSet(1)}
	}
	for name, items := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			s := jsonstream.New(&buf)
			require.NoError(t, items.MarshalFastJSONTo(s))
			require.NoError(t, s.Flush())
			require.Equal(t, expected(t, items), buf.String())
		})
	}
}

func expected(t *testing.T, items Items) string {
	t.Helper()
	if items == nil {
		return "null"
	}
	var out []byte
	for i, x := range items {
		if i > 0 {
			out = append(out, ',')
		}
		b, err := ethjsontest.ExpectedJSON(x)
		require.NoError(t, err)
		out = append(out, b...)
	}
	return "[" + string(out) + "]"
}
