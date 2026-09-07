// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

func TestRoundtrip(t *testing.T) {
	t.Parallel()
	for i, want := range []string{
		"0xf880806482520894d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0a1010000000000000000000000000000000000000000000000000000000000000001801ba0c16787a8e25e941d67691954642876c08f00996163ae7dfadbbfd6cd436f549da06180e5626cae31590f40641fe8f63734316c4bfeb4cdfab6714198c1044d2e28",
		"0xd5c0d3cb84746573742a2a808213378667617a6f6e6b",
		"0xc780c0c1c0825208",
	} {
		var out strings.Builder
		in := newInStream(bytes.NewReader(common.FromHex(want)), 0)
		err := rlpToText(in, &out)
		if err != nil {
			t.Fatal(err)
		}
		text := out.String()
		rlpBytes, err := textToRlp(strings.NewReader(text))
		if err != nil {
			t.Errorf("test %d: error %v", i, err)
			continue
		}
		have := fmt.Sprintf("%#x", rlpBytes)
		if have != want {
			t.Errorf("test %d: have\n%v\nwant:\n%v\n", i, have, want)
		}
	}
}

func TestDumpRejectsTruncatedList(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		hex  string
	}{
		{"flat list one byte short", "0xc201"},
		{"nested list one byte short", "0xc3c101"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out strings.Builder
			in := newInStream(bytes.NewReader(common.FromHex(tc.hex)), 0)
			err := rlpToText(in, &out)
			if err == nil {
				t.Fatalf("want error for truncated list %s, got nil (output: %s)", tc.hex, out.String())
			}
			if errors.Is(err, io.EOF) {
				t.Fatalf("truncated list %s must not be reported as a clean io.EOF, got: %v", tc.hex, err)
			}
		})
	}
}

func TestTextToRlp(t *testing.T) {
	t.Parallel()
	type tc struct {
		text string
		want string
	}
	cases := []tc{
		{
			text: `[
  "",
  [],
[     
 [],
    ],
  5208,
]`,
			want: "0xc780c0c1c0825208",
		},
	}
	for i, tc := range cases {
		have, err := textToRlp(strings.NewReader(tc.text))
		if err != nil {
			t.Errorf("test %d: error %v", i, err)
			continue
		}
		if hexutil.Encode(have) != tc.want {
			t.Errorf("test %d:\nhave %v\nwant %v", i, hexutil.Encode(have), tc.want)
		}
	}
}
