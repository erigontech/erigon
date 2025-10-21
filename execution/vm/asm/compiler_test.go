// Copyright 2019 The go-ethereum Authors
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

package asm

import (
	"testing"
)

func TestCompiler(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input, output string
	}{
		{
			input: `
	GAS
	label:
	PUSH @label
`,
			output: "5a5b6300000001",
		},
		{
			input: `
	PUSH @label
	label:
`,
			output: "63000000055b",
		},
		{
			input: `
	PUSH @label
	JUMP
	label:
`,
			output: "6300000006565b",
		},
		{
			input: `
	JUMP @label
	label:
`,
			output: "6300000006565b",
		},
	}
	for _, test := range tests {
		ch := Lex([]byte(test.input), false)
		c := NewCompiler(false)
		c.Feed(ch)
		output, err := c.Compile()
		if len(err) != 0 {
			t.Errorf("compile error: %v\ninput: %s", err, test.input)
			continue
		}
		if output != test.output {
			t.Errorf("incorrect output\ninput: %sgot:  %s\nwant: %s\n", test.input, output, test.output)
		}
	}
}
