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

package jsonrpc

import "testing"

func TestWitnessCacheShouldEnable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name              string
		blocks            uint
		commitmentHistory bool
		want              bool
	}{
		{"off: zero blocks, history off", 0, false, false},
		{"off: zero blocks, history on", 0, true, false},
		{"off: blocks set but history off", 8, false, false},
		{"on: blocks set and history on", 8, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := WitnessCacheShouldEnable(tc.blocks, tc.commitmentHistory); got != tc.want {
				t.Fatalf("WitnessCacheShouldEnable(%d, %v) = %v, want %v", tc.blocks, tc.commitmentHistory, got, tc.want)
			}
		})
	}
}

func TestNewWitnessCacheBuilderAPIDisabled(t *testing.T) {
	t.Parallel()
	// enable=false must short-circuit to (nil, nil) before touching any wiring arg,
	// so the disabled path is a genuine no-op and APIList receives a nil cache.
	cache, impl := NewWitnessCacheBuilderAPI(false, nil, nil, nil, nil, nil, nil, nil, nil)
	if cache != nil {
		t.Fatalf("disabled builder returned non-nil cache")
	}
	if impl != nil {
		t.Fatalf("disabled builder returned non-nil impl")
	}
}
