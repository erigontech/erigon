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
		headCapture       bool
		want              bool
	}{
		{"off: zero blocks, all off", 0, false, false, false},
		{"off: zero blocks, history on", 0, true, false, false},
		{"off: zero blocks, head-capture on", 0, false, true, false},
		{"off: zero blocks, both on", 0, true, true, false},
		{"off: blocks set, both off", 8, false, false, false},
		{"on: blocks set, history on", 8, true, false, true},
		{"on: blocks set, head-capture on", 8, false, true, true},
		{"on: blocks set, both on", 8, true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := WitnessCacheShouldEnable(tc.blocks, tc.commitmentHistory, tc.headCapture); got != tc.want {
				t.Fatalf("WitnessCacheShouldEnable(%d, %v, %v) = %v, want %v", tc.blocks, tc.commitmentHistory, tc.headCapture, got, tc.want)
			}
		})
	}
}

func TestWitnessCacheMode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name              string
		blocks            uint
		commitmentHistory bool
		headCaptureFlag   bool
		wantEnable        bool
		wantHeadCapture   bool
	}{
		{"disabled: zero blocks", 0, true, true, false, false},
		{"disabled: no history, flag off", 8, false, false, false, false},
		{"durable: history on, flag off", 8, true, false, true, false},
		{"durable: history on, flag on (flag ignored under history)", 8, true, true, true, false},
		{"head-capture: no history, flag on", 8, false, true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			enable, headCapture := WitnessCacheMode(tc.blocks, tc.commitmentHistory, tc.headCaptureFlag)
			if enable != tc.wantEnable || headCapture != tc.wantHeadCapture {
				t.Fatalf("WitnessCacheMode(%d, %v, %v) = (%v, %v), want (%v, %v)",
					tc.blocks, tc.commitmentHistory, tc.headCaptureFlag, enable, headCapture, tc.wantEnable, tc.wantHeadCapture)
			}
			if wantHC := enable && tc.headCaptureFlag && !tc.commitmentHistory; headCapture != wantHC {
				t.Fatalf("head-capture must be set iff (enabled && flag && !history): got %v, want %v", headCapture, wantHC)
			}
		})
	}
}

func TestNewWitnessCacheBuilderAPIDisabled(t *testing.T) {
	t.Parallel()
	// enable=false must short-circuit to (nil, nil) before touching any wiring arg,
	// so the disabled path is a genuine no-op and APIList receives a nil cache.
	cache, impl := NewWitnessCacheBuilderAPI(false, false, nil, nil, nil, nil, nil, nil, nil, nil)
	if cache != nil {
		t.Fatalf("disabled builder returned non-nil cache")
	}
	if impl != nil {
		t.Fatalf("disabled builder returned non-nil impl")
	}
}
