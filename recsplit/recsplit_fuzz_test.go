//go:build gofuzzbeta
// +build gofuzzbeta

/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package recsplit

import (
	"testing"
)

// gotip test -trimpath -v -fuzz=Fuzz -fuzztime=10s ./recsplit

func FuzzRecSplit(f *testing.F) {
	f.Add(2, "1stkey2ndkey")
	f.Fuzz(func(t *testing.T, count int, in []byte) {
		if count < 1 {
			t.Skip()
		}
		if len(in) < count {
			t.Skip()
		}
		// split in into count keys
		dups := make(map[string]struct{})
		// Length of one key
		l := (len(in) + count - 1) / count
		var i int
		for i = 0; i < len(in)-l; i += l {
			dups[string(in[i:i+l])] = struct{}{}
		}
		dups[string(in[i:])] = struct{}{}
		if len(dups) != count {
			t.Skip()
		}
		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount:   count,
			BucketSize: 10,
			Salt:       0,
			TmpDir:     t.TempDir(),
			LeafSize:   8,
			StartSeed:  []uint32{5, 100034, 405060, 60606, 70000, 80000, 90000, 10000, 11000, 12000},
		})
		if err != nil {
			t.Error(err)
		}
		for i = 0; i < len(in)-l; i += l {
			rs.AddKey(in[i : i+l])
		}
		rs.AddKey(in[i:])
		if err = rs.Build(); err != nil {
			t.Error(err)
		}
		// Check that there is a bijection
		bitCount := (count + 63) / 64
		bits := make([]uint64, bitCount)
		for i = 0; i < len(in)-l; i += l {
			idx := rs.Lookup(in[i : i+l])
			if idx >= count {
				t.Errorf("idx %d >= count %d", idx, count)
			}
			mask := uint64(1) << (idx & 63)
			if bits[idx>>6]&mask != 0 {
				t.Fatalf("no bijection count=%d", count)
			}
			bits[idx>>6] |= mask
		}
	})
}
