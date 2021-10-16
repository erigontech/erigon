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
	"path"
	"testing"
)

// gotip test -trimpath -v -fuzz=FuzzRecSplit -fuzztime=10s ./recsplit

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
		tmpDir := t.TempDir()
		indexFile := path.Join(tmpDir, "index")
		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount:   count,
			BucketSize: 10,
			Salt:       0,
			TmpDir:     tmpDir,
			IndexFile:  indexFile,
			LeafSize:   8,
			StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
				0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
				0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		})
		if err != nil {
			t.Fatal(err)
		}
		var off uint64
		for i = 0; i < len(in)-l; i += l {
			rs.AddKey(in[i:i+l], off)
			off++
		}
		rs.AddKey(in[i:], off)
		if err = rs.Build(); err != nil {
			t.Fatal(err)
		}
		// Check that there is a bijection
		var idx *Index
		if idx, err = NewIndex(indexFile); err != nil {
			t.Fatal(err)
		}
		bitCount := (count + 63) / 64
		bits := make([]uint64, bitCount)
		for i = 0; i < len(in)-l; i += l {
			off = idx.Lookup(in[i : i+l])
			if int(off) >= count {
				t.Errorf("off %d >= count %d", off, count)
			}
			mask := uint64(1) << (off & 63)
			if bits[off>>6]&mask != 0 {
				t.Fatalf("no bijection count=%d, i=%d", count, i/l)
			}
			bits[off>>6] |= mask
		}
	})
}
