//go:build !nofuzz

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
	"context"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
)

// go test -trimpath -v -fuzz=FuzzRecSplit -fuzztime=10s ./recsplit

func FuzzRecSplit(f *testing.F) {
	logger := log.New()
	f.Add(2, []byte("1stkey2ndkey"))
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
		indexFile := filepath.Join(tmpDir, "index")
		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount:   count,
			Enums:      true,
			BucketSize: 10,
			Salt:       0,
			TmpDir:     tmpDir,
			IndexFile:  indexFile,
			LeafSize:   8,
		}, logger)
		if err != nil {
			t.Fatal(err)
		}
		var off uint64
		for i = 0; i < len(in)-l; i += l {
			if err := rs.AddKey(in[i:i+l], off); err != nil {
				t.Fatal(err)
			}
			off++
		}
		if err := rs.AddKey(in[i:], off); err != nil {
			t.Fatal(err)
		}
		if err = rs.Build(context.Background()); err != nil {
			t.Fatal(err)
		}
		// Check that there is a bijection
		idx := MustOpen(indexFile)
		bitCount := (count + 63) / 64
		bits := make([]uint64, bitCount)
		reader := NewIndexReader(idx)
		for i = 0; i < len(in)-l; i += l {
			off = reader.Lookup(in[i : i+l])
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
