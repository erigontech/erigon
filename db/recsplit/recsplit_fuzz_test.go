// Copyright 2021 The Erigon Authors
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

package recsplit

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
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
		salt := uint32(1)
		rs, err := NewRecSplit(RecSplitArgs{
			KeyCount:   count,
			Enums:      true,
			BucketSize: 10,
			Salt:       &salt,
			TmpDir:     tmpDir,
			IndexFile:  indexFile,
			LeafSize:   8,
		}, logger)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
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
		defer idx.Close()
		bitCount := (count + 63) / 64
		bits := make([]uint64, bitCount)
		reader := NewIndexReader(idx)
		for i = 0; i < len(in)-l; i += l {
			off, _ = reader.Lookup(in[i : i+l])
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
