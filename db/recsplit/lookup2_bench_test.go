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

package recsplit

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
)

// buildLookup2BenchIndex builds an index over 20-byte random keys,
// looked up as the txnKey(8)||key(12) split that production Lookup2 callers use.
func buildLookup2BenchIndex(b *testing.B, keys [][]byte) *Index {
	b.Helper()
	logger := log.New()
	tmpDir := b.TempDir()
	salt := uint32(1)
	indexFile := filepath.Join(tmpDir, "index")
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   len(keys),
		BucketSize: DefaultBucketSize,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  indexFile,
		LeafSize:   DefaultLeafSize,
		Enums:      false, // production Lookup2 targets (.vi files) are built without enums
		NoFsync:    true,
	}, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer rs.Close()
	for j, key := range keys {
		if err = rs.AddKey(key, uint64(j*17)); err != nil {
			b.Fatal(err)
		}
	}
	if err := rs.Build(b.Context()); err != nil {
		b.Fatal(err)
	}
	idx := MustOpen(indexFile)
	b.Cleanup(idx.Close)
	return idx
}

func lookup2BenchKeys(b *testing.B) [][]byte {
	b.Helper()
	const KeysN = 200_000
	keys := make([][]byte, KeysN)
	rnd := rand.New(rand.NewSource(7))
	for j := range keys {
		keys[j] = make([]byte, 20)
		rnd.Read(keys[j])
	}
	return keys
}

func BenchmarkLookup2(b *testing.B) {
	keys := lookup2BenchKeys(b)
	reader := NewIndexReader(buildLookup2BenchIndex(b, keys))
	b.ResetTimer()
	for i := uint64(0); b.Loop(); i++ {
		key := keys[int((i*0xDEECE66D)%uint64(len(keys)))]
		if _, ok := reader.Lookup2(key[:8], key[8:]); !ok {
			b.Fatal("not found")
		}
	}
}

func BenchmarkLookup2Parallel(b *testing.B) {
	keys := lookup2BenchKeys(b)
	reader := NewIndexReader(buildLookup2BenchIndex(b, keys)) // one shared reader, like a shared pool entry under load
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			key := keys[int((i*0xDEECE66D)%uint64(len(keys)))]
			if _, ok := reader.Lookup2(key[:8], key[8:]); !ok {
				b.Fatal("not found")
			}
			i++
		}
	})
}
