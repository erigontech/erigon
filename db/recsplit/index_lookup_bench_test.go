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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/version"
)

var benchIndexCache = map[string]*Index{}

// buildBenchIndex builds (or returns cached) index so -count=N reruns don't pay the build cost
func buildBenchIndex(b *testing.B, keys [][]byte, enums, lessFalsePositives bool, version version.DataStructureVersion) *Index {
	b.Helper()
	cacheKey := fmt.Sprintf("%t_%t_%d_%x", enums, lessFalsePositives, version, keys[0][:4])
	if idx, ok := benchIndexCache[cacheKey]; ok {
		return idx
	}
	logger := log.New()
	tmpDir, err := os.MkdirTemp("", "recsplit_lookup_bench")
	if err != nil {
		b.Fatal(err)
	}
	salt := uint32(1)
	indexFile := filepath.Join(tmpDir, "index")
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:           len(keys),
		BucketSize:         DefaultBucketSize,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          indexFile,
		LeafSize:           DefaultLeafSize,
		Enums:              enums,
		LessFalsePositives: lessFalsePositives,
		Version:            version,
		NoFsync:            true,
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
	benchIndexCache[cacheKey] = idx
	return idx
}

// BenchmarkIndexLookupMiss mirrors the domain/ii read pattern: one key checked
// against a list of files where it is absent — the existence-filter fast path.
func BenchmarkIndexLookupMiss(b *testing.B) {
	const KeysN = 250_000
	const Files = 4
	indexes := make([]*Index, Files)
	for f := 0; f < Files; f++ {
		keys := make([][]byte, KeysN)
		rnd := rand.New(rand.NewSource(int64(100 + f)))
		for j := range keys {
			keys[j] = make([]byte, 20)
			rnd.Read(keys[j])
		}
		indexes[f] = buildBenchIndex(b, keys, true, true, 2)
	}
	missKeys := make([][]byte, KeysN)
	rnd := rand.New(rand.NewSource(999))
	for j := range missKeys {
		missKeys[j] = make([]byte, 20)
		rnd.Read(missKeys[j])
	}
	readers := make([]*IndexReader, Files)
	for f := range indexes {
		readers[f] = NewIndexReader(indexes[f])
	}
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := missKeys[int((uint64(i)*0xDEECE66D)%KeysN)]
		hi, lo := readers[0].Sum(key)
		for f := 0; f < Files; f++ {
			if _, ok := readers[f].TwoLayerLookupByHash(hi, lo); ok {
				break // false positive, ~0.4% per file
			}
		}
	}
}

func BenchmarkIndexLookup(b *testing.B) {
	const KeysN = 1_000_000
	// 20-byte pseudo-random keys, like the address/hash keys of production .idx/.kvi files
	keys := make([][]byte, KeysN)
	rnd := rand.New(rand.NewSource(7))
	for j := range keys {
		keys[j] = make([]byte, 20)
		rnd.Read(keys[j])
	}

	for _, tc := range []struct {
		name               string
		enums              bool
		lessFalsePositives bool
		version            version.DataStructureVersion
		lookup2            bool
	}{
		{name: "plain", enums: false, lessFalsePositives: false, version: 0},
		{name: "enums", enums: true, lessFalsePositives: false, version: 0},
		{name: "enums_lfp_v2", enums: true, lessFalsePositives: true, version: 2},
		// production Lookup2 targets (.vi files) are built without enums
		{name: "lookup2", enums: false, lessFalsePositives: false, version: 0, lookup2: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			idx := buildBenchIndex(b, keys, tc.enums, tc.lessFalsePositives, tc.version)
			reader := NewIndexReader(idx)
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				key := keys[int((uint64(i)*0xDEECE66D)%KeysN)]
				var offset uint64
				var ok bool
				switch {
				case tc.lookup2:
					offset, ok = reader.Lookup2(key[:8], key[8:])
				case tc.enums:
					offset, ok = reader.TwoLayerLookup(key)
				default:
					offset, ok = reader.Lookup(key)
				}
				if !ok {
					b.Fatalf("key not found: %s", key)
				}
				_ = offset
			}
		})
	}
}
