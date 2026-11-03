package state

import (
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// BenchmarkInvertedIndexSeekInFiles measures the per-seek cost of the file-scan path.
// Run with II_LRU_ENABLED=false to bypass the seek cache, otherwise the cache short-circuits
// before the sequence decoding this is meant to measure.
func BenchmarkInvertedIndexSeekInFiles(b *testing.B) {
	logger := log.New()
	db, ii, txs := filledInvIndexOfSize(b, 1000, 16, 31, logger)
	ctx := b.Context()

	tx, err := db.BeginRw(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	for step := kv.Step(0); step < kv.Step(txs/ii.stepSize)-1; step++ {
		if err := ii.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()); err != nil {
			b.Fatal(err)
		}
	}

	iit := ii.beginForTests()
	defer iit.Close()
	if len(iit.files) == 0 {
		b.Fatal("no visible files: benchmark would not reach the seek path")
	}

	keys := make([][8]byte, 31)
	for i := range keys {
		binary.BigEndian.PutUint64(keys[i][:], uint64(i+1))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[i%len(keys)]
		if _, _, err := iit.seekInFiles(k[:], uint64(i%900)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIISeekInFilesCacheHit(b *testing.B) {
	requireIISeekCache(b)
	iit := storageDomainTxWithFileKey(b, equalHashSalt, equalHashKeyA, []byte{1}).ht.iit
	if found, _, err := iit.seekInFiles(equalHashKeyA, 0); err != nil || !found {
		b.Fatal(found, err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := iit.seekInFiles(equalHashKeyA, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIISeekInFilesCacheInsert(b *testing.B) {
	requireIISeekCache(b)
	c := NewIISeekInFilesCache()
	b.ReportAllocs()
	for i := uint64(0); b.Loop(); i++ {
		c.add(equalHashKeyA, i*0x9E3779B97F4A7C15, iiSeekInFilesCacheItem{requested: i, found: i + 1, fileIdx: 1})
	}
}

func requireIISeekCache(b *testing.B) {
	if !iiGetFromFileCacheEnabled || iiGetFromFileCacheLimit == 0 {
		b.Fatal("the II seek cache is disabled (II_LRU_ENABLED=false or II_LRU=0): nothing to measure")
	}
}
