package bitmapdb2

import (
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
)

func TestUpsertBitmap(t *testing.T) {
	datadir, err := os.MkdirTemp("", "bitmapdb2")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(datadir)
	db := NewBitmapDB2(datadir)
	defer db.Close()

	seed := int64(497777)
	r := rand.New(rand.NewSource(seed))

	truthBitmaps := make([]*roaring.Bitmap, 16)
	for i := 0; i < 16; i++ {
		truthBitmaps[i] = roaring.New()
	}

	batch := db.NewBatch()
	for i := 0; i < 500; i++ {
		bitmap := roaring.NewBitmap()
		for j := 0; j < 100; j++ {
			bitmap.Add(uint32(r.Int31n(1_000_000)))
		}
		removeFrom := 1_000_000 - 1000*uint64(r.Int63n(100))
		batch.TruncateBitmap("test", []byte{byte(i % 16)}, removeFrom)
		truth := truthBitmaps[i%16]
		if !truth.IsEmpty() && uint64(truth.Maximum()) >= removeFrom {
			truth.RemoveRange(removeFrom, uint64(truth.Maximum())+1)
		}
		batch.UpsertBitmap("test", []byte{byte(i % 16)}, bitmap)
		truth.Or(bitmap)
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 16; i++ {
		m, err := db.GetBitmap("test", []byte{byte(i)}, 0, 1_000_000)
		if err != nil {
			t.Fatal(err)
		}
		b1 := truthBitmaps[i].ToArray()
		b2 := m.ToArray()
		if !reflect.DeepEqual(b1, b2) {
			t.Fatalf("bitmap not match, expected: %v, actual: %v", b1, b2)
		}
	}
}

func TestParallelLoad(t *testing.T) {
	datadir, err := os.MkdirTemp("", "bitmapdb2")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(datadir)
	db := NewBitmapDB2(datadir)
	defer db.Close()

	seed := int64(6666666)
	r := rand.New(rand.NewSource(seed))

	truthBitmaps := make([]*roaring.Bitmap, 16)
	for i := 0; i < 16; i++ {
		truthBitmaps[i] = roaring.NewBitmap()
	}

	pl := NewParallelLoader(db, 7, 4096, 16)
	defer pl.Close()
	for i := 0; i < 1000; i++ {
		bitmap := roaring.NewBitmap()
		for j := 0; j < 100; j++ {
			bitmap.Add(uint32(r.Int31n(1_000_000)))
		}
		truth := truthBitmaps[i%16]
		pl.Load("test", []byte{byte(i % 16)}, bitmap)
		truth.Or(bitmap)
	}
	if err := pl.Commit(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 16; i++ {
		m, err := db.GetBitmap("test", []byte{byte(i)}, 0, 1_000_000)
		if err != nil {
			t.Fatal(err)
		}
		b1 := truthBitmaps[i].ToArray()
		b2 := m.ToArray()
		if !reflect.DeepEqual(b1, b2) {
			t.Fatalf("bitmap not match, expected: %v, actual: %v", b1, b2)
		}
	}
}

func TestParallelLoad64(t *testing.T) {
	datadir, err := os.MkdirTemp("", "bitmapdb2")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(datadir)
	db := NewBitmapDB2(datadir)
	defer db.Close()

	seed := int64(6666666)
	r := rand.New(rand.NewSource(seed))

	truthBitmaps := make([]*roaring64.Bitmap, 16)
	for i := 0; i < 16; i++ {
		truthBitmaps[i] = roaring64.NewBitmap()
	}

	pl := NewParallelLoader(db, 7, 4096, 16)
	defer pl.Close()
	for i := 0; i < 1000; i++ {
		bitmap := roaring64.NewBitmap()
		for j := 0; j < 100; j++ {
			bitmap.Add(uint64(r.Int31n(1_000_000)))
		}
		truth := truthBitmaps[i%16]
		pl.Load64("test", []byte{byte(i % 16)}, bitmap)
		truth.Or(bitmap)
	}
	if err := pl.Commit(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 16; i++ {
		m, err := db.GetBitmap("test", []byte{byte(i)}, 0, 1_000_000)
		if err != nil {
			t.Fatal(err)
		}
		b1 := truthBitmaps[i].ToArray()
		b2 := m.ToArray()
		if len(b1) != len(b2) {
			t.Fatalf("bitmap not match, expected: %v, actual: %v", b1, b2)
		}
		for i := 0; i < len(b1); i++ {
			if uint32(b1[i]) != b2[i] {
				t.Fatalf("bitmap not match, expected: %v, actual: %v", b1, b2)
			}
		}
	}
}

func TestSeekFirstGTE(t *testing.T) {
	datadir, err := os.MkdirTemp("", "bitmapdb2")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(datadir)
	db := NewBitmapDB2(datadir)
	defer db.Close()

	batch := db.NewBatch()
	defer batch.Close()
	bitmap := roaring.NewBitmap()
	bitmap.Add(1)
	bitmap.Add(3)
	bitmap.Add(5)
	bitmap.Add(33335)
	bitmap.Add(1000000)
	batch.UpsertBitmap("test", []byte{0}, bitmap)
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}

	testCases := []uint32{
		0, 1,
		1, 1,
		2, 3,
		5, 5,
		6, 33335,
		33335, 33335,
		33336, 1000000,
		1000000, 1000000,
		1000001, 0,
	}
	for i := 0; i < len(testCases); i += 2 {
		v, err := db.SeekFirstGTE("test", []byte{0}, testCases[i])
		assert.NoError(t, err)
		assert.Equal(t, testCases[i+1], v)
	}
}
