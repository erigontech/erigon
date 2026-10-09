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

//go:build linux

package offlinebal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/erigontech/erigon/common/mmap"
)

func TestReleaseDropsRecordPages(t *testing.T) {
	dir := t.TempDir()
	pg := os.Getpagesize()
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(1, hashOf(1), bytes.Repeat([]byte{1}, 4*pg)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, ok := r.Get(1, hashOf(1))
	if !ok {
		t.Fatal("Get(1) missing")
	}
	var sum byte
	for i := 0; i < len(data); i += pg {
		sum += data[i]
	}
	r.Release(data)

	skip := (pg - int(uintptr(unsafe.Pointer(&data[0]))%uintptr(pg))) % pg
	if res, err := mmap.Resident(data[skip+pg : skip+2*pg]); err != nil || res {
		t.Fatalf("record page resident=%v err=%v after Release, want dropped (sum %d)", res, err, sum)
	}
}

func TestReleaseDropsSharedBoundaryPageOfPreviousRecord(t *testing.T) {
	dir := t.TempDir()
	pg := os.Getpagesize()
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for n := uint64(1); n <= 2; n++ {
		if err := w.Append(n, hashOf(byte(n)), bytes.Repeat([]byte{byte(n)}, 2*pg+pg/2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	first, _ := r.Get(1, hashOf(1))
	second, _ := r.Get(2, hashOf(2))
	var sum byte
	for _, d := range [][]byte{first, second} {
		for i := range d {
			sum += d[i]
		}
	}
	r.Release(first)
	r.Release(second)

	last := first[len(first)-1:]
	pageStart := uintptr(unsafe.Pointer(&last[0])) &^ uintptr(pg-1)
	boundary := unsafe.Slice((*byte)(unsafe.Pointer(pageStart)), pg)
	if res, err := mmap.Resident(boundary); err != nil || res {
		t.Fatalf("boundary page resident=%v err=%v after releasing both records, want dropped (sum %d)", res, err, sum)
	}
}
