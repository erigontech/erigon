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

package offlinebal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common"
	dirs "github.com/erigontech/erigon/common/dir"
)

func hashOf(b byte) common.Hash { return common.Hash{b} }

func TestWriteThenRead(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	blocks := []struct {
		num  uint64
		hash common.Hash
		bal  []byte
	}{
		{100, hashOf(1), []byte("bal-of-100")},
		{101, hashOf(2), []byte("")},
		{102, hashOf(3), bytes.Repeat([]byte{0xAB}, 4096)},
	}
	for _, b := range blocks {
		if err := w.Append(b.num, b.hash, b.bal); err != nil {
			t.Fatalf("append %d: %v", b.num, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	if r.Len() != len(blocks) {
		t.Fatalf("Len = %d, want %d", r.Len(), len(blocks))
	}
	for _, b := range blocks {
		got, ok := r.Get(b.num, b.hash)
		if !ok {
			t.Fatalf("Get(%d) not found", b.num)
		}
		if !bytes.Equal(got, b.bal) {
			t.Fatalf("Get(%d) = %q, want %q", b.num, got, b.bal)
		}
	}
	if _, ok := r.Get(103, hashOf(9)); ok {
		t.Fatal("Get(103) found, want miss")
	}
	// Hash guard: right block number, wrong hash → miss (stale/forked BAL must not be fed).
	if _, ok := r.Get(100, hashOf(0xFF)); ok {
		t.Fatal("Get(100, wrongHash) found, want miss")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendSkipsAlreadyStored(t *testing.T) {
	// A generation run that resumes after an interruption re-executes the last
	// committed block, so its BAL is re-appended; that must be a no-op skip, not
	// an error or a duplicate record.
	dir := t.TempDir()
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(100, hashOf(1), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(100, hashOf(1), []byte("dup")); err != nil {
		t.Fatalf("re-appending the last block should be skipped, got err: %v", err)
	}
	if err := w.Append(99, hashOf(1), []byte("older")); err != nil {
		t.Fatalf("appending an already-stored lower block should be skipped, got err: %v", err)
	}
	if err := w.Append(101, hashOf(2), []byte("next")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2 (100 and 101, no duplicate)", r.Len())
	}
	if got, ok := r.Get(100, hashOf(1)); !ok || string(got) != "first" {
		t.Fatalf("block 100 = %q,%v, want first,true (original kept, not overwritten)", got, ok)
	}
	if got, ok := r.Get(101, hashOf(2)); !ok || string(got) != "next" {
		t.Fatalf("block 101 = %q,%v", got, ok)
	}
}

func TestReopenWriterAppends(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(10, hashOf(1), []byte("ten")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Append(11, hashOf(2), []byte("eleven")); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2", r.Len())
	}
	if got, ok := r.Get(10, hashOf(1)); !ok || string(got) != "ten" {
		t.Fatalf("Get(10) = %q,%v", got, ok)
	}
	if got, ok := r.Get(11, hashOf(2)); !ok || string(got) != "eleven" {
		t.Fatalf("Get(11) = %q,%v", got, ok)
	}
}

func TestOpen(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(false, false, dir)
	if err != nil || s.Writer != nil || s.Reader != nil {
		t.Fatalf("Open(off) = %+v,%v, want empty store", s, err)
	}

	if _, err := Open(true, true, dir); err == nil {
		t.Fatal("Open(generate, use) succeeded, want error")
	}

	s, err = Open(true, false, dir)
	if err != nil || s.Writer == nil || s.Reader != nil {
		t.Fatalf("Open(generate) = %+v,%v, want writer only", s, err)
	}
	if err := s.Writer.Append(7, hashOf(7), []byte("bal-of-7")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(false, true, dir)
	if err != nil || s.Writer != nil || s.Reader == nil {
		t.Fatalf("Open(use) = %+v,%v, want reader only", s, err)
	}
	defer s.Close()
	if got, ok := s.Reader.Get(7, hashOf(7)); !ok || string(got) != "bal-of-7" {
		t.Fatalf("Get(7) = %q,%v, want bal-of-7", got, ok)
	}
}

func writeBlocks(t *testing.T, dir string, nums ...uint64) {
	t.Helper()
	w, err := NewWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nums {
		if err := w.Append(n, hashOf(byte(n)), []byte{byte(n)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func requireBlocks(t *testing.T, dir string, nums ...uint64) {
	t.Helper()
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Len() != len(nums) {
		t.Fatalf("Len = %d, want %d", r.Len(), len(nums))
	}
	for _, n := range nums {
		if got, ok := r.Get(n, hashOf(byte(n))); !ok || !bytes.Equal(got, []byte{byte(n)}) {
			t.Fatalf("Get(%d) = %v,%v", n, got, ok)
		}
	}
}

func TestReaderNeedsIndex(t *testing.T) {
	dir := t.TempDir()
	writeBlocks(t, dir, 1, 2)
	if err := dirs.RemoveFile(filepath.Join(dir, indexFileName)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(dir); err == nil {
		t.Fatal("OpenReader without an index succeeded, want error")
	}
}

func TestWriterBuildsMissingIndex(t *testing.T) {
	dir := t.TempDir()
	writeBlocks(t, dir, 1, 2)
	if err := dirs.RemoveFile(filepath.Join(dir, indexFileName)); err != nil {
		t.Fatal(err)
	}
	writeBlocks(t, dir, 3)
	requireBlocks(t, dir, 1, 2, 3)
}

func TestIndexGap(t *testing.T) {
	dir := t.TempDir()
	writeBlocks(t, dir, 5, 7)
	requireBlocks(t, dir, 5, 7)
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, n := range []uint64{4, 6, 8} {
		if _, ok := r.Get(n, hashOf(byte(n))); ok {
			t.Fatalf("Get(%d) found, want miss", n)
		}
	}
}

func TestWriterDropsUnindexedTail(t *testing.T) {
	dir := t.TempDir()
	writeBlocks(t, dir, 1)
	f, err := os.OpenFile(filepath.Join(dir, fileName), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("torn record without an index entry"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	writeBlocks(t, dir, 2)
	requireBlocks(t, dir, 1, 2)
}

func TestWriterRebuildsHeaderOnlyIndex(t *testing.T) {
	dir := t.TempDir()
	writeBlocks(t, dir, 1, 2)
	if err := os.Truncate(filepath.Join(dir, indexFileName), int64(indexHeaderSize)); err != nil {
		t.Fatal(err)
	}
	writeBlocks(t, dir, 3)
	requireBlocks(t, dir, 1, 2, 3)
}
