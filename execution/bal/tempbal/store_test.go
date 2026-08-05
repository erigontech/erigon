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

package tempbal

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common"
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
