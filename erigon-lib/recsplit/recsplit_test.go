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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
)

func TestRecSplit2(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(tmpDir, "index"),
		LeafSize:   8,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err = rs.AddKey([]byte("first_key"), 0); err != nil {
		t.Error(err)
	}
	if err = rs.Build(context.Background()); err == nil {
		t.Errorf("test is expected to fail, too few keys added")
	}
	if err = rs.AddKey([]byte("second_key"), 0); err != nil {
		t.Error(err)
	}
	if err = rs.Build(context.Background()); err != nil {
		t.Error(err)
	}
	if err = rs.Build(context.Background()); err == nil {
		t.Errorf("test is expected to fail, hash gunction was built already")
	}
	if err = rs.AddKey([]byte("key_to_fail"), 0); err == nil {
		t.Errorf("test is expected to fail, hash function was built")
	}
}

func TestRecSplitDuplicate(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(tmpDir, "index"),
		LeafSize:   8,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.AddKey([]byte("first_key"), 0); err != nil {
		t.Error(err)
	}
	if err := rs.AddKey([]byte("first_key"), 0); err != nil {
		t.Error(err)
	}
	if err := rs.Build(context.Background()); err == nil {
		t.Errorf("test is expected to fail, duplicate key")
	}
}

func TestRecSplitLeafSizeTooLarge(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	_, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(tmpDir, "index"),
		LeafSize:   64,
	}, logger)
	if err == nil {
		t.Errorf("test is expected to fail, leaf size too large")
	}
}

func TestIndexLookup(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index")
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   100,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     tmpDir,
		IndexFile:  indexFile,
		LeafSize:   8,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err = rs.AddKey([]byte(fmt.Sprintf("key %d", i)), uint64(i*17)); err != nil {
			t.Fatal(err)
		}
	}
	if err := rs.Build(context.Background()); err != nil {
		t.Fatal(err)
	}
	idx := MustOpen(indexFile)
	defer idx.Close()
	for i := 0; i < 100; i++ {
		reader := NewIndexReader(idx)
		offset := reader.Lookup([]byte(fmt.Sprintf("key %d", i)))
		if offset != uint64(i*17) {
			t.Errorf("expected offset: %d, looked up: %d", i*17, offset)
		}
	}
}

func TestTwoLayerIndex(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index")
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   100,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     tmpDir,
		IndexFile:  indexFile,
		LeafSize:   8,
		Enums:      true,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err = rs.AddKey([]byte(fmt.Sprintf("key %d", i)), uint64(i*17)); err != nil {
			t.Fatal(err)
		}
	}
	if err := rs.Build(context.Background()); err != nil {
		t.Fatal(err)
	}

	idx := MustOpen(indexFile)
	defer idx.Close()
	for i := 0; i < 100; i++ {
		reader := NewIndexReader(idx)
		e := reader.Lookup([]byte(fmt.Sprintf("key %d", i)))
		if e != uint64(i) {
			t.Errorf("expected enumeration: %d, lookup up: %d", i, e)
		}
		offset := idx.OrdinalLookup(e)
		if offset != uint64(i*17) {
			t.Errorf("expected offset: %d, looked up: %d", i*17, offset)
		}
	}
}
