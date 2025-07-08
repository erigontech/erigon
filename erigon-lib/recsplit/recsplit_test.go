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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/stretchr/testify/assert"
)

func TestRecSplit2(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	salt := uint32(1)
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(tmpDir, "index"),
		LeafSize:   8,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
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
	salt := uint32(1)
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(tmpDir, "index"),
		LeafSize:   8,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
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
	salt := uint32(1)
	_, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       &salt,
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
	salt := uint32(1)
	test := func(t *testing.T, cfg RecSplitArgs) {
		t.Helper()
		rs, err := NewRecSplit(cfg, logger)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
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
			offset, ok := reader.Lookup([]byte(fmt.Sprintf("key %d", i)))
			assert.True(t, ok)
			if offset != uint64(i*17) {
				t.Errorf("expected offset: %d, looked up: %d", i*17, offset)
			}
		}
	}
	cfg := RecSplitArgs{
		KeyCount:   100,
		BucketSize: 10,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  indexFile,
		LeafSize:   8,

		Enums:              false,
		LessFalsePositives: true, //must not impact index when `Enums: false`
	}
	t.Run("v0", func(t *testing.T) {
		test(t, cfg)
	})
	t.Run("v1", func(t *testing.T) {
		cfg := cfg
		cfg.Version = 1
		test(t, cfg)
	})
}

func TestTwoLayerIndex(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index")
	salt := uint32(1)
	N := 2571
	test := func(t *testing.T, cfg RecSplitArgs) {
		t.Helper()
		rs, err := NewRecSplit(cfg, logger)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		for i := 0; i < N; i++ {
			if err = rs.AddKey([]byte(fmt.Sprintf("key %d", i)), uint64(i*17)); err != nil {
				t.Fatal(err)
			}
		}
		if err := rs.Build(context.Background()); err != nil {
			t.Fatal(err)
		}

		idx := MustOpen(indexFile)
		defer idx.Close()
		for i := 0; i < N; i++ {
			reader := NewIndexReader(idx)
			e, _ := reader.Lookup([]byte(fmt.Sprintf("key %d", i)))
			if e != uint64(i) {
				t.Errorf("expected enumeration: %d, lookup up: %d", i, e)
			}
			offset := idx.OrdinalLookup(e)
			if offset != uint64(i*17) {
				t.Errorf("expected offset: %d, looked up: %d", i*17, offset)
			}
		}
	}
	cfg := RecSplitArgs{
		KeyCount:           N,
		BucketSize:         10,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          indexFile,
		LeafSize:           8,
		Enums:              true,
		LessFalsePositives: true,
	}
	t.Run("v0", func(t *testing.T) {
		test(t, cfg)
	})
	t.Run("v1", func(t *testing.T) {
		cfg := cfg
		cfg.Version = 1
		test(t, cfg)
	})
}
