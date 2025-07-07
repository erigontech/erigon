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

package seg

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
)

func TestCompressEmptyDict(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	cfg := DefaultCfg
	cfg.MinPatternScore = 100
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err = c.AddWord([]byte("word")); err != nil {
		t.Fatal(err)
	}
	if err = c.Compress(); err != nil {
		t.Fatal(err)
	}
	var d *Decompressor
	if d, err = NewDecompressor(file); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	g := d.MakeGetter()
	if !g.HasNext() {
		t.Fatalf("expected a word")
	}
	word, _ := g.Next(nil)
	if string(word) != "word" {
		t.Fatalf("expeced word, got (hex) %x", word)
	}
	if g.HasNext() {
		t.Fatalf("not expecting anything else")
	}
}

// nolint
func checksum(file string) uint32 {
	hasher := crc32.NewIEEE()
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err := io.Copy(hasher, f); err != nil {
		panic(err)
	}
	return hasher.Sum32()
}

func prepareDict(t *testing.T, multiplier int) *Decompressor {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	k := bytes.Repeat([]byte("long"), multiplier)
	v := bytes.Repeat([]byte("word"), multiplier)
	for i := 0; i < 100; i++ {
		if err = c.AddWord(nil); err != nil {
			panic(err)
		}
		if err = c.AddWord(k); err != nil {
			t.Fatal(err)
		}
		if err = c.AddWord(v); err != nil {
			t.Fatal(err)
		}
		if err = c.AddWord(bytes.Repeat([]byte(fmt.Sprintf("%d longlongword %d", i, i)), multiplier)); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.Compress(); err != nil {
		t.Fatal(err)
	}
	var d *Decompressor
	if d, err = NewDecompressor(file); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestCompressDict1(t *testing.T) {
	d := prepareDict(t, 1)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	g.Reset(0)
	for g.HasNext() {
		// next word is `nil`
		require.False(t, g.MatchPrefix([]byte("long")))
		require.True(t, g.MatchPrefix([]byte("")))
		require.True(t, g.MatchPrefix([]byte{}))

		word, _ := g.Next(nil)
		require.NotNil(t, word)
		require.Empty(t, word)

		// next word is `long`
		require.True(t, g.MatchPrefix([]byte("long")))
		require.False(t, g.MatchPrefix([]byte("longlong")))
		require.False(t, g.MatchPrefix([]byte("wordnotmatch")))
		require.False(t, g.MatchPrefix([]byte("longnotmatch")))
		require.True(t, g.MatchPrefix([]byte{}))

		_, _ = g.Next(nil)

		// next word is `word`
		require.False(t, g.MatchPrefix([]byte("long")))
		require.False(t, g.MatchPrefix([]byte("longlong")))
		require.True(t, g.MatchPrefix([]byte("word")))
		require.True(t, g.MatchPrefix([]byte("")))
		require.True(t, g.MatchPrefix(nil))
		require.False(t, g.MatchPrefix([]byte("wordnotmatch")))
		require.False(t, g.MatchPrefix([]byte("longnotmatch")))

		_, _ = g.Next(nil)

		// next word is `longlongword %d`
		expectPrefix := fmt.Sprintf("%d long", i)

		require.True(t, g.MatchPrefix([]byte(fmt.Sprintf("%d", i))))
		require.True(t, g.MatchPrefix([]byte(expectPrefix)))
		require.True(t, g.MatchPrefix([]byte(expectPrefix+"long")))
		require.True(t, g.MatchPrefix([]byte(expectPrefix+"longword ")))
		require.False(t, g.MatchPrefix([]byte("wordnotmatch")))
		require.False(t, g.MatchPrefix([]byte("longnotmatch")))
		require.True(t, g.MatchPrefix([]byte{}))

		savePos := g.dataP
		word, nextPos := g.Next(nil)
		expected := fmt.Sprintf("%d longlongword %d", i, i)
		g.Reset(savePos)
		require.Equal(t, 0, g.MatchCmp([]byte(expected)))
		g.Reset(nextPos)
		if string(word) != expected {
			t.Errorf("expected %s, got (hex) [%s]", expected, word)
		}
		i++
	}

	if cs := checksum(d.filePath); cs != 3153486123 {
		// it's ok if hash changed, but need re-generate all existing snapshot hashes
		// in https://github.com/erigontech/erigon-snapshot
		t.Errorf("result file hash changed, %d", cs)
	}
}

func TestCompressDictCmp(t *testing.T) {
	d := prepareDict(t, 1)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	g.Reset(0)
	for g.HasNext() {
		// next word is `nil`
		savePos := g.dataP
		require.Equal(t, 1, g.MatchCmp([]byte("long")))
		require.Equal(t, 0, g.MatchCmp([]byte(""))) // moves offset
		g.Reset(savePos)
		require.Equal(t, 0, g.MatchCmp([]byte{})) // moves offset
		g.Reset(savePos)

		word, _ := g.Next(nil)
		require.NotNil(t, word)
		require.Empty(t, word)

		// next word is `long`
		savePos = g.dataP
		require.Equal(t, 0, g.MatchCmp([]byte("long"))) // moves offset
		g.Reset(savePos)
		require.Equal(t, 1, g.MatchCmp([]byte("longlong")))
		require.Equal(t, 1, g.MatchCmp([]byte("wordnotmatch")))
		require.Equal(t, 1, g.MatchCmp([]byte("longnotmatch")))
		require.Equal(t, -1, g.MatchCmp([]byte{}))
		_, _ = g.Next(nil)

		// next word is `word`
		savePos = g.dataP
		require.Equal(t, -1, g.MatchCmp([]byte("long")))
		require.Equal(t, -1, g.MatchCmp([]byte("longlong")))
		require.Equal(t, 0, g.MatchCmp([]byte("word"))) // moves offset
		g.Reset(savePos)
		require.Equal(t, -1, g.MatchCmp([]byte("")))
		require.Equal(t, -1, g.MatchCmp(nil))
		require.Equal(t, 1, g.MatchCmp([]byte("wordnotmatch")))
		require.Equal(t, -1, g.MatchCmp([]byte("longnotmatch")))
		_, _ = g.Next(nil)

		// next word is `longlongword %d`
		expectPrefix := fmt.Sprintf("%d long", i)

		require.Equal(t, -1, g.MatchCmp([]byte(fmt.Sprintf("%d", i))))
		require.Equal(t, -1, g.MatchCmp([]byte(expectPrefix)))
		require.Equal(t, -1, g.MatchCmp([]byte(expectPrefix+"long")))
		require.Equal(t, -1, g.MatchCmp([]byte(expectPrefix+"longword ")))
		require.Equal(t, 1, g.MatchCmp([]byte("wordnotmatch")))
		require.Equal(t, 1, g.MatchCmp([]byte("longnotmatch")))
		require.Equal(t, -1, g.MatchCmp([]byte{}))
		savePos = g.dataP
		word, nextPos := g.Next(nil)
		expected := fmt.Sprintf("%d longlongword %d", i, i)
		g.Reset(savePos)
		require.Equal(t, 0, g.MatchCmp([]byte(expected)))
		g.Reset(nextPos)
		if string(word) != expected {
			t.Errorf("expected %s, got (hex) [%s]", expected, word)
		}
		i++
	}

	if cs := checksum(d.filePath); cs != 3153486123 {
		// it's ok if hash changed, but need re-generate all existing snapshot hashes
		// in https://github.com/erigontech/erigon-snapshot
		t.Errorf("result file hash changed, %d", cs)
	}
}
