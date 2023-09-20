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

package compress

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestCompressEmptyDict(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 100, 1, log.LvlDebug, logger)
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

func prepareDict(t *testing.T) *Decompressor {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 100; i++ {
		if err = c.AddWord(nil); err != nil {
			panic(err)
		}
		if err = c.AddWord([]byte("long")); err != nil {
			t.Fatal(err)
		}
		if err = c.AddWord([]byte("word")); err != nil {
			t.Fatal(err)
		}
		if err = c.AddWord([]byte(fmt.Sprintf("%d longlongword %d", i, i))); err != nil {
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
	d := prepareDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	g.Reset(0)
	for g.HasNext() {
		// next word is `nil`
		require.False(t, g.MatchPrefix([]byte("long")))
		require.True(t, g.MatchPrefix([]byte("")))
		require.True(t, g.MatchPrefix([]byte{}))

		require.Equal(t, 1, g.MatchPrefixCmp([]byte("long")))
		require.Equal(t, 0, g.MatchPrefixCmp([]byte("")))
		require.Equal(t, 0, g.MatchPrefixCmp([]byte{}))
		word, _ := g.Next(nil)
		require.NotNil(t, word)
		require.Zero(t, len(word))

		// next word is `long`
		require.True(t, g.MatchPrefix([]byte("long")))
		require.False(t, g.MatchPrefix([]byte("longlong")))
		require.False(t, g.MatchPrefix([]byte("wordnotmatch")))
		require.False(t, g.MatchPrefix([]byte("longnotmatch")))
		require.True(t, g.MatchPrefix([]byte{}))

		require.Equal(t, 0, g.MatchPrefixCmp([]byte("long")))
		require.Equal(t, 1, g.MatchPrefixCmp([]byte("longlong")))
		require.Equal(t, 1, g.MatchPrefixCmp([]byte("wordnotmatch")))
		require.Equal(t, 1, g.MatchPrefixCmp([]byte("longnotmatch")))
		require.Equal(t, 0, g.MatchPrefixCmp([]byte{}))
		_, _ = g.Next(nil)

		// next word is `word`
		require.False(t, g.MatchPrefix([]byte("long")))
		require.False(t, g.MatchPrefix([]byte("longlong")))
		require.True(t, g.MatchPrefix([]byte("word")))
		require.True(t, g.MatchPrefix([]byte("")))
		require.True(t, g.MatchPrefix(nil))
		require.False(t, g.MatchPrefix([]byte("wordnotmatch")))
		require.False(t, g.MatchPrefix([]byte("longnotmatch")))

		require.Equal(t, -1, g.MatchPrefixCmp([]byte("long")))
		require.Equal(t, -1, g.MatchPrefixCmp([]byte("longlong")))
		require.Equal(t, 0, g.MatchPrefixCmp([]byte("word")))
		require.Equal(t, 0, g.MatchPrefixCmp([]byte("")))
		require.Equal(t, 0, g.MatchPrefixCmp(nil))
		require.Equal(t, 1, g.MatchPrefixCmp([]byte("wordnotmatch")))
		require.Equal(t, -1, g.MatchPrefixCmp([]byte("longnotmatch")))
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

		require.Equal(t, 0, g.MatchPrefixCmp([]byte(fmt.Sprintf("%d", i))))
		require.Equal(t, 0, g.MatchPrefixCmp([]byte(expectPrefix)))
		require.Equal(t, 0, g.MatchPrefixCmp([]byte(expectPrefix+"long")))
		require.Equal(t, 0, g.MatchPrefixCmp([]byte(expectPrefix+"longword ")))
		require.Equal(t, 1, g.MatchPrefixCmp([]byte("wordnotmatch")))
		require.Equal(t, 1, g.MatchPrefixCmp([]byte("longnotmatch")))
		require.Equal(t, 0, g.MatchPrefixCmp([]byte{}))
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
		// in https://github.com/ledgerwatch/erigon-snapshot
		t.Errorf("result file hash changed, %d", cs)
	}
}

func TestCompressDictCmp(t *testing.T) {
	d := prepareDict(t)
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
		require.Zero(t, len(word))

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
		// in https://github.com/ledgerwatch/erigon-snapshot
		t.Errorf("result file hash changed, %d", cs)
	}
}
