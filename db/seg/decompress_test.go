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
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/dir"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func prepareLoremDict(t *testing.T) *Decompressor {
	t.Helper()
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
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
	for k, w := range loremStrings {
		if err = c.AddWord(fmt.Appendf(nil, "%s %d", w, k)); err != nil {
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

func TestDecompressSkip(t *testing.T) {
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		if i%2 == 0 {
			g.Skip()
		} else {
			word, _ := g.Next(nil)
			expected := fmt.Sprintf("%s %d", w, i)
			if string(word) != expected {
				t.Errorf("expected %s, got (hex) %s", expected, word)
			}
		}
		i++
	}

	g.Reset(0)
	_, offset := g.Next(nil)
	require.Equal(t, 8, int(offset))
	_, offset = g.Next(nil)
	require.Equal(t, 16, int(offset))
}

func TestDecompressMatchOK(t *testing.T) {
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		if i%2 != 0 {
			expected := fmt.Sprintf("%s %d", w, i)
			cmp := g.MatchCmp([]byte(expected))
			if cmp != 0 {
				t.Errorf("expexted match with %s", expected)
			}
		} else {
			word, _ := g.Next(nil)
			expected := fmt.Sprintf("%s %d", w, i)
			if string(word) != expected {
				t.Errorf("expected %s, got (hex) %s", expected, word)
			}
		}
		i++
	}
}

func TestDecompressMatchCmpOK(t *testing.T) {
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		if i%2 != 0 {
			expected := fmt.Sprintf("%s %d", w, i)
			result := g.MatchCmp([]byte(expected))
			if result != 0 {
				t.Errorf("expexted match with %s", expected)
			}
		} else {
			word, _ := g.Next(nil)
			expected := fmt.Sprintf("%s %d", w, i)
			if string(word) != expected {
				t.Errorf("expected %s, got (hex) %s", expected, word)
			}
		}
		i++
	}
}

func prepareStupidDict(t *testing.T, size int) *Decompressor {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed2")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < size; i++ {
		if err = c.AddWord(fmt.Appendf(nil, "word-%d", i)); err != nil {
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

func TestDecompressMatchOKCondensed(t *testing.T) {
	d := prepareStupidDict(t, 10000)
	defer d.Close()

	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		if i%2 != 0 {
			expected := fmt.Sprintf("word-%d", i)
			cmp := g.MatchCmp([]byte(expected))
			if cmp != 0 {
				t.Errorf("expexted match with %s", expected)
			}
		} else {
			word, _ := g.Next(nil)
			expected := fmt.Sprintf("word-%d", i)
			if string(word) != expected {
				t.Errorf("expected %s, got (hex) %s", expected, word)
			}
		}
		i++
	}
}

func TestDecompressMatchNotOK(t *testing.T) {
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	skipCount := 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := fmt.Sprintf("%s %d", w, i+1)
		cmp := g.MatchCmp([]byte(expected))
		if cmp == 0 {
			t.Errorf("not expexted match with %s", expected)
		} else {
			g.Skip()
			skipCount++
		}
		i++
	}
	if skipCount != i {
		t.Errorf("something wrong with match logic")
	}
}

func TestDecompressMatchPrefix(t *testing.T) {
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	skipCount := 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := fmt.Appendf(nil, "%s %d", w, i+1)
		expected = expected[:len(expected)/2]
		if !g.MatchPrefix(expected) {
			t.Errorf("expexted match with %s", expected)
		}
		g.Skip()
		skipCount++
		i++
	}
	if skipCount != i {
		t.Errorf("something wrong with match logic")
	}
	g.Reset(0)
	skipCount = 0
	i = 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := fmt.Appendf(nil, "%s %d", w, i+1)
		expected = expected[:len(expected)/2]
		if len(expected) > 0 {
			expected[len(expected)-1]++
			if g.MatchPrefix(expected) {
				t.Errorf("not expexted match with %s", expected)
			}
		}
		g.Skip()
		skipCount++
		i++
	}
}

func prepareLoremDictUncompressed(t *testing.T) *Decompressor {
	t.Helper()
	loremStrings := append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
	slices.Sort(loremStrings)

	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	for k, w := range loremStrings {
		if len(w) == 0 {
			err = c.AddUncompressedWord([]byte(w))
			require.NoError(t, err)
			continue
		}
		err = c.AddUncompressedWord(fmt.Appendf(nil, "%s %d", w, k))
		require.NoError(t, err)
	}
	err = c.Compress()
	require.NoError(t, err)
	d, err := NewDecompressor(file)
	require.NoError(t, err)
	t.Cleanup(d.Close)
	return d
}

func TestUncompressed(t *testing.T) {
	var loremStrings = append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
	d := prepareLoremDictUncompressed(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	var offsets []uint64
	offsets = append(offsets, 0)
	for g.HasNext() {
		w := loremStrings[i]
		expected := fmt.Appendf(nil, "%s %d", w, i+1)
		expected = expected[:len(expected)/2]
		actual, offset := g.NextUncompressed()
		if bytes.Equal(expected, actual) {
			t.Errorf("expected %s, actual %s", expected, actual)
		}
		i++
		offsets = append(offsets, offset)
	}

	t.Run("BinarySearch middle", func(t *testing.T) {
		require := require.New(t)
		_, ok := g.BinarySearch([]byte("ipsum"), d.Count(), func(i uint64) (offset uint64) { return offsets[i] })
		require.True(ok)
		k, _ := g.Next(nil)
		require.Equal("ipsum 38", string(k))
		_, ok = g.BinarySearch([]byte("ipsu"), d.Count(), func(i uint64) (offset uint64) { return offsets[i] })
		require.True(ok)
		k, _ = g.Next(nil)
		require.Equal("ipsum 38", string(k))
	})
	t.Run("BinarySearch end of file", func(t *testing.T) {
		require := require.New(t)
		//last word is `voluptate`
		_, ok := g.BinarySearch([]byte("voluptate"), d.Count(), func(i uint64) (offset uint64) { return offsets[i] })
		require.True(ok)
		k, _ := g.Next(nil)
		require.Equal("voluptate 69", string(k))
		_, ok = g.BinarySearch([]byte("voluptat"), d.Count(), func(i uint64) (offset uint64) { return offsets[i] })
		require.True(ok)
		k, _ = g.Next(nil)
		require.Equal("voluptate 69", string(k))
		_, ok = g.BinarySearch([]byte("voluptatez"), d.Count(), func(i uint64) (offset uint64) { return offsets[i] })
		require.False(ok)
	})

	t.Run("BinarySearch begin of file", func(t *testing.T) {
		require := require.New(t)
		//first word is ``
		_, ok := g.BinarySearch([]byte(""), d.Count(), func(i uint64) (offset uint64) { return offsets[i] })
		require.True(ok)
		k, _ := g.Next(nil)
		require.Empty(string(k))

		_, ok = g.BinarySearch(nil, d.Count(), func(i uint64) (offset uint64) { return offsets[i] })
		require.True(ok)
		k, _ = g.Next(nil)
		require.Empty(string(k))
	})

}

func TestDecompressor_OpenCorrupted(t *testing.T) {
	var loremStrings = append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
	logger := log.New()
	tmpDir := t.TempDir()

	t.Run("uncompressed", func(t *testing.T) {
		file := filepath.Join(tmpDir, "unc")
		cfg := DefaultCfg
		cfg.MinPatternScore = 1
		cfg.Workers = 2
		c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
		require.NoError(t, err)
		defer c.Close()
		for k, w := range loremStrings {
			if err = c.AddUncompressedWord(fmt.Appendf(nil, "%s %d", w, k)); err != nil {
				t.Fatal(err)
			}
		}
		err = c.Compress()
		require.NoError(t, err)

		d, err := NewDecompressor(file)
		require.NoError(t, err)
		require.NotNil(t, d)
		d.Close()
	})

	t.Run("uncompressed_empty", func(t *testing.T) {
		file := filepath.Join(tmpDir, "unc_empty")
		cfg := DefaultCfg
		cfg.MinPatternScore = 1
		cfg.Workers = 2
		c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
		require.NoError(t, err)
		defer c.Close()
		err = c.Compress()
		require.NoError(t, err)

		// this file is empty and its size will be 32 bytes, it's not corrupted
		d, err := NewDecompressor(file)
		require.NoError(t, err)
		require.NotNil(t, d)
		d.Close()
	})

	t.Run("compressed", func(t *testing.T) {
		file := filepath.Join(tmpDir, "comp")
		cfg := DefaultCfg
		cfg.MinPatternScore = 1
		cfg.Workers = 2
		c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
		require.NoError(t, err)
		defer c.Close()
		for k, w := range loremStrings {
			if err = c.AddWord(fmt.Appendf(nil, "%s %d", w, k)); err != nil {
				t.Fatal(err)
			}
		}
		err = c.Compress()
		require.NoError(t, err)

		d, err := NewDecompressor(file)
		require.NoError(t, err)
		require.NotNil(t, d)
		d.Close()
	})

	t.Run("compressed_empty", func(t *testing.T) {
		file := filepath.Join(tmpDir, "comp_empty")
		cfg := DefaultCfg
		cfg.MinPatternScore = 1
		cfg.Workers = 2
		c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
		require.NoError(t, err)
		defer c.Close()
		err = c.Compress()
		require.NoError(t, err)

		d, err := NewDecompressor(file)
		require.NoError(t, err)
		require.NotNil(t, d)
		d.Close()
	})

	t.Run("notExist", func(t *testing.T) {
		file := filepath.Join(tmpDir, "comp_bad")
		d, err := NewDecompressor(file)
		require.Error(t, err, "file is not exist")
		require.Nil(t, d)
	})

	t.Run("fileSize<compressedMinSize", func(t *testing.T) {
		aux := make([]byte, compressedMinSize-1)
		_, err := rand.Read(aux)
		require.NoError(t, err)

		fpath := filepath.Join(tmpDir, "1gibberish")
		err = os.WriteFile(fpath, aux, 0644)
		require.NoError(t, err)

		d, err := NewDecompressor(fpath)
		require.ErrorIsf(t, err, &ErrCompressedFileCorrupted{},
			"file is some garbage or smaller compressedMinSize(%d) bytes, got error %v", compressedMinSize, err)
		require.Nil(t, d)

		err = dir.RemoveFile(fpath)
		require.NoError(t, err)

		aux = make([]byte, compressedMinSize)
		err = os.WriteFile(fpath, aux, 0644)
		require.NoError(t, err)

		d, err = NewDecompressor(fpath)
		require.NoErrorf(t, err, "should read empty but correct file")
		require.NotNil(t, d)
		d.Close()
	})
	t.Run("invalidPatternDictionarySize", func(t *testing.T) {
		aux := make([]byte, 32)

		// wordCount=0
		// emptyWordCount=0
		binary.BigEndian.PutUint64(aux[16:24], 10) // pattern dict size in bytes

		fpath := filepath.Join(tmpDir, "invalidPatternDictionarySize")
		err := os.WriteFile(fpath, aux, 0644)
		require.NoError(t, err)

		d, err := NewDecompressor(fpath)
		require.ErrorIsf(t, err, &ErrCompressedFileCorrupted{},
			"file contains incorrect pattern dictionary size in bytes, got error %v", err)
		require.Nil(t, d)
	})
	t.Run("invalidDictionarySize", func(t *testing.T) {
		aux := make([]byte, 32)

		// wordCount=0
		// emptyWordCount=0
		binary.BigEndian.PutUint64(aux[24:32], 10) // dict size in bytes

		fpath := filepath.Join(tmpDir, "invalidDictionarySize")
		err := os.WriteFile(fpath, aux, 0644)
		require.NoError(t, err)

		d, err := NewDecompressor(fpath)
		require.ErrorIsf(t, err, &ErrCompressedFileCorrupted{},
			"file contains incorrect dictionary size in bytes, got error %v", err)
		require.Nil(t, d)
	})
	t.Run("fileSizeShouldBeMinimal", func(t *testing.T) {
		aux := make([]byte, 33)

		// wordCount=0
		// emptyWordCount=0
		// pattern dict size in bytes 0
		// dict size in bytes 0

		fpath := filepath.Join(tmpDir, "fileSizeShouldBeMinimal")
		err := os.WriteFile(fpath, aux, 0644)
		require.NoError(t, err)

		d, err := NewDecompressor(fpath)
		require.ErrorIsf(t, err, &ErrCompressedFileCorrupted{},
			"file contains incorrect dictionary size in bytes, got error %v", err)
		require.Nil(t, d)
	})
}

const lorem = `lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et
dolore magna aliqua ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
consequat duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur
excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum`

func rmNewLine(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", "")
}

func TestDecompressTorrent(t *testing.T) {
	t.Skip()

	fpath := "/mnt/data/chains/mainnet/snapshots/v1.0-014000-014500-transactions.seg"
	st, err := os.Stat(fpath)
	require.NoError(t, err)
	fmt.Printf("file: %v, size: %d\n", st.Name(), st.Size())

	d, err := NewDecompressor(fpath)

	require.NoError(t, err)
	defer d.Close()

	getter := d.MakeGetter()
	_ = getter

	for getter.HasNext() {
		_, sz := getter.Next(nil)
		// fmt.Printf("%x\n", buf)
		require.NotZero(t, sz)
	}
}

const N = 100

func randWord() []byte {
	size := rand.Intn(256) // size of the word
	word := make([]byte, size)
	for i := 0; i < size; i++ {
		word[i] = byte(rand.Intn(256))
	}
	return word
}

func generateRandWords() (WORDS [N][]byte, WORD_FLAGS [N]bool, INPUT_FLAGS []int) {
	WORDS = [N][]byte{}
	WORD_FLAGS = [N]bool{} // false - uncompressed word, true - compressed word
	INPUT_FLAGS = []int{}  // []byte or nil input

	for i := 0; i < N-2; i++ {
		WORDS[i] = randWord()
	}
	// make sure we have at least 2 emtpy []byte
	WORDS[N-2] = []byte{}
	WORDS[N-1] = []byte{}
	return
}

func prepareRandomDict(t *testing.T) (d *Decompressor, WORDS [N][]byte, WORD_FLAGS [N]bool, INPUT_FLAGS []int) {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "complex")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	if err != nil {
		t.Fatal(err)
	}
	// c.DisableFsync()
	defer c.Close()
	rand.Seed(time.Now().UnixNano())
	WORDS, WORD_FLAGS, INPUT_FLAGS = generateRandWords()

	idx := 0
	for idx < N {
		n := rand.Intn(2)
		switch n {
		case 0: // input case
			word := WORDS[idx]
			m := rand.Intn(2)
			if m == 1 {
				if err = c.AddWord(word); err != nil {
					t.Fatal(err)
				}
				WORD_FLAGS[idx] = true
			} else {
				if err = c.AddUncompressedWord(word); err != nil {
					t.Fatal(err)
				}
			}
			idx++
			INPUT_FLAGS = append(INPUT_FLAGS, n)
		case 1: // nil word
			if err = c.AddWord(nil); err != nil {
				t.Fatal(err)
			}
			INPUT_FLAGS = append(INPUT_FLAGS, n)
		default:
			t.Fatal(fmt.Errorf("case %d\n", n))
		}
	}

	if err = c.Compress(); err != nil {
		t.Fatal(err)
	}
	if d, err = NewDecompressor(file); err != nil {
		t.Fatal(err)
	}
	return d, WORDS, WORD_FLAGS, INPUT_FLAGS
}

// TestMatchCmpCompressedBinaryKeys tests MatchCmp with binary keys similar to real storage keys.
// Verifies comparison direction, offset advancement on match, and reset on non-match.
func TestMatchCmpCompressedBinaryKeys(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "binkeys")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)

	// Generate sorted binary keys (20 bytes, like address hashes)
	keys := make([][]byte, 200)
	for i := range keys {
		k := make([]byte, 20)
		binary.BigEndian.PutUint64(k[12:], uint64(i*37)) // sorted by suffix
		k[0] = byte(i / 256)
		k[1] = byte(i % 256)
		keys[i] = k
	}
	slices.SortFunc(keys, bytes.Compare)

	for _, k := range keys {
		require.NoError(t, c.AddWord(k))
	}
	require.NoError(t, c.Compress())
	c.Close()

	d, err := NewDecompressor(file)
	require.NoError(t, err)
	defer d.Close()
	g := d.MakeGetter()

	// Test 1: exact match advances position
	for i, k := range keys {
		require.True(t, g.HasNext(), "word %d", i)
		cmp := g.MatchCmp(k)
		require.Equal(t, 0, cmp, "word %d: expected match for %x", i, k)
	}
	require.False(t, g.HasNext())

	// Test 2: non-match does NOT advance position
	g.Reset(0)
	savePos := g.dataP
	wrongKey := make([]byte, 20)
	wrongKey[0] = 0xff // greater than any key
	cmp := g.MatchCmp(wrongKey)
	require.NotEqual(t, 0, cmp)
	require.Equal(t, savePos, g.dataP, "position should reset on non-match")

	// Test 3: comparison direction correctness
	g.Reset(0)
	for _, k := range keys {
		savePos := g.dataP

		// buf < word: build a key strictly smaller than k
		smaller := make([]byte, len(k))
		copy(smaller, k)
		smaller[len(smaller)-1] = 0
		if bytes.Compare(smaller, k) >= 0 {
			// k itself ends with 0, use a shorter key
			smaller = k[:len(k)-1]
		}
		if bytes.Compare(smaller, k) < 0 {
			cmp = g.MatchCmp(smaller)
			require.Equal(t, -1, cmp, "expected buf < word for key %x vs %x", smaller, k)
			require.Equal(t, savePos, g.dataP, "position should reset on non-match")
		}

		// buf > word: build a key strictly greater than k
		bigger := make([]byte, len(k))
		copy(bigger, k)
		bigger[len(bigger)-1] = 0xff
		if bytes.Compare(bigger, k) <= 0 {
			bigger = append(k, 0xff)
		}
		cmp = g.MatchCmp(bigger)
		require.Equal(t, 1, cmp, "expected buf > word for key %x vs %x", bigger, k)
		require.Equal(t, savePos, g.dataP, "position should reset on non-match")

		// exact match → advances
		cmp = g.MatchCmp(k)
		require.Equal(t, 0, cmp, "expected exact match for key %x", k)
	}

	// Test 4: prefix of key should not match
	g.Reset(0)
	prefix := keys[0][:10]
	cmp = g.MatchCmp(prefix)
	require.NotEqual(t, 0, cmp, "prefix should not match full key")
}

// TestMatchCmpUncompressedBinaryKeys tests MatchCmpUncompressed directly with binary keys.
func TestMatchCmpUncompressedBinaryKeys(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "binkeys_uncomp")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)

	keys := make([][]byte, 200)
	for i := range keys {
		k := make([]byte, 20)
		binary.BigEndian.PutUint64(k[12:], uint64(i*37))
		k[0] = byte(i / 256)
		k[1] = byte(i % 256)
		keys[i] = k
	}
	slices.SortFunc(keys, bytes.Compare)

	for _, k := range keys {
		require.NoError(t, c.AddUncompressedWord(k))
	}
	require.NoError(t, c.Compress())
	c.Close()

	d, err := NewDecompressor(file)
	require.NoError(t, err)
	defer d.Close()
	g := d.MakeGetter()

	// MatchCmpUncompressed advances position on match, resets on non-match (same as MatchCmp).

	// Test 1: sequential exact matches advance through all keys
	for i, k := range keys {
		require.True(t, g.HasNext(), "word %d", i)
		cmp := g.MatchCmpUncompressed(k)
		require.Equal(t, 0, cmp, "word %d: expected match for %x", i, k)
	}
	require.False(t, g.HasNext())

	// Test 2: non-match resets position, match advances
	g.Reset(0)
	for _, k := range keys {
		savePos := g.dataP

		// non-match: position resets
		wrongKey := bytes.Repeat([]byte{0xff}, 20)
		cmp := g.MatchCmpUncompressed(wrongKey)
		require.NotEqual(t, 0, cmp)
		require.Equal(t, savePos, g.dataP, "position should reset on non-match")

		// exact match: advances
		cmp = g.MatchCmpUncompressed(k)
		require.Equal(t, 0, cmp)
		require.NotEqual(t, savePos, g.dataP, "position should advance on match")
	}
}

// TestMatchCmpEmptyAndNil tests MatchCmp edge cases with empty and nil inputs on compressed data.
func TestMatchCmpEmptyAndNil(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "empty_nil")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)

	// Write: nil, empty, short, empty, nil
	require.NoError(t, c.AddWord(nil))
	require.NoError(t, c.AddWord([]byte{}))
	require.NoError(t, c.AddWord([]byte{0x01}))
	require.NoError(t, c.AddWord([]byte{}))
	require.NoError(t, c.AddWord(nil))
	require.NoError(t, c.Compress())
	c.Close()

	d, err := NewDecompressor(file)
	require.NoError(t, err)
	defer d.Close()
	g := d.MakeGetter()

	// word[0] = nil/empty → MatchCmp(nil) and MatchCmp([]byte{}) should both match
	require.Equal(t, 0, g.MatchCmp(nil))          // advances
	require.Equal(t, 0, g.MatchCmp([]byte{}))     // word[1] = empty, advances
	require.Equal(t, 0, g.MatchCmp([]byte{0x01})) // word[2], advances
	require.Equal(t, 0, g.MatchCmp([]byte{}))     // word[3] = empty, advances
	require.Equal(t, 0, g.MatchCmp(nil))          // word[4] = nil, advances
	require.False(t, g.HasNext())

	// Non-empty vs empty word
	g.Reset(0)
	cmp := g.MatchCmp([]byte{0x01})
	require.Equal(t, 1, cmp, "non-empty buf vs empty word should return 1 (buf > word)")
}

// TestMatchAllEdgeCases tests all four Match* methods with empty words/prefixes
// and verifies their semantics are consistent with Next().
func TestMatchAllEdgeCases(t *testing.T) {
	logger := log.New()

	// Helper to create a compressed file from words
	makeCompressed := func(t *testing.T, words [][]byte) *Decompressor {
		t.Helper()
		tmpDir := t.TempDir()
		file := filepath.Join(tmpDir, "match_edge")
		cfg := DefaultCfg
		cfg.MinPatternScore = 1
		cfg.Workers = 2
		c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
		require.NoError(t, err)
		for _, w := range words {
			require.NoError(t, c.AddWord(w))
		}
		require.NoError(t, c.Compress())
		c.Close()
		d, err := NewDecompressor(file)
		require.NoError(t, err)
		return d
	}

	// Helper to create an uncompressed file from words
	makeUncompressed := func(t *testing.T, words [][]byte) *Decompressor {
		t.Helper()
		tmpDir := t.TempDir()
		file := filepath.Join(tmpDir, "match_edge_uncomp")
		cfg := DefaultCfg
		cfg.MinPatternScore = 1
		cfg.Workers = 2
		c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
		require.NoError(t, err)
		for _, w := range words {
			require.NoError(t, c.AddUncompressedWord(w))
		}
		require.NoError(t, c.Compress())
		c.Close()
		d, err := NewDecompressor(file)
		require.NoError(t, err)
		return d
	}

	// Words: nil, empty, single byte, "hello", empty, nil, "world"
	words := [][]byte{nil, {}, {0x42}, []byte("hello"), {}, nil, []byte("world")}

	t.Run("compressed_MatchCmp_vs_Next", func(t *testing.T) {
		d := makeCompressed(t, words)
		defer d.Close()
		g := d.MakeGetter()

		for i, w := range words {
			pos := g.dataP
			// Next should return the word
			got, nextPos := g.Next(nil)
			if w == nil {
				w = []byte{} // nil is stored as empty
			}
			require.Equal(t, w, got, "word %d via Next", i)

			// Reset and verify MatchCmp matches with same semantics
			g.Reset(pos)
			cmp := g.MatchCmp(w)
			require.Equal(t, 0, cmp, "word %d: MatchCmp should match %x", i, w)
			// After match, position should be same as after Next
			require.Equal(t, nextPos, g.dataP, "word %d: MatchCmp should advance to same pos as Next", i)
		}
		require.False(t, g.HasNext())
	})

	t.Run("compressed_MatchCmp_empty_vs_nonempty", func(t *testing.T) {
		d := makeCompressed(t, words)
		defer d.Close()
		g := d.MakeGetter()

		// word[0] is nil/empty
		// MatchCmp with non-empty buf vs empty word: buf > word
		require.Equal(t, 1, g.MatchCmp([]byte{0x01}), "non-empty > empty word")
		// MatchCmp with nil: match
		require.Equal(t, 0, g.MatchCmp(nil), "nil matches empty word")

		// word[1] is empty
		require.Equal(t, 0, g.MatchCmp([]byte{}), "empty matches empty word")

		// word[2] is {0x42}
		// MatchCmp with empty buf vs non-empty word: buf < word
		savePos := g.dataP
		require.Equal(t, -1, g.MatchCmp(nil), "nil < non-empty word")
		require.Equal(t, savePos, g.dataP, "position reset on non-match")
		require.Equal(t, -1, g.MatchCmp([]byte{}), "empty < non-empty word")
		require.Equal(t, savePos, g.dataP, "position reset on non-match")
		// exact match
		require.Equal(t, 0, g.MatchCmp([]byte{0x42}))
	})

	t.Run("compressed_MatchPrefix_empty_word_and_prefix", func(t *testing.T) {
		d := makeCompressed(t, words)
		defer d.Close()
		g := d.MakeGetter()

		// word[0] is nil/empty
		pos0 := g.dataP
		require.True(t, g.MatchPrefix(nil), "nil prefix matches empty word")
		require.Equal(t, pos0, g.dataP, "MatchPrefix never advances")
		require.True(t, g.MatchPrefix([]byte{}), "empty prefix matches empty word")
		require.False(t, g.MatchPrefix([]byte{0x01}), "non-empty prefix doesn't match empty word")
		require.Equal(t, pos0, g.dataP, "MatchPrefix never advances")

		// advance past word[0]
		g.Next(nil)

		// word[1] is empty
		require.True(t, g.MatchPrefix(nil))
		require.True(t, g.MatchPrefix([]byte{}))
		require.False(t, g.MatchPrefix([]byte("x")))
		g.Next(nil)

		// word[2] is {0x42}
		pos2 := g.dataP
		require.True(t, g.MatchPrefix(nil), "nil prefix matches any word")
		require.True(t, g.MatchPrefix([]byte{}), "empty prefix matches any word")
		require.True(t, g.MatchPrefix([]byte{0x42}), "exact prefix match")
		require.False(t, g.MatchPrefix([]byte{0x42, 0x43}), "prefix longer than word")
		require.Equal(t, pos2, g.dataP, "MatchPrefix never advances")
	})

	t.Run("uncompressed_MatchCmpUncompressed_vs_Next", func(t *testing.T) {
		d := makeUncompressed(t, words)
		defer d.Close()
		g := d.MakeGetter()

		for i, w := range words {
			pos := g.dataP
			got, nextPos := g.NextUncompressed()
			got = append([]byte{}, got...) // copy since NextUncompressed returns slice into mmap
			if w == nil {
				w = []byte{}
			}
			require.Equal(t, w, got, "word %d via NextUncompressed", i)

			// Reset and verify MatchCmpUncompressed
			g.Reset(pos)
			cmp := g.MatchCmpUncompressed(w)
			require.Equal(t, 0, cmp, "word %d: MatchCmpUncompressed should match %x", i, w)
			require.Equal(t, nextPos, g.dataP, "word %d: MatchCmpUncompressed should advance to same pos as NextUncompressed", i)
		}
		require.False(t, g.HasNext())
	})

	t.Run("uncompressed_MatchCmpUncompressed_empty_vs_nonempty", func(t *testing.T) {
		d := makeUncompressed(t, words)
		defer d.Close()
		g := d.MakeGetter()

		// word[0] is nil/empty
		require.Equal(t, 1, g.MatchCmpUncompressed([]byte{0x01}), "non-empty > empty word")
		require.Equal(t, 0, g.MatchCmpUncompressed(nil), "nil matches empty word")

		// word[1] is empty
		require.Equal(t, 0, g.MatchCmpUncompressed([]byte{}), "empty matches empty word")

		// word[2] is {0x42}
		savePos := g.dataP
		require.Equal(t, -1, g.MatchCmpUncompressed(nil), "nil < non-empty word")
		require.Equal(t, savePos, g.dataP, "position reset on non-match")
		require.Equal(t, -1, g.MatchCmpUncompressed([]byte{}), "empty < non-empty word")
		require.Equal(t, savePos, g.dataP)
		require.Equal(t, 0, g.MatchCmpUncompressed([]byte{0x42}))
	})

	t.Run("uncompressed_MatchPrefixUncompressed_empty_word_and_prefix", func(t *testing.T) {
		d := makeUncompressed(t, words)
		defer d.Close()
		g := d.MakeGetter()

		// word[0] is nil/empty
		pos0 := g.dataP
		require.True(t, g.MatchPrefixUncompressed(nil), "nil prefix matches empty word")
		require.Equal(t, pos0, g.dataP, "MatchPrefixUncompressed never advances")
		require.True(t, g.MatchPrefixUncompressed([]byte{}), "empty prefix matches empty word")
		require.False(t, g.MatchPrefixUncompressed([]byte{0x01}), "non-empty prefix doesn't match empty word")
		require.Equal(t, pos0, g.dataP, "MatchPrefixUncompressed never advances")

		// advance past word[0]
		g.NextUncompressed()

		// word[1] is empty
		require.True(t, g.MatchPrefixUncompressed(nil))
		require.True(t, g.MatchPrefixUncompressed([]byte{}))
		require.False(t, g.MatchPrefixUncompressed([]byte("x")))
		g.NextUncompressed()

		// word[2] is {0x42}
		pos2 := g.dataP
		require.True(t, g.MatchPrefixUncompressed(nil), "nil prefix matches any word")
		require.True(t, g.MatchPrefixUncompressed([]byte{}), "empty prefix matches any word")
		require.True(t, g.MatchPrefixUncompressed([]byte{0x42}), "exact prefix match")
		require.False(t, g.MatchPrefixUncompressed([]byte{0x42, 0x43}), "prefix longer than word")
		require.Equal(t, pos2, g.dataP, "MatchPrefixUncompressed never advances")
	})
}

func TestDecompressRandomMatchCmp(t *testing.T) {
	d, WORDS, _, INPUT_FLAGS := prepareRandomDict(t)
	defer d.Close()

	if d.wordsCount != uint64(len(INPUT_FLAGS)) {
		t.Fatalf("TestDecompressRandomDict: d.wordsCount != len(INPUT_FLAGS)")
	}

	g := d.MakeGetter()

	word_idx := 0
	input_idx := 0
	total := 0
	// check for existing and non existing keys
	for g.HasNext() {
		pos := g.dataP
		if INPUT_FLAGS[input_idx] == 0 { // []byte input
			notExpected := string(WORDS[word_idx]) + "z"
			cmp := g.MatchCmp([]byte(notExpected))
			if cmp == 0 {
				t.Fatalf("not expected match: %v\n got: %v\n", []byte(notExpected), WORDS[word_idx])
			}

			expected := WORDS[word_idx]
			cmp = g.MatchCmp(expected) // move offset to the next pos
			if cmp != 0 {
				savePos := g.dataP
				g.Reset(pos)
				word, nextPos := g.Next(nil)
				if nextPos != savePos {
					t.Fatalf("nextPos %d != savePos %d\n", nextPos, savePos)
				}
				if bytes.Compare(expected, word) != cmp {
					fmt.Printf("1 expected: %v, acutal %v, cmp %d\n", expected, word, cmp)
				}
				t.Fatalf("expected match: %v\n got: %v\n", expected, word)
			}
			word_idx++
		} else { // nil input
			notExpected := []byte{0}
			cmp := g.MatchCmp(notExpected)
			if cmp == 0 {
				t.Fatal("not expected match []byte{0} with nil\n")
			}

			expected := []byte{}
			cmp = g.MatchCmp(nil)
			if cmp != 0 {
				savePos := g.dataP
				g.Reset(pos)
				word, nextPos := g.Next(nil)
				if nextPos != savePos {
					t.Fatalf("nextPos %d != savePos %d\n", nextPos, savePos)
				}
				if bytes.Compare(expected, word) != cmp {
					fmt.Printf("2 expected: %v, acutal %v, cmp %d\n", expected, word, cmp)
				}
				t.Fatalf("expected match: %v\n got: %v\n", expected, word)
			}
		}
		input_idx++
		total++
	}
	if total != int(d.wordsCount) {
		t.Fatalf("expected word count: %d, got %d\n", int(d.wordsCount), total)
	}
}

func TestDecompressRandomMatchBool(t *testing.T) {
	d, WORDS, _, INPUT_FLAGS := prepareRandomDict(t)
	defer d.Close()

	if d.wordsCount != uint64(len(INPUT_FLAGS)) {
		t.Fatalf("TestDecompressRandomDict: d.wordsCount != len(INPUT_FLAGS)")
	}

	g := d.MakeGetter()

	word_idx := 0
	input_idx := 0
	total := 0
	// check for existing and non existing keys
	for g.HasNext() {
		pos := g.dataP
		if INPUT_FLAGS[input_idx] == 0 { // []byte input
			notExpected := string(WORDS[word_idx]) + "z"
			if g.MatchCmp([]byte(notExpected)) == 0 {
				t.Fatalf("not expected match: %v\n got: %v\n", []byte(notExpected), WORDS[word_idx])
			}

			expected := WORDS[word_idx]
			if g.MatchCmp(expected) != 0 {
				g.Reset(pos)
				word, _ := g.Next(nil)
				if !bytes.Equal(expected, word) {
					fmt.Printf("1 expected: %v, acutal %v\n", expected, word)
				}
				t.Fatalf("expected match: %v\n got: %v\n", expected, word)
			}
			word_idx++
		} else { // nil input
			notExpected := []byte{0}
			if g.MatchCmp(notExpected) == 0 {
				t.Fatal("not expected match []byte{0} with nil\n")
			}

			expected := []byte{}
			if g.MatchCmp(nil) != 0 {
				g.Reset(pos)
				word, _ := g.Next(nil)
				if !bytes.Equal(expected, word) {
					fmt.Printf("2 expected: %v, acutal %v\n", expected, word)
				}
				t.Fatalf("expected match: %v\n got: %v\n", expected, word)
			}
		}
		input_idx++
		total++
	}
	if total != int(d.wordsCount) {
		t.Fatalf("expected word count: %d, got %d\n", int(d.wordsCount), total)
	}
}
