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
	"github.com/erigontech/erigon-lib/common/dir"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
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
		if err = c.AddWord([]byte(fmt.Sprintf("%s %d", w, k))); err != nil {
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
		if err = c.AddWord([]byte(fmt.Sprintf("word-%d", i))); err != nil {
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
	condensePatternTableBitThreshold = 4
	d := prepareStupidDict(t, 10000)
	defer func() { condensePatternTableBitThreshold = 9 }()
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
		expected := []byte(fmt.Sprintf("%s %d", w, i+1))
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
		expected := []byte(fmt.Sprintf("%s %d", w, i+1))
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
		err = c.AddUncompressedWord([]byte(fmt.Sprintf("%s %d", w, k)))
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
		expected := []byte(fmt.Sprintf("%s %d", w, i+1))
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
			if err = c.AddUncompressedWord([]byte(fmt.Sprintf("%s %d", w, k))); err != nil {
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
			if err = c.AddWord([]byte(fmt.Sprintf("%s %d", w, k))); err != nil {
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

	condensePatternTableBitThreshold = 9
	fmt.Printf("bit threshold: %d\n", condensePatternTableBitThreshold)
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
				if bytes.Compare(expected, word) != 0 {
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
				if bytes.Compare(expected, word) != 0 {
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
