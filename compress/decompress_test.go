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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func prepareLoremDict(t *testing.T) *Decompressor {
	t.Helper()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2, log.LvlDebug)
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
}

func TestDecompressMatchOK(t *testing.T) {
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		if i%2 != 0 {
			expected := fmt.Sprintf("%s %d", w, i)
			ok, _ := g.Match([]byte(expected))
			if !ok {
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
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed2")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2, log.LvlDebug)
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
			ok, _ := g.Match([]byte(expected))
			if !ok {
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
	d := prepareLoremDict(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	skipCount := 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := fmt.Sprintf("%s %d", w, i+1)

		ok, _ := g.Match([]byte(expected))
		if ok {
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
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")
	t.Name()
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, 1, 2, log.LvlDebug)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for k, w := range loremStrings {
		if err = c.AddUncompressedWord([]byte(fmt.Sprintf("%s %d", w, k))); err != nil {
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

func TestUncompressed(t *testing.T) {
	d := prepareLoremDictUncompressed(t)
	defer d.Close()
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		expected := []byte(fmt.Sprintf("%s %d", w, i+1))
		expected = expected[:len(expected)/2]
		actual, _ := g.NextUncompressed()
		if bytes.Equal(expected, actual) {
			t.Errorf("expected %s, actual %s", expected, actual)
		}
		i++
	}
}

const lorem = `Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et
dolore magna aliqua Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
consequat Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur
Excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum`

var loremStrings = strings.Split(lorem, " ")

func TestDecompressTorrent(t *testing.T) {
	t.Skip()

	fpath := "/mnt/data/chains/mainnet/snapshots/v1-014000-014500-transactions.seg"
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
