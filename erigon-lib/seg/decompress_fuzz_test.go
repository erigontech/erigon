// Copyright 2024 The Erigon Authors
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
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
)

func FuzzDecompressMatch(f *testing.F) {
	logger := log.New()
	f.Fuzz(func(t *testing.T, x []byte, pos []byte, workers int8) {
		t.Helper()
		t.Parallel()
		if len(pos) < 1 || workers < 1 {
			t.Skip()
			return
		}
		var a [][]byte
		j := 0
		for i := 0; i < len(pos) && j < len(x); i++ {
			if pos[i] == 0 {
				continue
			}
			next := min(j+int(pos[i]*10), len(x)-1)
			bbb := x[j:next]
			a = append(a, bbb)
			j = next
		}

		ctx := context.Background()
		tmpDir := t.TempDir()
		file := filepath.Join(tmpDir, fmt.Sprintf("compressed-%d", rand.Int31()))
		cfg := DefaultCfg
		cfg.MinPatternScore = 2
		cfg.Workers = int(workers)
		c, err := NewCompressor(ctx, t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
		if err != nil {
			t.Fatal(err)
		}
		c.DisableFsync()
		defer c.Close()
		for _, b := range a {
			if err = c.AddWord(b); err != nil {
				t.Fatal(err)
			}
		}
		if err = c.Compress(); err != nil {
			t.Fatal(err)
		}
		c.Close()
		d, err := NewDecompressor(file)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		g := d.MakeGetter()
		buf := make([]byte, (1 << 16))
		word_idx := 0
		for g.HasNext() {
			expected := a[word_idx]
			savePos := g.dataP
			cmp := g.MatchCmp(expected)
			pos1 := g.dataP
			if cmp != 0 {
				t.Fatalf("MatchCmp: expected match: %v\n", expected)
			}
			g.Reset(savePos)
			ok := g.MatchCmp(expected)
			pos2 := g.dataP
			if ok != 0 {
				t.Fatalf("MatchBool: expected match: %v\n", expected)
			}
			g.Reset(savePos)
			word, nexPos := g.Next(nil)
			if !bytes.Equal(word, expected) {
				t.Fatalf("bytes.Compare: expected match: %v with word %v\n", expected, word)
			}
			if pos1 != pos2 && pos2 != nexPos {
				t.Fatalf("pos1 %v != pos2 %v != nexPos %v\n", pos1, pos2, nexPos)
			}
			g.Reset(savePos)
			word2, nexPos2 := g.Next(buf[:0])
			if !bytes.Equal(word2, expected) {
				t.Fatalf("bytes.Compare: expected match: %v with word %v\n", expected, word)
			}
			if pos1 != pos2 && pos2 != nexPos && nexPos != nexPos2 {
				t.Fatalf("pos1 %v != pos2 %v != nexPos %v\n", pos1, pos2, nexPos)
			}
			word_idx++
		}
	})

}
