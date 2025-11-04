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
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
)

func FuzzCompress(f *testing.F) {
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
		buf := make([]byte, 0, 100)
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
		}
	})
}
