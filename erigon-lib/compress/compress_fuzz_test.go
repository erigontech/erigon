//go:build !nofuzz

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
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/log/v3"
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
			next := cmp.Min(j+int(pos[i]*10), len(x)-1)
			bbb := x[j:next]
			a = append(a, bbb)
			j = next
		}

		ctx := context.Background()
		tmpDir := t.TempDir()
		file := filepath.Join(tmpDir, fmt.Sprintf("compressed-%d", rand.Int31()))
		c, err := NewCompressor(ctx, t.Name(), file, tmpDir, 2, int(workers), log.LvlDebug, logger)
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
