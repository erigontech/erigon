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
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkDecompress(b *testing.B) {
	t := new(testing.T)
	d := prepareDict(t, 100_000)
	defer d.Close()

	b.Run("next", func(b *testing.B) {
		b.ReportAllocs()
		var buf []byte
		g := d.MakeGetter()
		for i := 0; i < b.N; i++ {
			buf, _ = g.Next(buf[:0])
			if !g.HasNext() {
				g.Reset(0)
			}
		}
	})
	b.Run("skip", func(b *testing.B) {
		b.ReportAllocs()
		g := d.MakeGetter()
		for i := 0; i < b.N; i++ {
			_, _ = g.Skip()
			if !g.HasNext() {
				g.Reset(0)
			}
		}
	})
	b.Run("matchcmp_non_existing_key", func(b *testing.B) {
		b.ReportAllocs()
		g := d.MakeGetter()
		for i := 0; i < b.N; i++ {
			_ = g.MatchCmp([]byte("longlongword"))
			if !g.HasNext() {
				g.Reset(0)
			}
		}
	})
}

func BenchmarkDecompressTorrent(t *testing.B) {
	t.Skip()

	//fpath := "/Volumes/wotah/mainnet/snapshots/v1.0-013500-014000-bodies.seg"
	fpath := "/Volumes/wotah/mainnet/snapshots/v1.0-013500-014000-transactions.seg"
	//fpath := "./v1.0-006000-006500-transactions.seg"
	st, err := os.Stat(fpath)
	require.NoError(t, err)
	fmt.Printf("file: %v, size: %d\n", st.Name(), st.Size())

	condensePatternTableBitThreshold = 5
	fmt.Printf("bit threshold: %d\n", condensePatternTableBitThreshold)

	t.Run("init", func(t *testing.B) {
		for i := 0; i < t.N; i++ {
			d, err := NewDecompressor(fpath)
			require.NoError(t, err)
			d.Close()
		}
	})
	t.Run("run", func(t *testing.B) {
		d, err := NewDecompressor(fpath)
		require.NoError(t, err)
		defer d.Close()

		getter := d.MakeGetter()

		for i := 0; i < t.N && getter.HasNext(); i++ {
			_, sz := getter.Next(nil)
			if sz == 0 {
				t.Fatal("sz == 0")
			}
		}
	})
}
