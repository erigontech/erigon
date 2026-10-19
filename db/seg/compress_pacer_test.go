// Copyright 2026 The Erigon Authors
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func compressWithPacer(t *testing.T, uncompressed bool, wait func() error) (string, error) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "compressed")
	c, err := NewCompressor(t.Context(), t.Name(), file, t.TempDir(), DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	c.SetOutputPacer(wait)
	for i := range 1000 {
		w := fmt.Appendf(nil, "word %d longlongword", i)
		if uncompressed {
			require.NoError(t, c.AddUncompressedWord(w))
		} else {
			require.NoError(t, c.AddWord(w))
		}
	}
	return file, c.Compress()
}

func TestCompressorOutputPacer(t *testing.T) {
	for _, uncompressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("uncompressed=%t", uncompressed), func(t *testing.T) {
			calls := 0
			file, err := compressWithPacer(t, uncompressed, func() error { calls++; return nil })
			require.NoError(t, err)
			require.Positive(t, calls)
			d, err := NewDecompressor(file)
			require.NoError(t, err)
			defer d.Close()
			require.Equal(t, 1000, d.Count())

			stop := errors.New("stop")
			file, err = compressWithPacer(t, uncompressed, func() error { return stop })
			require.ErrorIs(t, err, stop)
			_, err = os.Stat(file)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}
