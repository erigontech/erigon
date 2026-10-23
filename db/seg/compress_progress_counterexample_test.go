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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestCompressorProgressCountsEmptyWords(t *testing.T) {
	tmp := t.TempDir()
	c, err := NewCompressor(t.Context(), t.Name(), filepath.Join(tmp, "data.kv"), tmp, DefaultCfg, log.LvlTrace, log.New())
	require.NoError(t, err)
	defer c.Close()
	words := [][]byte{[]byte("key"), nil, []byte("other"), []byte("value")}
	for _, word := range words {
		require.NoError(t, c.AddUncompressedWord(word))
	}
	var done uint64
	c.SetProgress(func(n uint64) { done += n })
	require.NoError(t, c.Compress())
	require.EqualValues(t, c.Count(), done, "successful compression must report every word, including empty values; done=%d total=%d", done, c.Count())
}
