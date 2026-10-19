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

package dir

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPacedWriterWaitsBeforeEachChunk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out")
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	var sizesAtWait []int64
	w := NewPacedWriter(f, 4, func() error {
		fi, err := f.Stat()
		require.NoError(t, err)
		sizesAtWait = append(sizesAtWait, fi.Size())
		return nil
	})

	_, err = w.Write([]byte("abcdef"))
	require.NoError(t, err)
	_, err = w.Write([]byte("ghij"))
	require.NoError(t, err)

	require.Equal(t, []int64{0, 4, 8}, sizesAtWait)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "abcdefghij", string(got))
}

func TestPacedWriterStopsAtChunkWhenWaitFails(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "out"))
	require.NoError(t, err)
	defer f.Close()
	stop := errors.New("stop")
	calls := 0
	w := NewPacedWriter(f, 4, func() error {
		if calls++; calls == 2 {
			return stop
		}
		return nil
	})

	n, err := w.Write([]byte("abcdefgh"))
	require.ErrorIs(t, err, stop)
	require.Equal(t, 4, n)
}
