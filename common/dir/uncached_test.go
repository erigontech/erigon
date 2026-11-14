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
	"bufio"
	"bytes"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUncachedWriterWritesEveryByteInOrder(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "uncached-")
	require.NoError(t, err)
	defer f.Close()

	_, err = f.WriteString("head")
	require.NoError(t, err)

	data := make([]byte, 3<<20+17)
	rnd := rand.New(rand.NewPCG(1, 2))
	for i := range data {
		data[i] = byte(rnd.Uint32())
	}
	bw := bufio.NewWriterSize(UncachedWriter(f), 64<<10)
	for rest := data; len(rest) > 0; {
		n := min(len(rest), 1+rnd.IntN(200<<10))
		_, err = bw.Write(rest[:n])
		require.NoError(t, err)
		rest = rest[n:]
	}
	require.NoError(t, bw.Flush())
	_, err = f.WriteString("tail")
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	want := append(append([]byte("head"), data...), "tail"...)
	got, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	require.Equal(t, len(want), len(got))
	require.True(t, bytes.Equal(want, got))
}
