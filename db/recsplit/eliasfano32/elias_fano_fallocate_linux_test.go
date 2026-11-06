//go:build linux

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

package eliasfano32

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFallocateAllocatesBlocks(t *testing.T) {
	const size int64 = 8 << 20
	f, err := os.CreateTemp(t.TempDir(), "fallocate")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	require.NoError(t, fallocate(f, size))
	st, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, size, st.Size())
	require.GreaterOrEqual(t, st.Sys().(*syscall.Stat_t).Blocks*512, size)
}
