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

package mdbxtest

import (
	"testing"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/db/kv/mdbx"
)

// InMem is InMem tuned for tests: the temp dir is left to the testing framework,
// and the map size is capped because parallel unit tests pile 16GB VA
// reservations into the Go race heap window ("too many address space collisions
// for -race mode"). Benchmarks run sequentially and can need the full map.
func InMem(tb testing.TB, opts mdbx.MdbxOpts, tmpDir string) mdbx.MdbxOpts {
	tb.Helper()
	opts = opts.InMem(tmpDir).AutoRemove(false).DirtySpace(uint64(2 * datasize.MB))
	if _, isBench := tb.(*testing.B); !isBench {
		opts = opts.MapSize(1 * datasize.GB)
	}
	return opts
}
