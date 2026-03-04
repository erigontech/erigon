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

package discover

import (
	"fmt"
	"os"
	"runtime"
	"testing"
)

func TestMain(m *testing.M) {
	// p2p/discover goroutines trigger an intermittent Go 1.26 runtime crash on
	// Windows: the GC background worker trips on a stack frame it cannot unwind
	// while shrinking a parked goroutine stack (runtime.(*unwinder).next ACCESS_VIOLATION).
	// Skip the entire package on Windows until the upstream Go issue is resolved.
	if runtime.GOOS == "windows" {
		fmt.Println("Skipping p2p/discover tests on Windows (intermittent Go runtime GC crash)")
		os.Exit(0)
	}
	os.Exit(m.Run())
}
