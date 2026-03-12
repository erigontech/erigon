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

package testutil

import (
	"os"
	"testing"
)

// RunTestMain is the standard TestMain for execution test packages. If
// ERIGON_EXECUTION_TESTS_TMPDIR is set, the OS temp dir variables are
// overridden with that value so that t.TempDir() and os.MkdirTemp use it
// (e.g. a RAM disk).
func RunTestMain(m *testing.M) {
	if dir := os.Getenv("ERIGON_EXECUTION_TESTS_TMPDIR"); dir != "" {
		setTestTmpDir(dir)
	}
	os.Exit(m.Run())
}
