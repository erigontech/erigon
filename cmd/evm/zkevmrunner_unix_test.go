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

//go:build unix

package main

import (
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// zkevmtest reads its input twice (witnesses, then the run); a pipe can be read only once.
func TestRunZkevmTestsReadsAPipe(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	_, err = w.WriteString(`{"one": {"network":"Prague"}}`)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	path := fmt.Sprintf("/dev/fd/%d", r.Fd())
	if _, err := os.Stat("/proc/self/fd"); err == nil {
		path = fmt.Sprintf("/proc/self/fd/%d", r.Fd())
	}
	results, err := runZkevmTests(path, regexp.MustCompile("/one$"), 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
}
