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

package stagedsync

import (
	"os"
	"os/exec"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// The trace list is read once per process, so the test runs itself in a child process with TRACE_ACCOUNTS set.
func TestTraceAddrSkipsNilAddress(t *testing.T) {
	zero := common.Address{}.Hex()
	if os.Getenv("TRACE_ACCOUNTS") != zero {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.v", "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
		cmd.Env = append(os.Environ(), "TRACE_ACCOUNTS="+zero)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		require.Contains(t, string(out), "--- PASS: "+t.Name())
		return
	}
	require.True(t, traceAddr(accounts.ZeroAddress))
	require.False(t, traceAddr(accounts.NilAddress))
}
