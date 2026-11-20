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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
)

// A serial IBS (no versionMap) reports a usable stamp, so the vm
// frame slot cache is on; read tracing disables it, since tracing must observe
// every read.
// Not parallel: it toggles the global dbg.TraceTransactionIO, which other tests
// read (e.g. CreateAccount), so a parallel run would data-race on it.
func TestReadStamp_Gating(t *testing.T) {
	ibs := New(&emptyReader{})
	defer ibs.Close()

	_, ok := ibs.ReadStamp()
	require.True(t, ok, "serial IBS must report a usable stamp")

	prev := dbg.TraceTransactionIO
	dbg.TraceTransactionIO = true
	t.Cleanup(func() { dbg.TraceTransactionIO = prev })
	_, ok = ibs.ReadStamp()
	require.False(t, ok, "read tracing must disable the slot cache")
}
