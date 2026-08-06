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

package node

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/node/nodecfg"
)

// The read-tx floor must size against the worker count this node actually runs
// (nodecfg.ExecWorkerCount), not the process global: an embedded caller can
// configure its executor without touching dbg.Exec3Workers.
func TestExecWorkerCountPrefersNodeConfig(t *testing.T) {
	orig := dbg.Exec3Workers
	dbg.SetExec3Workers(8)
	defer dbg.SetExec3Workers(orig)

	require.Equal(t, 64, execWorkerCount(&nodecfg.Config{ExecWorkerCount: 64}))
	require.Equal(t, 8, execWorkerCount(&nodecfg.Config{}))
}
