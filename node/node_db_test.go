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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
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

func TestParallelCommitmentReaders(t *testing.T) {
	previous := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() { statecfg.ExperimentalParallelCommitment = previous })

	statecfg.ExperimentalParallelCommitment = false
	require.Zero(t, parallelCommitmentReaders())
	statecfg.ExperimentalParallelCommitment = true
	require.Equal(t, commitment.ParallelCommitmentReadTxs(), parallelCommitmentReaders())
}

// The chain database opens at Dirs.Chaindata, also when it is not
// <datadir>/chaindata.
func TestOpenDatabaseChaindata(t *testing.T) {
	cfg := testNodeConfig(t)
	cfg.Dirs.Chaindata = filepath.Join(cfg.Dirs.DataDir, "chain", "chaindata")
	db, err := OpenDatabase(t.Context(), cfg, dbcfg.ChainDB, "", false, log.New())
	require.NoError(t, err)
	db.Close()
	require.FileExists(t, filepath.Join(cfg.Dirs.Chaindata, "mdbx.dat"))
	require.NoFileExists(t, filepath.Join(cfg.Dirs.DataDir, "chaindata", "mdbx.dat"))
}
