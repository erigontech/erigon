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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestComputeAndCheckCommitmentDoesNotAdvanceStateVersionInOverlay(t *testing.T) {
	const childProcess = "ERIGON_TEST_STATE_VERSION_CHILD"
	if os.Getenv(childProcess) == "" {
		// DiscardCommitment is initialized before tests run, so use a child
		// process to exercise that early-return path.
		t.Setenv("DISCARD_COMMITMENT", "true")
		cmd := exec.Command(os.Args[0], "-test.run=^TestComputeAndCheckCommitmentDoesNotAdvanceStateVersionInOverlay$")
		cmd.Env = append(os.Environ(), childProcess+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}

	_, tx := temporaltest.NewTestTx(t)
	overlay, err := membatchwithdb.NewMemoryBatch(tx, t.TempDir(), log.New())
	require.NoError(t, err)
	defer overlay.Close()

	before, err := rawdb.GetStateVersion(overlay)
	require.NoError(t, err)

	stage := &StageState{ID: stages.Execution}
	ok, _, err := computeAndCheckCommitmentV3(
		t.Context(),
		&types.Header{Number: *uint256.NewInt(1)},
		overlay,
		nil,
		ExecuteBlockCfg{},
		stage,
		false,
		log.New(),
		nil,
	)
	require.NoError(t, err)
	require.True(t, ok)

	progress, err := stages.GetStageProgress(overlay, stages.Execution)
	require.NoError(t, err)
	require.Equal(t, uint64(1), progress)
	after, err := rawdb.GetStateVersion(overlay)
	require.NoError(t, err)
	require.Equal(t, before, after, "execution metadata must not advance the durable state generation")
}
