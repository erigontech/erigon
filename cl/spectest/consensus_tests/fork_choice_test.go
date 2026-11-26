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

package consensus_tests

import (
	"os"
	"os/exec"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/utils"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

const runForkChoiceHelperEnv = "RUN_FORK_CHOICE_HELPER"

func TestForkChoiceRunnerRunsStepsAfterPayloadInfo(t *testing.T) {
	if os.Getenv(runForkChoiceHelperEnv) == "1" {
		runForkChoiceWithTrailingInvalidTick(t)
		return
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestForkChoiceRunnerRunsStepsAfterPayloadInfo$")
	cmd.Env = append(os.Environ(), runForkChoiceHelperEnv+"=1")
	out, err := cmd.CombinedOutput()
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "invalid on_tick is not supported", string(out))
}

func runForkChoiceWithTrailingInvalidTick(t *testing.T) {
	genesis, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)
	anchor, err := utils.EncodeSSZSnappy(genesis)
	require.NoError(t, err)
	steps := `
- block_hash: '0x0000000000000000000000000000000000000000000000000000000000000001'
  payload_status: {status: SYNCING, latest_valid_hash: null, validation_error: null}
- tick: 1
  valid: false
`
	root := fstest.MapFS{
		"anchor_state.ssz_snappy": {Data: anchor},
		"steps.yaml":              {Data: []byte(steps)},
	}
	c := spectest.TestCase{ForkPhaseName: "phase0"}
	require.NoError(t, (&ForkChoice{}).Run(t, root, c))
}
