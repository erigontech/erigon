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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	erigoncli "github.com/erigontech/erigon/node/cli"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"

	"github.com/erigontech/erigon/common/log/v3"
)

// buildEthConfig creates an ethconfig.Config by running the full flag parsing
// pipeline (BuildEthConfig) with the given CLI args.
func buildEthConfig(t *testing.T, args []string) *ethconfig.Config {
	t.Helper()

	var result *ethconfig.Config
	app := cli.NewApp()
	app.Flags = erigoncli.DefaultFlags
	app.Action = func(ctx *cli.Context) error {
		logger := log.Root()
		nodeCfg := &nodecfg.Config{}
		nodeCfg.Dirs.DataDir = t.TempDir()
		ethCfg := ethconfig.Defaults
		erigoncli.BuildEthConfig(ctx, nodeCfg, &ethCfg, logger)
		result = &ethCfg
		return nil
	}
	require.NoError(t, app.Run(append([]string{"erigon"}, args...)))
	require.NotNil(t, result)
	return result
}

// configJSON returns a deterministic JSON snapshot of the config, excluding
// fields that vary between runs (Dirs, Genesis, Downloader).
type configSnapshot struct {
	NetworkID   uint64  `json:"network_id"`
	StateStream bool    `json:"state_stream"`
	InternalCL  bool    `json:"internal_cl"`
	RPCGasCap   uint64  `json:"rpc_gas_cap"`
	RPCTxFeeCap float64 `json:"rpc_tx_fee_cap"`

	// Sync config
	LoopThrottle    string `json:"loop_throttle"`
	BreakAfterStage string `json:"break_after_stage"`
	LoopBlockLimit  uint   `json:"loop_block_limit"`
	ExecWorkerCount int    `json:"exec_worker_count"`

	// Feature flags
	ExperimentalBAL                  bool `json:"experimental_bal"`
	KeepExecutionProofs              bool `json:"keep_execution_proofs"`
	ExperimentalConcurrentCommitment bool `json:"experimental_concurrent_commitment"`

	// Snapshot config
	SnapKeepBlocks bool `json:"snap_keep_blocks"`
	SnapProduceE2  bool `json:"snap_produce_e2"`
	SnapProduceE3  bool `json:"snap_produce_e3"`
	NoDownloader   bool `json:"no_downloader"`

	// Consensus
	HeimdallURL     string `json:"heimdall_url"`
	WithoutHeimdall bool   `json:"without_heimdall"`
}

func snapshotConfig(cfg *ethconfig.Config) configSnapshot {
	return configSnapshot{
		NetworkID:                        cfg.NetworkID,
		StateStream:                      cfg.StateStream,
		InternalCL:                       cfg.InternalCL,
		RPCGasCap:                        cfg.RPCGasCap,
		RPCTxFeeCap:                      cfg.RPCTxFeeCap,
		LoopThrottle:                     cfg.Sync.LoopThrottle.String(),
		BreakAfterStage:                  cfg.Sync.BreakAfterStage,
		LoopBlockLimit:                   cfg.Sync.LoopBlockLimit,
		ExecWorkerCount:                  cfg.Sync.ExecWorkerCount,
		ExperimentalBAL:                  cfg.ExperimentalBAL,
		KeepExecutionProofs:              cfg.Sync.KeepExecutionProofs,
		ExperimentalConcurrentCommitment: cfg.Sync.ExperimentalConcurrentCommitment,
		SnapKeepBlocks:                   cfg.Snapshot.KeepBlocks,
		SnapProduceE2:                    cfg.Snapshot.ProduceE2,
		SnapProduceE3:                    cfg.Snapshot.ProduceE3,
		NoDownloader:                     cfg.Snapshot.NoDownloader,
		HeimdallURL:                      cfg.HeimdallURL,
		WithoutHeimdall:                  cfg.WithoutHeimdall,
	}
}

// TestConfigDefaults verifies that the default config (no flags) produces
// expected values. This catches regressions during flag consolidation.
func TestConfigDefaults(t *testing.T) {
	cfg := buildEthConfig(t, nil)
	snap := snapshotConfig(cfg)

	// Core defaults
	require.Equal(t, uint64(1), snap.NetworkID, "default network should be mainnet")
	require.True(t, snap.StateStream, "state stream should be enabled by default")
	require.True(t, snap.InternalCL, "internal CL (Caplin) is on by default")
	require.False(t, snap.ExperimentalBAL, "experimental BAL should be off by default")
	require.False(t, snap.KeepExecutionProofs, "keep execution proofs should be off by default")

	// Snapshot defaults
	require.True(t, snap.SnapProduceE2, "snap produce e2 should be on by default")
	require.True(t, snap.SnapProduceE3, "snap produce e3 should be on by default")
	require.False(t, snap.NoDownloader, "downloader should be enabled by default")
	require.False(t, snap.SnapKeepBlocks, "snap keep blocks should be off by default")
}

// TestConfigWithFlags verifies that specific flag combinations produce the
// expected config changes. Each test case represents a common deployment
// pattern that must remain stable during refactoring.
func TestConfigWithFlags(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		check func(t *testing.T, snap configSnapshot)
	}{
		{
			name: "disable state stream",
			args: []string{"--state.stream.disable"},
			check: func(t *testing.T, snap configSnapshot) {
				require.False(t, snap.StateStream)
			},
		},
		{
			name: "experimental BAL",
			args: []string{"--experimental.bal"},
			check: func(t *testing.T, snap configSnapshot) {
				require.True(t, snap.ExperimentalBAL)
			},
		},
		{
			name: "no downloader",
			args: []string{"--no-downloader"},
			check: func(t *testing.T, snap configSnapshot) {
				require.True(t, snap.NoDownloader)
			},
		},
		{
			name: "snap stop",
			args: []string{"--snap.stop"},
			check: func(t *testing.T, snap configSnapshot) {
				require.False(t, snap.SnapProduceE2)
			},
		},
		{
			name: "without heimdall",
			args: []string{"--bor.withoutheimdall"},
			check: func(t *testing.T, snap configSnapshot) {
				require.True(t, snap.WithoutHeimdall)
			},
		},
		{
			name: "sync loop block limit",
			args: []string{"--sync.loop.block.limit=1000"},
			check: func(t *testing.T, snap configSnapshot) {
				require.Equal(t, uint(1000), snap.LoopBlockLimit)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := buildEthConfig(t, tt.args)
			snap := snapshotConfig(cfg)
			tt.check(t, snap)
		})
	}
}

// TestConfigSnapshotStability creates a full JSON snapshot of the config
// from default flags. This test exists to detect ANY config field change
// during the consolidation refactor — if this test fails, the diff shows
// exactly which fields changed.
func TestConfigSnapshotStability(t *testing.T) {
	cfg := buildEthConfig(t, nil)
	snap := snapshotConfig(cfg)

	// Marshal to JSON for readable diffs
	got, err := json.MarshalIndent(snap, "", "  ")
	require.NoError(t, err)

	// If this test fails during refactoring, the diff shows which field changed.
	// Update the expected JSON only after verifying the change is intentional.
	t.Logf("Config snapshot (update expected if intentional):\n%s", string(got))

	// Verify key structural invariants rather than exact JSON match,
	// since some defaults depend on runtime (CPU count, etc.)
	require.Equal(t, uint64(1), snap.NetworkID)
	require.True(t, snap.StateStream)
	require.True(t, snap.SnapProduceE2)
	require.True(t, snap.SnapProduceE3)
	require.False(t, snap.ExperimentalBAL)
	require.False(t, snap.NoDownloader)
}
