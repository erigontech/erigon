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

// Package forkexport is the shared writer for fork-chain CL config
// artefacts. Both `snapshots fork-from` (offline datadir builder) and
// `Ethereum.ApplyPostSwapHooks` (in-process debug_setFork transition)
// call WriteForkCLConfig so a byte-identical file lands on disk from
// either entry point. Equivalence is asserted by
// TestWriteForkCLConfig_TwoEntryPointsMatch in this package.
package forkexport

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
)

// ForkCLConfigFilename returns the datadir-relative filename for a
// fork chain's CL config artefact. The name embeds the fork chain
// name so a datadir can carry multiple fork configs across successive
// transitions without collisions. Empty forkChainName falls back to
// the pre-fork "cl-config.yaml" for backward compatibility with root-
// chain datadirs.
func ForkCLConfigFilename(forkChainName string) string {
	if forkChainName == "" {
		return "cl-config.yaml"
	}
	return "cl-config." + forkChainName + ".yaml"
}

// WriteForkCLConfig emits a CL config for a fork chain: loads the
// parent's BeaconChainConfig from clparams, overrides ConfigName to
// the fork's name, marshals as YAML, writes to
// <datadir>/<ForkCLConfigFilename(forkChainName)>.
//
// Fork inherits every other consensus parameter from the parent —
// sufficient for a Flavour-1 fork whose Caplin runs against the
// parent's CL via checkpoint sync. A true shadow fork (distinct
// GENESIS_FORK_VERSION, GenesisValidatorsRoot, MinGenesisTime,
// post-cut fork versions) needs operator-supplied overrides + a
// fresh genesis.ssz; that's a later milestone.
//
// Returns the absolute path written on success. Logs at Info level
// with the same "cl-config.yaml written" phrasing both entry points
// used previously so log-grep operators aren't disrupted.
func WriteForkCLConfig(datadir, parentChain, forkChainName string, logger log.Logger) (string, error) {
	if datadir == "" {
		return "", fmt.Errorf("WriteForkCLConfig: empty datadir")
	}
	if parentChain == "" {
		return "", fmt.Errorf("WriteForkCLConfig: empty parentChain")
	}
	if forkChainName == "" {
		return "", fmt.Errorf("WriteForkCLConfig: empty forkChainName")
	}
	_, beaconCfg, _, err := clparams.GetConfigsByNetworkName(parentChain)
	if err != nil {
		return "", fmt.Errorf("get parent CL config: %w", err)
	}
	cfg := *beaconCfg // copy so we don't mutate the package-level default
	cfg.ConfigName = forkChainName
	// Drop ForkVersionSchedule: yaml.v3 doesn't sort map keys, so
	// two invocations serialize this field in random order and
	// break byte-equal reproducibility. The field is DERIVED from
	// per-fork ForkVersion/ForkEpoch fields at load time via
	// configForkSchedule (cl/clparams/config.go:788) so dropping it
	// is lossless — the loader rebuilds it. Byte-equal output for
	// the same input is the invariant asserted by
	// TestWriteForkCLConfig_TwoEntryPointsMatch.
	cfg.ForkVersionSchedule = nil

	body, err := yaml.Marshal(&cfg)
	if err != nil {
		return "", fmt.Errorf("marshal beacon config: %w", err)
	}
	path := filepath.Join(datadir, ForkCLConfigFilename(forkChainName))
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	if logger != nil {
		logger.Info("cl-config yaml written",
			"path", path,
			"parent_config", beaconCfg.ConfigName,
			"fork_config", cfg.ConfigName)
	}
	return path, nil
}
