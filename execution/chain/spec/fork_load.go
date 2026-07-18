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

package chainspec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/execution/chain"
)

// LoadForkChainSpec constructs and registers a fork-chain's Spec by
// reading `<datadir>/chain.json` (written by `snapshots fork-from`)
// and inheriting the parent's genesis pieces from the local built-in
// registry. Enables `--chain=<fork-name> --datadir=<fork-datadir>` to
// boot a Flavour-1 fork without the fork name being in the registry
// at compile time.
//
// The lookup is intentionally a FALLBACK — callers try
// ChainSpecByName first and invoke this only if the chain name is
// unknown. Failure modes:
//   - chain.json missing or unreadable → returns the underlying error.
//   - chain.json ChainName mismatches the requested name → error.
//   - chain.json has no Parent (i.e. it's a root chain but the
//     operator declared it out of registry) → not our concern; error.
//   - Parent chain not in the local registry → error (we can't
//     inherit genesis from a chain we don't know).
//
// On success the fork spec is registered so subsequent
// ChainSpecByName calls succeed and NetworkNameByID picks it up.
// Fork genesis + genesisHash inherit from the parent for
// pre-CutBlock lookups; forward progression past CutBlock still
// requires the CL artefacts (Phase 2c-CL).
func LoadForkChainSpec(chainName, datadir string) (Spec, error) {
	if chainName == "" {
		return Spec{}, errors.New("LoadForkChainSpec: empty chain name")
	}
	if datadir == "" {
		return Spec{}, errors.New("LoadForkChainSpec: empty datadir")
	}
	chainJSONPath := filepath.Join(datadir, "chain.json")
	data, err := os.ReadFile(chainJSONPath)
	if err != nil {
		return Spec{}, fmt.Errorf("read fork chain.json: %w", err)
	}
	cfg := &chain.Config{}
	if err := json.Unmarshal(data, cfg); err != nil {
		return Spec{}, fmt.Errorf("parse fork chain.json: %w", err)
	}
	if cfg.ChainName != chainName {
		return Spec{}, fmt.Errorf("chain.json ChainName=%q does not match requested --chain=%q", cfg.ChainName, chainName)
	}
	if cfg.Parent == "" {
		return Spec{}, fmt.Errorf("chain.json has no parent — not a fork chain, and %q is not in the built-in registry", chainName)
	}
	if cfg.ChainID == nil || cfg.ChainID.IsZero() {
		return Spec{}, fmt.Errorf("chain.json has no ChainID")
	}
	parentSpec, err := ChainSpecByName(cfg.Parent)
	if err != nil {
		return Spec{}, fmt.Errorf("parent chain %q not in registry: %w", cfg.Parent, err)
	}
	networkID := cfg.NetworkID
	if networkID == 0 {
		networkID = cfg.ChainID.Uint64()
	}
	spec := Spec{
		Name:             chainName,
		GenesisHash:      parentSpec.GenesisHash,
		GenesisStateRoot: parentSpec.GenesisStateRoot,
		Genesis:          parentSpec.Genesis,
		Config:           cfg,
		Bootnodes:        nil, // fork-specific — populated separately when Phase 2c-CL / operator wiring supplies them
		NetworkID:        networkID,
	}
	RegisterChainSpec(chainName, spec)
	return spec, nil
}

// ChainSpecByNameOrForkDatadir tries the built-in registry first; on
// ErrChainSpecUnknown it falls back to LoadForkChainSpec against the
// supplied datadir. The intended call site is startup after the
// operator has run `snapshots fork-from` and boots erigon with
// `--chain=<fork-name> --datadir=<fork-datadir>`.
func ChainSpecByNameOrForkDatadir(chainName, datadir string) (Spec, error) {
	if spec, err := ChainSpecByName(chainName); err == nil {
		return spec, nil
	} else if !errors.Is(err, ErrChainSpecUnknown) {
		return Spec{}, err
	}
	return LoadForkChainSpec(chainName, datadir)
}
