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
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
)

// writeChainJSON writes a fork-shaped chain.Config to datadir/chain.json.
// Returns the datadir path so the caller can pass it to
// LoadForkChainSpec.
func writeChainJSON(t *testing.T, cfg *chain.Config) string {
	t.Helper()
	datadir := t.TempDir()
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(datadir, "chain.json"), data, 0o644))
	return datadir
}

func TestLoadForkChainSpec_LoadsFromDatadirAndInheritsParentGenesis(t *testing.T) {
	forkName := "mainnet-fork-testload-1"
	cfg := &chain.Config{
		ChainName: forkName,
		ChainID:   uint256.NewInt(1),
		Parent:    "mainnet",
		CutBlock:  20_000_000,
	}
	datadir := writeChainJSON(t, cfg)

	spec, err := LoadForkChainSpec(forkName, datadir)
	require.NoError(t, err)
	require.Equal(t, forkName, spec.Name)
	require.NotEmpty(t, spec.GenesisHash, "fork inherits parent's genesis hash")
	require.NotNil(t, spec.Config)
	require.Equal(t, "mainnet", spec.Config.Parent)

	// After LoadForkChainSpec, ChainSpecByName finds the fork.
	byName, err := ChainSpecByName(forkName)
	require.NoError(t, err)
	require.Equal(t, spec.Name, byName.Name)
}

func TestLoadForkChainSpec_MissingChainJSONErrors(t *testing.T) {
	_, err := LoadForkChainSpec("some-fork", t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "read fork chain.json")
}

func TestLoadForkChainSpec_ChainNameMismatchErrors(t *testing.T) {
	cfg := &chain.Config{
		ChainName: "declared-inside",
		ChainID:   uint256.NewInt(1),
		Parent:    "mainnet",
		CutBlock:  20_000_000,
	}
	datadir := writeChainJSON(t, cfg)

	_, err := LoadForkChainSpec("requested-outside", datadir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match")
}

func TestLoadForkChainSpec_NoParentErrors(t *testing.T) {
	cfg := &chain.Config{
		ChainName: "not-a-fork",
		ChainID:   uint256.NewInt(1),
	}
	datadir := writeChainJSON(t, cfg)

	_, err := LoadForkChainSpec("not-a-fork", datadir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no parent")
}

func TestLoadForkChainSpec_UnknownParentErrors(t *testing.T) {
	cfg := &chain.Config{
		ChainName: "no-such-parent-fork",
		ChainID:   uint256.NewInt(999),
		Parent:    "unregistered-chain-xyz",
		CutBlock:  100,
	}
	datadir := writeChainJSON(t, cfg)

	_, err := LoadForkChainSpec("no-such-parent-fork", datadir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parent chain")
}

func TestChainSpecByNameOrForkDatadir_HitsRegistryFirst(t *testing.T) {
	spec, err := ChainSpecByNameOrForkDatadir("mainnet", "/nonexistent/path")
	require.NoError(t, err)
	require.Equal(t, "mainnet", spec.Name)
}

func TestChainSpecByNameOrForkDatadir_FallsBackToDatadir(t *testing.T) {
	forkName := "mainnet-fork-fallback"
	cfg := &chain.Config{
		ChainName: forkName,
		ChainID:   uint256.NewInt(1),
		Parent:    "mainnet",
		CutBlock:  20_000_000,
	}
	datadir := writeChainJSON(t, cfg)

	spec, err := ChainSpecByNameOrForkDatadir(forkName, datadir)
	require.NoError(t, err)
	require.Equal(t, forkName, spec.Name)
}

func TestChainSpecByNameOrForkDatadir_FallbackFailsPropagates(t *testing.T) {
	_, err := ChainSpecByNameOrForkDatadir("no-such-fork", t.TempDir())
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrChainSpecUnknown) || err.Error() != "",
		"error must be actionable (either the registry-miss or the loader-miss)")
}
