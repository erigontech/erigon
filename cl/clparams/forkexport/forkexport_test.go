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

package forkexport

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// TestForkCLConfigFilename_Convention pins the fork-name-scoped
// filename convention. Callers rely on both branches (fork chain vs
// legacy fallback) — a change here breaks datadir discovery at boot.
func TestForkCLConfigFilename_Convention(t *testing.T) {
	require.Equal(t, "cl-config.hoodi-fork-42.yaml", ForkCLConfigFilename("hoodi-fork-42"))
	require.Equal(t, "cl-config.yaml", ForkCLConfigFilename(""))
}

// TestWriteForkCLConfig_TwoEntryPointsMatch asserts that the two
// callers of WriteForkCLConfig (`snapshots fork-from` in cmd/, and
// Ethereum.applyForkWriteCLConfig in node/eth/backend.go) produce
// BYTE-IDENTICAL output for the same (parentChain, forkChainName)
// pair. Since they now share the single WriteForkCLConfig
// implementation, "two entry points" is modelled as two invocations
// into separate datadirs — any drift here means the shared writer
// itself started producing non-deterministic output, which would
// silently divide fork datadirs into "produced by CLI" vs
// "produced by RPC" flavours.
func TestWriteForkCLConfig_TwoEntryPointsMatch(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()

	pathA, err := WriteForkCLConfig(dirA, "hoodi", "hoodi-fork-1", log.Root())
	require.NoError(t, err)
	pathB, err := WriteForkCLConfig(dirB, "hoodi", "hoodi-fork-1", log.Root())
	require.NoError(t, err)

	// Both writers land on the same fork-scoped filename.
	require.Equal(t, filepath.Base(pathA), filepath.Base(pathB))
	require.Equal(t, ForkCLConfigFilename("hoodi-fork-1"), filepath.Base(pathA))

	bodyA, err := os.ReadFile(pathA)
	require.NoError(t, err)
	bodyB, err := os.ReadFile(pathB)
	require.NoError(t, err)
	require.Equal(t, bodyA, bodyB,
		"two invocations for the same fork must emit byte-identical files — divergence here breaks the invariant that offline `snapshots fork-from` and in-process debug_setFork prepare interchangeable datadirs")
}

// TestWriteForkCLConfig_ForkNameOverridesConfigName confirms the
// marshalled YAML actually carries the fork's ConfigName (not the
// parent's). Guards against a future refactor that forgets to apply
// the override before Marshal.
func TestWriteForkCLConfig_ForkNameOverridesConfigName(t *testing.T) {
	dir := t.TempDir()
	path, err := WriteForkCLConfig(dir, "hoodi", "hoodi-fork-42", log.Root())
	require.NoError(t, err)

	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(body), "hoodi-fork-42",
		"emitted YAML must carry the fork's ConfigName, not the parent's")
}

// TestWriteForkCLConfig_RejectsEmptyInputs enforces the fail-fast
// contract at the writer boundary. Every caller relies on these
// checks — if we ever relax them, an empty forkChainName would land
// on disk as "cl-config..yaml", indistinguishable from the legacy
// fallback name.
func TestWriteForkCLConfig_RejectsEmptyInputs(t *testing.T) {
	_, err := WriteForkCLConfig("", "hoodi", "hoodi-fork-1", log.Root())
	require.ErrorContains(t, err, "empty datadir")

	_, err = WriteForkCLConfig(t.TempDir(), "", "hoodi-fork-1", log.Root())
	require.ErrorContains(t, err, "empty parentChain")

	_, err = WriteForkCLConfig(t.TempDir(), "hoodi", "", log.Root())
	require.ErrorContains(t, err, "empty forkChainName")
}

// TestWriteForkCLConfig_UnknownParentChainErrors covers the
// pass-through error from clparams — an unknown parent name means we
// can't derive a CL config, and the caller must abort rather than
// emit a bogus artefact.
func TestWriteForkCLConfig_UnknownParentChainErrors(t *testing.T) {
	_, err := WriteForkCLConfig(t.TempDir(), "not-a-real-chain", "not-a-real-chain-fork-1", log.Root())
	require.Error(t, err)
	require.Contains(t, err.Error(), "parent CL config")
}

// TestWriteForkGenesisSSZ_TwoEntryPointsMatch pins the genesis.ssz
// byte-equivalence contract: `snapshots fork-from` and
// Ethereum.applyForkWriteGenesisSSZ share the same writer and must
// land byte-identical files in two independent datadirs. Uses
// "sepolia" as the parent — its genesis is embedded in the binary
// (no network call), so this test runs offline.
func TestWriteForkGenesisSSZ_TwoEntryPointsMatch(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()

	pathA, err := WriteForkGenesisSSZ(dirA, "sepolia", "sepolia-fork-1", log.Root())
	require.NoError(t, err)
	pathB, err := WriteForkGenesisSSZ(dirB, "sepolia", "sepolia-fork-1", log.Root())
	require.NoError(t, err)

	require.Equal(t, ForkGenesisSSZFilename, filepath.Base(pathA))
	require.Equal(t, ForkGenesisSSZFilename, filepath.Base(pathB))

	bodyA, err := os.ReadFile(pathA)
	require.NoError(t, err)
	bodyB, err := os.ReadFile(pathB)
	require.NoError(t, err)
	require.Equal(t, bodyA, bodyB,
		"two invocations for the same parent must emit byte-identical genesis.ssz — divergence here breaks the invariant that offline `snapshots fork-from` and in-process debug_setFork prepare interchangeable datadirs")
	require.NotEmpty(t, bodyA, "genesis.ssz must not be empty")
}

// TestWriteForkGenesisSSZ_RejectsEmptyInputs enforces the fail-fast
// contract at the writer boundary — parallel to WriteForkCLConfig's
// checks.
func TestWriteForkGenesisSSZ_RejectsEmptyInputs(t *testing.T) {
	_, err := WriteForkGenesisSSZ("", "sepolia", "sepolia-fork-1", log.Root())
	require.ErrorContains(t, err, "empty datadir")

	_, err = WriteForkGenesisSSZ(t.TempDir(), "", "sepolia-fork-1", log.Root())
	require.ErrorContains(t, err, "empty parentChain")

	_, err = WriteForkGenesisSSZ(t.TempDir(), "sepolia", "", log.Root())
	require.ErrorContains(t, err, "empty forkChainName")
}

// TestWriteForkGenesisSSZ_UnknownParentChainErrors mirrors the CL
// config variant — an unknown parent means we can't derive genesis.
func TestWriteForkGenesisSSZ_UnknownParentChainErrors(t *testing.T) {
	_, err := WriteForkGenesisSSZ(t.TempDir(), "not-a-real-chain", "not-a-real-chain-fork-1", log.Root())
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve parent network id")
}
