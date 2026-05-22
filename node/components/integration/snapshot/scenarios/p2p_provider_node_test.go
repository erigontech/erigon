//go:build p2p_integration

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

package scenarios_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
)

// TestP2P_StorageProviderNode_Constructs is the smoke test for the
// real-storage-Provider harness node: NewP2PNodeWithStorageProvider must
// drive storage.Provider.Initialize to completion (real BlockRetire, real
// lifecycle driver, real orchestrator) and expose the Provider-owned bus
// + orchestrator. A node that compiles but panics inside Initialize is
// useless, so this guards the re-architecture before scenarios lean on it.
func TestP2P_StorageProviderNode_Constructs(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	node := harness.NewP2PNodeWithStorageProvider(t, logger)

	require.NotNil(t, node.Orch, "Provider must expose its orchestrator")
	require.NotNil(t, node.Bus, "Provider must expose its event bus")
	require.NotNil(t, node.Inventory, "Provider node must wire an inventory")
	require.NotNil(t, node.InitialStateReady, "Provider node must wire InitialStateReady")
	require.Nil(t, node.Storage, "Provider node has no MockStorage stand-in")
}
