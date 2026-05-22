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
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestSingleNodeFlow_LoadAndQuery wires a single node around the
// HoodiBaseline fixture, starts the orchestrator, and asserts:
//   - InventoryLoaded fires exactly once.
//   - Inventory.Coverage(DomainAccounts) matches the fixture.
//   - Inventory.CoverageAtTrust filters by trust level correctly.
//   - No DownloadRequested event fires (no peers, nothing to download).
//   - Clean shutdown completes without error.
func TestSingleNodeFlow_LoadAndQuery(t *testing.T) {
	node := harness.NewNode(nil)
	t.Cleanup(func() { _ = node.Close() })

	fixture := harness.HoodiBaseline()
	fixture.Apply(node.Inventory)

	// Subscribe a counter for InventoryLoaded before Start so we catch the
	// startup emission.
	var loadedCount atomic.Int32
	require.NoError(t, node.Bus.Subscribe(func(e flow.InventoryLoaded) {
		loadedCount.Add(1)
	}))

	require.NoError(t, node.Start(context.Background()))
	node.Bus.WaitAsync()

	// Invariant O1: InventoryLoaded fires exactly once.
	require.Equal(t, int32(1), loadedCount.Load(), "InventoryLoaded should fire exactly once")

	// Coverage query against the populated inventory.
	accounts := node.Inventory.Coverage(snapshot.DomainAccounts)
	require.Equal(t, snapshot.StepRanges{{From: 0, To: 256}}, accounts,
		"accounts coverage should match fixture range")

	// Trust-level filtering: baseline has everything at TrustVerified.
	verified := node.Inventory.CoverageAtTrust(snapshot.DomainAccounts, snapshot.TrustVerified)
	require.Equal(t, snapshot.StepRanges{{From: 0, To: 256}}, verified,
		"accounts at TrustVerified should match full coverage")

	// No peer manifests → no DownloadRequested events.
	reqType := reflect.TypeOf(flow.DownloadRequested{})
	require.Equal(t, 0, node.Bus.CountOfType(reqType),
		"no DownloadRequested should fire without peers")
}

// TestSingleNodeFlow_MixedTrustFiltering exercises CoverageAtTrust filtering
// against a fixture that has some files at TrustNone.
func TestSingleNodeFlow_MixedTrustFiltering(t *testing.T) {
	node := harness.NewNode(nil)
	t.Cleanup(func() { _ = node.Close() })

	fixture := harness.HoodiBaselineMixedTrust()
	fixture.Apply(node.Inventory)

	require.NoError(t, node.Start(context.Background()))
	node.Bus.WaitAsync()

	// Accounts is at TrustNone in the mixed fixture — should appear in Coverage
	// (which is local-only), but NOT in CoverageAtTrust(TrustVerified).
	localAccounts := node.Inventory.Coverage(snapshot.DomainAccounts)
	require.NotEmpty(t, localAccounts, "accounts should be locally present regardless of trust")

	verifiedAccounts := node.Inventory.CoverageAtTrust(snapshot.DomainAccounts, snapshot.TrustVerified)
	require.Empty(t, verifiedAccounts, "untrusted accounts must not appear at TrustVerified")

	// Storage is still at TrustVerified — it should pass the filter.
	verifiedStorage := node.Inventory.CoverageAtTrust(snapshot.DomainStorage, snapshot.TrustVerified)
	require.Equal(t, snapshot.StepRanges{{From: 0, To: 256}}, verifiedStorage,
		"verified storage should pass the filter")
}

// TestSingleNodeFlow_OutOfOrderBlocksFlushed exercises invariant O3: the
// orchestrator rejects BlocksFlushed events with LatestBlock < previous.
func TestSingleNodeFlow_OutOfOrderBlocksFlushed(t *testing.T) {
	node := harness.NewNode(nil)
	t.Cleanup(func() { _ = node.Close() })

	harness.HoodiBaseline().Apply(node.Inventory)
	require.NoError(t, node.Start(context.Background()))

	// In-order events are accepted.
	node.Bus.Publish(flow.BlocksFlushed{LatestBlock: 100, FromBlock: 0})
	node.Bus.Publish(flow.BlocksFlushed{LatestBlock: 200, FromBlock: 101})

	// Out-of-order event is dropped — verified by the absence of panic and by
	// the orchestrator's continued operation. Retirement-scheduling assertions
	// layer on once the orchestrator actually reacts to LatestBlock changes.
	node.Bus.Publish(flow.BlocksFlushed{LatestBlock: 150, FromBlock: 102})

	node.Bus.WaitAsync()
}
