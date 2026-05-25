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

package jsonrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// realExecModuleBackend wraps an ExecModuleTester so the RPC layer's
// privateapi.EthBackend.SetHead routes to the *real* ExecModule.SetHead
// (which runs the staged-unwind pipeline and commits state changes),
// not to the validation-only mockEthBackend used by TestSetHead. Every
// other EthBackend method returns the zero value — these tests only
// drive SetHead through the RPC layer.
type realExecModuleBackend struct {
	m *execmoduletester.ExecModuleTester
}

var _ privateapi.EthBackend = (*realExecModuleBackend)(nil)

func (r *realExecModuleBackend) Etherbase() (common.Address, error) { return common.Address{}, nil }
func (r *realExecModuleBackend) NetVersion() (uint64, error)        { return 1, nil }
func (r *realExecModuleBackend) NetPeerCount() (uint64, error)      { return 0, nil }
func (r *realExecModuleBackend) NodesInfo(int) (*remoteproto.NodesInfoReply, error) {
	return &remoteproto.NodesInfoReply{}, nil
}
func (r *realExecModuleBackend) Peers(context.Context) (*remoteproto.PeersReply, error) {
	return &remoteproto.PeersReply{}, nil
}
func (r *realExecModuleBackend) AddPeer(context.Context, *remoteproto.AddPeerRequest) (*remoteproto.AddPeerReply, error) {
	return &remoteproto.AddPeerReply{}, nil
}
func (r *realExecModuleBackend) RemovePeer(context.Context, *remoteproto.RemovePeerRequest) (*remoteproto.RemovePeerReply, error) {
	return &remoteproto.RemovePeerReply{}, nil
}
func (r *realExecModuleBackend) AddTrustedPeer(context.Context, *remoteproto.AddPeerRequest) (*remoteproto.AddPeerReply, error) {
	return nil, nil
}
func (r *realExecModuleBackend) RemoveTrustedPeer(context.Context, *remoteproto.RemovePeerRequest) (*remoteproto.RemovePeerReply, error) {
	return nil, nil
}
func (r *realExecModuleBackend) SetHead(ctx context.Context, targetBlock uint64) error {
	return r.m.ExecModule.SetHead(ctx, targetBlock)
}

// newSetHeadE2EAPI wires the real RPC chain (DebugAPIImpl → remote
// backend → in-process direct client → EthBackendServer → real
// ExecModule) so SetHead requests go through every layer a real node
// touches. Mirrors TestSetHead's makeAPI but with realExecModuleBackend
// in place of mockEthBackend.
func newSetHeadE2EAPI(t *testing.T, m *execmoduletester.ExecModuleTester) *DebugAPIImpl {
	t.Helper()
	backendServer := privateapi.NewEthBackendServer(
		m.Ctx, &realExecModuleBackend{m: m}, m.DB, m.Notifications, m.BlockReader,
		nil, log.New(), builder.NewLatestBlockBuiltStore(), nil,
	)
	backendClient := direct.NewEthBackendClientDirect(backendServer)
	backend := rpcservices.NewRemoteBackend(backendClient, m.DB, m.BlockReader)
	return NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, backend, 0, false)
}

// canonicalHeadAndMinUnwindable reads the values both scenarios anchor
// off of: the current canonical head + the lowest block SetHead's
// CanUnwindToBlockNum guard will accept as a target.
func canonicalHeadAndMinUnwindable(t *testing.T, m *execmoduletester.ExecModuleTester) (head, minUnwindable uint64) {
	t.Helper()
	roTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	head, err = rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	minUnwindable, err = rawtemporaldb.CanUnwindToBlockNum(roTx)
	require.NoError(t, err)
	return head, minUnwindable
}

// TestSetHead_E2E_LimitedRange — Scenario 1.
//
// Pins the current limited-range contract end-to-end through the RPC
// layer with a real ExecModule (not the validation-only mock that
// TestSetHead uses). Covers the bounds SetHead enforces today:
//
//   - target == head: no-op success (CurrentHead == target short-circuit).
//   - target == head + 1: rejection ("future block").
//   - target < minUnwindable: rejection ("minimum unwindable block"),
//     when minUnwindable > 0 — otherwise we pin that target = 0
//     succeeds (still meaningful: no panic on the boundary).
//
// Within-range successes that actually rewind + execute past the new
// head are TestSetHead_E2E_WithinDB_ExecutesPastNewHead's job.
func TestSetHead_E2E_LimitedRange(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := m.Ctx
	api := newSetHeadE2EAPI(t, m)
	head, minUnwindable := canonicalHeadAndMinUnwindable(t, m)
	require.Greater(t, head, uint64(1), "test chain must have at least 2 blocks")

	t.Run("target equals current head is no-op success", func(t *testing.T) {
		require.NoError(t, api.SetHead(ctx, hexutil.Uint64(head)))
		// Chain head must be unchanged — SetHead short-circuits the
		// targetBlock == currentHead branch before touching anything.
		stillHead, _ := canonicalHeadAndMinUnwindable(t, m)
		require.Equal(t, head, stillHead, "head must be unchanged after no-op SetHead")
	})

	t.Run("target one past head is rejected as future", func(t *testing.T) {
		err := api.SetHead(ctx, hexutil.Uint64(head+1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "future")
	})

	t.Run("CanUnwindToBlockNum guard", func(t *testing.T) {
		if minUnwindable > 0 {
			err := api.SetHead(ctx, hexutil.Uint64(minUnwindable-1))
			require.Error(t, err)
			require.Contains(t, err.Error(), "minimum unwindable block",
				"SetHead must surface the CanUnwindToBlockNum rejection unchanged")
		} else {
			// minUnwindable == 0 on this fixture (no commitment writes
			// established a higher floor). Pin that the boundary case
			// — target == 0 — at least reaches the backend without a
			// guard panic. Whether it succeeds or fails afterwards is
			// scenario-2 territory; here we just rule out a regression
			// where the guard check itself starts misbehaving on the
			// zero boundary.
			err := api.SetHead(ctx, hexutil.Uint64(0))
			if err != nil {
				require.NotContains(t, err.Error(), "minimum unwindable block",
					"guard must not reject target 0 when minUnwindable is 0")
			}
		}
	})
}

// TestSetHead_E2E_WithinDB_ExecutesPastNewHead — Scenario 2.
//
// The real end-to-end contract: an operator hits debug_setHead via
// RPC; the chain rewinds to a target that's still in the writable db
// (no snapshot files involved on this fixture); a subsequent Engine
// API call drives execution forward from the new head; the chain
// returns to the original head with execution succeeding for every
// re-executed block.
//
// "Run the RPC, then a block, and make sure it executes" — that's
// what this asserts. Without the trailing UpdateForkChoice +
// re-execution check, a passing SetHead test only proves the DB
// surface was tidied up; it doesn't prove the post-unwind state is
// actually usable.
//
// Scenario 3 (target lands inside a snapshot file range) ships
// alongside the SetHead→Provider.Unwind wiring; CreateTestExecModule
// produces no snapshot files so this fixture can't reach that
// codepath.
func TestSetHead_E2E_WithinDB_ExecutesPastNewHead(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := m.Ctx
	api := newSetHeadE2EAPI(t, m)
	head, minUnwindable := canonicalHeadAndMinUnwindable(t, m)
	require.Greater(t, head, uint64(2), "test chain needs at least 3 blocks for a meaningful rewind")

	// Pick a target strictly below head but at or above minUnwindable.
	// head-2 is the natural "within db" target on the 13-block fixture
	// — far enough back to actually trigger TxNums/canonical truncation
	// without bumping into the unwindable-floor.
	target := head - 2
	if target < minUnwindable {
		target = minUnwindable
	}
	require.Less(t, target, head, "target must be strictly below head for this scenario")

	// Snapshot the pre-rewind canonical chain so we know what hashes
	// should disappear (head-1..head) and what hash should survive
	// (target). Same shape as TestSetHeadCanonicalCleanup's setup.
	roTx, err := m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	originalHeadHash, err := rawdb.ReadCanonicalHash(roTx, head)
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, originalHeadHash)
	targetHash, err := rawdb.ReadCanonicalHash(roTx, target)
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, targetHash)
	staleHashes := make(map[uint64]common.Hash, head-target)
	for b := target + 1; b <= head; b++ {
		h, err := rawdb.ReadCanonicalHash(roTx, b)
		require.NoError(t, err)
		require.NotEqual(t, common.Hash{}, h, "block %d must be canonical before unwind", b)
		staleHashes[b] = h
	}
	roTx.Rollback()

	// --- Phase 1: the RPC call ---
	require.NoError(t, api.SetHead(ctx, hexutil.Uint64(target)),
		"debug_setHead via RPC + real ExecModule must succeed for an in-db target")

	// Verify the rewind actually happened end-to-end through the
	// RPC layer: canonical hashes above target are gone, target hash
	// survives, HeadHeaderHash points at target, latest-block lookup
	// returns target.
	roTx, err = m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	for b := range staleHashes {
		h, err := rawdb.ReadCanonicalHash(roTx, b)
		require.NoError(t, err)
		require.Equal(t, common.Hash{}, h, "canonical hash at %d must be cleared after SetHead(%d)", b, target)
	}
	survived, err := rawdb.ReadCanonicalHash(roTx, target)
	require.NoError(t, err)
	require.Equal(t, targetHash, survived, "target-block canonical hash must survive the unwind")
	require.Equal(t, targetHash, rawdb.ReadHeadHeaderHash(roTx), "HeadHeaderHash must point at target after SetHead")
	latest, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	require.Equal(t, target, latest, "GetLatestBlockNumber must return target post-SetHead")
	roTx.Rollback()

	// --- Phase 2: execute past the new head ---
	// Drive Engine API UpdateForkChoice to re-execute blocks
	// target+1..head. The chain must come back to the original head
	// with execution succeeding for every block — the post-SetHead
	// state has to be alive enough to grow forward, not just clean
	// on the surface.
	result, err := m.ExecModule.UpdateForkChoice(ctx, originalHeadHash, originalHeadHash, originalHeadHash)
	require.NoError(t, err, "UpdateForkChoice back to original head must drive execution successfully")
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status,
		"every re-executed block (target+1..head) must succeed; non-success means a block failed post-rewind")

	// Verify the chain is back at the original head with the right
	// canonical hash — proves the re-execution produced the same
	// blocks that were unwound.
	roTx, err = m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	restored, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	require.Equal(t, head, restored, "chain must be restored to original head after UpdateForkChoice")
	restoredHash, err := rawdb.ReadCanonicalHash(roTx, head)
	require.NoError(t, err)
	require.Equal(t, originalHeadHash, restoredHash,
		"re-executed head hash must match the original — proves identical block content was replayed")
}
