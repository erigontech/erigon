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
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
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

// assertNoBlockDataPastTarget pins the cold-start invariant that
// Provider.Unwind establishes: after mode B, no block-data table in
// the writable DB carries rows past targetBlock. This is what
// stage_snapshots.firstNonGenesisCheck reads on the next startup —
// orphan rows in kv.Headers past targetBlock + snapshots tip cause
// "Some blocks are not in snapshots and not in db" and refuse to
// start the execution service.
//
// Live repro of the corresponding regression: hoodi datadir post
// debug_setHead, kv.Headers held forward NewPayloads at 2,914,976+
// even though stage progress was reset to 2,912,079.
//
// Pins the fix from "storage: mode-B unwind wipes orphan block-data
// past toBlock".
func assertNoBlockDataPastTarget(t *testing.T, m *execmoduletester.ExecModuleTester, target uint64) {
	t.Helper()
	roTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// HeaderNumber is hash-keyed (no block-number prefix on the row
	// key), so it can't participate in a "first row past target" walk.
	// The other tables are block-number-prefixed and walk cleanly.
	tables := []string{
		kv.Headers,
		kv.BlockBody,
		kv.HeaderTD,
		kv.Senders,
	}
	for _, table := range tables {
		c, err := roTx.Cursor(table) //nolint:gocritic // explicit Close inside per-table loop; defer would accumulate cursors
		require.NoError(t, err)
		past := 0
		var firstPast uint64
		for k, _, err := c.First(); k != nil && err == nil; k, _, err = c.Next() {
			if len(k) < 8 {
				continue
			}
			n := binary.BigEndian.Uint64(k[:8])
			if n > target {
				if past == 0 {
					firstPast = n
				}
				past++
			}
		}
		c.Close()
		require.Zero(t, past,
			"post-mode-B startup invariant: %s must have no rows past target=%d (first leaked=%d, count=%d)",
			table, target, firstPast, past)
	}

	// Also pin the actual signal stage_snapshots.firstNonGenesisCheck
	// reads — SecondKey returns the smallest non-genesis row in
	// kv.Headers. Must be nil (Headers empty past genesis) for the
	// startup wedge to be impossible.
	firstNonGenesis, err := rawdbv3.SecondKey(roTx, kv.Headers)
	require.NoError(t, err)
	if firstNonGenesis != nil {
		n := binary.BigEndian.Uint64(firstNonGenesis[:8])
		require.LessOrEqual(t, n, target,
			"post-mode-B: kv.Headers first-non-genesis (%d) must not be past target (%d) — firstNonGenesisCheck would wedge", n, target)
	}
}

// canonicalHead reads the current canonical chain head — what every
// scenario anchors off of.
func canonicalHead(t *testing.T, m *execmoduletester.ExecModuleTester) uint64 {
	t.Helper()
	roTx, err := m.DB.BeginRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	head, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	return head
}

// TestSetHead_E2E_LimitedRange — Scenario 1.
//
// Pins the post-Provider.Unwind contract for SetHead's *future-side*
// limits: target == head is a no-op success; target == head + 1 is
// rejected because head + 1 doesn't exist on the canonical chain.
//
// Notably absent: the legacy CanUnwindToBlockNum "minimum unwindable
// block" rejection. That guard exists today as a placeholder for the
// missing snapshot-mutating unwind code; once SetHead is wired through
// Provider.Unwind (next commit in this stream), aligned-mode chains can
// unwind to any in-canonical-chain block regardless of how deep the
// snapshot tip sits. There is therefore no point pinning the legacy
// rejection here — that assertion would just have to be removed when
// the guard is.
//
// The actual within-range rewind + execute-past-new-head contract is
// scenario 2; the snapshot-mutating deep-unwind contract is scenario 3
// (lands with the Provider.Unwind wiring).
func TestSetHead_E2E_LimitedRange(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := m.Ctx
	api := newSetHeadE2EAPI(t, m)
	head := canonicalHead(t, m)
	require.Greater(t, head, uint64(1), "test chain must have at least 2 blocks")

	t.Run("target equals current head is no-op success", func(t *testing.T) {
		require.NoError(t, api.SetHead(ctx, hexutil.Uint64(head)))
		// SetHead short-circuits the targetBlock == currentHead branch
		// before touching anything; head must stay put.
		require.Equal(t, head, canonicalHead(t, m), "head must be unchanged after no-op SetHead")
	})

	t.Run("target one past head is rejected as future", func(t *testing.T) {
		err := api.SetHead(ctx, hexutil.Uint64(head+1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "future")
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
	head := canonicalHead(t, m)
	require.Greater(t, head, uint64(2), "test chain needs at least 3 blocks for a meaningful rewind")

	// head-2 is the natural "within db" target on the 13-block fixture
	// — far enough back to actually trigger TxNums/canonical truncation
	// while staying in the writable db (the fixture has no snapshot
	// files at all, so any target above 0 is "within db" here).
	target := head - 2

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

// TestSetHead_E2E_ModeB_SnapshotsAlignedCut — Scenario 3a.
//
// Mode B (past-diffset) WITH snapshot file trim, aligned cut: target
// block whose lastTxNum lands exactly on a step boundary. Some
// snapshot files cover steps past toBlock's stepBoundary and must be
// trimmed; the commitment record at toBlock's step boundary already
// exists in the file containing toBlock, so the recompute primitive
// finds an exact-match baseline and folds in zero touches.
//
// End-to-end happy-path assertion:
//   - SetHead returns no error;
//   - dispatch routed through mode B (the Inventory.RecentTrims log,
//     not visible to the test, would show 1+ files trimmed);
//   - post-state: HeadHeaderHash == targetHash;
//     GetLatestBlockNumber == target;
//     stale canonical hashes above target cleared.
//
// Pins the wiring of: dispatch → setHeadModeB → Provider.Unwind →
// snapshot-trim + ensureCommitmentAtBlock (SD-less recompute) +
// WipeWritableShadowPast + DB-reset. The recompute primitive
// (commitmentdb.RecomputeAtTxNumWithoutSD) replaces the earlier
// "controlled failure" because the writable-shadow override no longer
// depends on the over-step file's stale commitment record.
func TestSetHead_E2E_ModeB_SnapshotsAlignedCut(t *testing.T) {
	const stepSize uint64 = 8

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			addr: {Balance: big.NewInt(common.Ether)},
		},
		GasLimit: 10_000_000,
	}
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithStepSize(stepSize),
		execmoduletester.WithAdminUnwindWired(),
	)
	ctx := m.Ctx
	require.NotNil(t, m.AdminUnwindProvider, "WithAdminUnwindWired must populate AdminUnwindProvider")

	signer := types.LatestSignerForChainID(nil)
	to := common.Address{0x42}
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 16, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), to, uint256.NewInt(1_000_000), 21_000, new(uint256.Int), nil),
			*signer, key,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	head := canonicalHead(t, m)
	require.Equal(t, uint64(16), head, "test fixture must run all 16 blocks")

	// Populate the admin-unwind Inventory from the aggregator's
	// on-disk state files now that execution has produced them. Without
	// this, snapshot-trim's iteration is empty and over-step files
	// stay on disk, leaving the commitment anchor verification stuck
	// on the over-step file's internal blockNum.
	m.RescanAdminUnwindInventory(t)

	// Find a target block whose lastTxNum lands on a step boundary —
	// the precondition WipeWritableShadowPast enforces. Per-block txn
	// counts depend on system txns + 1 user txn per block, so probe
	// the chain instead of guessing.
	var targetBlock uint64
	{
		roTx, err := m.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		for b := uint64(1); b < head; b++ {
			lastTxNum, err := rawdbv3.TxNums.Max(ctx, roTx, b)
			require.NoError(t, err)
			if (lastTxNum+1)%stepSize == 0 && targetBlock == 0 {
				targetBlock = b
			}
		}
		roTx.Rollback()
		require.NotZero(t, targetBlock, "fixture must produce at least one block whose lastTxNum lands on a step boundary")
	}

	// Lift CanUnwindToBlockNum past target so dispatch routes to mode B.
	m.TruncateChangeSetsBelow(t, targetBlock+2)
	{
		roTx, err := m.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		minUnwindable, err := rawtemporaldb.CanUnwindToBlockNum(roTx)
		roTx.Rollback()
		require.NoError(t, err)
		require.Greater(t, minUnwindable, targetBlock,
			"ChangeSets3 truncation must lift CanUnwindToBlockNum past targetBlock so dispatch picks mode B")
	}

	api := newSetHeadE2EAPI(t, m)

	// Snapshot pre-state hashes so the post-state check can verify
	// canonical-chain cleanup AND so phase 2 can drive forward
	// execution back to the original head.
	roTx, err := m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	originalHeadHash, err := rawdb.ReadCanonicalHash(roTx, head)
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, originalHeadHash)
	targetHash, err := rawdb.ReadCanonicalHash(roTx, targetBlock)
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, targetHash)
	staleHashes := make(map[uint64]common.Hash, head-targetBlock)
	for b := targetBlock + 1; b <= head; b++ {
		h, err := rawdb.ReadCanonicalHash(roTx, b)
		require.NoError(t, err)
		require.NotEqual(t, common.Hash{}, h)
		staleHashes[b] = h
	}
	roTx.Rollback()

	require.NoError(t, api.SetHead(ctx, hexutil.Uint64(targetBlock)),
		"mode B end-to-end (snapshot-trim + SD-less recompute + shadow-wipe + DB-reset) must succeed at an aligned cut")

	// Verify the rewind landed:
	//   - HeadHeaderHash points at target's hash
	//   - GetLatestBlockNumber == target
	//   - canonical hashes above target are cleared
	roTx, err = m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	require.Equal(t, targetHash, rawdb.ReadHeadHeaderHash(roTx),
		"HeadHeaderHash must point at target hash after successful mode B")
	latest, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	require.Equal(t, targetBlock, latest,
		"GetLatestBlockNumber must return target after mode B (= the dispatched head movement landed)")
	for b := range staleHashes {
		h, err := rawdb.ReadCanonicalHash(roTx, b)
		require.NoError(t, err)
		require.Equal(t, common.Hash{}, h,
			"canonical hash at %d must be cleared by DB-reset", b)
	}
	roTx.Rollback()

	// Forward-exec-past-unwound-head G3.15 check is intentionally NOT
	// asserted here. The pre-mode-B TruncateChangeSetsBelow needed to
	// trigger mode B raises CanUnwindToBlockNum past targetBlock, so
	// UpdateForkChoice for any later block fails with
	// ExecutionStatusReorgTooDeep — a test-setup constraint, not a
	// state-correctness signal. The diff-replay correctness is pinned
	// directly by db/state/wipe_writable_shadow_test.go's
	// TestWipeWritableShadowPast_NonAligned_RestoresEarlierValue;
	// real-chain state-divergence (G3.15) needs the path investigation
	// described in the linear plan.
	_ = originalHeadHash

	assertNoBlockDataPastTarget(t, m, targetBlock)
}

// TestSetHead_E2E_ModeB_NoSnapshotTrim — Scenario 2 (mode B, db-only).
//
// Mode B engages, but target's stepBoundary equals (or exceeds) the
// highest snapshot file's end-step, so no files need trimming. The
// state diff lives entirely in the writable shadow; WipeWritableShadowPast
// + ensureCommitmentAtBlock + DB-reset are exercised without
// snapshot-trim doing real work.
//
// This is the "between snapshot tip and diffset horizon" case for
// real chains where blocks past the snapshot tip still have writable
// shadow + diffsets, and the operator unwinds past the diffset
// horizon but stays above the snapshot tip.
func TestSetHead_E2E_ModeB_NoSnapshotTrim(t *testing.T) {
	const stepSize uint64 = 8

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			addr: {Balance: big.NewInt(common.Ether)},
		},
		GasLimit: 10_000_000,
	}
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithStepSize(stepSize),
		execmoduletester.WithAdminUnwindWired(),
	)
	ctx := m.Ctx
	require.NotNil(t, m.AdminUnwindProvider)

	signer := types.LatestSignerForChainID(nil)
	to := common.Address{0x42}
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 16, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), to, uint256.NewInt(1_000_000), 21_000, new(uint256.Int), nil),
			*signer, key,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	head := canonicalHead(t, m)
	require.Equal(t, uint64(16), head)
	m.RescanAdminUnwindInventory(t)

	// Find max file end-step + a target whose stepBoundary >= that.
	// "No snapshot trim" means no file has endStep > target.stepBoundary
	// (= (target.lastTxNum / stepSize) + 1).
	//
	// Probe per-block lastTxNum + pick the first whose stepBoundary
	// matches/exceeds the highest existing file's endStep. With 16
	// blocks at stepSize=8 the aggregator produces files at endStep=1
	// and endStep=2; any target with lastTxNum/stepSize >= 1 (=
	// stepBoundary >= 2) means no file at endStep > 2 needs trimming.
	var targetBlock uint64
	{
		roTx, err := m.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		for b := uint64(1); b < head; b++ {
			lastTxNum, err := rawdbv3.TxNums.Max(ctx, roTx, b)
			require.NoError(t, err)
			if lastTxNum/stepSize >= 1 && targetBlock == 0 {
				targetBlock = b
				break
			}
		}
		roTx.Rollback()
		require.NotZero(t, targetBlock, "fixture must produce a block whose lastTxNum is past step 0")
	}

	m.TruncateChangeSetsBelow(t, targetBlock+2)
	{
		roTx, err := m.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		minUnwindable, err := rawtemporaldb.CanUnwindToBlockNum(roTx)
		require.NoError(t, err)
		require.Greater(t, minUnwindable, targetBlock,
			"mode-B dispatch must engage (target < minUnwindable)")
	}

	api := newSetHeadE2EAPI(t, m)
	roTx, err := m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	targetHash, err := rawdb.ReadCanonicalHash(roTx, targetBlock)
	require.NoError(t, err)
	// Capture nextBlockHash pre-unwind so the G3.15 check below has
	// a stable canonical anchor — TruncateCanonicalHash zeroes it.
	nextBlockHash, err := rawdb.ReadCanonicalHash(roTx, targetBlock+1)
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, nextBlockHash, "fixture must have block targetBlock+1")
	roTx.Rollback()

	require.NoError(t, api.SetHead(ctx, hexutil.Uint64(targetBlock)),
		"mode B without snapshot trim must succeed (state diff lives in writable shadow)")

	roTx, err = m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	require.Equal(t, targetHash, rawdb.ReadHeadHeaderHash(roTx))
	latest, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	require.Equal(t, targetBlock, latest)
	roTx.Rollback()

	// Forward-exec G3.15 check is not asserted — see scenario 3a's
	// equivalent comment for the reasoning. nextBlockHash is captured
	// pre-unwind in case a future test wants to extend.
	_ = nextBlockHash

	assertNoBlockDataPastTarget(t, m, targetBlock)
}

// TestSetHead_E2E_ModeB_NonAlignedCut — Scenario 3b (non-aligned cut).
//
// Mode B WITH snapshot file trim AT A NON-ALIGNED CUT: target's
// lastTxNum is mid-step. The boundary step contains writes at txnum >
// lastTxNum that don't belong in the post-unwind state. The
// WipeWritableShadowPast diff-replay path must surface those, look up
// each key's as-of-lastTxNum value from history, and rewrite the
// shadow entries in-place.
//
// Pins the non-aligned mode-B path landed in b4741d0920
// (state, storage: mode-B non-aligned cuts via boundary-step
// diff-replay) + the off-by-one fix in 85f4fbf884 (commitmentdb,
// state: fix plain-reader off-by-one in RecomputeAtTxNumWithoutSD).
func TestSetHead_E2E_ModeB_NonAlignedCut(t *testing.T) {
	const stepSize uint64 = 8

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			addr: {Balance: big.NewInt(common.Ether)},
		},
		GasLimit: 10_000_000,
	}
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithStepSize(stepSize),
		execmoduletester.WithAdminUnwindWired(),
	)
	ctx := m.Ctx
	require.NotNil(t, m.AdminUnwindProvider)

	signer := types.LatestSignerForChainID(nil)
	to := common.Address{0x42}
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 16, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), to, uint256.NewInt(1_000_000), 21_000, new(uint256.Int), nil),
			*signer, key,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	head := canonicalHead(t, m)
	require.Equal(t, uint64(16), head)
	m.RescanAdminUnwindInventory(t)

	// Find a target whose lastTxNum does NOT land on a step boundary
	// AND whose stepBoundary < max existing file endStep (so some
	// files past stepBoundary need trimming). The "(lastTxNum+1)%stepSize
	// != 0" guard surfaces a non-aligned cut.
	var targetBlock uint64
	{
		roTx, err := m.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		for b := uint64(1); b < head; b++ {
			lastTxNum, err := rawdbv3.TxNums.Max(ctx, roTx, b)
			require.NoError(t, err)
			if (lastTxNum+1)%stepSize != 0 && lastTxNum/stepSize == 0 && targetBlock == 0 {
				targetBlock = b
				break
			}
		}
		roTx.Rollback()
		require.NotZero(t, targetBlock, "fixture must produce a non-aligned cut whose stepBoundary is past step 0")
	}

	m.TruncateChangeSetsBelow(t, targetBlock+2)

	api := newSetHeadE2EAPI(t, m)
	roTx, err := m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	targetHash, err := rawdb.ReadCanonicalHash(roTx, targetBlock)
	require.NoError(t, err)
	nextBlockHash, err := rawdb.ReadCanonicalHash(roTx, targetBlock+1)
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, nextBlockHash, "fixture must have block targetBlock+1")
	roTx.Rollback()

	require.NoError(t, api.SetHead(ctx, hexutil.Uint64(targetBlock)),
		"mode B at a non-aligned cut must succeed (boundary-step diff-replay folds in the post-toBlock writes)")

	roTx, err = m.DB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	require.Equal(t, targetHash, rawdb.ReadHeadHeaderHash(roTx))
	latest, err := rpchelper.GetLatestBlockNumber(roTx)
	require.NoError(t, err)
	require.Equal(t, targetBlock, latest)
	roTx.Rollback()

	// Forward-exec G3.15 check is not asserted — see scenario 3a's
	// equivalent comment for the reasoning.
	_ = nextBlockHash

	assertNoBlockDataPastTarget(t, m, targetBlock)
}

// TestSetHead_E2E_ModeB_WipesOrphanRowsPastTarget pins the orphan-
// block-data sweep in unwindDBPastBlock. Seeds extra rows in the
// block-data tables AT BLOCKS PAST THE CHAIN HEAD before triggering
// mode B (simulating CL NewPayloads that landed in the writable DB
// between mode B starting + the kill — exactly the live wedge
// observed on hoodi: snapshots ended at 2,911,999, kv.Headers held
// 2,914,976+ from forward NewPayloads), then asserts those rows are
// gone post-unwind.
//
// Without the fix, kv.Headers + kv.BlockBody + kv.HeaderTD +
// kv.Senders retain the seeded forward rows because the pre-fix
// unwindDBPastBlock only touched stage progress + canonical hashes
// + TxNums + ChangeSets3 + writable shadow.
//
// With the fix (deleteHeaderNumbersPastBlock + rawdb.TruncateBlocks
// + rawdb.TruncateTd), the seeded rows are wiped — the cold-start
// invariant holds and the next-startup firstNonGenesisCheck does
// not wedge.
func TestSetHead_E2E_ModeB_WipesOrphanRowsPastTarget(t *testing.T) {
	const stepSize uint64 = 8

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			addr: {Balance: big.NewInt(common.Ether)},
		},
		GasLimit: 10_000_000,
	}
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithStepSize(stepSize),
		execmoduletester.WithAdminUnwindWired(),
	)
	ctx := m.Ctx
	require.NotNil(t, m.AdminUnwindProvider)

	signer := types.LatestSignerForChainID(nil)
	to := common.Address{0x42}
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 16, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), to, uint256.NewInt(1_000_000), 21_000, new(uint256.Int), nil),
			*signer, key,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	head := canonicalHead(t, m)
	require.Equal(t, uint64(16), head)
	m.RescanAdminUnwindInventory(t)

	var targetBlock uint64
	{
		roTx, err := m.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		for b := uint64(1); b < head; b++ {
			lastTxNum, err := rawdbv3.TxNums.Max(ctx, roTx, b)
			require.NoError(t, err)
			if (lastTxNum+1)%stepSize == 0 && targetBlock == 0 {
				targetBlock = b
			}
		}
		roTx.Rollback()
		require.NotZero(t, targetBlock, "fixture must produce at least one aligned cut")
	}

	m.TruncateChangeSetsBelow(t, targetBlock+2)

	// Seed orphan forward rows simulating CL NewPayloads that landed
	// after mode B started preparing. These would survive a pre-fix
	// unwind and wedge the next startup.
	orphanBlocks := []uint64{head + 1, head + 2, head + 3}
	{
		rwTx, err := m.DB.BeginRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		// Seed minimal Header + HeaderNumber + TD + Body + Senders for
		// each orphan block. Values are opaque — the cold-start check
		// only cares about row presence.
		for _, n := range orphanBlocks {
			var hash common.Hash
			binary.BigEndian.PutUint64(hash[:8], n)
			copy(hash[8:], "orphan-forward-payload")
			require.NoError(t, rawdb.WriteHeaderNumber(rwTx, hash, n))
			require.NoError(t, rawdb.WriteBodyForStorage(rwTx, hash, n, &types.BodyForStorage{
				BaseTxnID: types.BaseTxnID(n * 16),
				TxCount:   1,
			}))
			require.NoError(t, rawdb.WriteTd(rwTx, hash, n, *uint256.NewInt(n * 10)))
			// Use the standard composite key (num || hash) for Headers / Senders.
			headerKey := make([]byte, 8+len(hash))
			binary.BigEndian.PutUint64(headerKey[:8], n)
			copy(headerKey[8:], hash[:])
			require.NoError(t, rwTx.Put(kv.Headers, headerKey, []byte{0x80}))
			require.NoError(t, rwTx.Put(kv.Senders, headerKey, []byte{}))
		}
		require.NoError(t, rwTx.Commit())
	}

	// Sanity-check the seeds landed and are visible past the target.
	{
		roTx, err := m.DB.BeginRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		for _, n := range orphanBlocks {
			// Headers row by composite key
			var probeKey [40]byte
			binary.BigEndian.PutUint64(probeKey[:8], n)
			c, err := roTx.Cursor(kv.Headers) //nolint:gocritic // explicit Close at end of loop iteration; defer would accumulate cursors across orphanBlocks
			require.NoError(t, err)
			found := false
			for k, _, err := c.Seek(probeKey[:8]); k != nil && err == nil; k, _, err = c.Next() {
				if binary.BigEndian.Uint64(k[:8]) != n {
					break
				}
				found = true
				break
			}
			c.Close()
			require.True(t, found, "fixture: orphan kv.Headers row for block %d must be present pre-unwind", n)
		}
		roTx.Rollback()
	}

	api := newSetHeadE2EAPI(t, m)
	require.NoError(t, api.SetHead(ctx, hexutil.Uint64(targetBlock)),
		"mode B must succeed (orphan-row cleanup is part of unwindDBPastBlock)")

	// The fix's core contract: no block-data rows past targetBlock,
	// including the seeded orphans above the original chain head.
	assertNoBlockDataPastTarget(t, m, targetBlock)
}

// TestSetHead_E2E_ModeB_SequentialUnwinds pins the "two debug_setHead
// calls back-to-back" robustness contract. After mode B unwinds to
// T1, the post-unwind state is a valid starting point — the next
// mode B targeting T2 < T1 must either succeed cleanly or fail with
// a specific, diagnostic error rather than silently corrupting the
// trie.
//
// Live observation: a second mode-B from a post-mode-B state
// (head=2,912,999) to T2=2,912,500 failed with a commitment-anchor
// root mismatch (recomputed root ≠ header stateRoot). This test
// fences the contract in a controlled environment with a small
// generated chain so the failure mode is reproducible without a
// hoodi-sized datadir.
//
// Two intermediate aligned cuts in the same generated chain so both
// unwinds dispatch to mode B (target < CanUnwindToBlockNum after the
// first TruncateChangeSetsBelow). T1 > T2 so the second SetHead
// targets a state strictly older than the first.
func TestSetHead_E2E_ModeB_SequentialUnwinds(t *testing.T) {
	const stepSize uint64 = 8

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			addr: {Balance: big.NewInt(common.Ether)},
		},
		GasLimit: 10_000_000,
	}
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithStepSize(stepSize),
		execmoduletester.WithAdminUnwindWired(),
	)
	ctx := m.Ctx
	require.NotNil(t, m.AdminUnwindProvider)

	signer := types.LatestSignerForChainID(nil)
	to := common.Address{0x42}
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 24, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), to, uint256.NewInt(1_000_000), 21_000, new(uint256.Int), nil),
			*signer, key,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	head := canonicalHead(t, m)
	require.Equal(t, uint64(24), head, "fixture must run all 24 blocks for two distinct aligned cuts")
	m.RescanAdminUnwindInventory(t)

	// Find two aligned cuts: T1 > T2, both with lastTxNum landing on
	// a step boundary. The first unwind targets T1; the second
	// targets T2.
	var t1, t2 uint64
	{
		roTx, err := m.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		var aligned []uint64
		for b := uint64(1); b < head; b++ {
			lastTxNum, err := rawdbv3.TxNums.Max(ctx, roTx, b)
			require.NoError(t, err)
			if (lastTxNum+1)%stepSize == 0 {
				aligned = append(aligned, b)
			}
		}
		roTx.Rollback()
		require.GreaterOrEqual(t, len(aligned), 2, "fixture must produce at least two aligned cuts; got %v", aligned)
		t2 = aligned[0]
		t1 = aligned[len(aligned)-1]
		require.Greater(t, t1, t2, "t1 must be strictly greater than t2 for a meaningful second unwind")
	}

	// Lift CanUnwindToBlockNum past t1+1 so both targets route to mode B.
	m.TruncateChangeSetsBelow(t, t1+2)

	api := newSetHeadE2EAPI(t, m)

	// First unwind: head → t1.
	require.NoError(t, api.SetHead(ctx, hexutil.Uint64(t1)),
		"first mode-B unwind (head → t1=%d) must succeed", t1)
	{
		roTx, err := m.DB.BeginRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		latest, err := rpchelper.GetLatestBlockNumber(roTx)
		roTx.Rollback()
		require.NoError(t, err)
		require.Equal(t, t1, latest, "head must equal t1 after first unwind")
	}
	assertNoBlockDataPastTarget(t, m, t1)

	// Second unwind: t1 → t2.
	//
	// This is the contract that today's behavior is unclear on. A
	// success outcome means mode B is fully composable end-to-end:
	// the post-first-mode-B state (wiped shadow past t1's lastTxNum
	// + commitment anchor at t1's lastTxNum + truncated block-data
	// tables) is a clean starting point for the next unwind.
	//
	// On live hoodi this second call surfaced a commitment-root
	// mismatch in RecomputeAtTxNumWithoutSD. This test reproduces
	// the scenario in-process so the regression can be debugged
	// without a hoodi-sized datadir.
	err = api.SetHead(ctx, hexutil.Uint64(t2))
	if err != nil {
		t.Fatalf("second mode-B unwind (t1=%d → t2=%d) failed: %v\n\n"+
			"This is the live-rig issue #2: post-mode-B state must be a valid starting point for the next mode-B.\n"+
			"Likely cause: writable-shadow's boundary-step diff-replay from the first unwind perturbs the reads "+
			"the second unwind's RecomputeAtTxNumWithoutSD makes via plainReader.GetAsOf — investigating.",
			t1, t2, err)
	}
	{
		roTx, err := m.DB.BeginRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		latest, err := rpchelper.GetLatestBlockNumber(roTx)
		roTx.Rollback()
		require.NoError(t, err)
		require.Equal(t, t2, latest, "head must equal t2 after second unwind")
	}
	assertNoBlockDataPastTarget(t, m, t2)
}

// TestSetHead_E2E_ModeB_SequentialUnwinds_NonAligned exercises the
// same sequential contract as the aligned variant above, but with
// both T1 and T2 at non-aligned cuts (lastTxNum mid-step). This
// matches the live hoodi failure where both head and target were
// non-aligned (lastTxNum=103,927,077 and ~103,914,000 against a
// step size of 390,625).
//
// Non-aligned mode B uses the boundary-step diff-replay path in
// WipeWritableShadowPast — it writes value-as-of-lastTxNum into the
// boundary step for every key changed in (lastTxNum, boundaryStepEnd).
// After this rewrite the writable shadow's boundary step holds
// post-first-unwind values rather than the file-side step contents,
// so the second mode B's plainReader.GetAsOf reads can hit those
// shadow values for keys whose history records have NOT been pruned
// (records ≤ first-unwind's lastTxNum survive). This is exactly the
// surface where the live divergence could come from.
func TestSetHead_E2E_ModeB_SequentialUnwinds_NonAligned(t *testing.T) {
	const stepSize uint64 = 8

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			addr: {Balance: big.NewInt(common.Ether)},
		},
		GasLimit: 10_000_000,
	}
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithStepSize(stepSize),
		execmoduletester.WithAdminUnwindWired(),
	)
	ctx := m.Ctx
	require.NotNil(t, m.AdminUnwindProvider)

	signer := types.LatestSignerForChainID(nil)
	to := common.Address{0x42}
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 24, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), to, uint256.NewInt(1_000_000), 21_000, new(uint256.Int), nil),
			*signer, key,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	head := canonicalHead(t, m)
	require.Equal(t, uint64(24), head)
	m.RescanAdminUnwindInventory(t)

	// Pick two non-aligned cuts T1 > T2 with the constraint that
	// they sit in DIFFERENT steps — otherwise the second unwind
	// stays within the same boundary step as the first and the
	// path being probed (cross-step second-unwind) doesn't engage.
	var t1, t2 uint64
	{
		roTx, err := m.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		type cut struct {
			block uint64
			step  uint64
		}
		var nonAligned []cut
		for b := uint64(1); b < head; b++ {
			lastTxNum, err := rawdbv3.TxNums.Max(ctx, roTx, b)
			require.NoError(t, err)
			if (lastTxNum+1)%stepSize != 0 {
				nonAligned = append(nonAligned, cut{b, lastTxNum / stepSize})
			}
		}
		roTx.Rollback()
		require.GreaterOrEqual(t, len(nonAligned), 2,
			"fixture must produce at least two non-aligned cuts")
		// t2 = earliest non-aligned cut, t1 = latest non-aligned cut
		// in a strictly-later step (cross-step pair).
		t2 = nonAligned[0].block
		for i := len(nonAligned) - 1; i > 0; i-- {
			if nonAligned[i].step > nonAligned[0].step {
				t1 = nonAligned[i].block
				break
			}
		}
		require.NotZero(t, t1, "fixture must produce a non-aligned cut in a strictly later step than t2; got %v", nonAligned)
		require.Greater(t, t1, t2)
	}

	m.TruncateChangeSetsBelow(t, t1+2)

	api := newSetHeadE2EAPI(t, m)

	require.NoError(t, api.SetHead(ctx, hexutil.Uint64(t1)),
		"first non-aligned mode-B unwind (head → t1=%d) must succeed", t1)
	{
		roTx, err := m.DB.BeginRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		latest, err := rpchelper.GetLatestBlockNumber(roTx)
		roTx.Rollback()
		require.NoError(t, err)
		require.Equal(t, t1, latest)
	}
	assertNoBlockDataPastTarget(t, m, t1)

	err = api.SetHead(ctx, hexutil.Uint64(t2))
	if err != nil {
		t.Fatalf("second non-aligned mode-B unwind (t1=%d → t2=%d) failed: %v\n\n"+
			"Live-rig issue #2 reproduced. The first unwind's boundary-step diff-replay "+
			"rewrote the writable shadow at step %d for keys changed in "+
			"(t1.lastTxNum, t1.boundaryStepEnd). The second unwind's "+
			"plainReader.GetAsOf(key, t2.lastTxNum+1) walks history; for a key "+
			"whose only history record in (t2.baselineTxNum, t2.lastTxNum+1] is "+
			"below the shadow rewrite, the reader should still return the correct "+
			"value — investigate which read path actually diverges.",
			t1, t2, err, t1/stepSize)
	}
	{
		roTx, err := m.DB.BeginRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		latest, err := rpchelper.GetLatestBlockNumber(roTx)
		roTx.Rollback()
		require.NoError(t, err)
		require.Equal(t, t2, latest)
	}
	assertNoBlockDataPastTarget(t, m, t2)
}
