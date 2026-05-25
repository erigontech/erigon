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

// TestSetHead_E2E_ModeB_RoutesToProviderUnwind — Scenario 3 (focused).
//
// Pins the *dispatch + sub-op-chain wiring* for the admin past-diffset
// SetHead path:
//
//   - SetHead routes to setHeadModeB when targetBlock < CanUnwindToBlockNum
//     AND an Unwinder is wired with BlockAligned()==true. The legacy
//     "minimum unwindable block" rejection is bypassed.
//   - setHeadModeB invokes the Unwinder which delegates to
//     *storage.Provider.Unwind, exercising the snapshot-trim + DB-reset +
//     commitment-anchor sub-op chain on a real temporal DB.
//   - The error surface — when one of the sub-ops can't satisfy its
//     contract on the small fixture — is a mode-B-prefixed error
//     containing the offending sub-op name, not a generic panic / wedge
//     / legacy rejection.
//
// What this scenario *does not* assert: full cold-start equivalence
// after a successful Unwind. That needs a fixture where (a) snapshot
// files past toBlock physically exist on disk for sub-op #1 to trim
// (requires BlockRetire to fire, gated on Erigon2MinSegmentSize=1000 +
// MaxReorgDepth=96 → 1100+ blocks), and (b) the writable shadow has
// nothing past toBlock's step boundary so sub-op #3's commitment-anchor
// verify finds LatestBlockNumWithCommitment == toBlock. The current
// wipe primitive retains writes at step == stepBoundary, so any fixture
// where execution continued past toBlock leaves an entry that shadows
// the file's anchor. Both pieces are tracked in the post-compact
// handoff under "Full E2E scenario 3 deferred".
//
// On this fixture the expected outcome is: mode B engages, snapshot-trim
// is a no-op (no Inventory wired), DB-reset + WipeWritableShadowPast
// run, ensureCommitmentAtBlock returns a chain-malformed error pointing
// at the wedged anchor. The test pins all of that as observable wiring
// evidence.
func TestSetHead_E2E_ModeB_RoutesToProviderUnwind(t *testing.T) {
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
	setHeadErr := api.SetHead(ctx, hexutil.Uint64(targetBlock))

	// On this fixture mode B must engage and reach Provider.Unwind's
	// sub-op chain. Acceptable outcomes:
	//   (a) full success — every sub-op satisfies its contract.
	//   (b) a chain-malformed err with the mode-B prefix containing
	//       which sub-op couldn't satisfy its contract.
	//
	// What's NOT acceptable: the legacy "minimum unwindable block"
	// rejection. That would mean dispatch failed to route to mode B.
	require.Error(t, setHeadErr,
		"on this small fixture (no snapshot files past toBlock; writable shadow holds entries past stepBoundary "+
			"from continued execution) sub-op #3 cannot satisfy its anchor invariant — the err is the wiring evidence")
	require.NotContains(t, setHeadErr.Error(), "minimum unwindable block",
		"dispatch must route to mode B (Provider.Unwind), not the legacy mode-A rejection")
	require.Contains(t, setHeadErr.Error(), "SetHead mode B",
		"mode-B errors must carry the mode-B prefix so operators see which path ran")
	require.Contains(t, setHeadErr.Error(), "storage.Provider.Unwind",
		"the mode-B dispatch must delegate into *storage.Provider.Unwind, not some other Unwinder")
	require.Contains(t, setHeadErr.Error(), "commitment-anchor",
		"snapshot-trim (no Inventory) is a no-op and DB-reset succeeds on this fixture; the controlled failure point is sub-op #3 ensureCommitmentAtBlock")
}
