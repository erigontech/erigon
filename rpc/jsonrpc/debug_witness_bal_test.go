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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	witnesstypes "github.com/erigontech/erigon/execution/commitment/witness"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
)

// amsterdamBALHarness is the shared setup used by Task 1's tests. It returns
// the tester, the inserted ChainPack, and a constructed DebugAPIImpl with
// commitment history enabled. Each test installs its own
// recordingStateConstructedHookForTest counter.
type amsterdamBALHarness struct {
	m         *execmoduletester.ExecModuleTester
	chainPack *blockgen.ChainPack
	api       *DebugAPIImpl
}

func setupAmsterdamBALHarness(t *testing.T, cfg *chain.Config, numBlocks int) *amsterdamBALHarness {
	t.Helper()

	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
	})

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config: cfg,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	)

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := m.Genesis.BaseFee().Uint64()
	gasPrice := baseFee * 2
	toAddr := common.HexToAddress("0x00000000000000000000000000000000000000aa")

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, numBlocks, func(i int, gen *blockgen.BlockGen) {
		nonce := gen.TxNonce(senderAddr)
		tx, txErr := types.SignTx(
			types.NewTransaction(nonce, toAddr, uint256.NewInt(1), 21000, uint256.NewInt(gasPrice), nil),
			*signer, privKey)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	require.NoError(t, m.DB.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))

	return &amsterdamBALHarness{
		m:         m,
		chainPack: chainPack,
		api:       NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false),
	}
}

// installRecordingHookCounter installs a fresh counter on the
// recordingStateConstructedHookForTest seam and returns a closure that reads
// the current value. Test cleanup restores the previous hook.
func installRecordingHookCounter(t *testing.T) func() int {
	t.Helper()
	previous := recordingStateConstructedHookForTest
	count := 0
	recordingStateConstructedHookForTest = func() {
		count++
	}
	t.Cleanup(func() {
		recordingStateConstructedHookForTest = previous
	})
	return func() int { return count }
}

// TestExecutionWitnessAmsterdamBAL asserts that an Amsterdam block goes
// through the BAL access-set provider rather than the RecordingState re-exec
// path. The path assertion (hookCount == 0) is the live merge gate for this
// PR; the verifyWitnessStateless-passes assertion is t.Skip'd pending
// Change 4, per the explicit user override of the no-skip rule.
func TestExecutionWitnessAmsterdamBAL(t *testing.T) {
	h := setupAmsterdamBALHarness(t, chain.AllProtocolChanges, 1)
	require.True(t, h.m.ChainConfig.IsAmsterdam(h.chainPack.Headers[0].Time),
		"AllProtocolChanges must activate Amsterdam for the inserted block")
	require.NotEmpty(t, h.chainPack.BlockAccessLists[0],
		"BAL must be produced for an Amsterdam block")

	hookCount := installRecordingHookCounter(t)

	bn := rpc.BlockNumber(h.chainPack.Blocks[0].NumberU64())
	_, err := h.api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})

	require.Equal(t, 0, hookCount(), "Amsterdam block must NOT take the re-exec path — IsAmsterdam gate failed to flip; RecordingState was constructed %d time(s)", hookCount())

	t.Skip("blocked on Change 4: Amsterdam commitment-pipeline divergence — see docs/plans/20260528-witness-amsterdam-commitment-divergence.md")
	require.NoError(t, err, "ExecutionWitness must succeed on the Amsterdam block via the BAL branch (verifyWitnessStateless is the merge gate)")
}

// TestExecutionWitnessAmsterdamActivationBlock locks the IsAmsterdam boundary
// at header.Time == AmsterdamTime. AllProtocolChanges sets AmsterdamTime=0,
// which makes the genesis block sit exactly on the boundary; we assert
// IsAmsterdam is inclusive at that point at the chain-config level, then
// exercise the first Amsterdam-era block through the handler.
//
// A non-zero AmsterdamTime can't be tested via the in-test chain builder:
// when parent.Time < AmsterdamTime, the parallel-exec BAL infra isn't
// allocated (chain_makers.go gates on IsAmsterdam(parent.Time)), and
// InsertChain then fails because the serial executor refuses Amsterdam
// blocks. The unit-level boundary check below covers that case structurally.
func TestExecutionWitnessAmsterdamActivationBlock(t *testing.T) {
	cfg := chain.AllProtocolChanges
	require.NotNil(t, cfg.AmsterdamTime, "AllProtocolChanges must schedule Amsterdam")
	require.True(t, cfg.IsAmsterdam(*cfg.AmsterdamTime),
		"IsAmsterdam must be inclusive at header.Time == AmsterdamTime")

	h := setupAmsterdamBALHarness(t, cfg, 1)
	require.Equal(t, *cfg.AmsterdamTime, h.m.Genesis.Time(),
		"genesis must sit exactly on the AmsterdamTime boundary for this fixture")

	hookCount := installRecordingHookCounter(t)

	bn := rpc.BlockNumber(h.chainPack.Blocks[0].NumberU64())
	_, err := h.api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})

	require.Equal(t, 0, hookCount(),
		"activation-era block must take the BAL branch — IsAmsterdam gate failed to flip; RecordingState was constructed %d time(s)", hookCount())

	t.Skip("blocked on Change 4: Amsterdam commitment-pipeline divergence — see docs/plans/20260528-witness-amsterdam-commitment-divergence.md")
	require.NoError(t, err, "first Amsterdam-era block must produce a verifiable witness via the BAL branch")
}

// fakeBALCodeReader is a unit-test code lookup that returns a canned code blob
// per address. Missing entries return (nil, nil) — same shape as
// HistoryReaderV3.ReadAccountCode for absent/empty-code addresses.
type fakeBALCodeReader map[common.Address][]byte

func (f fakeBALCodeReader) read(addr common.Address) ([]byte, error) {
	return f[addr], nil
}

// keccak is a tiny test helper returning the keccak256 hash of b as a
// common.Hash, matching how accessedStateFromBAL keys CodeReads/SortedCodes.
func keccak(b []byte) common.Hash {
	return crypto.Keccak256Hash(b)
}

// TestAccessedStateFromBAL_EmptyBAL: a BAL that decoded to zero entries is
// legitimate (e.g. a zero-tx Amsterdam block); the mapper must produce an
// empty (but non-nil) accessedState.
func TestAccessedStateFromBAL_EmptyBAL(t *testing.T) {
	out, err := accessedStateFromBAL(types.BlockAccessList{}, fakeBALCodeReader{}.read)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.True(t, out.isEmpty(), "empty BAL must yield empty accessedState")
	require.NotNil(t, out.Addresses)
	require.NotNil(t, out.Storage)
	require.NotNil(t, out.CodeAddrs)
	require.NotNil(t, out.CodeReads)
	require.NotNil(t, out.SortedCodes, "SortedCodes must be non-nil so result.Codes never serializes as null")
	require.Empty(t, out.SortedCodes)
}

// TestAccessedStateFromBAL_AddressesAndStorage: a BAL with addresses,
// storage reads, and slot changes maps into Addresses + Storage. Reads ∪
// Changes are deduplicated per address.
func TestAccessedStateFromBAL_AddressesAndStorage(t *testing.T) {
	addrA := common.HexToAddress("0x000000000000000000000000000000000000000a")
	addrB := common.HexToAddress("0x000000000000000000000000000000000000000b")
	slot1 := common.HexToHash("0x01")
	slot2 := common.HexToHash("0x02")
	slot3 := common.HexToHash("0x03")

	bal := types.BlockAccessList{
		{
			Address: accounts.InternAddress(addrA),
			StorageReads: []accounts.StorageKey{
				accounts.InternKey(slot1),
				accounts.InternKey(slot2),
			},
			StorageChanges: []*types.SlotChanges{
				{Slot: accounts.InternKey(slot2)}, // overlaps slot2 read
				{Slot: accounts.InternKey(slot3)},
			},
		},
		{
			Address: accounts.InternAddress(addrB),
		},
	}

	out, err := accessedStateFromBAL(bal, fakeBALCodeReader{}.read)
	require.NoError(t, err)

	require.Contains(t, out.Addresses, addrA)
	require.Contains(t, out.Addresses, addrB)
	require.Len(t, out.Addresses, 2)

	require.Len(t, out.Storage[addrA], 3, "Reads ∪ Changes must dedupe slot2")
	require.Contains(t, out.Storage[addrA], slot1)
	require.Contains(t, out.Storage[addrA], slot2)
	require.Contains(t, out.Storage[addrA], slot3)
	require.NotContains(t, out.Storage, addrB, "addrB has no storage touches")
}

// TestAccessedStateFromBAL_SystemAddressPreserved: the BAL itself encodes
// EIP-7928's consensus inclusion rule for SystemAddress, so the mapper
// must not apply the RecordingState-path heuristic that strips system
// entries lacking changes/storage.
func TestAccessedStateFromBAL_SystemAddressPreserved(t *testing.T) {
	sysAddr := params.SystemAddress.Value()

	bal := types.BlockAccessList{
		{Address: params.SystemAddress},
	}

	out, err := accessedStateFromBAL(bal, fakeBALCodeReader{}.read)
	require.NoError(t, err)
	require.Contains(t, out.Addresses, sysAddr, "SystemAddress entry from BAL must be preserved as-is (no heuristic stripping)")
}

// TestAccessedStateFromBAL_CodeOverApproximation: every BAL address that
// has parent-state code must be included in CodeAddrs/CodeReads/SortedCodes.
// Addresses with no parent code do NOT pollute Code* fields. SortedCodes is
// hash-sorted and deduplicated when two addresses share bytecode.
func TestAccessedStateFromBAL_CodeOverApproximation(t *testing.T) {
	addrA := common.HexToAddress("0x000000000000000000000000000000000000000a")
	addrB := common.HexToAddress("0x000000000000000000000000000000000000000b")
	addrEOA := common.HexToAddress("0x000000000000000000000000000000000000000c")
	codeA := []byte{0x60, 0x00, 0x60, 0x00, 0xFD}
	codeB := []byte{0x60, 0x01, 0xFE}

	bal := types.BlockAccessList{
		{Address: accounts.InternAddress(addrA)},
		{Address: accounts.InternAddress(addrB)},
		{Address: accounts.InternAddress(addrEOA)},
	}

	codes := fakeBALCodeReader{
		addrA:   codeA,
		addrB:   codeB,
		addrEOA: nil, // EOA: no code at parent state
	}

	out, err := accessedStateFromBAL(bal, codes.read)
	require.NoError(t, err)

	require.Contains(t, out.CodeAddrs, addrA)
	require.Contains(t, out.CodeAddrs, addrB)
	require.NotContains(t, out.CodeAddrs, addrEOA, "EOA without parent-state code must not appear in CodeAddrs")

	hashA := keccak(codeA)
	hashB := keccak(codeB)
	require.Equal(t, witnesstypes.CodeWithHash{Code: codeA, CodeHash: accounts.InternCodeHash(hashA)}, out.CodeReads[keccak(addrA.Bytes())])
	require.Equal(t, witnesstypes.CodeWithHash{Code: codeB, CodeHash: accounts.InternCodeHash(hashB)}, out.CodeReads[keccak(addrB.Bytes())])
	require.NotContains(t, out.CodeReads, keccak(addrEOA.Bytes()))

	require.Len(t, out.SortedCodes, 2)
	if hashA.Cmp(hashB) < 0 {
		require.Equal(t, []byte(out.SortedCodes[0]), codeA)
		require.Equal(t, []byte(out.SortedCodes[1]), codeB)
	} else {
		require.Equal(t, []byte(out.SortedCodes[0]), codeB)
		require.Equal(t, []byte(out.SortedCodes[1]), codeA)
	}
}

// TestAccessedStateFromBAL_DedupSharedCode: two BAL addresses sharing the
// same bytecode produce a single SortedCodes entry but two CodeReads entries
// (per-address keys).
func TestAccessedStateFromBAL_DedupSharedCode(t *testing.T) {
	addrA := common.HexToAddress("0x000000000000000000000000000000000000000a")
	addrB := common.HexToAddress("0x000000000000000000000000000000000000000b")
	code := []byte{0x60, 0x00, 0xFE}

	bal := types.BlockAccessList{
		{Address: accounts.InternAddress(addrA)},
		{Address: accounts.InternAddress(addrB)},
	}

	out, err := accessedStateFromBAL(bal, fakeBALCodeReader{addrA: code, addrB: code}.read)
	require.NoError(t, err)

	require.Len(t, out.SortedCodes, 1, "shared bytecode must appear once in SortedCodes")
	require.Equal(t, []byte(out.SortedCodes[0]), code)
	require.Len(t, out.CodeReads, 2, "CodeReads is keyed by keccak(addr), so two callers share one code blob")
}

// TestBuildAccessedStateFromBAL_PrunedBALReturnsError: when
// ReadBlockAccessListBytes returns nil (no entry in db), buildAccessedStateFromBAL
// must surface a distinct "pruned" error rather than silently returning an
// empty accessedState (which would mask a real outage).
func TestBuildAccessedStateFromBAL_PrunedBALReturnsError(t *testing.T) {
	h := setupAmsterdamBALHarness(t, chain.AllProtocolChanges, 1)

	tx, err := h.m.DB.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// A random block hash that was never written: ReadBlockAccessListBytes
	// returns nil bytes, which is the discriminator for "pruned" per the plan.
	unknownHash := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	_, err = buildAccessedStateFromBAL(context.Background(), tx, unknownHash, 9999, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pruned", "pruned BAL must yield a distinct error message — got: %v", err)
}
