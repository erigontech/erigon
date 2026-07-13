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
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
)

// fakeStateReader is a minimal state.StateReader backed by an in-memory account
// map, used to exercise RecordingState predicates without a database.
type fakeStateReader struct {
	accounts map[common.Address]*accounts.Account
}

func (r *fakeStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	return r.accounts[address.Value()], nil
}
func (r *fakeStateReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.accounts[address.Value()], nil
}
func (r *fakeStateReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (r *fakeStateReader) HasStorage(address accounts.Address) (bool, error)         { return false, nil }
func (r *fakeStateReader) ReadAccountCode(address accounts.Address) ([]byte, error)  { return nil, nil }
func (r *fakeStateReader) ReadAccountCodeSize(address accounts.Address) (int, error) { return 0, nil }
func (r *fakeStateReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return 0, nil
}
func (r *fakeStateReader) SetTrace(_ bool, _ string) {}
func (r *fakeStateReader) Trace() bool               { return false }
func (r *fakeStateReader) TracePrefix() string       { return "" }

// countingStateReader counts ReadAccountData calls per address so a test can pin that the
// witness-keys gate consults the pre-block reader at most once.
type countingStateReader struct {
	*fakeStateReader
	reads map[common.Address]int
}

func (r *countingStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	r.reads[address.Value()]++
	return r.fakeStateReader.ReadAccountData(address)
}

// TestRecordingState_hasWitnessLeaf_SingleInnerRead pins the gate's single-read property:
// an account absent from both pre- and post-state is excluded with exactly one inner read,
// not the two a plain accountExists() || existedPreBlock() composition would issue.
func TestRecordingState_hasWitnessLeaf_SingleInnerRead(t *testing.T) {
	never := common.HexToAddress("0x2222222222222222222222222222222222222222")
	reader := &countingStateReader{
		fakeStateReader: &fakeStateReader{accounts: map[common.Address]*accounts.Account{}},
		reads:           map[common.Address]int{},
	}
	rs := NewRecordingState(reader)

	if rs.hasWitnessLeaf(never) {
		t.Fatal("account absent from pre- and post-state must be excluded from keys[]")
	}
	if got := reader.reads[never]; got != 1 {
		t.Fatalf("hasWitnessLeaf consulted the pre-block reader %d times, want 1", got)
	}
}

func TestRecordingState_accountExists(t *testing.T) {
	existing := common.HexToAddress("0x1111111111111111111111111111111111111111")
	missing := common.HexToAddress("0x2222222222222222222222222222222222222222")
	created := common.HexToAddress("0x3333333333333333333333333333333333333333")
	deleted := common.HexToAddress("0x4444444444444444444444444444444444444444")
	createdThenDeleted := common.HexToAddress("0x5555555555555555555555555555555555555555")

	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{
		existing:           {Nonce: 1},
		deleted:            {Nonce: 1},
		createdThenDeleted: nil,
	}}
	rs := NewRecordingState(inner)

	// created in-block: present in the write overlay
	rs.accountOverlay[created] = &accounts.Account{Nonce: 1}
	// deleted in-block
	rs.DeletedAccounts[deleted] = struct{}{}
	// created in-block then deleted: overlay cleared, recorded as deleted
	rs.DeletedAccounts[createdThenDeleted] = struct{}{}

	cases := []struct {
		name string
		addr common.Address
		want bool
	}{
		{"exists in inner", existing, true},
		{"nonexistent", missing, false},
		{"created in overlay", created, true},
		{"deleted", deleted, false},
		{"created then deleted", createdThenDeleted, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rs.accountExists(tc.addr); got != tc.want {
				t.Fatalf("accountExists(%s) = %v, want %v", tc.addr.Hex(), got, tc.want)
			}
		})
	}
}

// hasEmptyCode reports whether the legacy code set carries the single empty `0x`
// bytecode entry.
func hasEmptyCode(accessed *accessedState) bool {
	for _, c := range accessed.SortedCodes {
		if len(c) == 0 {
			return true
		}
	}
	return false
}

// TestEmptyCodeTrigger_OnAccountLoad asserts the legacy empty-`0x` entry is
// emitted when an empty-code account is materialized, which happens on a plain
// account load (ReadAccountData) as well as on a code load (ReadAccountCode).
func TestEmptyCodeTrigger_OnAccountLoad(t *testing.T) {
	emptyAcc := common.HexToAddress("0x1111111111111111111111111111111111111111")
	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{
		emptyAcc: {Nonce: 1},
	}}

	t.Run("data read triggers", func(t *testing.T) {
		rs := NewRecordingState(inner)
		if _, err := rs.ReadAccountData(accounts.InternAddress(emptyAcc)); err != nil {
			t.Fatal(err)
		}
		if !rs.emptyCodeAccessed {
			t.Error("emptyCodeAccessed not set by an empty-code account data read")
		}
		if !hasEmptyCode(collectAccessedState(rs, witnessModeLegacy)) {
			t.Error("empty 0x code entry missing after an empty-code account load")
		}
	})

	t.Run("code load triggers", func(t *testing.T) {
		rs := NewRecordingState(inner)
		if _, err := rs.ReadAccountCode(accounts.InternAddress(emptyAcc)); err != nil {
			t.Fatal(err)
		}
		if !rs.emptyCodeAccessed {
			t.Error("emptyCodeAccessed not set by a code load")
		}
		if !hasEmptyCode(collectAccessedState(rs, witnessModeLegacy)) {
			t.Error("empty 0x code entry missing after a code load")
		}
	})
}

// TestCollectAccessedState_Legacy7702Designator asserts the legacy code set carries the
// EIP-7702 delegation designator. For a delegated account the designator is read first
// (AccessedCode[A]=designator) then overwritten by the resolved target code on the second
// read (AccessedCode[A]=target, last-write-wins); the designator survives only in
// PreStateCode. Without it, stateless verification of a block touching the delegated
// account recomputes the wrong state root.
func TestCollectAccessedState_Legacy7702Designator(t *testing.T) {
	eoa := common.HexToAddress("0x1111111111111111111111111111111111111111")
	target := common.HexToAddress("0x3fc8c2b6e4ea901721aef251c67bc7c2591a1e1f")
	designator := append([]byte{0xef, 0x01, 0x00}, target[:]...)
	targetCode := []byte{0x60, 0x00, 0x60, 0x00, 0xfd}

	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{
		eoa: {Nonce: 1},
	}}
	rs := NewRecordingState(inner)
	rs.PreStateCode[eoa] = designator
	rs.AccessedCode[eoa] = targetCode

	accessed := collectAccessedState(rs, witnessModeLegacy)

	var sawDesignator bool
	for _, c := range accessed.SortedCodes {
		if bytes.Equal(c, designator) {
			sawDesignator = true
		}
	}
	if !sawDesignator {
		t.Fatal("legacy witness codes missing the EIP-7702 designator (present only in PreStateCode)")
	}
}

// TestCollectAccessedState_KeysOnlyExistingAccounts asserts that a 20-byte address
// key is emitted only for accounts represented in the witness trie (present in pre-
// or post-state); an address that exists in neither is excluded, while its accessed
// storage slots are still emitted.
func TestCollectAccessedState_KeysOnlyExistingAccounts(t *testing.T) {
	existing := common.HexToAddress("0x1111111111111111111111111111111111111111")
	missing := common.HexToAddress("0x2222222222222222222222222222222222222222")
	slot := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000aa")

	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{
		existing: {Nonce: 1},
	}}
	rs := NewRecordingState(inner)
	rs.AccessedAccounts[existing] = struct{}{}
	rs.AccessedAccounts[missing] = struct{}{}
	rs.AccessedStorage[missing] = map[common.Hash]struct{}{slot: {}}

	accessed := collectAccessedState(rs, witnessModeLegacy)

	var sawExisting, sawMissing, sawSlot bool
	for _, k := range accessed.WitnessKeys {
		switch {
		case len(k) == 20 && common.BytesToAddress(k) == existing:
			sawExisting = true
		case len(k) == 20 && common.BytesToAddress(k) == missing:
			sawMissing = true
		case len(k) == 32 && common.BytesToHash(k) == slot:
			sawSlot = true
		}
	}
	if !sawExisting {
		t.Error("expected key for existing account, missing")
	}
	if sawMissing {
		t.Error("address key emitted for nonexistent account; should be excluded")
	}
	if !sawSlot {
		t.Error("expected storage slot key to still be emitted for nonexistent account")
	}
}

// TestCollectAccessedState_KeysIncludeDeletedPreExisting is a regression test for the
// missing-preimage bug: an account present in the parent state but emptied and
// state-cleared in-block (EIP-161) keeps its leaf in the parent-state witness trie,
// so its 20-byte preimage must stay in keys[] even though it no longer exists
// post-state. An account that never existed pre-state and is deleted in-block has no
// parent-trie leaf and stays excluded.
func TestCollectAccessedState_KeysIncludeDeletedPreExisting(t *testing.T) {
	preExistingDeleted := common.HexToAddress("0x16fd7629978addaf41c426601176c37977a0faa7")
	createdThenDeleted := common.HexToAddress("0x2222222222222222222222222222222222222222")

	inner := &fakeStateReader{accounts: map[common.Address]*accounts.Account{
		preExistingDeleted: {Balance: *uint256.NewInt(1)},
	}}
	rs := NewRecordingState(inner)
	rs.AccessedAccounts[preExistingDeleted] = struct{}{}
	rs.DeletedAccounts[preExistingDeleted] = struct{}{}
	rs.AccessedAccounts[createdThenDeleted] = struct{}{}
	rs.DeletedAccounts[createdThenDeleted] = struct{}{}

	accessed := collectAccessedState(rs, witnessModeLegacy)

	var sawPreExisting, sawCreatedThenDeleted bool
	for _, k := range accessed.WitnessKeys {
		if len(k) != 20 {
			continue
		}
		switch common.BytesToAddress(k) {
		case preExistingDeleted:
			sawPreExisting = true
		case createdThenDeleted:
			sawCreatedThenDeleted = true
		}
	}
	if !sawPreExisting {
		t.Error("preimage for a pre-existing account deleted in-block must remain in keys[]")
	}
	if sawCreatedThenDeleted {
		t.Error("preimage for an account that never existed pre-state and was deleted in-block must be excluded")
	}
}

// TestCheckWitnessKeysComplete pins the internal verifier's keys[]-completeness check:
// every account/storage leaf the stateless re-execution resolved from the witness trie
// must have its preimage in keys[]; the protocol system address is exempt.
func TestCheckWitnessKeysComplete(t *testing.T) {
	addrA := common.HexToAddress("0x16fd7629978addaf41c426601176c37977a0faa7")
	addrB := common.HexToAddress("0x2222222222222222222222222222222222222222")
	slotS := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000aa")
	sys := common.Address(params.SystemAddress.Value())
	key20 := func(a common.Address) hexutil.Bytes { return bytes.Clone(a[:]) }
	key32 := func(h common.Hash) hexutil.Bytes { return bytes.Clone(h[:]) }

	t.Run("missing account preimage flagged", func(t *testing.T) {
		used := map[common.Address]struct{}{addrA: {}}
		if err := checkWitnessKeysComplete(used, nil, nil); err == nil {
			t.Fatal("expected error: accessed account leaf missing from keys[]")
		}
	})
	t.Run("present account preimage ok", func(t *testing.T) {
		used := map[common.Address]struct{}{addrA: {}}
		if err := checkWitnessKeysComplete(used, nil, []hexutil.Bytes{key20(addrA)}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("system address exempt", func(t *testing.T) {
		used := map[common.Address]struct{}{sys: {}}
		if err := checkWitnessKeysComplete(used, nil, nil); err != nil {
			t.Fatalf("system address must not require a preimage: %v", err)
		}
	})
	t.Run("missing slot preimage flagged", func(t *testing.T) {
		usedS := map[common.Hash]struct{}{slotS: {}}
		if err := checkWitnessKeysComplete(nil, usedS, []hexutil.Bytes{key20(addrB)}); err == nil {
			t.Fatal("expected error: accessed storage leaf missing from keys[]")
		}
	})
	t.Run("present slot preimage ok", func(t *testing.T) {
		usedS := map[common.Hash]struct{}{slotS: {}}
		if err := checkWitnessKeysComplete(nil, usedS, []hexutil.Bytes{key32(slotS)}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("large missing list truncated, total preserved", func(t *testing.T) {
		used := make(map[common.Address]struct{})
		for i := 0; i < 50; i++ {
			var a common.Address
			a[19] = byte(i)
			used[a] = struct{}{}
		}
		err := checkWitnessKeysComplete(used, nil, nil)
		if err == nil {
			t.Fatal("expected error: 50 accessed account leaves missing from keys[]")
		}
		msg := err.Error()
		if !strings.Contains(msg, "50 preimage(s)") {
			t.Errorf("error must report the full count; got: %s", msg)
		}
		if !strings.Contains(msg, "more)") {
			t.Errorf("error must mark the list as truncated; got: %s", msg)
		}
		if n := strings.Count(msg, "0x"); n > 16 {
			t.Errorf("error listed %d preimages; want the list capped at 16", n)
		}
	})
}

func TestResolveWitnessMode(t *testing.T) {
	str := func(s string) *string { return &s }

	t.Run("param selects mode", func(t *testing.T) {
		got, err := resolveWitnessMode(str("legacy"))
		if err != nil {
			t.Fatal(err)
		}
		if got != witnessModeLegacy {
			t.Errorf("param legacy should resolve to legacy mode, got %v", got)
		}

		got, err = resolveWitnessMode(str("canonical"))
		if err != nil {
			t.Fatal(err)
		}
		if got != witnessModeCanonical {
			t.Errorf("param canonical, got %v", got)
		}
	})

	t.Run("unknown param rejected", func(t *testing.T) {
		if _, err := resolveWitnessMode(str("bogus")); err == nil {
			t.Error("expected error for unknown mode param")
		}
	})

	t.Run("legacy default when param nil", func(t *testing.T) {
		got, err := resolveWitnessMode(nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != witnessModeLegacy {
			t.Errorf("default should be legacy, got %v", got)
		}
	})
}

// TestWitnessReaderComposition pins the per-phase reader the two build phases install:
// head-capture reads commitment from the pinned parent and plain state at the phase's
// txNum (block-end for collapse detection, parent for the trie), while the durable path
// reads both planes from a single committed tx.
func TestWitnessReaderComposition(t *testing.T) {
	t.Parallel()

	const (
		firstTxNumInBlock = uint64(500)
		endTxNum          = uint64(1000)
	)
	hc := &headCaptureSource{}

	collapse := collapseReaderFor(hc, nil, firstTxNumInBlock, endTxNum)
	hcCollapse, ok := collapse.(*commitmentdb.CommitmentReplayStateReader)
	require.True(t, ok, "head-capture collapse phase must install the dual-tx reader")
	asOf, ok := hcCollapse.PlainStateAsOf()
	require.True(t, ok)
	require.Equal(t, endTxNum, asOf, "collapse detection reads plain state at block end")
	require.False(t, collapse.WithHistory())

	trie := trieReaderFor(hc, nil, firstTxNumInBlock)
	hcTrie, ok := trie.(*commitmentdb.CommitmentReplayStateReader)
	require.True(t, ok, "head-capture trie phase must install the dual-tx reader")
	asOf, ok = hcTrie.PlainStateAsOf()
	require.True(t, ok)
	require.Equal(t, firstTxNumInBlock, asOf, "trie phase reads plain state at the parent")
	require.True(t, trie.WithHistory(), "trie phase is read-only: PutBranch must no-op during witness capture")

	durableCollapse := collapseReaderFor(nil, nil, firstTxNumInBlock, endTxNum)
	splitReader, ok := durableCollapse.(*commitmentdb.SplitStateReader)
	require.True(t, ok, "durable collapse phase installs the split-history reader")
	asOf, ok = splitReader.PlainStateAsOf()
	require.True(t, ok)
	require.Equal(t, endTxNum, asOf, "durable collapse reads plain state at block end")

	durableTrie := trieReaderFor(nil, nil, firstTxNumInBlock)
	historyReader, ok := durableTrie.(*commitmentdb.HistoryStateReader)
	require.True(t, ok, "durable trie phase installs a plain history reader")
	require.Equal(t, firstTxNumInBlock, historyReader.AsOf())
}

// TestBuildWitnessResultHeadCapture_FailsClosedOnBadParent drives the head-capture build
// with a tip-pinned tx used as the parent of an older block, so the pinned commitment
// plane does not match parent(B). The build must fail a validation gate and return no
// result — a wrong witness is never produced.
func TestBuildWitnessResultHeadCapture_FailsClosedOnBadParent(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()

	require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))

	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)

	tx, err := api.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// Block 3 is a non-empty old block; a tx pinned at the committed tip carries the
	// wrong parent commitment for it.
	const blockNum = uint64(3)
	bn := rpc.BlockNumber(blockNum)
	info, err := api.resolveWitnessBlock(ctx, tx, rpc.BlockNumberOrHash{BlockNumber: &bn})
	require.NoError(t, err)

	result, err := api.buildWitnessResultHeadCapture(ctx, tx, tx, info, witnessModeLegacy)
	require.Error(t, err, "a mispinned parent must fail a validation gate")
	require.Nil(t, result, "no witness is produced on gate failure")
}

// TestExecutionWitnessCacheOnlyServe pins the Task 7 cache-only serve contract for a
// head-capture minimal node (no commitment history, never recomputes): a by-number miss
// is out-of-window, a by-number hit serves the cached pointer, and a by-hash request for
// a still-resident but no-longer-canonical hash is rejected as reorged-away rather than
// served as canonical.
func TestExecutionWitnessCacheOnlyServe(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)
	ctx := context.Background()

	var block1Hash common.Hash
	require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
		var err error
		block1Hash, _, err = m.BlockReader.CanonicalHash(ctx, tx, 1)
		return err
	}))

	bn := rpc.BlockNumber(1)
	sentinel := &ExecutionWitnessResult{State: []hexutil.Bytes{{0xde, 0xad, 0xbe, 0xef}}}

	t.Run("by-number miss returns out-of-window, never recomputes", func(t *testing.T) {
		api.witnessCache = newWitnessResultCache(96, 0, true /*headCapture*/, true /*cacheOnly*/)
		t.Cleanup(func() { api.witnessCache = nil })

		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn}, nil)
		require.ErrorIs(t, err, errWitnessOutOfWindow)
		require.Nil(t, result, "a cache-only miss must not build a witness")
	})

	t.Run("by-number hit serves cached pointer", func(t *testing.T) {
		cache := newWitnessResultCache(96, 0, true, true)
		cache.Add(block1Hash, sentinel)
		api.witnessCache = cache
		t.Cleanup(func() { api.witnessCache = nil })

		result, err := api.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn}, nil)
		require.NoError(t, err)
		require.Same(t, sentinel, result, "a cached by-number request serves the stored pointer")
	})

	t.Run("by-hash orphan is reorged-away, never serves the resident entry", func(t *testing.T) {
		// Store a non-canonical fork header at height 1 so a by-hash request resolves to
		// block 1 but the hash differs from the canonical one.
		var hdr *types.Header
		require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
			var err error
			hdr, err = m.BlockReader.HeaderByNumber(ctx, tx, 1)
			return err
		}))
		require.NotNil(t, hdr)
		fork := types.CopyHeader(hdr)
		fork.Extra = append(append([]byte{}, fork.Extra...), 0xff)
		forkHash := fork.Hash()
		require.NotEqual(t, block1Hash, forkHash)
		require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
			return rawdb.WriteHeader(tx, fork)
		}))

		cache := newWitnessResultCache(96, 0, true, true)
		cache.Add(forkHash, sentinel) // resident under the orphan hash
		api.witnessCache = cache
		t.Cleanup(func() { api.witnessCache = nil })

		byHash := rpc.BlockNumberOrHash{BlockHash: &forkHash}

		tx, err := api.db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		result, hit, reorgedAway := api.serveFromWitnessCache(ctx, tx, byHash, witnessModeLegacy)
		require.False(t, hit, "a non-canonical by-hash request must not serve its resident entry")
		require.True(t, reorgedAway, "an orphan hash must be flagged reorged-away")
		require.Nil(t, result)

		_, err = api.ExecutionWitness(ctx, byHash, nil)
		require.ErrorIs(t, err, errWitnessReorgedAway)
	})
}

// TestGetWitnessHeadCaptureOutOfWindow pins that eth_getWitness on a head-capture minimal
// node (no commitment history, RLP/uncached, cannot recompute) returns the typed
// out-of-window error rather than the commitment-history hard gate.
func TestGetWitnessHeadCaptureOutOfWindow(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()

	base := newBaseApiForTest(m)
	base.witnessCache = newWitnessResultCache(96, 0, true /*headCapture*/, true /*cacheOnly*/)
	api := newEthApiForTest(base, m.DB, nil, nil)

	bn := rpc.BlockNumber(1)
	_, err := api.GetWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn})
	require.ErrorIs(t, err, errWitnessOutOfWindow)
}
