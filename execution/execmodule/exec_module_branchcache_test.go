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

package execmodule_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
)

// gateTxnProvider parks the payload builder inside its transaction loop:
// the first ProvideTxns call signals entered and blocks until release, then
// serves txn once. This pins the build's read snapshot while the test
// advances the chain underneath it.
type gateTxnProvider struct {
	entered chan struct{}
	release chan struct{}
	txn     types.Transaction
	once    sync.Once
	served  atomic.Bool
}

func newGateTxnProvider(txn types.Transaction) *gateTxnProvider {
	return &gateTxnProvider{entered: make(chan struct{}), release: make(chan struct{}), txn: txn}
}

func (p *gateTxnProvider) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if p.served.CompareAndSwap(false, true) {
		return []types.Transaction{p.txn}, nil
	}
	return nil, nil
}

func keccakNibble0(addr common.Address) byte {
	return crypto.Keccak256(addr[:])[0] >> 4
}

// grindRecipients picks recipients whose state-trie positions isolate one
// branch row: a and b share the first hashed-key nibble (their own depth-1
// branch row), c and every per-block actor (sender, coinbases, system
// contracts) live under other nibbles, so only transfers to a or b rewrite
// that row.
func grindRecipients(t *testing.T, sender common.Address) (a, b, c common.Address) {
	t.Helper()
	forbidden := map[byte]bool{
		keccakNibble0(sender):                                     true,
		keccakNibble0(common.Address{}):                           true, // blockgen coinbase
		keccakNibble0(common.Address{1}):                          true, // junk-build fee recipient
		keccakNibble0(params.BeaconRootsAddress.Value()):          true,
		keccakNibble0(params.HistoryStorageAddress.Value()):       true,
		keccakNibble0(params.WithdrawalRequestAddress.Value()):    true,
		keccakNibble0(params.ConsolidationRequestAddress.Value()): true,
		keccakNibble0(params.SystemAddress.Value()):               true,
	}
	var abNibble byte
	var haveA, haveB, haveC bool
	for i := 1; i < 1<<16; i++ {
		addr := common.Address{0xAA, byte(i >> 8), byte(i)}
		n := keccakNibble0(addr)
		if forbidden[n] {
			continue
		}
		switch {
		case !haveA:
			a, abNibble, haveA = addr, n, true
		case !haveB && n == abNibble && crypto.Keccak256(addr[:])[0] != crypto.Keccak256(a[:])[0]:
			b, haveB = addr, true
		case !haveC && n != abNibble:
			c, haveC = addr, true
		}
		if haveA && haveB && haveC {
			return a, b, c
		}
	}
	t.Fatal("could not grind recipient addresses")
	return
}

func sharedBranchCache(t *testing.T, tx kv.TemporalTx) *commitment.BranchCache {
	t.Helper()
	provider, ok := tx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	bc := provider.BranchCache()
	require.NotNil(t, bc)
	return bc
}

func clearSharedBranchCache(t *testing.T, db kv.TemporalRwDB) {
	t.Helper()
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	sharedBranchCache(t, roTx).Clear()
}

// latestBranchRow returns the committed value of one commitment branch row.
func latestBranchRow(t *testing.T, db kv.TemporalRwDB, key []byte) []byte {
	t.Helper()
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	v, _, err := roTx.GetLatest(kv.CommitmentDomain, key)
	require.NoError(t, err)
	require.NotEmpty(t, v, "commitment branch row missing")
	return bytes.Clone(v)
}

// TestBuilderStaleSnapshotMustNotPoisonBranchCache pins the cache-poisoning
// mechanism: a payload build runs outside the exec-module semaphore on its
// own read snapshot, and its commitment fold read-fills the shared
// BranchCache. When the build outlives head progression, those read-fills
// write branches from the older snapshot; once the fresher entries are gone
// (LRU eviction, or the fill landing after the flush refresh), every later
// commitment folds the stale branch and the node computes wrong trie roots
// for valid blocks — two identically-built nodes then disagree at the tip.
//
// The build is parked inside its transaction loop while the chain advances
// two blocks, the cache is cleared to model the eviction, and the release
// lets the fold repopulate it from the stale snapshot. Block 4 rewrites the
// branch row that blocks 1-2 shaped; a node with coherent caches must accept
// it.
func TestBuilderStaleSnapshotMustNotPoisonBranchCache(t *testing.T) {
	prevParallel := dbg.Exec3Parallel
	dbg.Exec3Parallel = false
	t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })

	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	exec := m.ExecModule
	addrA, addrB, addrC := grindRecipients(t, m.Address)

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(i int, gen *blockgen.BlockGen) {
		transfer := func(to common.Address) {
			txn, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), to, uint256.NewInt(10_000), params.TxGas, gasPrice, nil), *signer, m.Key)
			require.NoError(t, err)
			gen.AddTx(txn)
		}
		switch i {
		case 0:
			transfer(addrA) // A and B exist from block 1 on: their shared
			transfer(addrB) // depth-1 branch row is materialized
		case 1:
			transfer(addrA) // rewrites the shared row
		case 2:
			transfer(addrC) // leaves the shared row untouched
		case 3:
			transfer(addrB) // block 4's commitment folds the shared row
		}
	})
	require.NoError(t, err)

	require.NoError(t, insertValidateAndUfc1By1(ctx, exec, chainPack.Blocks[:1]))

	// The row shared by A and B is the depth-1 commitment branch at their common
	// first hashed nibble. Asserting how blocks 2 and 3 touch it makes
	// grindRecipients' isolation self-checking and pins the staleness the repro
	// needs: the head-1 row the build reads must differ from the head-3 row
	// block 4 folds.
	abBranchKey := nibbles.HexToCompact([]byte{keccakNibble0(addrA)})
	rowAfterBlock1 := latestBranchRow(t, m.DB, abBranchKey)

	// Junk build on head 1, parked before it pulls transactions. Its transfer
	// to A makes the eventual commitment fold walk A's path, reading the
	// shared branch row through the pinned head-1 snapshot.
	junkTxn, err := types.SignTx(types.NewTransaction(2, addrA, uint256.NewInt(1), params.TxGas, gasPrice, nil), *signer, m.Key)
	require.NoError(t, err)
	// The builder drops transactions without a recovered sender; the txpool
	// normally guarantees it.
	junkTxn.SetSender(accounts.InternAddress(m.Address))
	gate := newGateTxnProvider(junkTxn)
	var parentBeaconBlockRoot common.Hash
	_, err = rand.Read(parentBeaconBlockRoot[:])
	require.NoError(t, err)
	head := chainPack.Blocks[0]
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            head.Hash(),
		Timestamp:             head.Header().Time + 1,
		PrevRandao:            head.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
		CustomTxnProvider:     gate,
	})
	require.NoError(t, err)
	select {
	case <-gate.entered:
	case <-time.After(time.Minute):
		t.Fatal("build did not reach its transaction loop")
	}

	require.NoError(t, insertValidateAndUfc1By1(ctx, exec, chainPack.Blocks[1:2]))
	rowAfterBlock2 := latestBranchRow(t, m.DB, abBranchKey)
	require.NotEqual(t, rowAfterBlock1, rowAfterBlock2, "block 2 (transfer to A) must rewrite the shared branch row")

	require.NoError(t, insertValidateAndUfc1By1(ctx, exec, chainPack.Blocks[2:3]))
	rowAfterBlock3 := latestBranchRow(t, m.DB, abBranchKey)
	require.Equal(t, rowAfterBlock2, rowAfterBlock3, "block 3 (transfer to C) must leave the shared branch row untouched")

	clearSharedBranchCache(t, m.DB)

	close(gate.release)
	junkBlock, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	require.Len(t, junkBlock.Transactions(), 1)

	require.NoError(t, insertValidateAndUfc1By1(ctx, exec, chainPack.Blocks[3:4]))
}

// someBranchKey returns a commitment branch row present in the latest state.
func someBranchKey(t *testing.T, tx kv.TemporalTx) []byte {
	t.Helper()
	it, err := tx.Debug().RangeLatest(kv.CommitmentDomain, nil, nil, 1<<20)
	require.NoError(t, err)
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		if !bytes.Equal(k, commitment.KeyCommitmentState) && len(v) > 0 {
			return bytes.Clone(k)
		}
	}
	t.Fatal("no commitment branch row found")
	return nil
}

// TestWithoutBranchCacheNeverTouchesSharedCache pins the WithoutBranchCache
// contract: reads through such a SharedDomains must not read-fill the shared
// BranchCache, while a default SharedDomains does.
func TestWithoutBranchCacheNeverTouchesSharedCache(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, gen *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{0xAA, byte(i)}, uint256.NewInt(10_000), params.TxGas, gasPrice, nil), *signer, m.Key)
		require.NoError(t, err)
		gen.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, insertValidateAndUfc1By1(ctx, m.ExecModule, chainPack.Blocks))

	roTx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	branchKey := someBranchKey(t, roTx)
	bc := sharedBranchCache(t, roTx)

	readBranch := func(opts ...execctx.SharedDomainOption) {
		sd, err := execctx.NewSharedDomains(ctx, roTx, log.New(), opts...)
		require.NoError(t, err)
		defer sd.Close()
		v, _, err := sd.GetLatest(kv.CommitmentDomain, roTx, branchKey)
		require.NoError(t, err)
		require.NotEmpty(t, v)
	}

	bc.Clear()
	readBranch(execctx.WithoutBranchCache())
	_, _, ok := bc.Get(branchKey)
	require.False(t, ok, "detached SharedDomains read-filled the shared BranchCache")

	readBranch()
	_, _, ok = bc.Get(branchKey)
	require.True(t, ok, "default SharedDomains should read-fill the shared BranchCache")
}
