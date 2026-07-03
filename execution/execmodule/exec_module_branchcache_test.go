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
	"context"
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"

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
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
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

func clearSharedBranchCache(t *testing.T, db kv.TemporalRwDB) {
	t.Helper()
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.ClearBranchCache()
}

// TestBuilderStaleSnapshotMustNotPoisonBranchCache pins the cache-poisoning
// mechanism behind https://github.com/erigontech/erigon/issues/22152: a
// payload build runs outside the exec-module semaphore on its own read
// snapshot, and its commitment fold read-fills the shared BranchCache. When
// the build outlives head progression, those read-fills write branches from
// the older snapshot; once the fresher entries are gone (LRU eviction, or the
// fill landing after the flush refresh), every later commitment folds the
// stale branch and the node computes wrong trie roots for valid blocks —
// two identically-built nodes then disagree at the tip.
//
// The build is parked inside its transaction loop while the chain advances
// two blocks, the cache is cleared to model the eviction, and the release
// lets the fold repopulate it from the stale snapshot. Block 4 rewrites the
// branch row that blocks 1-2 shaped; a node with coherent caches must accept
// it.
func TestBuilderStaleSnapshotMustNotPoisonBranchCache(t *testing.T) {
	prevParallel := dbg.Exec3Parallel
	dbg.Exec3Parallel = false // serial executor, as in the kurtosis serial job
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

	// Junk build on head 1, parked before it pulls transactions. Its transfer
	// to A makes the eventual commitment fold walk A's path, reading the
	// shared branch row through the pinned head-1 snapshot.
	junkTxn, err := types.SignTx(types.NewTransaction(2, addrA, uint256.NewInt(1), params.TxGas, gasPrice, nil), *signer, m.Key)
	require.NoError(t, err)
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
	<-gate.entered

	require.NoError(t, insertValidateAndUfc1By1(ctx, exec, chainPack.Blocks[1:3]))

	clearSharedBranchCache(t, m.DB)

	close(gate.release)
	_, err = getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)

	require.NoError(t, insertValidateAndUfc1By1(ctx, exec, chainPack.Blocks[3:4]))
}
