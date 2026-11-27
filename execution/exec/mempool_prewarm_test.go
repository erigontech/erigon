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

package exec

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	commitmentv3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestMempoolPrewarmReadsContractAndDropsWrites(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	previousCache := dbg.UseStateCache
	dbg.SetUseStateCache(true)
	t.Cleanup(func() { dbg.SetUseStateCache(previousCache) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
	stateCache := newTestStateCache()
	t.Cleanup(stateCache.Close)
	config := chain.TestChainBerlinConfig
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 1_000_000, Coinbase: common.Address{19: 1}}
	to := common.Address{19: 2, 18: 1}
	from := common.Address{19: 3}
	bytecode := []byte{0x60, 0, 0x54, 0x60, 1, 0x01, 0x60, 0, 0x55, 0x00}
	sender := accounts.SerialiseV3(&accounts.Account{Balance: *uint256.NewInt(1_000_000)})
	contract := accounts.SerialiseV3(&accounts.Account{CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(bytecode))})
	slot := make([]byte, 52)
	copy(slot, to[:])
	secondSlot := bytes.Clone(slot)
	secondSlot[len(secondSlot)-1] = 1
	rwTx, domains := temporaltest.NewTestTxSD(t, db)
	domains.BindStateCache(stateCache)
	domains.SetTxNum(10)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, from[:], sender, 10, nil))
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, to[:], contract, 10, nil))
	require.NoError(t, domains.DomainPut(kv.CodeDomain, rwTx, to[:], bytecode, 10, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, slot, []byte{1}, 10, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, secondSlot, []byte{2}, 10, nil))
	_, err := domains.ComputeCommitment(ctx, rwTx, true, header.Number.Uint64(), 10, "", nil)
	require.NoError(t, err)
	rawdb.WriteHeadBlockHash(rwTx, header.Hash())
	require.NoError(t, stages.SaveStageProgress(rwTx, stages.Execution, header.Number.Uint64()))
	require.NoError(t, domains.Commit(ctx, rwTx))
	stateCache.Applier().Clear()

	txn := types.NewTransaction(0, to, new(uint256.Int), 60_000, uint256.NewInt(1), nil)
	txn.SetSender(accounts.InternAddress(from))
	bra := NewBlockReadAheader()
	bra.SetStateCache(stateCache)
	offered := false
	bra.PrewarmMempool(ctx, db, nil, config, nil, header, func(context.Context, common.Hash, uint64, uint64, int, mapset.Set[[32]byte]) (types.Transactions, error) {
		if offered {
			cancel()
			return nil, nil
		}
		offered = true
		return types.Transactions{txn}, nil
	}, log.New())

	for _, read := range []struct {
		domain     kv.Domain
		key, value []byte
	}{
		{kv.AccountsDomain, from[:], sender},
		{kv.AccountsDomain, to[:], contract},
		{kv.CodeDomain, to[:], bytecode},
		{kv.StorageDomain, slot, []byte{1}},
	} {
		cached, ok := stateCache.View(nil).Get(read.domain, read.key)
		require.True(t, ok, "the pass must read %s", read.domain)
		require.Equal(t, read.value, cached)
		require.NoError(t, db.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
			durable, _, err := tx.GetLatest(read.domain, read.key, kv.GetLatestOptions{})
			require.NoError(t, err)
			require.Equal(t, read.value, durable)
			return nil
		}))
	}
}

func TestMempoolPrewarmRunsLateTransactions(t *testing.T) {
	first := common.Address{18: 1}
	late := common.Address{18: 2}
	output, stateCache := runMempoolPrewarmTest(t, func(round int, txn func(uint64, common.Address) types.Transaction) types.Transactions {
		switch round {
		case 1:
			return types.Transactions{txn(0, first)}
		case 2:
			return types.Transactions{txn(1, late)}
		default:
			return nil
		}
	})
	_, warmed := stateCache.View(nil).Get(kv.AccountsDomain, late[:])
	require.True(t, warmed, "transaction added after round one should warm its recipient")
	require.Contains(t, output, "txs=2")
}

func TestMempoolPrewarmRunsHashOncePerHead(t *testing.T) {
	to := common.Address{18: 1}
	output, _ := runMempoolPrewarmTest(t, func(round int, txn func(uint64, common.Address) types.Transaction) types.Transactions {
		if round <= 2 {
			return types.Transactions{txn(0, to)}
		}
		return nil
	})
	require.Contains(t, output, "rounds=3", "the provider should run again on the same head")
	require.Contains(t, output, "txs=1 ", "only one execution should count for a repeated hash")
	require.Contains(t, output, "accounts=3 ", "a repeated hash should not repeat account reads")
}

func TestMempoolPrewarmStopsAtHeadCap(t *testing.T) {
	const expectedCap = 20_000
	to := common.Address{18: 1}
	output, _ := runMempoolPrewarmTest(t, func(round int, txn func(uint64, common.Address) types.Transaction) types.Transactions {
		if round > 2 {
			return nil
		}
		txns := make(types.Transactions, expectedCap/2+1)
		for i := range txns {
			txns[i] = txn(uint64((round-1)*len(txns)+i), to)
		}
		return txns
	})
	require.Contains(t, output, "txs=20000 ", "the cap should apply across rounds")
	require.Contains(t, output, "rounds=2")
	require.Contains(t, output, "cancelled=false")
}

func runMempoolPrewarmTest(t *testing.T, candidates func(round int, txn func(uint64, common.Address) types.Transaction) types.Transactions) (string, *cache.StateCache) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	db, bra, head, from, stateCache := newMempoolPrewarmTestState(t)

	var output bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&output, log.LogfmtFormat()))
	var round int
	bra.PrewarmMempool(ctx, db, nil, chain.TestChainBerlinConfig, nil, head, func(context.Context, common.Hash, uint64, uint64, int, mapset.Set[[32]byte]) (types.Transactions, error) {
		round++
		txns := candidates(round, func(nonce uint64, to common.Address) types.Transaction {
			txn := types.NewTransaction(nonce, to, new(uint256.Int), 21_000, uint256.NewInt(1), nil)
			txn.SetSender(accounts.InternAddress(from))
			return txn
		})
		if len(txns) == 0 {
			cancel()
		}
		return txns, nil
	}, logger)
	require.Equal(t, 1, bytes.Count(output.Bytes(), []byte("[mempool prewarm]")), "log one summary per head")
	return output.String(), stateCache
}

func newMempoolPrewarmTestState(t *testing.T) (kv.TemporalRoDB, *BlockReadAheader, *types.Header, common.Address, *cache.StateCache) {
	t.Helper()
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
	stateCache := newTestStateCache()
	t.Cleanup(stateCache.Close)
	head := &types.Header{Number: *uint256.NewInt(1), GasLimit: 1_000_000_000}
	from := common.Address{19: 1}
	rwTx, domains := temporaltest.NewTestTxSD(t, db)
	domains.BindStateCache(stateCache)
	domains.SetTxNum(10)
	sender := accounts.SerialiseV3(&accounts.Account{Balance: *uint256.NewInt(1_000_000)})
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, from[:], sender, 10, nil))
	rawdb.WriteHeadBlockHash(rwTx, head.Hash())
	require.NoError(t, stages.SaveStageProgress(rwTx, stages.Execution, head.Number.Uint64()))
	require.NoError(t, domains.Commit(t.Context(), rwTx))
	stateCache.Applier().Clear()
	bra := NewBlockReadAheader()
	bra.SetStateCache(stateCache)
	return db, bra, head, from, stateCache
}

type mempoolV3BranchReadDB struct {
	kv.TemporalRoDB
	onRead func([]byte, kv.GetLatestOptions)
}

func (db *mempoolV3BranchReadDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	return db.wrap(db.TemporalRoDB.BeginTemporalRo(ctx))
}

func (db *mempoolV3BranchReadDB) wrap(tx kv.TemporalTx, err error) (kv.TemporalTx, error) {
	if err != nil {
		return nil, err
	}
	return &mempoolV3BranchReadTx{TemporalTx: tx, onRead: db.onRead}, nil
}

type mempoolV3BranchReadTx struct {
	kv.TemporalTx
	onRead func([]byte, kv.GetLatestOptions)
}

func (tx *mempoolV3BranchReadTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if domain == kv.CommitmentDomain {
		tx.onRead(key, opts)
	}
	return tx.TemporalTx.GetLatest(domain, key, opts)
}

func TestMempoolPrewarmWalksV3AccountAndSlot(t *testing.T) {
	db, head, txn, accountNode, storageNode := newMempoolV3Contract(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	reads := make(map[string]int)
	var branchCacheUsed bool
	var mu sync.Mutex
	counted := &mempoolV3BranchReadDB{TemporalRoDB: db, onRead: func(key []byte, opts kv.GetLatestOptions) {
		mu.Lock()
		defer mu.Unlock()
		reads[string(key)]++
		branchCacheUsed = branchCacheUsed || opts.BranchCache()
	}}
	offered := false
	NewBlockReadAheader().PrewarmMempool(ctx, counted, nil, chain.TestChainBerlinConfig, nil, head, func(context.Context, common.Hash, uint64, uint64, int, mapset.Set[[32]byte]) (types.Transactions, error) {
		if offered {
			cancel()
			return nil, nil
		}
		offered = true
		return types.Transactions{txn}, nil
	}, log.New())

	require.Positive(t, reads[string(accountNode)], "the pass must read the touched account's v3 branch")
	require.Positive(t, reads[string(storageNode)], "the pass must read the touched slot's v3 branch")
	require.False(t, branchCacheUsed)
}

func TestMempoolPrewarmWaitsForV3WalkOnCancel(t *testing.T) {
	db, head, txn, _, _ := newMempoolV3Contract(t)
	ctx, cancel := context.WithCancel(t.Context())
	started, release, passDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	allowReadToFinish := sync.OnceFunc(func() { close(release) })
	defer func() {
		cancel()
		allowReadToFinish()
		<-passDone
	}()
	var startRead sync.Once
	var reads atomic.Int64
	blocking := &mempoolV3BranchReadDB{TemporalRoDB: db, onRead: func([]byte, kv.GetLatestOptions) {
		reads.Add(1)
		startRead.Do(func() { close(started) })
		<-release
	}}
	go func() {
		defer close(passDone)
		NewBlockReadAheader().PrewarmMempool(ctx, blocking, nil, chain.TestChainBerlinConfig, nil, head, func(context.Context, common.Hash, uint64, uint64, int, mapset.Set[[32]byte]) (types.Transactions, error) {
			return types.Transactions{txn}, nil
		}, log.New())
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the pass did not start a v3 branch read")
	}
	cancel()
	select {
	case <-passDone:
		t.Fatal("the pass returned before its v3 branch read finished")
	case <-time.After(25 * time.Millisecond):
	}
	allowReadToFinish()
	select {
	case <-passDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the pass did not stop after its v3 branch read finished")
	}
	require.Equal(t, int64(1), reads.Load(), "cancellation must stop the walk before its next branch read")
}

func newMempoolV3Contract(t *testing.T) (kv.TemporalRoDB, *types.Header, types.Transaction, []byte, []byte) {
	t.Helper()
	t.Cleanup(statecfg.SetCommitmentV3(true))
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
	head := &types.Header{Number: *uint256.NewInt(1), GasLimit: 1_000_000, Coinbase: common.Address{19: 4}}
	from, to := common.Address{19: 1}, common.Address{18: 1}
	slotKey := make([]byte, 52)
	copy(slotKey, to[:])
	accountPath := commitment.KeyToHexNibbleHash(from[:])
	storagePath := commitment.KeyToHexNibbleHash(slotKey)
	accountNodes := commitmentv3.PathNodeKeys(nil, accountPath, 1)
	storageNodes := commitmentv3.PathNodeKeys(nil, storagePath, 1)
	accountNode, storageNode := accountNodes[len(accountNodes)-1], storageNodes[len(storageNodes)-1]

	accountSibling := common.Address{18: 0xff}
	for id := range 256 {
		accountSibling[19] = byte(id)
		path := commitment.KeyToHexNibbleHash(accountSibling[:])
		if path[0] == accountPath[0] && path[1] != accountPath[1] {
			break
		}
	}
	slotSibling := bytes.Clone(slotKey)
	for id := 1; id < 256; id++ {
		slotSibling[len(slotSibling)-1] = byte(id)
		path := commitment.KeyToHexNibbleHash(slotSibling)
		if path[64] == storagePath[64] && path[65] != storagePath[65] {
			break
		}
	}

	code := []byte{0x60, 0, 0x54, 0x00}
	funded := accounts.SerialiseV3(&accounts.Account{Balance: *uint256.NewInt(1_000_000)})
	contract := accounts.SerialiseV3(&accounts.Account{CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))})
	ctx := t.Context()
	rw, sd := temporaltest.NewTestTxSD(t, db)
	sd.SetTxNum(10)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rw, from[:], funded, 10, nil))
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rw, accountSibling[:], funded, 10, nil))
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rw, to[:], contract, 10, nil))
	require.NoError(t, sd.DomainPut(kv.CodeDomain, rw, to[:], code, 10, nil))
	require.NoError(t, sd.DomainPut(kv.StorageDomain, rw, slotKey, []byte{1}, 10, nil))
	require.NoError(t, sd.DomainPut(kv.StorageDomain, rw, slotSibling, []byte{2}, 10, nil))
	_, err := sd.ComputeCommitment(ctx, rw, true, head.Number.Uint64(), 10, "", nil)
	require.NoError(t, err)
	rawdb.WriteHeadBlockHash(rw, head.Hash())
	require.NoError(t, stages.SaveStageProgress(rw, stages.Execution, head.Number.Uint64()))
	require.NoError(t, sd.Commit(ctx, rw))
	require.NoError(t, db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		for _, key := range [][]byte{accountNode, storageNode} {
			data, _, err := tx.GetLatest(kv.CommitmentDomain, key, kv.GetLatestOptions{})
			require.NoError(t, err)
			require.NotEmpty(t, data, "fixture must have a v3 branch at %x", key)
		}
		return nil
	}))

	txn := types.NewTransaction(0, to, new(uint256.Int), 60_000, uint256.NewInt(1), nil)
	txn.SetSender(accounts.InternAddress(from))
	return db, head, txn, accountNode, storageNode
}
