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
	"crypto/ecdsa"
	"slices"
	"testing"
	"time"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type zeroCanonical struct {
	dbservices.CanonicalReader
}

func (zeroCanonical) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{}, true, nil
}

type preExecFixture struct {
	db          kv.TemporalRwDB
	key         *ecdsa.PrivateKey
	contract    common.Address
	slotKey     []byte
	config      *chain.Config
	stateCache  *cache.StateCache
	readAheader *BlockReadAheader
}

func newPreExecFixture(t *testing.T, code []byte) *preExecFixture {
	oldReadAhead, oldOn := dbg.ReadAhead, preExecOn
	dbg.SetReadAhead(true)
	preExecOn = true
	t.Cleanup(func() { dbg.SetReadAhead(oldReadAhead); preExecOn = oldOn })
	ctx := t.Context()
	f := &preExecFixture{db: temporaltest.NewTestDB(t, datadir.New(t.TempDir())), contract: common.Address{19: 0x42}}

	var err error
	f.key, err = crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(f.key.PublicKey)
	slot := common.Hash{31: 0x07}
	senderAcc := accounts.Account{Balance: *uint256.NewInt(1e18), CodeHash: accounts.EmptyCodeHash}
	contractAcc := accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}
	f.slotKey = append(append([]byte{}, f.contract[:]...), slot[:]...)

	rwTx, err := f.db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, sender[:], accounts.SerialiseV3(&senderAcc), 1, nil))
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, f.contract[:], accounts.SerialiseV3(&contractAcc), 1, nil))
	require.NoError(t, domains.DomainPut(kv.CodeDomain, rwTx, f.contract[:], code, 1, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, f.slotKey, []byte{0x2a}, 1, nil))
	require.NoError(t, domains.Commit(ctx, rwTx))
	domains.Close()
	require.NoError(t, rwTx.Commit())

	f.config = chain.AllProtocolChanges.Copy()
	f.config.AmsterdamTime = nil
	f.stateCache = newTestStateCache()
	t.Cleanup(f.stateCache.Close)
	require.NoError(t, f.db.View(ctx, func(tx kv.Tx) error {
		stateVersion, err := rawdb.GetStateVersion(tx)
		f.stateCache.Applier().Initialize(stateVersion)
		return err
	}))
	f.readAheader = NewBlockReadAheader()
	f.readAheader.SetStateCache(f.stateCache)
	f.readAheader.SetPreExec(f.config, merge.NewFaker(ethash.NewFaker()), zeroCanonical{})
	return f
}

func (f *preExecFixture) header() *types.Header {
	return &types.Header{Number: *uint256.NewInt(1), Time: 1000, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1)}
}

func (f *preExecFixture) txn(t *testing.T, gas uint64, gasPrice uint64) types.Transaction {
	txn, err := types.SignNewTx(f.key, *types.MakeSigner(f.config, 1, 1000), &types.LegacyTx{
		CommonTx: types.CommonTx{To: &f.contract, GasLimit: gas},
		GasPrice: *uint256.NewInt(gasPrice),
	})
	require.NoError(t, err)
	return txn
}

func TestBlockReadAheaderPreExecWarmsSlotReadByTx(t *testing.T) {
	f := newPreExecFixture(t, []byte{0x60, 0x07, 0x54, 0x00})
	ctx := t.Context()
	require.NoError(t, f.db.View(ctx, func(tx kv.Tx) error {
		f.readAheader.AddHeaderAndBody(ctx, f.db, tx, f.header(), &types.Body{Transactions: types.Transactions{f.txn(t, 100_000, 1)}})
		return nil
	}))
	f.readAheader.WaitForWarmup(ctx)

	_, ok := f.stateCache.View(nil).Get(kv.AccountsDomain, f.contract[:])
	require.True(t, ok, "the tx's To account was not warmed")
	got, ok := f.stateCache.View(nil).Get(kv.StorageDomain, f.slotKey)
	require.True(t, ok, "slot the tx reads was not warmed")
	require.Equal(t, []byte{0x2a}, got)
}

func TestBlockReadAheaderPreExecRejectsBlobGasPriceOverflow(t *testing.T) {
	f := newPreExecFixture(t, []byte{0x00})
	header := f.header()
	excess := uint64(1) << 62
	header.ExcessBlobGas = &excess
	var err error
	require.NotPanics(t, func() {
		err = f.readAheader.preExecTxns(t.Context(), f.db, header, types.Transactions{f.txn(t, 100_000, 1)})
	})
	require.Error(t, err)
}

func TestBlockReadAheaderPreExecBoundsLoopingTx(t *testing.T) {
	f := newPreExecFixture(t, []byte{0x5b, 0x60, 0x00, 0x56})
	f.config.OsakaTime = nil
	ctx, cancel := context.WithCancel(t.Context())
	txns := types.Transactions{f.txn(t, 1<<62, 0)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = f.readAheader.preExecTxns(ctx, f.db, f.header(), txns)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("pre-exec of a looping tx did not stop after cancellation")
	}
}

func TestBlockReadAheaderPreExecUsesEffectiveGasPrice(t *testing.T) {
	f := newPreExecFixture(t, []byte{0x3a, 0x54, 0x00})
	header := f.header()
	header.BaseFee = uint256.NewInt(10)
	txn, err := types.SignNewTx(f.key, *types.MakeSigner(f.config, 1, 1000), &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{To: &f.contract, GasLimit: 100_000},
		ChainID:  *f.config.ChainID,
		TipCap:   *uint256.NewInt(2),
		FeeCap:   *uint256.NewInt(100),
	})
	require.NoError(t, err)
	require.NoError(t, f.readAheader.preExecTxns(t.Context(), f.db, header, types.Transactions{txn}))

	effective, feeCap := common.Hash{31: 12}, common.Hash{31: 100}
	_, ok := f.stateCache.View(nil).Get(kv.StorageDomain, append(f.contract[:], effective[:]...))
	require.True(t, ok, "the slot keyed by the effective gas price was not read")
	_, ok = f.stateCache.View(nil).Get(kv.StorageDomain, append(f.contract[:], feeCap[:]...))
	require.False(t, ok, "the slot keyed by the fee cap was read")
}

func TestBlockReadAheaderPreExecClampsGasPriceToFeeCap(t *testing.T) {
	f := newPreExecFixture(t, []byte{0x3a, 0x54, 0x00})
	header := f.header()
	header.BaseFee = uint256.NewInt(10)
	txn, err := types.SignNewTx(f.key, *types.MakeSigner(f.config, 1, 1000), &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{To: &f.contract, GasLimit: 100_000},
		ChainID:  *f.config.ChainID,
	})
	require.NoError(t, err)
	require.NoError(t, f.readAheader.preExecTxns(t.Context(), f.db, header, types.Transactions{txn}))

	feeCap, baseFee := common.Hash{}, common.Hash{31: 10}
	_, ok := f.stateCache.View(nil).Get(kv.StorageDomain, append(f.contract[:], feeCap[:]...))
	require.True(t, ok, "the slot keyed by the fee cap was not read")
	_, ok = f.stateCache.View(nil).Get(kv.StorageDomain, append(f.contract[:], baseFee[:]...))
	require.False(t, ok, "the slot keyed by the base fee was read")
}

func TestBlockReadAheaderPreExecSkipsFrameTxn(t *testing.T) {
	f := newPreExecFixture(t, []byte{0x00})
	pe := f.readAheader.preExec
	ttx, err := f.db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer ttx.Rollback()
	reader := state.NewReaderV3(readAheadGetter(ttx, f.stateCache, true))
	noHash := func(uint64) (common.Hash, error) { return common.Hash{}, nil }
	blockCtx := protocol.NewEVMBlockContext(f.header(), noHash, pe.engine, accounts.NilAddress, f.config)
	toucher := &commitmentToucher{ctx: t.Context(), read: func([]byte) []byte { return nil }}
	sender := crypto.PubkeyToAddress(f.key.PublicKey)
	pe.run(t.Context(), blockCtx, blockCtx.Rules(f.config), accounts.InternAddress(sender), &types.FrameTransaction{Nonce: 3}, 100_000, reader, toucher)

	created := types.CreateAddress(sender, 3)
	_, ok := f.stateCache.View(nil).Get(kv.AccountsDomain, created[:])
	require.False(t, ok, "a frame transaction was pre-executed as a contract creation")
}

func TestBlockReadAheaderPreExecCreatesAtTxNonce(t *testing.T) {
	f := newPreExecFixture(t, []byte{0x00})
	signer := *types.MakeSigner(f.config, 1, 1000)
	call := f.txn(t, 100_000, 1)
	create, err := types.SignNewTx(f.key, signer, &types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: 1, GasLimit: 200_000, Data: []byte{0x60, 0x2a, 0x60, 0x07, 0x55, 0x00}},
		GasPrice: *uint256.NewInt(1),
	})
	require.NoError(t, err)
	require.NoError(t, f.readAheader.preExecTxns(t.Context(), f.db, f.header(), types.Transactions{call, create}))

	sender := crypto.PubkeyToAddress(f.key.PublicKey)
	atTxNonce, atParentNonce := types.CreateAddress(sender, 1), types.CreateAddress(sender, 0)
	_, ok := f.stateCache.View(nil).Get(kv.AccountsDomain, atTxNonce[:])
	require.True(t, ok, "the contract address at the tx nonce was not read")
	_, ok = f.stateCache.View(nil).Get(kv.AccountsDomain, atParentNonce[:])
	require.False(t, ok, "the contract address at the parent nonce was read")
}

func TestCommitmentToucherStopsAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	reads := 0
	toucher := &commitmentToucher{ctx: ctx, read: func([]byte) []byte { reads++; return nil }}
	require.NoError(t, toucher.UpdateAccountData(accounts.InternAddress(common.Address{1}), nil, nil))
	require.Equal(t, 1, reads)
	cancel()
	require.NoError(t, toucher.UpdateAccountData(accounts.InternAddress(common.Address{2}), nil, nil))
	require.Equal(t, 1, reads, "the toucher read commitment after cancellation")
}

func TestBlockReadAheaderPreExecWalksStoragePathOfTxWrite(t *testing.T) {
	f := newPreExecFixture(t, []byte{0x60, 0x2a, 0x60, 0x08, 0x55, 0x00})
	pe := f.readAheader.preExec
	ttx, err := f.db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer ttx.Rollback()
	reader := state.NewReaderV3(readAheadGetter(ttx, f.stateCache, true))
	noHash := func(uint64) (common.Hash, error) { return common.Hash{}, nil }
	blockCtx := protocol.NewEVMBlockContext(f.header(), noHash, pe.engine, accounts.NilAddress, f.config)
	var reads []string
	toucher := &commitmentToucher{ctx: t.Context(), read: func(key []byte) []byte { reads = append(reads, string(key)); return nil }}
	from := accounts.InternAddress(crypto.PubkeyToAddress(f.key.PublicKey))
	pe.run(t.Context(), blockCtx, blockCtx.Rules(f.config), from, f.txn(t, 100_000, 1), 100_000, reader, toucher)

	contractHash := keccak.Sum256(f.contract[:])
	require.Contains(t, reads, string(v3.StorageNodeKey(contractHash, nil, nil)), "the tx's SSTORE must walk the contract's storage path")
}

func TestCommitmentToucherWalksAndQueuesSortedBatches(t *testing.T) {
	touches := &commitmentdb.PreExecTouches{C: make(chan []commitmentdb.PathTouch, 1)}
	reads := 0
	toucher := &commitmentToucher{ctx: context.Background(), out: touches, read: func([]byte) []byte { reads++; return nil }}
	for i := range preExecBatch + 1 {
		toucher.touch(bytes.Repeat([]byte{byte(250 - i)}, 32), nil)
	}
	require.Equal(t, preExecBatch+1, reads, "queueing a path must not replace pre-exec's own walk")
	batch := <-touches.C
	require.Len(t, batch, preExecBatch)
	require.True(t, slices.IsSortedFunc(batch, commitmentdb.ComparePathTouch))
	toucher.flush()
	require.Equal(t, []commitmentdb.PathTouch{{Account: [32]byte(bytes.Repeat([]byte{byte(250 - preExecBatch)}, 32))}}, <-touches.C)
	require.Equal(t, preExecBatch+1, toucher.paths)
}

func TestCommitmentToucherCountsBatchesTheQueueRefused(t *testing.T) {
	touches := &commitmentdb.PreExecTouches{C: make(chan []commitmentdb.PathTouch)}
	toucher := &commitmentToucher{ctx: context.Background(), out: touches, read: func([]byte) []byte { return nil }}
	toucher.touch(bytes.Repeat([]byte{1}, 32), nil)
	toucher.flush()
	require.EqualValues(t, 1, touches.Dropped.Load())
}

func TestStopPreExecCancelsOnlyTheNamedBlock(t *testing.T) {
	ctx, untrack := trackPreExec(context.Background(), common.Hash{7})
	defer untrack()
	StopPreExec(common.Hash{8})
	require.NoError(t, ctx.Err())
	StopPreExec(common.Hash{7})
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestStopPreExecCancelsARunThatStartsLater(t *testing.T) {
	StopPreExec(common.Hash{9})
	ctx, untrack := trackPreExec(context.Background(), common.Hash{9})
	defer untrack()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
