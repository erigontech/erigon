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
	"context"
	"crypto/ecdsa"
	"testing"
	"time"

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
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type zeroCanonical struct{}

func (zeroCanonical) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{}, true, nil
}

func (zeroCanonical) IsCanonical(context.Context, kv.Getter, common.Hash, uint64) (bool, error) {
	return true, nil
}

func (zeroCanonical) BadHeaderNumber(context.Context, kv.Getter, common.Hash) (*uint64, error) {
	return nil, nil
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
