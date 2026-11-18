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
	"testing"

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

func TestBlockReadAheaderPreExecWarmsSlotReadByTx(t *testing.T) {
	oldReadAhead, oldOn := dbg.ReadAhead, preExecOn
	dbg.SetReadAhead(true)
	preExecOn = true
	t.Cleanup(func() { dbg.SetReadAhead(oldReadAhead); preExecOn = oldOn })
	ctx := t.Context()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	contract := common.Address{19: 0x42}
	code := []byte{0x60, 0x07, 0x54, 0x00}
	slot := common.Hash{31: 0x07}
	senderAcc := accounts.Account{Balance: *uint256.NewInt(1e18), CodeHash: accounts.EmptyCodeHash}
	contractAcc := accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}
	slotKey := append(append([]byte{}, contract[:]...), slot[:]...)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, sender[:], accounts.SerialiseV3(&senderAcc), 1, nil))
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, contract[:], accounts.SerialiseV3(&contractAcc), 1, nil))
	require.NoError(t, domains.DomainPut(kv.CodeDomain, rwTx, contract[:], code, 1, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, slotKey, []byte{0x2a}, 1, nil))
	require.NoError(t, domains.Commit(ctx, rwTx))
	domains.Close()
	require.NoError(t, rwTx.Commit())

	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = nil
	header := &types.Header{Number: *uint256.NewInt(1), Time: 1000, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1)}
	txn, err := types.SignNewTx(key, *types.MakeSigner(config, 1, 1000), &types.LegacyTx{
		CommonTx: types.CommonTx{To: &contract, GasLimit: 100_000},
		GasPrice: *uint256.NewInt(1),
	})
	require.NoError(t, err)

	stateCache := newTestStateCache()
	t.Cleanup(stateCache.Close)
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		stateVersion, err := rawdb.GetStateVersion(tx)
		stateCache.Applier().Initialize(stateVersion)
		return err
	}))
	readAheader := NewBlockReadAheader()
	readAheader.SetStateCache(stateCache)
	readAheader.SetPreExec(config, merge.NewFaker(ethash.NewFaker()), zeroCanonical{})
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		readAheader.AddHeaderAndBody(ctx, db, tx, header, &types.Body{Transactions: types.Transactions{txn}})
		return nil
	}))
	readAheader.WaitForWarmup(ctx)

	_, ok := stateCache.View(nil).Get(kv.AccountsDomain, contract[:])
	require.True(t, ok, "the tx's To account was not warmed")
	got, ok := stateCache.View(nil).Get(kv.StorageDomain, slotKey)
	require.True(t, ok, "slot the tx reads was not warmed")
	require.Equal(t, []byte{0x2a}, got)
}
