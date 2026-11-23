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

package txpool

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// A remote txn that never reaches a sub-pool still allocates a sender id, and
// the only eviction walks p.deletedTxns, which such a txn never enters. Without
// a sweep, one peer can grow senderIDs/senderID2Addr for the process lifetime.
func TestProcessRemoteTxnsDoesNotLeakSenderIDsForRejectedTxns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pool := seedBlobKZGTestPool(t, ctx)
	require.NoError(t, pool.start(ctx))

	const senders = 16
	var slots TxnSlots
	for i := range senders {
		slot := makeBlobSlot(byte(0x40+i), false)
		var addr [20]byte
		binary.BigEndian.PutUint64(addr[12:], uint64(i+1))
		slots.Append(&slot, addr[:], false)
	}

	pool.lock.Lock()
	idsBefore, addrsBefore := len(pool.senders.senderIDs), len(pool.senders.senderID2Addr)
	pool.lock.Unlock()

	pool.AddRemoteTxns(ctx, slots, gointerfaces.ConvertHashToH512([64]byte{0x41}), nil)
	require.NoError(t, pool.processRemoteTxns(ctx))

	pool.lock.Lock()
	defer pool.lock.Unlock()
	require.Equal(t, idsBefore, len(pool.senders.senderIDs),
		"sender ids for txns that reached no sub-pool must not be retained")
	require.Equal(t, addrsBefore, len(pool.senders.senderID2Addr),
		"senderID2Addr must be swept in step with senderIDs")
}

// The same defect on the local path: AddLocalTxns registers senders, then
// validateTxns can reject every txn.
func TestAddLocalTxnsDoesNotLeakSenderIDsForRejectedTxns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pool := seedBlobKZGTestPool(t, ctx)
	require.NoError(t, pool.start(ctx))

	const senders = 16
	var slots TxnSlots
	for i := range senders {
		slot := makeBlobSlot(byte(0x60+i), false)
		var addr [20]byte
		binary.BigEndian.PutUint64(addr[12:], uint64(i+1+senders))
		slots.Append(&slot, addr[:], false)
	}

	pool.lock.Lock()
	idsBefore, addrsBefore := len(pool.senders.senderIDs), len(pool.senders.senderID2Addr)
	pool.lock.Unlock()

	_, err := pool.AddLocalTxns(ctx, slots)
	require.NoError(t, err)

	pool.lock.Lock()
	defer pool.lock.Unlock()
	require.Equal(t, idsBefore, len(pool.senders.senderIDs),
		"sender ids for txns that reached no sub-pool must not be retained")
	require.Equal(t, addrsBefore, len(pool.senders.senderID2Addr))
}

func newBlockChangeForAccounts(blockHash common.Hash, nonce uint64, addrs ...common.Address) *remoteproto.StateChangeBatch {
	account := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts.EmptyCodeHash}
	changes := make([]*remoteproto.AccountChange, 0, len(addrs))
	for _, addr := range addrs {
		changes = append(changes, &remoteproto.AccountChange{
			Action:  remoteproto.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(addr),
			Data:    accounts.SerialiseV3(&account),
		})
	}
	return &remoteproto.StateChangeBatch{
		PendingBlockBaseFee: 1,
		BlockGasLimit:       1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHash: gointerfaces.ConvertHashToH256(blockHash),
			Changes:   changes,
		}},
	}
}

func TestOnNewBlockDoesNotLeakSenderIDsOfAccountsOutsidePool(t *testing.T) {
	ctx, pool, _, _, sender := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)

	txn := newTestTxnSlot(0, 0, 1, 2, 100_000)
	txn.IDHash[0] = 1
	var txns TxnSlots
	txns.Append(txn, sender[:], true)
	_, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)

	pool.lock.Lock()
	idsBefore, addrsBefore := len(pool.senders.senderIDs), len(pool.senders.senderID2Addr)
	pool.lock.Unlock()

	touched := []common.Address{{0x10}, {0x11}, {0x12}}
	var minedTxns TxnSlots
	for i, minedSender := range []common.Address{{0x20}, {0x21}} {
		mined := newTestTxnSlot(0, 0, 1, 2, 100_000)
		mined.IDHash[0] = byte(0x20 + i)
		minedTxns.Append(mined, minedSender[:], false)
		touched = append(touched, minedSender)
	}
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 1, touched...), TxnSlots{}, TxnSlots{}, minedTxns))

	pool.lock.Lock()
	defer pool.lock.Unlock()
	require.Equal(t, idsBefore, len(pool.senders.senderIDs))
	require.Equal(t, addrsBefore, len(pool.senders.senderID2Addr))
	_, ok := pool.senders.getID(sender)
	require.True(t, ok)
}

func TestOnNewBlockKeepsSenderIDOfMinedSenderWithUnwoundTxn(t *testing.T) {
	ctx, pool, _, _, _ := newTestPoolWithFundedSender(t, accounts.EmptyCodeHash)
	sender := common.Address{0x30}

	mined := newTestTxnSlot(0, 0, 1, 2, 100_000)
	mined.IDHash[0] = 0x30
	var minedTxns TxnSlots
	minedTxns.Append(mined, sender[:], false)
	unwound := newTestTxnSlot(1, 0, 1, 2, 100_000)
	unwound.IDHash[0] = 0x31
	var unwindTxns TxnSlots
	unwindTxns.Append(unwound, sender[:], false)
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 1, sender), unwindTxns, TxnSlots{}, minedTxns))

	pool.lock.Lock()
	defer pool.lock.Unlock()
	require.Contains(t, pool.byHash, string(unwound.IDHash[:]))
	senderID, ok := pool.senders.getID(sender)
	require.True(t, ok)
	require.Equal(t, senderID, pool.byHash[string(unwound.IDHash[:])].TxnSlot.SenderID)
}
