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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces"
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
