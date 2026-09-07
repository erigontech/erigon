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

	pool.AddRemoteTxns(ctx, slots, gointerfaces.ConvertHashToH512([64]byte{0x41}), nil)
	require.NoError(t, pool.processRemoteTxns(ctx))

	pool.lock.Lock()
	defer pool.lock.Unlock()
	for i := range senders {
		var addr [20]byte
		binary.BigEndian.PutUint64(addr[12:], uint64(i+1))
		id, ok := pool.senders.senderIDs[addr]
		require.False(t, ok,
			"sender id for a txn that reached no sub-pool must not be retained, got %d for %x", id, addr)
		require.NotContains(t, pool.senders.senderID2Addr, id)
	}
}
