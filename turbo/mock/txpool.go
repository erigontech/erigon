// Copyright 2024 The Erigon Authors
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

package mock

import (
	"sort"
	"sync"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/event"
)

// TestTxPool is a mock transaction pool that blindly accepts all transactions.
// Its goal is to get around setting up a valid statedb for the balance and nonce
// checks.
type TestTxPool struct {
	pool map[libcommon.Hash]types.Transaction // Hash map of collected transactions

	txFeed event.Feed   // Notification feed to allow waiting for inclusion
	lock   sync.RWMutex // Protects the transaction pool
}

// NewTestTxPool creates a mock transaction pool.
func NewTestTxPool() *TestTxPool {
	return &TestTxPool{
		pool: make(map[libcommon.Hash]types.Transaction),
	}
}

// Has returns an indicator whether txpool has a transaction
// cached with the given hash.
func (p *TestTxPool) Has(hash libcommon.Hash) bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.pool[hash] != nil
}

// Get retrieves the transaction from local txpool with given
// txn hash.
func (p *TestTxPool) Get(hash libcommon.Hash) types.Transaction {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.pool[hash]
}

func (p *TestTxPool) add(txs []types.Transaction) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, txn := range txs {
		p.pool[txn.Hash()] = txn
	}
	p.txFeed.Send(core.NewTxsEvent{Txs: txs})
}

// AddRemotes appends a batch of transactions to the pool, and notifies any
// listeners if the addition channel is non nil
func (p *TestTxPool) AddRemotes(txs []types.Transaction) []error {
	p.add(txs)
	return make([]error, len(txs))
}

func (p *TestTxPool) AddLocals(txs []types.Transaction) []error {
	p.add(txs)
	return make([]error, len(txs))
}

// Pending returns all the transactions known to the pool
func (p *TestTxPool) Pending() (types.TransactionsGroupedBySender, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	batches := make(map[libcommon.Address]types.Transactions)
	for _, txn := range p.pool {
		from, _ := txn.Sender(*types.LatestSignerForChainID(nil))
		batches[from] = append(batches[from], txn)
	}
	groups := types.TransactionsGroupedBySender{}
	for _, batch := range batches {
		sort.Sort(types.TxByNonce(batch))
		groups = append(groups, batch)
	}
	return groups, nil
}

// Content returns all the transactions known to the pool
func (p *TestTxPool) Content() (map[libcommon.Address]types.Transactions, map[libcommon.Address]types.Transactions) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	batches := make(map[libcommon.Address]types.Transactions)
	for _, txn := range p.pool {
		from, _ := txn.Sender(*types.LatestSignerForChainID(nil))
		batches[from] = append(batches[from], txn)
	}
	return batches, nil
}

// CountContent returns the number of pending and queued transactions
// in the transaction pool.
func (p *TestTxPool) CountContent() (pending uint, queued uint) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	pending = uint(len(p.pool))
	return
}

// SubscribeNewTxsEvent should return an event subscription of NewTxsEvent and
// send events to the given channel.
func (p *TestTxPool) SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription {
	return p.txFeed.Subscribe(ch)
}
