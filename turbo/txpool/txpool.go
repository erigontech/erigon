package txpool

import (
	"context"
	"sort"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/event"
)

// TestTxPool is a mock transaction pool that blindly accepts all transactions.
// Its goal is to get around setting up a valid statedb for the balance and nonce
// checks.
type TestTxPool struct {
	pool map[common.Hash]*types.Transaction // Hash map of collected transactions

	txFeed         event.Feed   // Notification feed to allow waiting for inclusion
	lock           sync.RWMutex // Protects the transaction pool
	FailAddRemotes func(txs []*types.Transaction) []error
}

// NewTestTxPool creates a mock transaction pool.
func NewTestTxPool() (*ClientDirect, *Server, *TestTxPool) {
	deprecated := &TestTxPool{
		pool: make(map[common.Hash]*types.Transaction),
	}
	server := NewServer(context.Background(), deprecated)
	return NewClientDirect(server), server, deprecated
}

// Has returns an indicator whether txpool has a transaction
// cached with the given hash.
func (p *TestTxPool) Has(hash common.Hash) bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.pool[hash] != nil
}

// Get retrieves the transaction from local txpool with given
// tx hash.
func (p *TestTxPool) Get(hash common.Hash) *types.Transaction {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.pool[hash]
}

// AddRemotes appends a batch of transactions to the pool, and notifies any
// listeners if the addition channel is non nil
func (p *TestTxPool) AddRemotes(txs []*types.Transaction) []error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.FailAddRemotes != nil {
		errs := p.FailAddRemotes(txs)
		anounce := types.Transactions{}
		for i := range errs {
			if errs[i] == nil {
				anounce = append(anounce, txs[i])
			}
		}
		if len(anounce) > 0 {
			p.txFeed.Send(core.NewTxsEvent{Txs: txs})
		}
		return errs
	}

	for _, tx := range txs {
		p.pool[tx.Hash()] = tx
	}
	p.txFeed.Send(core.NewTxsEvent{Txs: txs})
	return make([]error, len(txs))
}

// Pending returns all the transactions known to the pool
func (p *TestTxPool) Pending() (types.TransactionsGroupedBySender, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	batches := make(map[common.Address]types.Transactions)
	for _, tx := range p.pool {
		from, _ := types.Sender(types.HomesteadSigner{}, tx)
		batches[from] = append(batches[from], tx)
	}
	groups := types.TransactionsGroupedBySender{}
	for _, batch := range batches {
		sort.Sort(types.TxByNonce(batch))
		groups = append(groups, batch)
	}
	return groups, nil
}

// SubscribeNewTxsEvent should return an event subscription of NewTxsEvent and
// send events to the given channel.
func (p *TestTxPool) SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription {
	return p.txFeed.Subscribe(ch)
}
