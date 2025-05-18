package state

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
	libstate "github.com/erigontech/erigon-lib/state"
)

const workersCount = 8

type CommitmentPrefetcher struct {
	ctx      context.Context
	db       kv.RwDB
	running  bool
	shutdown chan struct{}
	data     chan []byte // hashed commitment keys
}

func NewCommitmentPrefetcher(db kv.RwDB, ctx context.Context) *CommitmentPrefetcher {
	return &CommitmentPrefetcher{
		db:  db,
		ctx: ctx,
	}
}

// start n workers to prefetch commitments
func (c *CommitmentPrefetcher) Start() {
	if c.running {
		panic("CommitmentPrefetcher already started")
	}
	for i := 0; i < workersCount; i++ {
		go c.worker()
	}
	c.running = true
}

func (c *CommitmentPrefetcher) worker() {
	tx, err := c.db.BeginRo(c.ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.shutdown:
			return
		case data := <-c.data:
			warmimngDepth := 8
			for i := 0; i < warmimngDepth; i++ {
				if i >= len(data) {
					break
				}
				tx.(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).GetLatest(kv.CommitmentDomain, data[:len(data)-i], tx)
			}
		}
	}
}

func (c *CommitmentPrefetcher) Stop() {
	if !c.running {
		panic("CommitmentPrefetcher not started")
	}
	c.shutdown <- struct{}{}
	c.running = false
}
