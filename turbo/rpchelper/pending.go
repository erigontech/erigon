package rpchelper

import (
	"context"
	"sync"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type Pending struct {
	sync.RWMutex
	block *types.Block
	logs  types.Logs
}

func NewPending(ctx context.Context, filters *filters.Filters) *Pending {
	pending := &Pending{}
	filters.SubscribePendingLogs(ctx, func(l types.Logs) {
		pending.Lock()
		pending.logs = l
		pending.Unlock()
	})
	filters.SubscribePendingBlock(ctx, func(block *types.Block) {
		pending.Lock()
		pending.block = block
		pending.Unlock()
	})
	return pending
}

func (p *Pending) Block() *types.Block {
	p.RLock()
	defer p.RUnlock()
	return p.block
}

func (p *Pending) Logs() types.Logs {
	p.RLock()
	defer p.RUnlock()
	return p.logs
}
