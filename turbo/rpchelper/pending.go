package rpchelper

import (
	"sync"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type Pending struct {
	sync.RWMutex
	block *types.Block
	logs  types.Logs
}

func NewPending(filters *filters.Filters, quit <-chan struct{}) *Pending {
	pending := &Pending{}
	go func() {
		if filters == nil {
			return
		}

		logs := make(chan types.Logs)
		defer close(logs)
		logsId := filters.SubscribePendingLogs(logs)
		defer filters.UnsubscribePendingLogs(logsId)

		blocks := make(chan *types.Block)
		defer close(blocks)
		blocksId := filters.SubscribePendingBlock(blocks)
		defer filters.UnsubscribePendingBlock(blocksId)

		for {
			select {
			case l := <-logs:
				pending.Lock()
				pending.logs = l
				pending.Unlock()
			case block := <-blocks:
				pending.Lock()
				pending.block = block
				pending.Unlock()
			case <-quit:
				return
			}
		}
	}()
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
