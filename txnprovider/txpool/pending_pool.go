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

package txpool

import (
	"container/heap"
	"fmt"
	"sort"

	"github.com/erigontech/erigon-lib/log/v3"
)

// PendingPool - is different from other pools - it's best is Slice instead of Heap
// It's more expensive to maintain "slice sort" invariant, but it allow do cheap copy of
// pending.best slice for mining (because we consider txns and metaTxn are immutable)
type PendingPool struct {
	best  *bestSlice
	worst *WorstQueue
	limit int
	t     SubPoolType
}

func NewPendingSubPool(t SubPoolType, limit int) *PendingPool {
	return &PendingPool{limit: limit, t: t, best: &bestSlice{ms: []*metaTxn{}}, worst: &WorstQueue{ms: []*metaTxn{}}}
}

func (p *PendingPool) EnforceWorstInvariants() {
	heap.Init(p.worst)
}
func (p *PendingPool) EnforceBestInvariants() {
	sort.Sort(p.best)
}

func (p *PendingPool) Best() *metaTxn { //nolint
	if len(p.best.ms) == 0 {
		return nil
	}
	return p.best.ms[0]
}

func (p *PendingPool) Worst() *metaTxn { //nolint
	if len(p.worst.ms) == 0 {
		return nil
	}
	return (p.worst.ms)[0]
}

func (p *PendingPool) PopWorst() *metaTxn { //nolint
	i := heap.Pop(p.worst).(*metaTxn)
	if i.bestIndex >= 0 {
		p.best.UnsafeRemove(i)
	}
	return i
}

func (p *PendingPool) Updated(mt *metaTxn) {
	heap.Fix(p.worst, mt.worstIndex)
}

func (p *PendingPool) Len() int {
	return len(p.best.ms)
}

func (p *PendingPool) Remove(i *metaTxn, reason string, logger log.Logger) {
	if i.TxnSlot.Traced {
		logger.Info(fmt.Sprintf("TX TRACING: removed from subpool %s", p.t), "idHash", fmt.Sprintf("%x", i.TxnSlot.IDHash), "sender", i.TxnSlot.SenderID, "nonce", i.TxnSlot.Nonce, "reason", reason)
	}
	if i.worstIndex >= 0 {
		heap.Remove(p.worst, i.worstIndex)
	}
	if i.bestIndex >= 0 {
		p.best.UnsafeRemove(i)
	}
	i.currentSubPool = 0
}

func (p *PendingPool) Add(i *metaTxn, logger log.Logger) {
	if i.TxnSlot.Traced {
		logger.Info(fmt.Sprintf("TX TRACING: added to subpool %s, IdHash=%x, sender=%d, nonce=%d", p.t, i.TxnSlot.IDHash, i.TxnSlot.SenderID, i.TxnSlot.Nonce))
	}
	i.currentSubPool = p.t
	heap.Push(p.worst, i)
	p.best.UnsafeAdd(i)
}

func (p *PendingPool) DebugPrint(prefix string) {
	for i, it := range p.best.ms {
		fmt.Printf("%s.best: %d, %d, %d,%d\n", prefix, i, it.subPool, it.bestIndex, it.TxnSlot.Nonce)
	}
	for i, it := range p.worst.ms {
		fmt.Printf("%s.worst: %d, %d, %d,%d\n", prefix, i, it.subPool, it.worstIndex, it.TxnSlot.Nonce)
	}
}
