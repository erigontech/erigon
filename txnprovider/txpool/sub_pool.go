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

	"github.com/erigontech/erigon-lib/log/v3"
)

type SubPoolType uint8

const PendingSubPool SubPoolType = 1
const BaseFeeSubPool SubPoolType = 2
const QueuedSubPool SubPoolType = 3

func (sp SubPoolType) String() string {
	switch sp {
	case PendingSubPool:
		return "Pending"
	case BaseFeeSubPool:
		return "BaseFee"
	case QueuedSubPool:
		return "Queued"
	}
	return fmt.Sprintf("Unknown:%d", sp)
}

// SubPoolMarker is an ordered bitset of five bits that's used to sort transactions into sub-pools. Bits meaning:
// 1. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for the sender is M, and there are transactions for all nonces between M and N from the same sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
// 2. Sufficient balance for gas. Set to 1 if the balance of sender's account in the state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the sum of feeCap x gasLimit + transferred_value of all transactions from this sender with nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is set if there is currently a guarantee that the transaction and all its required prior transactions will be able to pay for gas.
// 3. Not too much gas: Set to 1 if the transaction doesn't use too much gas
// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than baseFee of the currently pending block. Set to 0 otherwise.
// 5. Local transaction. Set to 1 if transaction is local.
type SubPoolMarker uint8

const (
	NoNonceGaps       SubPoolMarker = 0b010000
	EnoughBalance     SubPoolMarker = 0b001000
	NotTooMuchGas     SubPoolMarker = 0b000100
	EnoughFeeCapBlock SubPoolMarker = 0b000010
	IsLocal           SubPoolMarker = 0b000001

	BaseFeePoolBits = NoNonceGaps + EnoughBalance + NotTooMuchGas
)

type SubPool struct {
	best  *BestQueue
	worst *WorstQueue
	limit int
	t     SubPoolType
}

func NewSubPool(t SubPoolType, limit int) *SubPool {
	return &SubPool{limit: limit, t: t, best: &BestQueue{}, worst: &WorstQueue{}}
}

func (p *SubPool) EnforceInvariants() {
	heap.Init(p.worst)
	heap.Init(p.best)
}

func (p *SubPool) Best() *metaTxn { //nolint
	if len(p.best.ms) == 0 {
		return nil
	}
	return p.best.ms[0]
}

func (p *SubPool) Worst() *metaTxn { //nolint
	if len(p.worst.ms) == 0 {
		return nil
	}
	return p.worst.ms[0]
}

func (p *SubPool) PopBest() *metaTxn { //nolint
	i := heap.Pop(p.best).(*metaTxn)
	heap.Remove(p.worst, i.worstIndex)
	return i
}

func (p *SubPool) PopWorst() *metaTxn { //nolint
	i := heap.Pop(p.worst).(*metaTxn)
	heap.Remove(p.best, i.bestIndex)
	return i
}

func (p *SubPool) Len() int {
	return p.best.Len()
}

func (p *SubPool) Add(i *metaTxn, reason string, logger log.Logger) {
	if i.TxnSlot.Traced {
		logger.Info(fmt.Sprintf("TX TRACING: added to subpool %s", p.t), "idHash", fmt.Sprintf("%x", i.TxnSlot.IDHash), "sender", i.TxnSlot.SenderID, "nonce", i.TxnSlot.Nonce, "reason", reason)
	}
	i.currentSubPool = p.t
	heap.Push(p.best, i)
	heap.Push(p.worst, i)
}

func (p *SubPool) Remove(i *metaTxn, reason string, logger log.Logger) {
	if i.TxnSlot.Traced {
		logger.Info(fmt.Sprintf("TX TRACING: removed from subpool %s", p.t), "idHash", fmt.Sprintf("%x", i.TxnSlot.IDHash), "sender", i.TxnSlot.SenderID, "nonce", i.TxnSlot.Nonce, "reason", reason)
	}
	heap.Remove(p.best, i.bestIndex)
	heap.Remove(p.worst, i.worstIndex)
	i.currentSubPool = 0
}

func (p *SubPool) Updated(i *metaTxn) {
	heap.Fix(p.best, i.bestIndex)
	heap.Fix(p.worst, i.worstIndex)
}

func (p *SubPool) DebugPrint(prefix string) {
	for i, it := range p.best.ms {
		fmt.Printf("%s.best: %d, %d, %d\n", prefix, i, it.subPool, it.bestIndex)
	}
	for i, it := range p.worst.ms {
		fmt.Printf("%s.worst: %d, %d, %d\n", prefix, i, it.subPool, it.worstIndex)
	}
}
