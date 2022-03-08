package engineapi

import (
	"sync"

	"github.com/emirpasic/gods/maps/treemap"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

// The message we are going to send to the stage sync in NewPayload
type PayloadMessage struct {
	Header *types.Header
	Body   *types.RawBody
}

// The message we are going to send to the stage sync in ForkchoiceUpdated
type ForkChoiceMessage struct {
	HeadBlockHash      common.Hash
	SafeBlockHash      common.Hash
	FinalizedBlockHash common.Hash
}

type RequestList struct {
	requestId int
	requests  *treemap.Map
	interrupt bool
	syncCond  *sync.Cond
}

func NewRequestList() *RequestList {
	rl := &RequestList{
		requests: treemap.NewWithIntComparator(),
		syncCond: sync.NewCond(&sync.Mutex{}),
	}
	return rl
}

func (rl *RequestList) AddPayloadRequest(message *PayloadMessage) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	rl.requests.Put(rl.requestId, message)
	rl.requestId++

	rl.syncCond.Broadcast()
}

func (rl *RequestList) AddForkChoiceRequest(message *ForkChoiceMessage) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	rl.requests.Put(rl.requestId, message)
	rl.requestId++

	rl.syncCond.Broadcast()
}

func (rl *RequestList) WaitForRequest() (interrupted bool, id int, request interface{}) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	for !rl.interrupt && rl.requests.Empty() {
		rl.syncCond.Wait()
	}

	if rl.interrupt {
		rl.interrupt = false
		return true, 0, nil
	}

	key, value := rl.requests.Min()
	return false, key.(int), value
}

func (rl *RequestList) Interrupt() {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	rl.interrupt = true

	rl.syncCond.Broadcast()
}

func (rl *RequestList) Remove(id int) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	rl.requests.Remove(id)
}
