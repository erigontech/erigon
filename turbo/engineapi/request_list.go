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

type RequestStatus int

const ( // RequestStatus values
	New = iota
	DataWasMissing
)

type RequestWithStatus struct {
	Message interface{} // *PayloadMessage or *ForkChoiceMessage
	Status  RequestStatus
}

type RequestList struct {
	requestId int
	requests  *treemap.Map // map[int]*RequestWithStatus
	interrupt bool         // TODO(yperbasis): interruption type (Sync Finished, Skip Cycle, Stopped)
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

	rl.requestId++

	rl.requests.Put(rl.requestId, &RequestWithStatus{
		Message: message,
		Status:  New,
	})

	rl.syncCond.Broadcast()
}

func (rl *RequestList) AddForkChoiceRequest(message *ForkChoiceMessage) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	rl.requestId++

	/*
		// purge previous fork choices that are still syncing
		rl.requests = rl.requests.Select(func(key interface{}, value interface{}) bool {
			req := value.(*RequestWithStatus)
			_, isForkChoice := req.Message.(*ForkChoiceMessage)
			return req.Status != Syncing || !isForkChoice
		})
		// TODO(yperbasis): potentially purge some non-syncing old fork choices?
	*/

	rl.requests.Put(rl.requestId, &RequestWithStatus{
		Message: message,
		Status:  New,
	})

	rl.syncCond.Broadcast()
}

func (rl *RequestList) firstRequest(onlyNew bool) (id int, request *RequestWithStatus) {
	foundKey, foundValue := rl.requests.Min()
	if onlyNew {
		foundKey, foundValue = rl.requests.Find(func(key interface{}, value interface{}) bool {
			return value.(*RequestWithStatus).Status == New
		})
	}
	if foundKey != nil {
		return foundKey.(int), foundValue.(*RequestWithStatus)
	}
	return 0, nil
}

func (rl *RequestList) WaitForRequest(onlyNew bool) (interrupted bool, id int, request *RequestWithStatus) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	for {
		interrupted = rl.interrupt
		if interrupted {
			rl.interrupt = false
			return
		}
		id, request = rl.firstRequest(onlyNew)
		if request != nil {
			return
		}
		rl.syncCond.Wait()
	}
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

func (rl *RequestList) SetStatus(id int, status RequestStatus) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	value, found := rl.requests.Get(id)
	if found {
		value.(*RequestWithStatus).Status = status
	}
}
