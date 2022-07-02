package engineapi

import (
	"sync"
	"sync/atomic"

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

type Interrupt int

const ( // Interrupt values
	None = iota
	Synced
	Stopping
)

type RequestList struct {
	requestId int
	requests  *treemap.Map // map[int]*RequestWithStatus
	interrupt Interrupt
	waiting   uint32
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

	// purge previous fork choices that are still syncing
	rl.requests = rl.requests.Select(func(key interface{}, value interface{}) bool {
		req := value.(*RequestWithStatus)
		_, isForkChoice := req.Message.(*ForkChoiceMessage)
		return req.Status == New || !isForkChoice
	})
	// TODO(yperbasis): potentially purge some non-syncing old fork choices?

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

func (rl *RequestList) WaitForRequest(onlyNew bool) (interrupt Interrupt, id int, request *RequestWithStatus) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	atomic.StoreUint32(&rl.waiting, 1)
	defer atomic.StoreUint32(&rl.waiting, 0)

	for {
		interrupt = rl.interrupt
		if interrupt != None {
			if interrupt != Stopping {
				// clear the interrupt
				rl.interrupt = None
			}
			return
		}
		id, request = rl.firstRequest(onlyNew)
		if request != nil {
			return
		}
		rl.syncCond.Wait()
	}
}

func (rl *RequestList) IsWaiting() bool {
	return atomic.LoadUint32(&rl.waiting) != 0
}

func (rl *RequestList) Interrupt(kind Interrupt) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	rl.interrupt = kind

	rl.syncCond.Broadcast()
}

func (rl *RequestList) Remove(id int) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	rl.requests.Remove(id)
	// no need to broadcast
}

func (rl *RequestList) SetStatus(id int, status RequestStatus) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	value, found := rl.requests.Get(id)
	if found {
		value.(*RequestWithStatus).Status = status
	}

	rl.syncCond.Broadcast()
}
