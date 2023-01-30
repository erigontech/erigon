package engineapi

import (
	"sync"
	"sync/atomic"

	"github.com/emirpasic/gods/maps/treemap"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"

	"github.com/ledgerwatch/erigon/core/types"
)

// This is the status of a newly execute block.
// Hash: Block hash
// Status: block's status
type PayloadStatus struct {
	Status          remote.EngineStatus
	LatestValidHash libcommon.Hash
	ValidationError error
	CriticalError   error
}

// The message we are going to send to the stage sync in ForkchoiceUpdated
type ForkChoiceMessage struct {
	HeadBlockHash      libcommon.Hash
	SafeBlockHash      libcommon.Hash
	FinalizedBlockHash libcommon.Hash
}

type RequestStatus int

const ( // RequestStatus values
	New = iota
	DataWasMissing
)

type RequestWithStatus struct {
	Message interface{} // *Block or *ForkChoiceMessage
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
	waiterMtx sync.Mutex
	waiters   []chan struct{}
}

func NewRequestList() *RequestList {
	rl := &RequestList{
		requests:  treemap.NewWithIntComparator(),
		syncCond:  sync.NewCond(&sync.Mutex{}),
		waiterMtx: sync.Mutex{},
		waiters:   make([]chan struct{}, 0),
	}
	return rl
}

func (rl *RequestList) AddPayloadRequest(message *types.Block) {
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

func (rl *RequestList) WaitForRequest(onlyNew bool, noWait bool) (interrupt Interrupt, id int, request *RequestWithStatus) {
	rl.syncCond.L.Lock()
	defer rl.syncCond.L.Unlock()

	rl.updateWaiting(1)
	defer rl.updateWaiting(0)

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
		if request != nil || noWait {
			return
		}
		rl.syncCond.Wait()
	}
}

func (rl *RequestList) IsWaiting() bool {
	return atomic.LoadUint32(&rl.waiting) != 0
}

func (rl *RequestList) updateWaiting(val uint32) {
	rl.waiterMtx.Lock()
	defer rl.waiterMtx.Unlock()
	atomic.StoreUint32(&rl.waiting, val)

	if val == 1 {
		// something might be waiting to be notified of the waiting state being ready
		for _, c := range rl.waiters {
			c <- struct{}{}
		}
		rl.waiters = make([]chan struct{}, 0)
	}
}

func (rl *RequestList) WaitForWaiting(c chan struct{}) {
	rl.waiterMtx.Lock()
	defer rl.waiterMtx.Unlock()
	val := atomic.LoadUint32(&rl.waiting)
	if val == 1 {
		// we are already waiting so just send to the channel and quit
		c <- struct{}{}
	} else {
		// we need to register a waiter now to be notified when we are ready
		rl.waiters = append(rl.waiters, c)
	}
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
