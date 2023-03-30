package engineapi

import (
	"context"
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
	waiting   atomic.Uint32
	syncCond  *sync.Cond

	// waiterMtx is used to place locks around `waiters` so the slice can be read/modified safely
	// and also serves as a lock around the `waiting` field so that this can't be modified whilst it
	// is being read.  waiters is modified as part of updating waiter so we need to ensure these
	// two things lock at the same time
	waiterMtx sync.Mutex

	waiters []chan struct{}
}

func NewRequestList() *RequestList {
	rl := &RequestList{
		requests:  treemap.NewWithIntComparator(),
		syncCond:  sync.NewCond(&sync.Mutex{}),
		waiterMtx: sync.Mutex{},
		waiters:   make([]chan struct{}, 0),
		waiting:   atomic.Uint32{},
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

	rl.waiterMtx.Lock()
	rl.updateWaiting(1)
	rl.waiterMtx.Unlock()
	defer func() {
		rl.waiterMtx.Lock()
		rl.updateWaiting(0)
		rl.waiterMtx.Unlock()
	}()

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
	return rl.waiting.Load() != 0
}

// update waiting should always be called from a locked context using rl.waiterMtx as it
// updates rl.waiters in certain scenarios
func (rl *RequestList) updateWaiting(val uint32) {
	rl.waiting.Store(val)

	if val == 1 {
		for i, c := range rl.waiters {
			close(c)
			rl.waiters[i] = nil
		}
		rl.waiters = rl.waiters[:0]
	}
}

func (rl *RequestList) WaitForWaiting(ctx context.Context) (chan struct{}, bool) {
	rl.waiterMtx.Lock()
	defer rl.waiterMtx.Unlock()

	isWaiting := rl.waiting.Load()

	if isWaiting == 1 {
		// we are already waiting so just return
		return nil, true
	} else {
		c := make(chan struct{}, 1)
		rl.waiters = append(rl.waiters, c)
		return c, false
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
