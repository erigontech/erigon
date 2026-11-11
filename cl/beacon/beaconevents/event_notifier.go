package beaconevents

import (
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
)

// Sized for every column of two blocks.
const dataColumnStoredBufferSize = 256

type EventEmitter struct {
	stateFeed                    *stateFeed     // block state feed
	operationFeed                *operationFeed // block operation feed
	headEventMu                  sync.Mutex
	lastHeadRoot                 common.Hash
	lastHeadV2Root               common.Hash
	lastHeadV2PayloadStatus      string
	hasHeadEvent                 bool
	hasHeadV2Event               bool
	payloadAttributesSubscribers atomic.Int64
	blockIndicesMu               sync.Mutex
	blockIndicesChanged          chan struct{}
	dataColumnStored             chan common.Hash
}

// BlockIndicesChanged returns a channel that closes when an import commits block
// indices. Capture it before reading so a commit between checking and waiting
// cannot be missed.
func (e *EventEmitter) BlockIndicesChanged() <-chan struct{} {
	if e == nil {
		return nil
	}
	e.blockIndicesMu.Lock()
	defer e.blockIndicesMu.Unlock()
	if e.blockIndicesChanged == nil {
		e.blockIndicesChanged = make(chan struct{})
	}
	return e.blockIndicesChanged
}

// NotifyBlockIndicesChanged must be called after the index transaction commits.
func (e *EventEmitter) NotifyBlockIndicesChanged() {
	if e == nil {
		return
	}
	e.blockIndicesMu.Lock()
	defer e.blockIndicesMu.Unlock()
	if e.blockIndicesChanged != nil {
		close(e.blockIndicesChanged)
		e.blockIndicesChanged = nil
	}
}

func (e *EventEmitter) TrackPayloadAttributesSubscriber() func() {
	e.payloadAttributesSubscribers.Add(1)
	return func() { e.payloadAttributesSubscribers.Add(-1) }
}

func (e *EventEmitter) HasPayloadAttributesSubscribers() bool {
	return e.payloadAttributesSubscribers.Load() > 0
}

func (e *EventEmitter) WithHeadEventLock(fn func()) {
	e.headEventMu.Lock()
	defer e.headEventMu.Unlock()
	fn()
}

func (e *EventEmitter) EmitHeadEventsIfCurrent(head *HeadData, headV2 *HeadV2Data, current func() bool) bool {
	e.headEventMu.Lock()
	defer e.headEventMu.Unlock()
	if !current() {
		return false
	}
	if !e.hasHeadEvent || e.lastHeadRoot != head.Block {
		e.stateFeed.TrySendHead(head)
		e.lastHeadRoot = head.Block
		e.hasHeadEvent = true
	}
	e.sendHeadV2IfChanged(headV2)
	return true
}

func (e *EventEmitter) EmitHeadV2IfCurrent(headV2 *HeadV2Data, current func() bool) bool {
	e.headEventMu.Lock()
	defer e.headEventMu.Unlock()
	if !current() {
		return false
	}
	e.sendHeadV2IfChanged(headV2)
	return true
}

func (e *EventEmitter) sendHeadV2IfChanged(headV2 *HeadV2Data) {
	if !e.hasHeadV2Event || e.lastHeadV2Root != headV2.Data.Block || e.lastHeadV2PayloadStatus != headV2.Data.PayloadStatus {
		e.stateFeed.SendHeadV2(headV2)
		e.lastHeadV2Root = headV2.Data.Block
		e.lastHeadV2PayloadStatus = headV2.Data.PayloadStatus
		e.hasHeadV2Event = true
	}
}

func NewEventEmitter() *EventEmitter {
	return &EventEmitter{
		operationFeed:    newOpFeed(),
		stateFeed:        newStateFeed(),
		dataColumnStored: make(chan common.Hash, dataColumnStoredBufferSize),
	}
}

func (e *EventEmitter) DataColumnStored() <-chan common.Hash {
	if e == nil {
		return nil
	}
	return e.dataColumnStored
}

func (e *EventEmitter) NotifyDataColumnStored(blockRoot common.Hash) {
	if e == nil {
		return
	}
	select {
	case e.dataColumnStored <- blockRoot:
	default:
	}
}

func (e *EventEmitter) State() *stateFeed {
	return e.stateFeed
}

func (e *EventEmitter) Operation() *operationFeed {
	return e.operationFeed
}
