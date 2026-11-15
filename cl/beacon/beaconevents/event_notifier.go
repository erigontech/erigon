package beaconevents

import (
	"maps"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
)

// Sized for every column of two blocks.
const dataColumnStoredBufferSize = 256

// Matches the size of the fork-choice queue of envelopes that wait for columns.
const pendingEnvelopeRetryRootsCap = 128

type EventEmitter struct {
	stateFeed                    *stateFeed     // block state feed
	operationFeed                *operationFeed // block operation feed
	headEventMu                  sync.Mutex
	payloadAttributesSubscribers atomic.Int64
	blockIndicesMu               sync.Mutex
	blockIndicesChanged          chan struct{}
	dataColumnStored             chan common.Hash
	envelopeRetryMu              sync.Mutex
	envelopeRetryRoots           map[common.Hash]uint64 // number of the last report of each root
	envelopeRetryReports         uint64
	envelopeRetryWake            chan struct{}
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

func NewEventEmitter() *EventEmitter {
	return &EventEmitter{
		operationFeed:      newOpFeed(),
		stateFeed:          newStateFeed(),
		dataColumnStored:   make(chan common.Hash, dataColumnStoredBufferSize),
		envelopeRetryRoots: make(map[common.Hash]uint64),
		envelopeRetryWake:  make(chan struct{}, 1),
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
	e.WakePendingEnvelopeRetry(blockRoot)
}

// WakePendingEnvelopeRetry marks blockRoot as worth a retry and signals the waiting retry loop. A full set evicts the
// root reported longest ago. The signal and the root set are hints: an evicted root is still found by the loop's own
// poll.
func (e *EventEmitter) WakePendingEnvelopeRetry(blockRoot common.Hash) {
	if e == nil {
		return
	}
	e.envelopeRetryMu.Lock()
	if _, marked := e.envelopeRetryRoots[blockRoot]; !marked && len(e.envelopeRetryRoots) >= pendingEnvelopeRetryRootsCap {
		delete(e.envelopeRetryRoots, e.oldestEnvelopeRetryRoot())
	}
	e.envelopeRetryReports++
	e.envelopeRetryRoots[blockRoot] = e.envelopeRetryReports
	e.envelopeRetryMu.Unlock()
	e.RearmPendingEnvelopeRetryWake()
}

func (e *EventEmitter) oldestEnvelopeRetryRoot() (oldest common.Hash) {
	lowest := uint64(math.MaxUint64)
	for root, report := range e.envelopeRetryRoots {
		if report < lowest {
			oldest, lowest = root, report
		}
	}
	return oldest
}

func (e *EventEmitter) PendingEnvelopeRetryWake() <-chan struct{} {
	if e == nil {
		return nil
	}
	return e.envelopeRetryWake
}

// TakePendingEnvelopeRetryRoots returns the roots marked since the last call and empties the set.
func (e *EventEmitter) TakePendingEnvelopeRetryRoots() []common.Hash {
	if e == nil {
		return nil
	}
	e.envelopeRetryMu.Lock()
	defer e.envelopeRetryMu.Unlock()
	roots := slices.Collect(maps.Keys(e.envelopeRetryRoots))
	clear(e.envelopeRetryRoots)
	return roots
}

// PrunePendingEnvelopeRetryRoots drops the marked roots for which keep is false and reports whether keep held for any.
// keep runs outside the lock, and a root reported again meanwhile stays marked.
func (e *EventEmitter) PrunePendingEnvelopeRetryRoots(keep func(common.Hash) bool) bool {
	if e == nil {
		return false
	}
	e.envelopeRetryMu.Lock()
	checked := maps.Clone(e.envelopeRetryRoots)
	e.envelopeRetryMu.Unlock()

	kept := false
	var dropped []common.Hash
	for root := range checked {
		if keep(root) {
			kept = true
		} else {
			dropped = append(dropped, root)
		}
	}

	e.envelopeRetryMu.Lock()
	defer e.envelopeRetryMu.Unlock()
	for _, root := range dropped {
		if e.envelopeRetryRoots[root] == checked[root] {
			delete(e.envelopeRetryRoots, root)
		}
	}
	return kept
}

// RearmPendingEnvelopeRetryWake signals the wake again, for a waiter that took the signal but did not take the roots.
func (e *EventEmitter) RearmPendingEnvelopeRetryWake() {
	if e == nil {
		return
	}
	select {
	case e.envelopeRetryWake <- struct{}{}:
	default:
	}
}

func (e *EventEmitter) State() *stateFeed {
	return e.stateFeed
}

func (e *EventEmitter) Operation() *operationFeed {
	return e.operationFeed
}
