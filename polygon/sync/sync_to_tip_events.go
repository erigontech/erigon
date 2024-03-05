package sync

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
)

const EventTypeNewHeader = "new-header"
const EventTypeNewHeaderHashes = "new-header-hashes"
const EventTypeMilestone = "milestone"
const EventTypeNewSpan = "new-span"

type Event struct {
	Type string

	// EventTypeNewHeader
	NewHeader *types.Header
	PeerId    p2p.PeerId

	// EventTypeNewHeaderHashes
	NewHeaderHashes eth.NewBlockHashesPacket

	// EventTypeMilestone
	Milestone *heimdall.Milestone

	// EventTypeNewSpan
	NewSpan *heimdall.Span
}

type SyncToTipEvents struct {
	events    chan Event
	pollDelay time.Duration

	queue      *list.List
	queueCap   int
	queueMutex sync.Mutex

	p2pService      p2p.Service
	heimdallService heimdall.HeimdallNoStore
}

func NewSyncToTipEvents(
	p2pService p2p.Service,
	heimdallService heimdall.HeimdallNoStore,
) *SyncToTipEvents {
	return &SyncToTipEvents{
		events:    make(chan Event),
		pollDelay: time.Second,

		queue:    list.New(),
		queueCap: 1000, // more than 3 milestones

		p2pService:      p2pService,
		heimdallService: heimdallService,
	}
}

func (se *SyncToTipEvents) Events() chan Event {
	return se.events
}

func (se *SyncToTipEvents) pushEvent(e Event) {
	se.queueMutex.Lock()
	defer se.queueMutex.Unlock()

	if se.queue.Len() == se.queueCap {
		se.queue.Remove(se.queue.Front())
	}

	se.queue.PushBack(e)
}

func (se *SyncToTipEvents) takeEvent() (Event, bool) {
	se.queueMutex.Lock()
	defer se.queueMutex.Unlock()

	if elem := se.queue.Front(); elem != nil {
		e := se.queue.Remove(elem).(Event)
		return e, true
	} else {
		return Event{}, false
	}
}

func (se *SyncToTipEvents) Run(ctx context.Context) error {
	newBlockObserverCancel := se.p2pService.GetMessageListener().RegisterNewBlockObserver(func(message *p2p.DecodedInboundMessage[*eth.NewBlockPacket]) {
		if message.Decoded == nil {
			return
		}
		block := message.Decoded.Block
		se.pushEvent(Event{
			Type:      EventTypeNewHeader,
			NewHeader: block.Header(),
			PeerId:    message.PeerId,
		})
	})
	defer newBlockObserverCancel()

	newBlockHashesObserverCancel := se.p2pService.GetMessageListener().RegisterNewBlockHashesObserver(func(message *p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]) {
		if message.Decoded == nil {
			return
		}
		se.pushEvent(Event{
			Type:            EventTypeNewHeaderHashes,
			NewHeaderHashes: *message.Decoded,
			PeerId:          message.PeerId,
		})
	})
	defer newBlockHashesObserverCancel()

	err := se.heimdallService.OnMilestoneEvent(ctx, func(milestone *heimdall.Milestone) {
		se.pushEvent(Event{
			Type:      EventTypeMilestone,
			Milestone: milestone,
		})
	})
	if err != nil {
		return err
	}

	err = se.heimdallService.OnSpanEvent(ctx, func(span *heimdall.Span) {
		se.pushEvent(Event{
			Type:    EventTypeNewSpan,
			NewSpan: span,
		})
	})
	if err != nil {
		return err
	}

	// pump events from the ring buffer to the events channel
	for {
		e, ok := se.takeEvent()
		if !ok {
			pollDelayTimer := time.NewTimer(se.pollDelay)
			select {
			case <-pollDelayTimer.C:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		select {
		case se.events <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
