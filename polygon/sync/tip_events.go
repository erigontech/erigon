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

type TipEvents struct {
	events    chan Event
	pollDelay time.Duration

	queue      *list.List
	queueCap   int
	queueMutex sync.Mutex

	p2pService      p2p.Service
	heimdallService heimdall.HeimdallNoStore
}

func NewTipEvents(
	p2pService p2p.Service,
	heimdallService heimdall.HeimdallNoStore,
) *TipEvents {
	return &TipEvents{
		events:    make(chan Event),
		pollDelay: time.Second,

		queue:    list.New(),
		queueCap: 1000, // more than 3 milestones

		p2pService:      p2pService,
		heimdallService: heimdallService,
	}
}

func (te *TipEvents) Events() chan Event {
	return te.events
}

func (te *TipEvents) pushEvent(e Event) {
	te.queueMutex.Lock()
	defer te.queueMutex.Unlock()

	if te.queue.Len() == te.queueCap {
		te.queue.Remove(te.queue.Front())
	}

	te.queue.PushBack(e)
}

func (te *TipEvents) takeEvent() (Event, bool) {
	te.queueMutex.Lock()
	defer te.queueMutex.Unlock()

	if elem := te.queue.Front(); elem != nil {
		e := te.queue.Remove(elem).(Event)
		return e, true
	} else {
		return Event{}, false
	}
}

func (te *TipEvents) Run(ctx context.Context) error {
	newBlockObserverCancel := te.p2pService.GetMessageListener().RegisterNewBlockObserver(func(message *p2p.DecodedInboundMessage[*eth.NewBlockPacket]) {
		if message.Decoded == nil {
			return
		}
		block := message.Decoded.Block
		te.pushEvent(Event{
			Type:      EventTypeNewHeader,
			NewHeader: block.Header(),
			PeerId:    message.PeerId,
		})
	})
	defer newBlockObserverCancel()

	newBlockHashesObserverCancel := te.p2pService.GetMessageListener().RegisterNewBlockHashesObserver(func(message *p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]) {
		if message.Decoded == nil {
			return
		}
		te.pushEvent(Event{
			Type:            EventTypeNewHeaderHashes,
			NewHeaderHashes: *message.Decoded,
			PeerId:          message.PeerId,
		})
	})
	defer newBlockHashesObserverCancel()

	err := te.heimdallService.OnMilestoneEvent(ctx, func(milestone *heimdall.Milestone) {
		te.pushEvent(Event{
			Type:      EventTypeMilestone,
			Milestone: milestone,
		})
	})
	if err != nil {
		return err
	}

	err = te.heimdallService.OnSpanEvent(ctx, func(span *heimdall.Span) {
		te.pushEvent(Event{
			Type:    EventTypeNewSpan,
			NewSpan: span,
		})
	})
	if err != nil {
		return err
	}

	// pump events from the ring buffer to the events channel
	for {
		e, ok := te.takeEvent()
		if !ok {
			pollDelayTimer := time.NewTimer(te.pollDelay)
			select {
			case <-pollDelayTimer.C:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		select {
		case te.events <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
