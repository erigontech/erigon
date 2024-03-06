package sync

import (
	"context"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
)

const EventTypeNewHeader = "new-header"
const EventTypeNewHeaderHashes = "new-header-hashes"
const EventTypeNewMilestone = "new-milestone"
const EventTypeNewSpan = "new-span"

type EventNewHeader struct {
	NewHeader *types.Header
	PeerId    *p2p.PeerId
}

type EventNewHeaderHashes struct {
	NewHeaderHashes eth.NewBlockHashesPacket
	PeerId          *p2p.PeerId
}

type EventNewMilestone = *heimdall.Milestone

type EventNewSpan = *heimdall.Span

type Event struct {
	Type string

	newHeader       EventNewHeader
	newHeaderHashes EventNewHeaderHashes
	newMilestone    EventNewMilestone
	newSpan         EventNewSpan
}

func (e Event) AsNewHeader() EventNewHeader {
	if e.Type != EventTypeNewHeader {
		panic("Event type mismatch")
	}
	return e.newHeader
}

func (e Event) AsNewHeaderHashes() EventNewHeaderHashes {
	if e.Type != EventTypeNewHeaderHashes {
		panic("Event type mismatch")
	}
	return e.newHeaderHashes
}

func (e Event) AsNewMilestone() EventNewMilestone {
	if e.Type != EventTypeNewMilestone {
		panic("Event type mismatch")
	}
	return e.newMilestone
}

func (e Event) AsNewSpan() EventNewSpan {
	if e.Type != EventTypeNewSpan {
		panic("Event type mismatch")
	}
	return e.newSpan
}

type TipEvents struct {
	events *EventChannel[Event]

	p2pService      p2p.Service
	heimdallService heimdall.HeimdallNoStore
}

func NewTipEvents(
	p2pService p2p.Service,
	heimdallService heimdall.HeimdallNoStore,
) *TipEvents {
	eventsCapacity := uint(1000) // more than 3 milestones

	return &TipEvents{
		events: NewEventChannel[Event](eventsCapacity),

		p2pService:      p2pService,
		heimdallService: heimdallService,
	}
}

func (te *TipEvents) Events() <-chan Event {
	return te.events.Events()
}

func (te *TipEvents) Run(ctx context.Context) error {
	newBlockObserverCancel := te.p2pService.RegisterNewBlockObserver(func(message *p2p.DecodedInboundMessage[*eth.NewBlockPacket]) {
		block := message.Decoded.Block
		te.events.PushEvent(Event{
			Type: EventTypeNewHeader,
			newHeader: EventNewHeader{
				NewHeader: block.Header(),
				PeerId:    message.PeerId,
			},
		})
	})
	defer newBlockObserverCancel()

	newBlockHashesObserverCancel := te.p2pService.RegisterNewBlockHashesObserver(func(message *p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]) {
		te.events.PushEvent(Event{
			Type: EventTypeNewHeaderHashes,
			newHeaderHashes: EventNewHeaderHashes{
				NewHeaderHashes: *message.Decoded,
				PeerId:          message.PeerId,
			},
		})
	})
	defer newBlockHashesObserverCancel()

	err := te.heimdallService.OnMilestoneEvent(ctx, func(milestone *heimdall.Milestone) {
		te.events.PushEvent(Event{
			Type:         EventTypeNewMilestone,
			newMilestone: milestone,
		})
	})
	if err != nil {
		return err
	}

	err = te.heimdallService.OnSpanEvent(ctx, func(span *heimdall.Span) {
		te.events.PushEvent(Event{
			Type:    EventTypeNewSpan,
			newSpan: span,
		})
	})
	if err != nil {
		return err
	}

	return te.events.Run(ctx)
}
