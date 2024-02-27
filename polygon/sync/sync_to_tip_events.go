package sync

import (
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
)

const EventTypeMilestone = "milestone"
const EventTypeNewHeader = "new-header"
const EventTypeNewSpan = "new-span"

type Event struct {
	Type string

	Milestone *heimdall.Milestone

	NewHeader *types.Header
	PeerId    p2p.PeerId

	NewSpan *heimdall.Span
}

type SyncToTipEvents struct {
	events chan Event
}

func NewSyncToTipEvents() *SyncToTipEvents {
	return &SyncToTipEvents{make(chan Event)}
}

func (e *SyncToTipEvents) Events() chan Event {
	return e.events
}
