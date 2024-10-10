// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package sync

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/p2p"
	"github.com/erigontech/erigon/polygon/polygoncommon"

	lru "github.com/hashicorp/golang-lru/v2"
)

type EventType string

const EventTypeNewBlock EventType = "new-block"
const EventTypeNewBlockHashes EventType = "new-block-hashes"
const EventTypeNewMilestone EventType = "new-milestone"

type EventSource string

const EventSourceP2PNewBlockHashes EventSource = "p2p-new-block-hashes-source"
const EventSourceP2PNewBlock EventSource = "p2p-new-block-source"

type EventTopic string

func (t EventTopic) String() string {
	return string(t)
}

const EventTopicHeimdall EventTopic = "heimdall"
const EventTopicP2P EventTopic = "p2p"

type EventNewBlock struct {
	NewBlock *types.Block
	PeerId   *p2p.PeerId
	Source   EventSource
}

type EventNewBlockHashes struct {
	NewBlockHashes eth.NewBlockHashesPacket
	PeerId         *p2p.PeerId
}

type EventNewMilestone = *heimdall.Milestone

type Event struct {
	Type EventType

	newBlock       EventNewBlock
	newBlockHashes EventNewBlockHashes
	newMilestone   EventNewMilestone
}

func (e Event) Topic() string {
	switch e.Type {
	case EventTypeNewBlock:
		return EventTopicHeimdall.String()
	case EventTypeNewBlockHashes, EventTypeNewMilestone:
		return EventTopicP2P.String()
	default:
		panic(fmt.Sprintf("unknown event type: %s", e.Type))
	}
}

func (e Event) AsNewBlock() EventNewBlock {
	if e.Type != EventTypeNewBlock {
		panic("Event type mismatch")
	}
	return e.newBlock
}

func (e Event) AsNewBlockHashes() EventNewBlockHashes {
	if e.Type != EventTypeNewBlockHashes {
		panic("Event type mismatch")
	}
	return e.newBlockHashes
}

func (e Event) AsNewMilestone() EventNewMilestone {
	if e.Type != EventTypeNewMilestone {
		panic("Event type mismatch")
	}
	return e.newMilestone
}

type p2pObserverRegistrar interface {
	RegisterNewBlockObserver(polygoncommon.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockPacket]]) polygoncommon.UnregisterFunc
	RegisterNewBlockHashesObserver(polygoncommon.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]]) polygoncommon.UnregisterFunc
}

type heimdallObserverRegistrar interface {
	RegisterMilestoneObserver(callback func(*heimdall.Milestone), opts ...heimdall.ObserverOption) polygoncommon.UnregisterFunc
}

type TipEvents struct {
	logger                    log.Logger
	events                    *CompositeEventChannel[Event]
	p2pObserverRegistrar      p2pObserverRegistrar
	heimdallObserverRegistrar heimdallObserverRegistrar
	blockEventsSpamGuard      blockEventsSpamGuard
}

func NewTipEvents(
	logger log.Logger,
	p2pObserverRegistrar p2pObserverRegistrar,
	heimdallObserverRegistrar heimdallObserverRegistrar,
) *TipEvents {
	eventChannel := NewCompositeEventChannel[Event](map[string]*EventChannel[Event]{
		EventTopicHeimdall.String(): NewEventChannel[Event](
			10,
			WithEventChannelLogging(logger, log.LvlWarn, EventTopicHeimdall.String()),
		),
		EventTopicP2P.String(): NewEventChannel[Event](
			1000,
			WithEventChannelLogging(logger, log.LvlWarn, EventTopicP2P.String()),
		),
	})
	return &TipEvents{
		logger:                    logger,
		events:                    eventChannel,
		p2pObserverRegistrar:      p2pObserverRegistrar,
		heimdallObserverRegistrar: heimdallObserverRegistrar,
		blockEventsSpamGuard:      newBlockEventsSpamGuard(logger),
	}
}

func (te *TipEvents) Events() <-chan Event {
	return te.events.Events()
}

func (te *TipEvents) Run(ctx context.Context) error {
	te.logger.Debug(syncLogPrefix("running tip events component"))

	newBlockObserverCancel := te.p2pObserverRegistrar.RegisterNewBlockObserver(func(message *p2p.DecodedInboundMessage[*eth.NewBlockPacket]) {
		block := message.Decoded.Block

		if te.blockEventsSpamGuard.Spam(message.PeerId, block.Hash(), block.NumberU64()) {
			return
		}

		te.logger.Trace(
			"[tip-events] new block event received from peer",
			"peerId", message.PeerId,
			"hash", block.Hash(),
			"number", block.NumberU64(),
		)

		te.events.PushEvent(Event{
			Type: EventTypeNewBlock,
			newBlock: EventNewBlock{
				NewBlock: block,
				PeerId:   message.PeerId,
				Source:   EventSourceP2PNewBlock,
			},
		})
	})
	defer newBlockObserverCancel()

	newBlockHashesObserverCancel := te.p2pObserverRegistrar.RegisterNewBlockHashesObserver(func(message *p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]) {
		blockHashes := *message.Decoded

		if te.blockEventsSpamGuard.Spam(message.PeerId, blockHashes[0].Hash, blockHashes[0].Number) {
			return
		}

		te.logger.Trace(
			"[tip-events] new block hashes event received from peer",
			"peerId", message.PeerId,
			"hash", blockHashes[0].Hash,
			"number", blockHashes[0].Number,
		)

		te.events.PushEvent(Event{
			Type: EventTypeNewBlockHashes,
			newBlockHashes: EventNewBlockHashes{
				NewBlockHashes: blockHashes,
				PeerId:         message.PeerId,
			},
		})
	})
	defer newBlockHashesObserverCancel()

	milestoneObserverCancel := te.heimdallObserverRegistrar.RegisterMilestoneObserver(func(milestone *heimdall.Milestone) {
		te.logger.Trace("[tip-events] new milestone event received", "id", milestone.RawId())
		te.events.PushEvent(Event{
			Type:         EventTypeNewMilestone,
			newMilestone: milestone,
		})
	}, heimdall.WithEventsLimit(5))
	defer milestoneObserverCancel()

	return te.events.Run(ctx)
}

// blockEventKey is a comparable struct used for spam detection, to protect ourselves from noisy and/or
// malicious peers overflowing our event channels. Note that all the struct fields must be comparable.
// One example of why we need something like this is, Erigon (non-Astrid) nodes do not keep track of
// whether they have already notified a peer or whether a peer has notified them about a given block hash,
// and instead they always announce new block hashes to all of their peers whenever they receive a new
// block event. In this case, if we are connected to lots of Erigon (non-Astrid) peers we will get
// a lot of spam that can overflow our events channel. In the future, we may find more situations
// where we need to filter spam from say malicious peers that try to trick us or DDoS us, so this may
// also serve as a base to build on top of.
type blockEventKey struct {
	peerId    p2p.PeerId
	blockHash common.Hash
	blockNum  uint64
}

func newBlockEventsSpamGuard(logger log.Logger) blockEventsSpamGuard {
	// 1 key is 104 bytes, 10 keys is ~1KB, 10_000 keys is ~1MB
	// assume 200 peers, that should be enough to keep roughly on average
	// the last 50 messages from each peer - that should be plenty!
	seenPeerBlockHashes, err := lru.New[blockEventKey, struct{}](10_000)
	if err != nil {
		panic(err)
	}
	return blockEventsSpamGuard{
		logger:              logger,
		seenPeerBlockHashes: seenPeerBlockHashes,
	}
}

type blockEventsSpamGuard struct {
	logger              log.Logger
	seenPeerBlockHashes *lru.Cache[blockEventKey, struct{}]
}

func (g blockEventsSpamGuard) Spam(peerId *p2p.PeerId, blockHash common.Hash, blockNum uint64) bool {
	key := blockEventKey{
		peerId:    *peerId,
		blockHash: blockHash,
		blockNum:  blockNum,
	}

	if g.seenPeerBlockHashes.Contains(key) {
		g.logger.Trace("[block-events-spam-guard] detected spam", "key", key)
		return true
	}

	g.seenPeerBlockHashes.Add(key, struct{}{})
	return false
}
