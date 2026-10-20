// Copyright 2026 The Erigon Authors
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

package bscsync

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	bscp2p "github.com/erigontech/erigon/bsc/p2p"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

type EventType string

const EventTypeNewBlock EventType = "new-block"
const EventTypeNewBlockBatch EventType = "new-block-batch"
const EventTypeNewBlockHashes EventType = "new-block-hashes"

type EventSource string

const EventSourceP2PNewBlockHashes EventSource = "p2p-new-block-hashes-source"
const EventSourceP2PNewBlock EventSource = "p2p-new-block-source"

type EventNewBlock struct {
	NewBlock *types.Block
	PeerId   *p2p.PeerId
	Source   EventSource
}

type EventNewBlockBatch struct { // new batch of blocks from peer
	NewBlocks []*types.Block
	PeerId    *p2p.PeerId
	Source    EventSource
	Processed chan<- error // buffered; closed when processed successfully, otherwise the error is sent
}

type EventNewBlockHashes struct {
	NewBlockHashes eth.NewBlockHashesPacket
	PeerId         *p2p.PeerId
}

type Event struct {
	Type EventType

	newBlock       EventNewBlock
	newBlockBatch  EventNewBlockBatch
	newBlockHashes EventNewBlockHashes
}

func (e Event) AsNewBlock() EventNewBlock {
	if e.Type != EventTypeNewBlock {
		panic("Event type mismatch")
	}
	return e.newBlock
}

func (e Event) AsNewBlockBatch() EventNewBlockBatch {
	if e.Type != EventTypeNewBlockBatch {
		panic("Event type mismatch")
	}
	return e.newBlockBatch
}

func (e Event) AsNewBlockHashes() EventNewBlockHashes {
	if e.Type != EventTypeNewBlockHashes {
		panic("Event type mismatch")
	}
	return e.newBlockHashes
}

type p2pObserverRegistrar interface {
	RegisterNewBlockObserver(event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockPacket]]) event.UnregisterFunc
	RegisterNewBlockHashesObserver(event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]]) event.UnregisterFunc
	RegisterPeerEventObserver(event.Observer[*sentryproto.PeerEvent], ...p2p.RegisterOpt) event.UnregisterFunc
	PeerEthVersion(ctx context.Context, peerId *p2p.PeerId) (uint, bool)
}

const announcementSummaryInterval = time.Minute

type announcementCounts struct {
	NewBlock       uint64
	NewBlockHashes uint64
}

func NewTipEvents(logger log.Logger, p2pReg p2pObserverRegistrar) *TipEvents {
	return &TipEvents{
		logger:               logger,
		events:               NewEventChannel[Event](1000, WithEventChannelLogging(logger, log.LvlTrace, "p2p")),
		p2pObserverRegistrar: p2pReg,
		blockEventsSpamGuard: newBlockEventsSpamGuard(logger),
		announcedHeads:       newAnnouncedHeads(),
		announcements:        map[p2p.PeerId]*announcementCounts{},
	}
}

// TipEvents turns NewBlock and NewBlockHashes messages into tip events and
// tracks the highest block number that each peer announced.
type TipEvents struct {
	logger               log.Logger
	events               *EventChannel[Event]
	p2pObserverRegistrar p2pObserverRegistrar
	blockEventsSpamGuard blockEventsSpamGuard
	announcedHeads       *announcedHeads

	announcementsMu sync.Mutex
	announcements   map[p2p.PeerId]*announcementCounts
}

// Next blocks until there is at least one event and returns all queued events, oldest first.
func (te *TipEvents) Next(ctx context.Context) ([]Event, error) {
	return te.events.TakeAll(ctx)
}

func (te *TipEvents) PushEvent(e Event) {
	te.events.PushEvent(e)
}

// AnnouncedHead returns the second-highest head announced by the connected
// peers, so that a single peer cannot move it. With one peer it returns that
// peer's head.
func (te *TipEvents) AnnouncedHead() (uint64, bool) {
	return te.announcedHeads.Head()
}

func (te *TipEvents) Run(ctx context.Context) error {
	te.logger.Info("[bsc] running tip events component")

	peerEventObserverCancel := te.p2pObserverRegistrar.RegisterPeerEventObserver(func(message *sentryproto.PeerEvent) {
		if message.EventId == sentryproto.PeerEvent_Disconnect {
			te.announcedHeads.Remove(p2p.PeerIdFromH512(message.PeerId))
		}
	})
	defer peerEventObserverCancel()

	newBlockObserverCancel := te.p2pObserverRegistrar.RegisterNewBlockObserver(func(message *p2p.DecodedInboundMessage[*eth.NewBlockPacket]) {
		block := message.Decoded.Block
		te.announcedHeads.Update(message.PeerId, block.NumberU64())
		te.countAnnouncement(message.PeerId, func(c *announcementCounts) { c.NewBlock++ })

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
		// A peer can send an empty NewBlockHashes packet; skip it to avoid the blockHashes[0] panics below.
		if len(blockHashes) == 0 {
			return
		}

		for _, h := range blockHashes {
			te.announcedHeads.Update(message.PeerId, h.Number)
		}
		te.countAnnouncement(message.PeerId, func(c *announcementCounts) { c.NewBlockHashes++ })

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

	ticker := time.NewTicker(announcementSummaryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			summary := te.announcementSummary(ctx)
			args := make([]any, 0, 2*len(summary))
			for _, key := range slices.Sorted(maps.Keys(summary)) {
				args = append(args, key, fmt.Sprintf("newBlock=%d hashes=%d", summary[key].NewBlock, summary[key].NewBlockHashes))
			}
			te.logger.Info("[bsc] block announcements in the last minute", args...)
		}
	}
}

func (te *TipEvents) countAnnouncement(peerId *p2p.PeerId, inc func(*announcementCounts)) {
	te.announcementsMu.Lock()
	defer te.announcementsMu.Unlock()
	c, ok := te.announcements[*peerId]
	if !ok {
		c = &announcementCounts{}
		te.announcements[*peerId] = c
	}
	inc(c)
}

// announcementSummary returns the NewBlock and NewBlockHashes counts since the last call,
// grouped by the eth version of the sending peer.
func (te *TipEvents) announcementSummary(ctx context.Context) map[string]announcementCounts {
	te.announcementsMu.Lock()
	perPeer := te.announcements
	te.announcements = map[p2p.PeerId]*announcementCounts{}
	te.announcementsMu.Unlock()

	summary := map[string]announcementCounts{}
	for peerId, c := range perPeer {
		key := "unknown"
		if version, ok := te.p2pObserverRegistrar.PeerEthVersion(ctx, &peerId); ok {
			key = fmt.Sprintf("eth%d", version)
		}
		total := summary[key]
		total.NewBlock += c.NewBlock
		total.NewBlockHashes += c.NewBlockHashes
		summary[key] = total
	}
	return summary
}

type announcedHeads struct {
	mu    sync.Mutex
	heads map[p2p.PeerId]uint64
}

func newAnnouncedHeads() *announcedHeads {
	return &announcedHeads{heads: map[p2p.PeerId]uint64{}}
}

func (a *announcedHeads) Update(peerId *p2p.PeerId, num uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if num > a.heads[*peerId] {
		a.heads[*peerId] = num
	}
}

func (a *announcedHeads) Remove(peerId *p2p.PeerId) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.heads, *peerId)
}

func (a *announcedHeads) Head() (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.heads) == 0 {
		return 0, false
	}
	nums := make([]uint64, 0, len(a.heads))
	for _, n := range a.heads {
		nums = append(nums, n)
	}
	slices.Sort(nums)
	if len(nums) == 1 {
		return nums[0], true
	}
	return nums[len(nums)-2], true
}

// blockEventKey is a comparable struct used for spam detection, to protect ourselves from noisy and/or
// malicious peers overflowing our event channels. Note that all the struct fields must be comparable.
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
		panic(fmt.Errorf("spam guard lru: %w", err))
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
		g.logger.Trace(
			"[block-events-spam-guard] detected spam",
			"peerId", peerId,
			"blockHash", blockHash,
			"blockNum", blockNum,
		)

		return true
	}

	g.seenPeerBlockHashes.Add(key, struct{}{})
	return false
}

var _ p2pObserverRegistrar = (*bscp2p.Service)(nil)
