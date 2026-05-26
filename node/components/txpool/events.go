// Copyright 2025 The Erigon Authors
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

package txpool

import (
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

type EventType int

const (
	EventTypeUnknown EventType = iota
	EventTypeTransactionAdded
	EventTypeTransactionRemoved
	EventTypeBlockMined
	EventTypeStateChange
	EventTypePeerConnected
	EventTypePeerDisconnected
)

func (e EventType) String() string {
	switch e {
	case EventTypeTransactionAdded:
		return "transaction_added"
	case EventTypeTransactionRemoved:
		return "transaction_removed"
	case EventTypeBlockMined:
		return "block_mined"
	case EventTypeStateChange:
		return "state_change"
	case EventTypePeerConnected:
		return "peer_connected"
	case EventTypePeerDisconnected:
		return "peer_disconnected"
	default:
		return "unknown"
	}
}

type Event struct {
	Type            EventType
	Timestamp       time.Time
	TransactionHash *common.Hash
	BlockNumber     *uint64
	PeerID          []byte
	Data            interface{}
}

type TransactionAddedEvent struct {
	Hash      common.Hash
	From      common.Address
	Nonce     uint64
	IsLocal   bool
	Timestamp time.Time
}

type TransactionRemovedEvent struct {
	Hash   common.Hash
	Reason string
}

type BlockMinedEvent struct {
	Block     *types.Block
	Timestamp time.Time
}

type StateChangeEvent struct {
	BlockNumber uint64
	BlockHash   common.Hash
}

type PeerEvent struct {
	PeerID    []byte
	Connected bool
}

type EventHandler func(Event)

type EventBus struct {
	handlers map[EventType][]EventHandler
}

func NewEventBus() *EventBus {
	return &EventBus{
		handlers: make(map[EventType][]EventHandler),
	}
}

func (b *EventBus) Subscribe(eventType EventType, handler EventHandler) {
	b.handlers[eventType] = append(b.handlers[eventType], handler)
}

func (b *EventBus) Publish(event Event) {
	handlers := b.handlers[event.Type]
	for _, h := range handlers {
		h(event)
	}
}

func (b *EventBus) UnsubscribeAll(eventType EventType) {
	delete(b.handlers, eventType)
}
