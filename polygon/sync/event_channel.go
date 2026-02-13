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
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
)

// EventChannel is a buffered channel that drops oldest events when full.
type EventChannel[TEvent any] struct {
	opts       eventChannelOptions
	events     chan TEvent
	queue      *list.List
	queueCap   uint
	queueMutex sync.Mutex
	notify     chan struct{} // signal channel replacing sync.Cond for synctest compatibility
}

func NewEventChannel[TEvent any](capacity uint, opts ...EventChannelOption) *EventChannel[TEvent] {
	if capacity == 0 {
		panic("NewEventChannel: capacity must be > 0")
	}

	defaultOpts := eventChannelOptions{}
	for _, opt := range opts {
		opt(&defaultOpts)
	}

	return &EventChannel[TEvent]{
		opts:     defaultOpts,
		events:   make(chan TEvent),
		queue:    list.New(),
		queueCap: capacity,
		notify:   make(chan struct{}, 1),
	}
}

// Events returns a channel for reading events.
func (ec *EventChannel[TEvent]) Events() <-chan TEvent {
	return ec.events
}

// PushEvent queues an event. If the queue is full, it drops the oldest event to make space.
func (ec *EventChannel[TEvent]) PushEvent(e TEvent) {
	ec.queueMutex.Lock()

	var dropped bool
	if uint(ec.queue.Len()) == ec.queueCap {
		ec.queue.Remove(ec.queue.Front())
		dropped = true
	}
	if ec.opts.logger != nil && dropped {
		ec.opts.logger.Log(ec.opts.loggerLvl, fmt.Sprintf("[event-channel-%s] dropping event", ec.opts.loggerId))
	}

	ec.queue.PushBack(e)
	ec.queueMutex.Unlock()

	// Non-blocking signal to notify the Run loop that an event is available.
	// Using a channel instead of sync.Cond for compatibility with testing/synctest.
	select {
	case ec.notify <- struct{}{}:
	default:
	}
}

// takeEvent dequeues an event. If the queue was empty, it returns false.
func (ec *EventChannel[TEvent]) takeEvent() (TEvent, bool) {
	ec.queueMutex.Lock()
	defer ec.queueMutex.Unlock()
	if elem := ec.queue.Front(); elem != nil {
		e := ec.queue.Remove(elem).(TEvent)
		return e, true
	}
	var emptyEvent TEvent
	return emptyEvent, false
}

// Run pumps events from the queue to the events channel.
func (ec *EventChannel[TEvent]) Run(ctx context.Context) error {
	for {
		// Try to dequeue first without waiting
		if e, ok := ec.takeEvent(); ok {
			select {
			case ec.events <- e:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Queue is empty - wait for a signal or context cancellation
		select {
		case <-ec.notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type eventChannelOptions struct {
	logger    log.Logger
	loggerLvl log.Lvl
	loggerId  string
}

type EventChannelOption func(opts *eventChannelOptions)

func WithEventChannelLogging(logger log.Logger, lvl log.Lvl, id string) EventChannelOption {
	return func(opts *eventChannelOptions) {
		opts.logger = logger
		opts.loggerLvl = lvl
		opts.loggerId = id
	}
}
