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

package service

import (
	"errors"
	"sync"
)

const (
	maxSubscribers = 100 // only 100 clients per sentinel
)

type gossipObject struct {
	data     []byte // gossip data
	t        string // determine which gossip message we are notifying of
	pid      string // pid is the peer id of the sender
	subnetId *uint64
}

type gossipNotifier struct {
	notifiers []chan gossipObject

	mu sync.Mutex
}

func newGossipNotifier() *gossipNotifier {
	return &gossipNotifier{
		notifiers: make([]chan gossipObject, 0, maxSubscribers),
	}
}

func (g *gossipNotifier) notify(obj *gossipObject) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, ch := range g.notifiers {
		ch <- *obj
	}
}

func (g *gossipNotifier) addSubscriber() (chan gossipObject, int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.notifiers) >= maxSubscribers {
		return nil, -1, errors.New("too many subsribers, try again later")
	}
	ch := make(chan gossipObject, 1<<16)
	g.notifiers = append(g.notifiers, ch)
	return ch, len(g.notifiers) - 1, nil
}

func (g *gossipNotifier) removeSubscriber(id int) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.notifiers) <= id {
		return errors.New("invalid id, no subscription exist with this id")
	}
	close(g.notifiers[id])
	g.notifiers = append(g.notifiers[:id], g.notifiers[id+1:]...)
	return nil
}
