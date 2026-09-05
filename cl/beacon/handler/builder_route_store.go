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

package handler

import (
	"bytes"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
)

const (
	builderRouteCapacity = 16
	builderRouteTTL      = 2 * time.Minute
)

type builderRouteState uint8

const (
	builderRouteIdle builderRouteState = iota
	builderRouteInFlight
	builderRouteDelivered
)

type builderRouteKey struct {
	root common.Hash
	url  string
}

type builderRoute struct {
	state     builderRouteState
	expiresAt time.Time
}

type builderRouteStore struct {
	mu       sync.Mutex
	routes   map[builderRouteKey]*builderRoute
	capacity int
	reserved int
	ttl      time.Duration
	now      func() time.Time
}

func newBuilderRouteStore(capacity int, ttl time.Duration, now func() time.Time) *builderRouteStore {
	return &builderRouteStore{
		routes:   make(map[builderRouteKey]*builderRoute, capacity),
		capacity: capacity,
		ttl:      ttl,
		now:      now,
	}
}

func (s *builderRouteStore) Add(root common.Hash, url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.pruneExpired(now)
	key := builderRouteKey{root: root, url: url}
	if route, ok := s.routes[key]; ok {
		route.expiresAt = now.Add(s.ttl)
		return true
	}
	if len(s.routes)+s.reserved >= s.capacity {
		s.evictOldestDelivered()
		if len(s.routes)+s.reserved >= s.capacity {
			return false
		}
	}
	s.routes[key] = &builderRoute{state: builderRouteIdle, expiresAt: now.Add(s.ttl)}
	return true
}

func (s *builderRouteStore) Reserve() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpired(s.now())
	if len(s.routes)+s.reserved >= s.capacity {
		s.evictOldestDelivered()
		if len(s.routes)+s.reserved >= s.capacity {
			return false
		}
	}
	s.reserved++
	return true
}

func (s *builderRouteStore) CommitReservation(root common.Hash, url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reserved == 0 {
		return false
	}
	s.reserved--
	now := s.now()
	s.pruneExpired(now)
	key := builderRouteKey{root: root, url: url}
	if route, ok := s.routes[key]; ok {
		route.expiresAt = now.Add(s.ttl)
		return true
	}
	s.routes[key] = &builderRoute{state: builderRouteIdle, expiresAt: now.Add(s.ttl)}
	return true
}

func (s *builderRouteStore) ReleaseReservation() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reserved > 0 {
		s.reserved--
	}
}

func builderRouteKeyLess(left, right builderRouteKey) bool {
	if order := bytes.Compare(left.root[:], right.root[:]); order != 0 {
		return order < 0
	}
	return left.url < right.url
}

func (s *builderRouteStore) Claim(root common.Hash, url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.pruneExpired(now)
	route, ok := s.routes[builderRouteKey{root: root, url: url}]
	if !ok || route.state != builderRouteIdle {
		return false
	}
	route.state = builderRouteInFlight
	return true
}

func (s *builderRouteStore) ClaimOrAdd(root common.Hash, url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.pruneExpired(now)
	key := builderRouteKey{root: root, url: url}
	route, ok := s.routes[key]
	if !ok {
		if len(s.routes)+s.reserved >= s.capacity {
			s.evictOldestDelivered()
			if len(s.routes)+s.reserved >= s.capacity {
				return false
			}
		}
		route = &builderRoute{state: builderRouteIdle, expiresAt: now.Add(s.ttl)}
		s.routes[key] = route
	}
	if route.state != builderRouteIdle {
		return false
	}
	route.state = builderRouteInFlight
	route.expiresAt = now.Add(s.ttl)
	return true
}

func (s *builderRouteStore) evictOldestDelivered() bool {
	var oldestKey builderRouteKey
	var oldestRoute *builderRoute
	for key, route := range s.routes {
		if route.state == builderRouteDelivered && (oldestRoute == nil || route.expiresAt.Before(oldestRoute.expiresAt) ||
			route.expiresAt.Equal(oldestRoute.expiresAt) && builderRouteKeyLess(key, oldestKey)) {
			oldestKey = key
			oldestRoute = route
		}
	}
	if oldestRoute == nil {
		return false
	}
	delete(s.routes, oldestKey)
	return true
}
func (s *builderRouteStore) Complete(root common.Hash, url string, delivered bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	route, ok := s.routes[builderRouteKey{root: root, url: url}]
	if !ok || route.state != builderRouteInFlight {
		return
	}
	if delivered {
		route.state = builderRouteDelivered
	} else {
		route.state = builderRouteIdle
	}
}

func (s *builderRouteStore) Discard(root common.Hash, url string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := builderRouteKey{root: root, url: url}
	if route, ok := s.routes[key]; ok && route.state == builderRouteInFlight {
		delete(s.routes, key)
	}
}

func (s *builderRouteStore) pruneExpired(now time.Time) {
	for key, route := range s.routes {
		if route.state != builderRouteInFlight && !now.Before(route.expiresAt) {
			delete(s.routes, key)
		}
	}
}
