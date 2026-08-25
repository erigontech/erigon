// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
// SPDX-License-Identifier: LGPL-3.0-or-later

package handler

import (
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
	if len(s.routes) >= s.capacity {
		for existingKey, route := range s.routes {
			if route.state == builderRouteDelivered {
				delete(s.routes, existingKey)
				break
			}
		}
		if len(s.routes) >= s.capacity {
			return false
		}
	}
	s.routes[key] = &builderRoute{state: builderRouteIdle, expiresAt: now.Add(s.ttl)}
	return true
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

func (s *builderRouteStore) pruneExpired(now time.Time) {
	for key, route := range s.routes {
		if route.state != builderRouteInFlight && !now.Before(route.expiresAt) {
			delete(s.routes, key)
		}
	}
}
