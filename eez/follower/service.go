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

package follower

import (
	"context"
	"errors"
	"sync"
)

// Service runs a Follower for a node's lifetime.
type Service struct {
	follower *Follower
	// halt receives the error the follower stopped with before Stop.
	halt   func(error)
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	err    error
}

// NewService returns a service running f. halt is called once if f stops
// on its own, with the error it stopped with.
func NewService(f *Follower, halt func(error)) *Service {
	return &Service{follower: f, halt: halt}
}

// Start runs the follower in the background.
func (s *Service) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	go func() {
		defer close(s.done)
		err := s.follower.Run(ctx)
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return
		}
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		if s.halt != nil {
			s.halt(err)
		}
	}()
	return nil
}

// Stop stops the follower and waits for it.
func (s *Service) Stop() error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	<-s.done
	return nil
}

// Err returns the error the follower stopped with on its own, if it did.
func (s *Service) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}
