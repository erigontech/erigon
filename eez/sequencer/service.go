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

package sequencer

import (
	"context"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
)

// Service runs a Sequencer and its Schedule for a node's lifetime.
type Service struct {
	seq      *Sequencer
	schedule *Schedule
	heads    chan l1.Header
	halt     func(error)
	mu       sync.Mutex
	err      error
	logger   log.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewService returns a service producing with seq on the triggers of
// schedule, which the L1 heads given to Head drive. halt takes the error that
// stops the schedule.
func NewService(seq *Sequencer, schedule *Schedule, halt func(error), logger log.Logger) *Service {
	return &Service{seq: seq, schedule: schedule, heads: make(chan l1.Header, 1), halt: halt, logger: logger}
}

// Sequencer is the service's sequencer.
func (s *Service) Sequencer() *Sequencer { return s.seq }

// Head hands L1 head h to the schedule; it replaces a head not yet taken. It
// must have a single caller at a time.
func (s *Service) Head(h l1.Header) {
	select {
	case s.heads <- h:
	default:
		select {
		case <-s.heads:
		default:
		}
		s.heads <- h
	}
}

// Start runs the schedule and the sequencer in the background.
func (s *Service) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	// Holds one event while the sequencer produces; Live ticks past it are
	// dropped.
	events := make(chan Event, 1)
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		if err := s.schedule.Run(ctx, s.heads, events, s.logger); err != nil {
			s.mu.Lock()
			s.err = err
			s.mu.Unlock()
			s.halt(err)
		}
	}()
	go func() {
		defer s.wg.Done()
		s.seq.Run(ctx, events)
	}()
	return nil
}

// Err returns the error that stopped the schedule, if one did.
func (s *Service) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Stop stops the sequencer and waits for it.
func (s *Service) Stop() error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	s.wg.Wait()
	return nil
}
