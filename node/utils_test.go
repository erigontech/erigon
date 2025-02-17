// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Contains a batch of utility type declarations used by the tests. As the node
// operates on unique types, a lot of them are needed to check various features.

package node

// NoopLifecycle is a trivial implementation of the Service interface.
type NoopLifecycle struct{}

func (s *NoopLifecycle) Start() error { return nil }
func (s *NoopLifecycle) Stop() error  { return nil }

func NewNoop() *Noop {
	noop := new(Noop)
	return noop
}

// Set of services all wrapping the base NoopLifecycle resulting in the same method
// signatures but different outer types.
type Noop struct{ NoopLifecycle }

// InstrumentedService is an implementation of Lifecycle for which all interface
// methods can be instrumented both return value as well as event hook wise.
type InstrumentedService struct {
	start error
	stop  error

	startHook func()
	stopHook  func()
}

func (s *InstrumentedService) Start() error {
	if s.startHook != nil {
		s.startHook()
	}
	return s.start
}

func (s *InstrumentedService) Stop() error {
	if s.stopHook != nil {
		s.stopHook()
	}
	return s.stop
}
