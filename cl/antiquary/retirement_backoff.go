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

package antiquary

// maxRetirementSkipTicks caps the gap at roughly an hour of retirementLoop ticks.
const maxRetirementSkipTicks = 300

// retirementBackoff spaces out a retirement step that keeps failing. Not every
// failure is transient: a data gap the step cannot get past fails identically on
// every tick, and without a gap it would redo the same work and log the same error
// for the life of the process.
type retirementBackoff struct {
	skipTicks int
	remaining int
}

func (b *retirementBackoff) ready() bool {
	if b.remaining > 0 {
		b.remaining--
		return false
	}
	return true
}

func (b *retirementBackoff) failed() {
	b.skipTicks = min(max(b.skipTicks*2, 1), maxRetirementSkipTicks)
	b.remaining = b.skipTicks
}

func (b *retirementBackoff) succeeded() {
	b.skipTicks, b.remaining = 0, 0
}

// retirementStep pairs one retirement operation with its own backoff, so a step that
// wedges on a data gap cannot slow down the other.
type retirementStep struct {
	run     func() error
	onError func(error)
	backoff retirementBackoff
}

func (s *retirementStep) attempt(shuttingDown func() bool) {
	if !s.backoff.ready() {
		return
	}
	err := s.run()
	if err == nil {
		s.backoff.succeeded()
		return
	}
	// A cancelled context is a shutdown, not the step's failure.
	if shuttingDown() {
		return
	}
	s.backoff.failed()
	s.onError(err)
}
