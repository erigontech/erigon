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

package errors

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Group joins goroutines like errgroup but keeps every member's return
// instead of only the first: Wait reports all real failures together, and a
// cancellation-only member exit can never occupy the first-error slot and
// hide a concurrent real failure. Members return their errors raw — no
// self-filtering.
type Group struct {
	g    *errgroup.Group
	mu   sync.Mutex
	errs []error
}

// NewGroup mirrors errgroup.WithContext: the returned context is canceled on
// the first member error, which is how sibling goroutines learn to stop.
func NewGroup(ctx context.Context) (*Group, context.Context) {
	g, gctx := errgroup.WithContext(ctx)
	return &Group{g: g}, gctx
}

func (eg *Group) Go(fn func() error) {
	eg.g.Go(func() error {
		err := fn()
		if err != nil {
			eg.mu.Lock()
			eg.errs = append(eg.errs, err)
			eg.mu.Unlock()
		}
		return err
	})
}

// Wait joins every member, then reports the recorded real failures together.
// Cancellation-only member exits are routine teardown.
func (eg *Group) Wait() error {
	_ = eg.g.Wait()
	eg.mu.Lock()
	defer eg.mu.Unlock()
	real := make([]error, 0, len(eg.errs))
	for _, err := range eg.errs {
		if NilIfCanceled(err) != nil {
			real = append(real, err)
		}
	}
	if len(real) == 1 {
		return real[0]
	}
	return errors.Join(real...)
}
