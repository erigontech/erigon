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

package execmodule

import (
	"context"
	"sync"
)

// doneObservedContext signals on observed the first time Done is asked for, which lets a test order
// an action strictly after a select has committed to waiting on the context.
type doneObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *doneObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

// NewDoneObservedContext is exported so the external execmodule_test package shares the helper
// instead of keeping its own copy.
func NewDoneObservedContext(parent context.Context) (context.Context, <-chan struct{}) {
	observed := make(chan struct{})
	return &doneObservedContext{Context: parent, observed: observed}, observed
}
