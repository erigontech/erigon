// Copyright 2021 The Erigon Authors
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

package common

import (
	"errors"

	"golang.org/x/net/context"
)

var ErrStopped = errors.New("stopped")
var ErrUnwind = errors.New("unwound")

// FastContextErr is faster than ctx.Err() because usually it doesn't lock an internal mutex.
// It locks it only if the context is done and at the first call.
// See implementation of cancelCtx in context/context.go.
func FastContextErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func Stopped(ch <-chan struct{}) error {
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return ErrStopped
	default:
	}
	return nil
}

func SafeClose(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
		// Channel was already closed
	default:
		close(ch)
	}
}

// PrioritizedSend message to channel, but if channel is full (slow consumer) - drop half of old messages (not new)
func PrioritizedSend[t any](ch chan t, msg t) {
	select {
	case ch <- msg:
	default: //if channel is full (slow consumer), drop old messages (not new)
		for i := 0; i < cap(ch)/2; i++ {
			select {
			case <-ch:
			default:
			}
		}
		ch <- msg
	}
}
