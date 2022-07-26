/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package common

import "errors"

var ErrStopped = errors.New("stopped")
var ErrUnwind = errors.New("unwound")

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
