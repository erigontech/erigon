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

package caplin1

import (
	"context"
	"time"
)

func runForkChoiceClock(ctx context.Context, onTick, onFastConfirmation func(uint64)) {
	var confirmationTicks chan struct{}
	if onFastConfirmation != nil {
		confirmationTicks = make(chan struct{})
		go func() {
			for ctx.Err() == nil {
				select {
				case <-confirmationTicks:
					onFastConfirmation(uint64(time.Now().UnixMilli()))
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			nowMs := uint64(time.Now().UnixMilli())
			onTick(nowMs)
			select {
			case confirmationTicks <- struct{}{}:
			default:
			}
		case <-ctx.Done():
			return
		}
	}
}
