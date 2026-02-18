// Copyright 2024 The Erigon Authors
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
	"time"
)

const (
	semaphoreWaitTimeout  = 2 * time.Second
	semaphoreWaitInterval = 10 * time.Millisecond
)

func (e *EthereumExecutionModule) tryWaitForUnlock(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return false
	}
	if e.semaphore.TryAcquire(1) {
		return true
	}

	timer := time.NewTimer(semaphoreWaitTimeout)
	ticker := time.NewTicker(semaphoreWaitInterval)
	defer timer.Stop()
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return false
		case <-ticker.C:
			if e.semaphore.TryAcquire(1) {
				return true
			}
		}
	}
}
