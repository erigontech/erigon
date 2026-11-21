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
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestForkChoiceClock(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "slow fast confirmation"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var ticks, confirmations atomic.Uint64
				var onFastConfirmation func(uint64)
				if enabled {
					onFastConfirmation = func(uint64) {
						confirmations.Add(1)
						<-ctx.Done()
					}
				}
				go runForkChoiceClock(ctx, func(uint64) { ticks.Add(1) }, onFastConfirmation)
				time.Sleep(10 * time.Millisecond)
				synctest.Wait()
				before := ticks.Load()
				time.Sleep(20 * time.Millisecond)
				synctest.Wait()
				require.Greater(t, ticks.Load(), before, "fast confirmation must not stop clock updates")
				if enabled {
					require.EqualValues(t, 1, confirmations.Load(), "slow calculations must not overlap")
				}
				cancel()
				synctest.Wait()
				stopped := ticks.Load()
				time.Sleep(20 * time.Millisecond)
				require.Equal(t, stopped, ticks.Load())
			})
		})
	}
}
