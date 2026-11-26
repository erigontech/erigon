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

package services

import (
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type benchClock struct{ eth_clock.EthereumClock }

func (benchClock) GetCurrentSlot() uint64       { return timelineSlot }
func (benchClock) GetSlotTime(uint64) time.Time { return timelineSlotStart }

// BenchmarkBlockTimelineImport measures what one gossip block costs the import path: its arrival, one import attempt
// and the clock reads around them, which the gossip path makes also with debug logging off. The line itself is written
// by the writer goroutine.
func BenchmarkBlockTimelineImport(b *testing.B) {
	for _, tc := range []struct {
		name string
		lvl  log.Lvl
	}{{"debug off", log.LvlInfo}, {"debug on", log.LvlDebug}} {
		b.Run(tc.name, func(b *testing.B) {
			logger := log.New()
			logger.SetHandler(log.LvlFilterHandler(tc.lvl, log.DiscardHandler()))
			timelines := newBlockTimelines(benchClock{}, logger)
			go timelines.run(b.Context())
			block, root := timelineBlock(timelineSlot, 6), common.Hash{1}
			b.ReportAllocs()
			for b.Loop() {
				timelines.seen(block, time.Now())
				start := time.Now()
				timelines.importStarted(block, start)
				timelines.importEnded(block, root, start, time.Now(), true, nil)
				// A logged block ignores later imports, so drop its entry to measure a new block each time.
				delete(timelines.pending, block.Signature)
			}
		})
	}
}
