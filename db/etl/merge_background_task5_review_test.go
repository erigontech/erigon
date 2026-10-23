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

package etl

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestReviewBackgroundCancellationWithDeduplicatedInputs(t *testing.T) {
	for _, background := range []bool{false, true} {
		name := "serial"
		if background {
			name = "background"
		}
		t.Run(name, func(t *testing.T) {
			collectors := make([]*Collector, 2048)
			dir := t.TempDir()
			for i := range collectors {
				collector := NewCollector(t.Name(), dir, NewOldestEntryBuffer(BufferOptimalSize), log.New()).MergeInBackground(background)
				t.Cleanup(collector.Close)
				require.NoError(t, collector.Collect([]byte("same-key"), []byte("value")))
				collectors[i] = collector
			}
			quit := make(chan struct{})
			loads := 0
			err := MergeLoad(t.Name(), collectors, func(_, _ []byte) error {
				loads++
				if loads == 1 {
					close(quit)
				}
				return nil
			}, TransformArgs{Quit: quit})
			if !errors.Is(err, common.ErrStopped) {
				t.Errorf("cancellation after first deduplicated load: got err=%v loads=%d; want ErrStopped", err, loads)
			}
		})
	}
}
