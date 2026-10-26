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

package state_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPBinResumeMergeErrorReleasesAggregatorLock(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	run.writer.SetResumeSourceLayoutForTest()
	run.writer.SetPipelineHookForTest(func(stage string, _ int) error {
		if stage == "resume-merge" {
			return errors.New("injected resume merge error")
		}
		return nil
	})
	_, err := run.write(t.Context(), pbinRangeWriterCorpora()[0].leaves)
	require.ErrorContains(t, err, "injected resume merge error")
	closed := make(chan struct{})
	go func() {
		run.agg.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("aggregator close blocked after resume merge error")
	}
}
