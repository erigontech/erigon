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
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state"
)

func TestReviewPBinLeafProgressAllocatedBytes(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	writePBinLeafStreamRange(t, db, 0, 2*pbinLeafStreamStepSize, []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), nonce: 1, balance: 1},
		{address: pbinLeafStreamAddress(0x55), nonce: 2, balance: 2},
	})
	require.NoError(t, agg.BuildFiles(db, 2*pbinLeafStreamStepSize, unboundedFinalityCtx))
	previousInterval := state.PBTProgressInterval
	state.PBTProgressInterval = time.Nanosecond
	t.Cleanup(func() { state.PBTProgressInterval = previousInterval })
	previousHandler := log.Root().GetHandler()
	var logs bytes.Buffer
	log.Root().SetHandler(log.SyncHandler(log.StreamHandler(&logs, log.LogfmtFormat())))
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })
	at := agg.BeginFilesRo()
	defer at.Close()
	leaves := 0
	require.NoError(t, state.ForEachPBinLeafWithWorkers(at, nil, true, 16, nil, func(state.PBinLeaf) error {
		leaves++
		return nil
	}))
	require.Positive(t, leaves)
	donePattern := regexp.MustCompile(`done=(\d+)`)
	bytesPattern := regexp.MustCompile(`etl_a_buffer_bytes=(\d+)`)
	samples := 0
	for line := range strings.SplitSeq(logs.String(), "\n") {
		if !strings.Contains(line, `phase="source leaf walk"`) {
			continue
		}
		done, allocated := donePattern.FindStringSubmatch(line), bytesPattern.FindStringSubmatch(line)
		if len(done) != 2 || done[1] == "0" {
			continue
		}
		samples++
		if len(allocated) == 2 && allocated[1] == "0" {
			t.Errorf("source walk done=%s reports etl_a_buffer_bytes=0 after collecting nonzero leaves; want allocated bytes > 0", done[1])
			break
		}
	}
	require.Positive(t, samples, "must observe a source walk sample after collecting leaves")
}
