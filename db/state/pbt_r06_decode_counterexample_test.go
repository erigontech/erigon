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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func TestPBinR06DecodeFailureStopsQueuedCompute(t *testing.T) {
	selectPBinRangeWriterHash(t)
	run := openPBinRangeWriterRun(t, 30, 0, 23)
	leaves := make([]state.PBinLeaf, 100)
	for i := range leaves {
		key := make([]byte, eip8297.StorageKeyLength)
		key[0] = eip8297.StorageZone
		binary.BigEndian.PutUint64(key[len(key)-8:], uint64(i+1))
		leaves[i] = state.PBinLeaf{Key: key, Value: pbinRangeWriterValue(1), Stamp: 1}
	}
	encoded, encodeErr := pbt.EncodeOp(pbt.Op{Key: leaves[0].Key})
	require.NoError(t, encodeErr)
	var path string
	var queuedBatchFlushes atomic.Int32
	run.writer.SetPipelineHookForTest(func(stage string, index int) error {
		if stage == "stage1" && index == 0 {
			paths, err := filepath.Glob(filepath.Join(run.agg.Dirs().Tmp, "pbin-ops-*.bin"))
			if err != nil {
				return err
			}
			if len(paths) != 1 {
				return fmt.Errorf("want one ops file, got %v", paths)
			}
			path = paths[0]
			file, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = file.WriteAt([]byte{0, 0, 0, 0}, int64(70*(4+8+len(encoded))))
			return err
		}
		if stage == "stage2" && index == 0 {
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if _, err := os.Stat(path); os.IsNotExist(err) {
					return nil
				}
				time.Sleep(time.Millisecond)
			}
			return errors.New("decoder did not finish")
		}
		if stage == "stage3" && index == 1 {
			queuedBatchFlushes.Add(1)
		}
		return nil
	})
	_, writeErr := run.write(t.Context(), leaves)
	require.ErrorContains(t, writeErr, "operation record of 0 bytes")
	require.Zero(t, queuedBatchFlushes.Load(), "stage 1 decode failed, but queued batch 1 still completed compute and reached collect: %d", queuedBatchFlushes.Load())
}
