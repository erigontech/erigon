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
	"encoding/binary"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestForEachPBinLeafWithWorkersStopsSiblingsOnError(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	const valid = 20000
	accounts := make([]pbinLeafStreamAccount, valid)
	for i := range accounts {
		address := make([]byte, length.Addr)
		address[0] = 0xff
		binary.BigEndian.PutUint32(address[length.Addr-4:], uint32(i+1))
		accounts[i] = pbinLeafStreamAccount{address: address, nonce: 1, balance: 1}
	}
	writePBinLeafStreamRange(t, db, 0, pbinLeafStreamStepSize, accounts)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer sd.Close()
	corrupt := make([]byte, length.Addr)
	corrupt[length.Addr-1] = 1
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, corrupt, []byte{0, 0, 0}, 1, nil))
	require.NoError(t, sd.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()

	previousHandler := log.Root().GetHandler()
	var logs bytes.Buffer
	log.Root().SetHandler(log.SyncHandler(log.StreamHandler(&logs, log.LogfmtFormat())))
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })
	at := agg.BeginFilesRo()
	defer at.Close()
	err = state.ForEachPBinLeafWithWorkers(t.Context(), at, nil, true, 2, func(state.PBinLeaf) error { return nil })
	require.ErrorContains(t, err, "pbin leaf stream: account")

	donePattern := regexp.MustCompile(`done=(\d+)`)
	var walked uint64
	for line := range strings.SplitSeq(logs.String(), "\n") {
		if !strings.Contains(line, `phase="source leaf walk"`) {
			continue
		}
		if done := donePattern.FindStringSubmatch(line); len(done) == 2 {
			value, parseErr := strconv.ParseUint(done[1], 10, 64)
			require.NoError(t, parseErr)
			walked = max(walked, value)
		}
	}
	require.Less(t, walked, uint64(valid), "workers must stop once one worker fails")
}

func TestForEachPBinLeafWithWorkersMatchesSerial(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := pbinLeafStreamDatadir(t)
	serial := pbinLeafStreamLeaves(t, db, agg, true)
	serialRoot, err := state.PBinRootFromStream(eip8297.SelectedHash(), func(emit func(state.PBinLeaf) error) error {
		for _, leaf := range serial {
			if err := emit(leaf); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	walkWorkers := temporal.LeafWalkWorkers(t)
	for _, workers := range []int{1, 4, 16} {
		at := agg.BeginFilesRo()
		var parallel []state.PBinLeaf
		err := state.ForEachPBinLeafWithWorkers(t.Context(), at, nil, true, workers, func(leaf state.PBinLeaf) error {
			parallel = append(parallel, state.PBinLeaf{Key: bytes.Clone(leaf.Key), Value: bytes.Clone(leaf.Value), Stamp: leaf.Stamp})
			return nil
		})
		at.Close()
		require.NoError(t, err)
		require.Equal(t, serial, parallel, "workers=%d", workers)
		parallelRoot, err := state.PBinRootFromStream(eip8297.SelectedHash(), func(emit func(state.PBinLeaf) error) error {
			for _, leaf := range parallel {
				if emitErr := emit(leaf); emitErr != nil {
					return emitErr
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, serialRoot, parallelRoot, "workers=%d", workers)
		if walked := walkWorkers(); workers > 1 {
			require.GreaterOrEqual(t, walked, 2, "workers=%d", workers)
		} else {
			require.Zero(t, walked)
		}
	}
}
