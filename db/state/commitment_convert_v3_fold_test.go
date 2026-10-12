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
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func commitmentFilesByEnd(t *testing.T, snapDomain string) []string {
	t.Helper()
	kvs, err := filepath.Glob(filepath.Join(snapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	end := func(p string) int {
		var from, to int
		name := filepath.Base(p)
		name = name[strings.Index(name, "commitment.")+len("commitment."):]
		_, scanErr := fmt.Sscanf(name, "%d-%d.kv", &from, &to)
		require.NoError(t, scanErr)
		return to*1000 + from
	}
	slices.SortFunc(kvs, func(a, b string) int { return end(a) - end(b) })
	return kvs
}

func buildRandomWriteFiles(t *testing.T, convert bool) (kv.TemporalRwDB, *state.Aggregator) {
	t.Helper()
	const stepSize, steps = 4, 8
	useCommitmentSchema(t, !convert)
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	_, _, putAccount := domainWriter(t, domains, rwTx)
	addrs, _ := generateInputData(t, length.Addr, 1, 64)
	rnd := newRnd(7)
	for txNum := range uint64(stepSize * steps) {
		if txNum == 0 {
			for _, a := range addrs {
				putAccount(a, txNum)
			}
		}
		putAccount(addrs[rnd.IntN(len(addrs))], txNum)
		if txNum%stepSize == stepSize-1 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum/stepSize, txNum, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))
	if convert {
		runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
	}
	return db, agg
}

func TestConvertCommitmentFiles_V3PrefixFoldMatchesStateRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	fixtures := map[string]func(t *testing.T, convert bool) (kv.TemporalRwDB, *state.Aggregator){
		"random_writes": buildRandomWriteFiles,
		"storage_deleted": func(t *testing.T, convert bool) (kv.TemporalRwDB, *state.Aggregator) {
			return buildWipedStorageFiles(t, convert, false)
		},
		"storage_recreated": func(t *testing.T, convert bool) (kv.TemporalRwDB, *state.Aggregator) {
			return buildWipedStorageFiles(t, convert, true)
		},
	}
	for _, name := range slices.Sorted(maps.Keys(fixtures)) {
		for _, convert := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/converted=%t", name, convert), func(t *testing.T) {
				testPrefixFoldMatchesStateRecord(t, fixtures[name], convert)
			})
		}
	}
}

func testPrefixFoldMatchesStateRecord(t *testing.T, build func(t *testing.T, convert bool) (kv.TemporalRwDB, *state.Aggregator), convert bool) {
	db, agg := build(t, convert)
	latest, err := state.FoldCommitmentV3(t.Context(), agg, math.MaxUint64)
	require.NoError(t, err)
	require.Zero(t, latest.Orphans)
	kvs := commitmentFilesByEnd(t, agg.Dirs().SnapDomain)
	require.Greater(t, len(kvs), 1)
	type result struct {
		name, folded, recorded string
		err                    error
	}
	var results []result
	for _, path := range slices.Backward(kvs) {
		keys, vals := readKVFile(t, agg, path)
		idx := slices.IndexFunc(keys, func(k []byte) bool { return bytes.Equal(k, commitment.KeyCommitmentV3State) })
		require.GreaterOrEqual(t, idx, 0, "%s has no v3 state record", filepath.Base(path))
		_, _, recorded, err := commitment.DecodeCommitmentV3State(vals[idx])
		require.NoError(t, err)
		c, foldErr := state.FoldCommitmentV3(t.Context(), agg, math.MaxUint64)
		results = append(results, result{filepath.Base(path), fmt.Sprintf("%x", c.Root), fmt.Sprintf("%x", recorded), foldErr})

		base := strings.TrimSuffix(path, ".kv")
		for _, ext := range []string{".kv", ".bt", ".kvei", ".kvi"} {
			if rmErr := dir.RemoveFile(base + ext); rmErr != nil {
				require.ErrorIs(t, rmErr, os.ErrNotExist)
			}
		}
		require.NoError(t, agg.OpenFolder(db))
	}
	var failed []string
	for _, r := range slices.Backward(results) {
		t.Logf("file=%s folded=%s record=%s err=%v", r.name, r.folded, r.recorded, r.err)
		if r.err != nil || r.folded != r.recorded {
			failed = append(failed, r.name)
		}
	}
	require.Empty(t, failed, "files whose prefix fold does not match their own v3 state record")
}

func buildWipedStorageFiles(t *testing.T, convert, recreate bool) (kv.TemporalRwDB, *state.Aggregator) {
	t.Helper()
	const stepSize, steps, wipeTx = 4, 8, 17
	useCommitmentSchema(t, !convert)
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	put, del, putAccount := domainWriter(t, domains, rwTx)
	addrs, _ := generateInputData(t, length.Addr, 1, 16)
	next := uint64(1)
	owner := addressWithHashPrefix(t, []byte{0x7}, &next)
	slotsOf := func(prefixes ...[]byte) [][]byte {
		var slots [][]byte
		for _, p := range prefixes {
			slots = append(slots, append(bytes.Clone(owner), storageSlotWithHashPrefix(t, p, &next)...))
		}
		return slots
	}
	old := slotsOf([]byte{0xa, 0x1, 0x2}, []byte{0xa, 0x1, 0x5}, []byte{0xa, 0x3}, []byte{0x4, 0x2}, []byte{0x4, 0x9})
	recreated := slotsOf([]byte{0x5, 0x1}, []byte{0x5, 0x2}, []byte{0xc})
	putOwner := func(incarnation, txNum uint64) {
		acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(txNum + 1), CodeHash: accounts.EmptyCodeHash, Incarnation: incarnation}
		put(kv.AccountsDomain, owner, accounts.SerialiseV3(&acc), txNum)
	}
	rnd := newRnd(3)
	for txNum := range uint64(stepSize * steps) {
		switch txNum {
		case 0:
			putOwner(1, txNum)
			for _, s := range old {
				put(kv.StorageDomain, s, []byte{1}, txNum)
			}
		case wipeTx:
			for _, s := range old {
				del(kv.StorageDomain, s, txNum)
			}
			del(kv.AccountsDomain, owner, txNum)
		case wipeTx + stepSize:
			if recreate {
				putOwner(2, txNum)
				for _, s := range recreated {
					put(kv.StorageDomain, s, []byte{2}, txNum)
				}
			}
		}
		putAccount(addrs[rnd.IntN(len(addrs))], txNum)
		if txNum%stepSize == stepSize-1 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum/stepSize, txNum, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))
	if convert {
		runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
	}
	return db, agg
}
