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
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	_ "github.com/erigontech/erigon/execution/commitment/v4"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func storageSlotsSharingFirstNibble(tb testing.TB) [][]byte {
	tb.Helper()
	byNibble := map[byte][][]byte{}
	for i := uint64(1); ; i++ {
		slot := make([]byte, length.Hash)
		binary.BigEndian.PutUint64(slot[length.Hash-8:], i)
		nibble := crypto.Keccak256(slot)[0] >> 4
		byNibble[nibble] = append(byNibble[nibble], slot)
		if len(byNibble[nibble]) < 2 {
			continue
		}
		for other := range byte(16) {
			if other != nibble && len(byNibble[other]) != 0 {
				return [][]byte{byNibble[nibble][0], byNibble[nibble][1], byNibble[other][0]}
			}
		}
	}
}

func testDbAggregatorWithCommitmentHistory(t *testing.T, stepSize uint64, steps int) (kv.TemporalRwDB, *state.Aggregator, map[uint64][]byte) {
	t.Helper()
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	db, agg := testDbAndAggregatorv3(t, stepSize)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	ctx := t.Context()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New(), execctx.WithParaTrieDB(db))
	require.NoError(t, err)
	defer domains.Close()

	rnd := newRnd(7)
	addrs, _ := generateInputData(t, length.Addr, 1, 40)
	slots := storageSlotsSharingFirstNibble(t)
	storageKey := func(a, j int) []byte { return append(bytes.Clone(addrs[a]), slots[j]...) }
	put := func(d kv.Domain, k, v []byte, txNum uint64) {
		prev, _, getErr := domains.GetLatest(d, rwTx, k)
		require.NoError(t, getErr)
		require.NoError(t, domains.DomainPut(d, rwTx, k, v, txNum, prev))
	}
	del := func(d kv.Domain, k []byte, txNum uint64) {
		prev, _, getErr := domains.GetLatest(d, rwTx, k)
		require.NoError(t, getErr)
		if len(prev) == 0 {
			return
		}
		require.NoError(t, domains.DomainDel(d, rwTx, k, txNum, prev))
	}

	roots := map[uint64][]byte{}
	txCount := stepSize * uint64(steps)
	var blockNum uint64
	for txNum := range txCount {
		for range 3 {
			a := rnd.IntN(len(addrs))
			acc := accounts.Account{Nonce: txNum, Balance: *uint256.NewInt(rnd.Uint64()), CodeHash: accounts.EmptyCodeHash}
			put(kv.AccountsDomain, addrs[a], accounts.SerialiseV3(&acc), txNum)
			j := rnd.IntN(len(slots))
			if rnd.IntN(4) == 0 {
				del(kv.StorageDomain, storageKey(a, j), txNum)
			} else {
				put(kv.StorageDomain, storageKey(a, j), []byte{byte(rnd.IntN(255) + 1)}, txNum)
			}
		}
		if rnd.IntN(8) == 0 {
			a := rnd.IntN(len(addrs))
			for j := range slots {
				del(kv.StorageDomain, storageKey(a, j), txNum)
			}
			del(kv.AccountsDomain, addrs[a], txNum)
		}
		if txNum%3 == 2 || (txNum+1)%stepSize == 0 {
			root, rootErr := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, rootErr)
			roots[txNum] = root
			blockNum++
		}
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, txCount, unboundedFinalityCtx))
	return db, agg, roots
}

func TestConvertCommitmentFiles_V3History(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	db, agg, roots := testDbAggregatorWithCommitmentHistory(t, 10, 32)
	legacyHistory, err := filepath.Glob(filepath.Join(agg.Dirs().SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.NotEmpty(t, legacyHistory)

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	converted, err := filepath.Glob(filepath.Join(agg.Dirs().SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.Len(t, converted, len(legacyHistory))
	for _, p := range converted {
		require.Truef(t, strings.HasPrefix(filepath.Base(p), "v3.0-"), "%s is not a v3.0 history file", filepath.Base(p))
	}
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	latestState, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentV4State, kv.GetLatestOptions{})
	require.NoError(t, err)
	_, lastFrozenCommit, _, err := commitment.DecodeCommitmentV4State(latestState)
	require.NoError(t, err)
	tx.Rollback()
	checked := 0
	for _, txNum := range slices.Sorted(maps.Keys(roots)) {
		if txNum > lastFrozenCommit {
			break
		}
		checked++
		root, rootErr := state.DebugCommitmentV3RootAsOf(t.Context(), agg, txNum+1)
		require.NoErrorf(t, rootErr, "records as of txNum %d", txNum+1)
		require.Equalf(t, roots[txNum], root, "root as of txNum %d", txNum+1)
	}
	require.Greater(t, checked, 90)
}
