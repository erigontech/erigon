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
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCommitmentFilesRepeatedUnwind(t *testing.T) {
	const stepSize = uint64(10)
	const filesEnd = 5 * stepSize
	for _, references := range []bool{false, true} {
		for _, branch := range []string{"existing", "recreated_db", "recreated_files"} {
			t.Run(fmt.Sprintf("references_%t/%s", references, branch), func(t *testing.T) {
				ctx := t.Context()
				db, agg := testDbAndAggregatorv3(t, stepSize)
				agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, references)
				keys := mkAddrs(0x10, 24)
				nibble := func(key []byte) byte { return crypto.Keccak256(key)[0] >> 4 }
				x, y, other := -1, -1, -1
				var buckets [16][]int
				for i, key := range keys {
					buckets[nibble(key)] = append(buckets[nibble(key)], i)
				}
				for _, bucket := range buckets {
					if len(bucket) == 2 && crypto.Keccak256(keys[bucket[0]])[0] != crypto.Keccak256(keys[bucket[1]])[0] {
						x, y = bucket[0], bucket[1]
						break
					}
				}
				require.NotEqual(t, -1, x, "need exactly two accounts below a one-nibble branch")
				for i := range keys {
					if nibble(keys[i]) != nibble(keys[x]) {
						other = i
						break
					}
				}
				require.NotEqual(t, -1, other, "need an account in another branch")
				account := func(nonce uint64) []byte {
					acc := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(nonce), CodeHash: accounts.EmptyCodeHash}
					return accounts.SerialiseV3(&acc)
				}
				write := func(tx kv.TemporalRwTx, txNum uint64, key, value []byte) (*changeset.StateChangeSet, []byte) {
					t.Helper()
					doms, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithParaTrieDB(db), execctx.WithoutSharedBranchCache())
					require.NoError(t, err)
					defer doms.Close()
					cs := &changeset.StateChangeSet{}
					doms.SetChangesetAccumulator(cs)
					if value == nil {
						require.NoError(t, doms.DomainDel(kv.AccountsDomain, tx, key, txNum, nil))
					} else {
						require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, key, value, txNum, nil))
					}
					root, err := doms.ComputeCommitment(ctx, tx, true, 0, txNum, "", nil)
					require.NoError(t, err)
					require.NoError(t, doms.FlushPendingUpdates(tx))
					doms.SetChangesetAccumulator(nil)
					require.NoError(t, doms.Flush(ctx, tx))
					return cs, bytes.Clone(root)
				}

				writeStepsKeys(t, db, agg, keys, 0, 4)
				require.NoError(t, db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
					if branch == "recreated_files" {
						write(tx, filesEnd-1, keys[x], nil)
					}
					write(tx, filesEnd+1, keys[other], account(1))
					return nil
				}))
				require.NoError(t, agg.BuildFiles(db, filesEnd+stepSize, unboundedFinalityCtx))
				require.NoError(t, agg.MergeLoop(ctx))
				var shortened int
				for _, file := range commitmentKVFiles(t, agg.Dirs().SnapDomain) {
					_, count := branchKeyKinds(t, file.path)
					shortened += count
				}
				require.Equal(t, references, shortened > 0, "exercise the requested file encoding")

				tx, openErr := db.BeginTemporalRw(ctx)
				require.NoError(t, openErr)
				defer tx.Rollback()
				_, pruneErr := tx.PruneSmallBatches(ctx, time.Hour)
				require.NoError(t, pruneErr)
				require.Equal(t, filesEnd, tx.Debug().TxNumsInFiles(kv.CommitmentDomain))
				unwind := func(txNum uint64, cs *changeset.StateChangeSet) {
					t.Helper()
					doms, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithoutSharedBranchCache())
					require.NoError(t, err)
					defer doms.Close()
					var diffs [kv.DomainLen][]kv.DomainEntryDiff
					for idx := range cs.Diffs {
						diffs[idx] = changeset.DeserializeDiffSet(changeset.SerializeDiffSet(cs.Diffs[idx].GetDiffSet(), nil))
					}
					doms.Unwind(txNum, &diffs)
					doms.SetTxNum(txNum)
					require.NoError(t, doms.Flush(ctx, tx))
				}

				if branch == "recreated_db" {
					write(tx, filesEnd+6, keys[x], nil)
				}
				if branch != "existing" {
					prefix := nibbles.HexToCompact([]byte{nibble(keys[x])})
					tombstone, _, err := tx.GetLatest(kv.CommitmentDomain, prefix, kv.GetLatestOptions{})
					require.NoError(t, err)
					require.Len(t, tombstone, 4)
					require.Zero(t, commitment.BranchData(tombstone).ChildCount())
					_, _, inDB, err := tx.Debug().GetLatestFromDB(kv.CommitmentDomain, prefix)
					require.NoError(t, err)
					require.Equal(t, branch == "recreated_db", inDB, "exercise the requested tombstone source")
				}

				// The deeper unwind touches another subtree, so it cannot remove an
				// unnecessary restore left by the first unwind in this subtree.
				first, _ := write(tx, filesEnd+stepSize+6, keys[other], account(500))
				second, _ := write(tx, filesEnd+2*stepSize+3, keys[x], account(1000))
				unwind(filesEnd+2*stepSize+3, second)
				unwind(filesEnd+stepSize+6, first)

				cleanDB, _ := testDbAndAggregatorv3(t, stepSize)
				cleanTx, err := cleanDB.BeginTemporalRw(ctx)
				require.NoError(t, err)
				defer cleanTx.Rollback()
				clean, err := execctx.NewSharedDomains(ctx, cleanTx, log.New(), execctx.WithParaTrieDB(cleanDB))
				require.NoError(t, err)
				defer clean.Close()
				expectedValues := make([][]byte, len(keys))
				for i := range keys {
					acc := accounts.Account{Nonce: 4 * stepSize, Balance: *uint256.NewInt((4*stepSize-1)*100_000 + uint64(i)), CodeHash: accounts.EmptyCodeHash}
					expectedValues[i] = accounts.SerialiseV3(&acc)
				}
				expectedValues[other] = account(1)
				for round, changed := range []int{x, y} {
					expectedValues[changed] = account(2000 + 1000*uint64(round))
					_, actual := write(tx, filesEnd+stepSize+6+uint64(round), keys[changed], expectedValues[changed])
					for i, key := range keys {
						got, _, err := tx.GetLatest(kv.AccountsDomain, key, kv.GetLatestOptions{})
						require.NoError(t, err)
						require.Equalf(t, expectedValues[i], got, "replacement block %d, account %x", round+1, key)
						require.NoError(t, clean.DomainPut(kv.AccountsDomain, cleanTx, key, expectedValues[i], uint64(round+1), nil))
					}
					expected, err := clean.ComputeCommitment(ctx, cleanTx, false, 0, uint64(round+1), "", nil)
					require.NoError(t, err)
					require.Equalf(t, expected, actual, "replacement block %d", round+1)
				}
			})
		}
	}
}
