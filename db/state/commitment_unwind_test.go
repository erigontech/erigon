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
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCommitmentFilesRepeatedUnwind(t *testing.T) {
	const (
		stepSize           = uint64(10)
		filesEnd           = 5 * stepSize
		deepUnwindTxNum    = filesEnd + stepSize + 6
		shallowUnwindTxNum = filesEnd + 2*stepSize + 3
	)
	keys := mkAddrs(0x10, 24)
	nibble := func(key []byte) byte { return crypto.Keccak256(key)[0] >> 4 }
	targetIdx, siblingIdx, otherBranchIdx := -1, -1, -1
	var buckets [16][]int
	for i, key := range keys {
		buckets[nibble(key)] = append(buckets[nibble(key)], i)
	}
	for _, bucket := range buckets {
		if len(bucket) == 2 && crypto.Keccak256(keys[bucket[0]])[0] != crypto.Keccak256(keys[bucket[1]])[0] {
			targetIdx, siblingIdx = bucket[0], bucket[1]
			break
		}
	}
	require.NotEqual(t, -1, targetIdx, "need exactly two accounts below a one-nibble branch")
	for i := range keys {
		if nibble(keys[i]) != nibble(keys[targetIdx]) {
			otherBranchIdx = i
			break
		}
	}
	require.NotEqual(t, -1, otherBranchIdx, "need an account in another branch")

	for _, references := range []bool{false, true} {
		for _, branch := range []string{"existing", "recreated_db", "recreated_files"} {
			t.Run(fmt.Sprintf("references_%t/%s", references, branch), func(t *testing.T) {
				ctx := t.Context()
				db, agg := testDbAndAggregatorv3(t, stepSize)
				agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, references)
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
						write(tx, filesEnd-1, keys[targetIdx], nil)
					}
					write(tx, filesEnd+1, keys[otherBranchIdx], account(1))
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
				if branch == "recreated_db" {
					write(tx, filesEnd+6, keys[targetIdx], nil)
				}
				if branch != "existing" {
					prefix := nibbles.HexToCompact([]byte{nibble(keys[targetIdx])})
					emptyBranch, _, err := tx.GetLatest(kv.CommitmentDomain, prefix, kv.GetLatestOptions{})
					require.NoError(t, err)
					require.Len(t, emptyBranch, 4)
					require.Zero(t, commitment.BranchData(emptyBranch).ChildCount())
					_, _, inDB, err := tx.Debug().GetLatestFromDB(kv.CommitmentDomain, prefix)
					require.NoError(t, err)
					require.Equal(t, branch == "recreated_db", inDB, "exercise the requested empty branch source")
				}

				// The deeper unwind touches another subtree, so it cannot remove an
				// unnecessary restore left by the first unwind in this subtree.
				deepChanges, _ := write(tx, deepUnwindTxNum, keys[otherBranchIdx], account(500))
				shallowChanges, _ := write(tx, shallowUnwindTxNum, keys[targetIdx], account(1000))
				unwindStateChanges(t, tx, shallowUnwindTxNum, shallowChanges)
				unwindStateChanges(t, tx, deepUnwindTxNum, deepChanges)

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
				expectedValues[otherBranchIdx] = account(1)
				for round, changed := range []int{targetIdx, siblingIdx} {
					expectedValues[changed] = account(2000 + 1000*uint64(round))
					_, actual := write(tx, deepUnwindTxNum+uint64(round), keys[changed], expectedValues[changed])
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

func TestCommitmentStorageBranchRepeatedUnwind(t *testing.T) {
	const stepSize = uint64(16)
	for _, files := range []bool{false, true} {
		for _, deferred := range []bool{false, true} {
			t.Run(fmt.Sprintf("files_%t/deferred_%t", files, deferred), func(t *testing.T) {
				ctx := t.Context()
				db, agg := testDbAndAggregatorv3(t, stepSize)
				keys, contract := mkAddrs(0x10, 3), mkAddrs(0x20, 1)[0]
				slot := func(i byte) []byte {
					key := make([]byte, length.Addr+length.Hash)
					copy(key, contract)
					key[length.Addr], key[len(key)-1] = i*37, i
					return key
				}
				account := func(nonce uint64) []byte {
					acc := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(nonce + 7), CodeHash: accounts.EmptyCodeHash}
					return accounts.SerialiseV3(&acc)
				}
				write := func(database kv.TemporalRwDB, txNum uint64, update func(*execctx.SharedDomains, kv.TemporalRwTx)) (*changeset.StateChangeSet, []byte) {
					t.Helper()
					cs := &changeset.StateChangeSet{}
					var root []byte
					require.NoError(t, database.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
						doms, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithParaTrieDB(database), execctx.WithoutSharedBranchCache())
						require.NoError(t, err)
						defer doms.Close()
						doms.SetDeferCommitmentUpdates(deferred)
						doms.SetChangesetAccumulator(cs)
						update(doms, tx)
						root, err = doms.ComputeCommitment(ctx, tx, true, 0, txNum, "", nil)
						require.NoError(t, err)
						root = bytes.Clone(root)
						require.NoError(t, doms.FlushPendingUpdates(tx))
						doms.SetChangesetAccumulator(nil)
						return doms.Flush(ctx, tx)
					}))
					return cs, root
				}
				unwind := func(txNum uint64, cs *changeset.StateChangeSet) {
					t.Helper()
					require.NoError(t, db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
						unwindStateChanges(t, tx, txNum, cs)
						return nil
					}))
				}
				putAccounts := func(doms *execctx.SharedDomains, tx kv.TemporalRwTx, txNum uint64) {
					for i, key := range keys {
						require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, key, account(uint64(i+1)), txNum, nil))
					}
				}
				putContract := func(doms *execctx.SharedDomains, tx kv.TemporalRwTx, txNum uint64, value byte) {
					require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, contract, account(200+uint64(value)), txNum, nil))
					for i := byte(1); i <= 2; i++ {
						require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, slot(i), []byte{value, i}, txNum, nil))
					}
				}
				write(db, 2, func(doms *execctx.SharedDomains, tx kv.TemporalRwTx) {
					putAccounts(doms, tx, 2)
					putContract(doms, tx, 2, 1)
				})
				write(db, 5, func(doms *execctx.SharedDomains, tx kv.TemporalRwTx) {
					require.NoError(t, doms.DomainDel(kv.AccountsDomain, tx, contract, 5, nil))
				})
				deepChanges, _ := write(db, 20, func(doms *execctx.SharedDomains, tx kv.TemporalRwTx) {
					require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, keys[0], account(20), 20, nil))
				})
				if files {
					require.NoError(t, agg.BuildFiles(db, 21, unboundedFinalityCtx))
					require.NoError(t, db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
						_, err := tx.PruneSmallBatches(ctx, time.Hour)
						require.NoError(t, err)
						require.Equal(t, stepSize, tx.Debug().TxNumsInFiles(kv.CommitmentDomain))
						return nil
					}))
				}
				// Deleting an account can leave its unreachable storage branch in the domain.
				require.NoError(t, db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
					path := nibbles.KeybytesToHex(crypto.Keccak256(contract))
					prefix := nibbles.HexToCompact(path[:len(path)-1])
					branch, _, err := tx.GetLatest(kv.CommitmentDomain, prefix, kv.GetLatestOptions{})
					require.NoError(t, err)
					require.Equal(t, 2, commitment.BranchData(branch).ChildCount())
					return nil
				}))
				shallowChanges, _ := write(db, 37, func(doms *execctx.SharedDomains, tx kv.TemporalRwTx) {
					putContract(doms, tx, 37, 7)
				})
				unwind(37, shallowChanges)
				unwind(20, deepChanges)
				write(db, 20, func(doms *execctx.SharedDomains, tx kv.TemporalRwTx) {
					putContract(doms, tx, 20, 9)
				})
				_, actual := write(db, 21, func(doms *execctx.SharedDomains, tx kv.TemporalRwTx) {
					require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, slot(1), []byte{0xEE}, 21, nil))
				})

				cleanDB, _ := testDbAndAggregatorv3(t, stepSize)
				_, expected := write(cleanDB, 21, func(doms *execctx.SharedDomains, tx kv.TemporalRwTx) {
					putAccounts(doms, tx, 21)
					putContract(doms, tx, 21, 9)
					require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, slot(1), []byte{0xEE}, 21, nil))
				})
				require.Equal(t, expected, actual)
			})
		}
	}
}

func unwindStateChanges(t *testing.T, tx kv.TemporalRwTx, txNum uint64, cs *changeset.StateChangeSet) {
	t.Helper()
	ctx := t.Context()
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
