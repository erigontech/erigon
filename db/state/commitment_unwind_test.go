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
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCommitmentFilesRepeatedUnwind(t *testing.T) {
	const stepSize = uint64(10)
	for _, references := range []bool{false, true} {
		t.Run(fmt.Sprintf("references_%t", references), func(t *testing.T) {
			ctx := t.Context()
			db, agg := testDbAndAggregatorv3(t, stepSize)
			agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, references)
			keys := mkAddrs(0x10, 24)
			nibble := func(key []byte) byte { return crypto.Keccak256(key)[0] >> 4 }
			x, y, other := -1, -1, -1
			for i := range keys {
				for j := i + 1; j < len(keys) && x < 0; j++ {
					if nibble(keys[i]) == nibble(keys[j]) {
						x, y = i, j
					}
				}
			}
			require.NotEqual(t, -1, x, "need two accounts in the same trie branch")
			for i := range keys {
				if nibble(keys[i]) != nibble(keys[x]) {
					other = i
					break
				}
			}
			require.NotEqual(t, -1, other, "need an account in another branch")
			writeStepsKeys(t, db, agg, keys, 0, 4)
			require.NoError(t, db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
				doms, err := execctx.NewSharedDomains(ctx, tx, log.New())
				require.NoError(t, err)
				defer doms.Close()
				acc := accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}
				require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, bytes.Repeat([]byte{0xee}, 20), accounts.SerialiseV3(&acc), 4*stepSize+1, nil))
				return doms.Flush(ctx, tx)
			}))
			require.NoError(t, agg.BuildFiles(db, 5*stepSize, unboundedFinalityCtx))
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
			require.Equal(t, 4*stepSize, tx.Debug().TxNumsInFiles(kv.CommitmentDomain))
			write := func(txNum uint64, key []byte, nonce uint64) (*changeset.StateChangeSet, []byte) {
				doms, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithoutSharedBranchCache())
				require.NoError(t, err)
				defer doms.Close()
				cs := &changeset.StateChangeSet{}
				doms.SetChangesetAccumulator(cs)
				acc := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(nonce), CodeHash: accounts.EmptyCodeHash}
				require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, key, accounts.SerialiseV3(&acc), txNum, nil))
				root, err := doms.ComputeCommitment(ctx, tx, true, 0, txNum, "", nil)
				require.NoError(t, err)
				doms.SetChangesetAccumulator(nil)
				require.NoError(t, doms.Flush(ctx, tx))
				return cs, bytes.Clone(root)
			}
			unwind := func(txNum uint64, cs *changeset.StateChangeSet) {
				doms, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithoutSharedBranchCache())
				require.NoError(t, err)
				defer doms.Close()
				var diffs [kv.DomainLen][]kv.DomainEntryDiff
				for idx := range cs.Diffs {
					diffs[idx] = cs.Diffs[idx].GetDiffSet()
				}
				doms.Unwind(txNum, &diffs)
				doms.SetTxNum(txNum)
				require.NoError(t, doms.Flush(ctx, tx))
			}

			// The deeper unwind touches another subtree, so it cannot remove an
			// unnecessary restore left by the first unwind in this subtree.
			first, _ := write(5*stepSize+6, keys[other], 500)
			second, _ := write(6*stepSize+3, keys[x], 1000)
			unwind(6*stepSize+3, second)
			unwind(5*stepSize+6, first)
			_, replacementRoot := write(5*stepSize+6, keys[x], 2000)
			_, nextRoot := write(5*stepSize+7, keys[y], 3000)

			cleanDB, _ := testDbAndAggregatorv3(t, stepSize)
			cleanTx, err := cleanDB.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer cleanTx.Rollback()
			clean, err := execctx.NewSharedDomains(ctx, cleanTx, log.New())
			require.NoError(t, err)
			defer clean.Close()
			for round, actual := range [][]byte{replacementRoot, nextRoot} {
				for i, key := range keys {
					acc := accounts.Account{Nonce: 4 * stepSize, Balance: *uint256.NewInt((4*stepSize-1)*100_000 + uint64(i)), CodeHash: accounts.EmptyCodeHash}
					if i == x {
						acc.Nonce, acc.Balance = 2000, *uint256.NewInt(2000)
					}
					if i == y && round == 1 {
						acc.Nonce, acc.Balance = 3000, *uint256.NewInt(3000)
					}
					require.NoError(t, clean.DomainPut(kv.AccountsDomain, cleanTx, key, accounts.SerialiseV3(&acc), uint64(round+1), nil))
				}
				expected, err := clean.ComputeCommitment(ctx, cleanTx, false, 0, uint64(round+1), "", nil)
				require.NoError(t, err)
				require.Equalf(t, expected, actual, "replacement block %d", round+1)
			}
		})
	}
}
