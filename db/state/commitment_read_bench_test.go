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
	"context"
	"encoding/binary"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// setupCommitmentReadBench builds a populated commitment domain with a known
// tx-count of account/storage updates and collects every commitment key from
// the resulting snapshot files. It returns the temporal DB, the aggregator,
// and the collected keys (copied — safe to retain past the RO tx).
func setupCommitmentReadBench(b *testing.B, stepSize uint64, nAccts, nStor int, totalSteps uint64) (kv.TemporalRwDB, *state.Aggregator, [][]byte) {
	b.Helper()
	db, agg := testDbAndAggregatorv3(b, stepSize)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(b, err)
	defer domains.Close()

	totalTxs := stepSize * totalSteps
	var blockNum uint64
	acctRng := rand.New(rand.NewSource(1))
	storRng := rand.New(rand.NewSource(2))
	addr := make([]byte, length.Addr)
	stor := make([]byte, length.Addr+length.Hash)
	var val [32]byte
	for txNum := uint64(0); txNum < totalTxs; txNum++ {
		for i := 0; i < nAccts; i++ {
			acctRng.Read(addr)
			binary.BigEndian.PutUint64(addr[:8], uint64(i))
			acc := accounts.Account{
				Nonce:    txNum,
				Balance:  *uint256.NewInt(txNum*1000 + uint64(i)),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			require.NoError(b, domains.DomainPut(kv.AccountsDomain, rwTx, addr, buf, txNum, nil))
		}
		for i := 0; i < nStor; i++ {
			storRng.Read(stor)
			binary.BigEndian.PutUint64(stor[:8], uint64(i%nAccts))
			storRng.Read(val[:])
			require.NoError(b, domains.DomainPut(kv.StorageDomain, rwTx, stor, val[:], txNum, nil))
		}
		if (txNum+1)%stepSize == 0 {
			_, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(b, err)
			require.NoError(b, domains.Flush(ctx, rwTx))
			require.NoError(b, rawdbv3.TxNums.Append(rwTx, blockNum, txNum))
			blockNum++
		}
	}
	require.NoError(b, domains.Flush(ctx, rwTx))
	domains.Close()
	require.NoError(b, rwTx.Commit())
	require.NoError(b, agg.BuildFiles(totalTxs))

	acRo := agg.BeginFilesRo()
	defer acRo.Close()
	iter, err := acRo.DebugRangeLatestFromFiles(kv.CommitmentDomain, nil, nil, -1)
	require.NoError(b, err)
	var keys [][]byte
	for iter.HasNext() {
		k, _, err := iter.Next()
		require.NoError(b, err)
		keys = append(keys, append([]byte(nil), k...))
	}
	return db, agg, keys
}

// BenchmarkCommitmentReadPaths populates a commitment domain under both
// embedded and de-embedded branch storage and measures the cost of the
// four logical read paths:
//
//   - raw:        one DomainGet on the commitment key (one entry, whether
//     an embedded blob or a de-embed meta/child).
//   - branch:     TrieContext.Branch(prefix) — logical full-branch fetch
//     (in de-embed mode this is 1 meta + popcount(bitmap) children).
//   - meta:       TrieContext.BranchMeta(prefix) — bitmaps + packed hashes
//     only (models parent-hash recompute in de-embed: 1 read).
//   - meta+child: TrieContext.BranchMeta + one TrieContext.BranchChild
//     (models descent into a single nibble: 2 reads in de-embed).
//
// The interesting ratios are within the de-embed sub-benchmark:
//
//	branch / meta      — upper bound on savings from lazy parent-hash recompute
//	branch / (meta+child) — upper bound on savings from lazy descent
func BenchmarkCommitmentReadPaths(b *testing.B) {
	for _, deembed := range []bool{false, true} {
		label := "embedded"
		if deembed {
			label = "deembed"
		}
		b.Run(label, func(b *testing.B) {
			prev := commitment.DeEmbedCommitment
			commitment.DeEmbedCommitment = deembed
			b.Cleanup(func() { commitment.DeEmbedCommitment = prev })

			const stepSize = 64
			const totalSteps = 2
			db, agg, keys := setupCommitmentReadBench(b, stepSize, 4000, 1000, totalSteps)
			if len(keys) == 0 {
				b.Fatal("no commitment keys collected — populate step produced no snapshot files")
			}

			// Build the list of unique branch prefixes. In embedded mode each
			// domain key IS a branch prefix. In de-embed each branch contributes
			// 1 meta + popcount(bitmap) children, so strip the trailing byte.
			prefixes := make([][]byte, 0, len(keys))
			if deembed {
				seen := make(map[string]struct{}, len(keys))
				for _, k := range keys {
					if len(k) == 0 {
						continue
					}
					p := k[:len(k)-1]
					if _, ok := seen[string(p)]; ok {
						continue
					}
					seen[string(p)] = struct{}{}
					prefixes = append(prefixes, append([]byte(nil), p...))
				}
			} else {
				for _, k := range keys {
					prefixes = append(prefixes, append([]byte(nil), k...))
				}
			}
			b.Logf("populated: %d commitment keys, %d unique branch prefixes (deembed=%t)", len(keys), len(prefixes), deembed)

			ctx := context.Background()
			roTx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
			require.NoError(b, err)
			defer roTx.Rollback()

			roDomains, err := execctx.NewSharedDomains(ctx, roTx, log.New())
			require.NoError(b, err)
			b.Cleanup(roDomains.Close)

			reader := commitmentdb.NewLatestStateReader(roTx, roDomains)
			trieCtx := commitmentdb.NewTrieContextRo(reader, agg.StepSize())
			stepSz := agg.StepSize()

			b.Run("raw", func(b *testing.B) {
				b.ReportAllocs()
				var idx int
				for b.Loop() {
					if _, _, err := reader.Read(kv.CommitmentDomain, keys[idx%len(keys)], stepSz); err != nil {
						b.Fatal(err)
					}
					idx++
				}
			})
			b.Run("branch", func(b *testing.B) {
				b.ReportAllocs()
				var idx int
				for b.Loop() {
					if _, _, err := trieCtx.Branch(prefixes[idx%len(prefixes)]); err != nil {
						b.Fatal(err)
					}
					idx++
				}
			})
			b.Run("meta", func(b *testing.B) {
				b.ReportAllocs()
				var idx int
				for b.Loop() {
					if _, _, _, _, _, err := trieCtx.BranchMeta(prefixes[idx%len(prefixes)]); err != nil {
						b.Fatal(err)
					}
					idx++
				}
			})
			b.Run("meta+child", func(b *testing.B) {
				b.ReportAllocs()
				rng := rand.New(rand.NewSource(7))
				var idx int
				for b.Loop() {
					p := prefixes[idx%len(prefixes)]
					tMap, aMap, _, _, _, err := trieCtx.BranchMeta(p)
					if err != nil {
						b.Fatal(err)
					}
					bm := tMap & aMap
					if bm != 0 {
						pop := bits.OnesCount16(bm)
						target := rng.Intn(pop)
						var nibble byte
						for bs, seen := bm, 0; bs != 0; {
							bit := bs & -bs
							if seen == target {
								nibble = byte(bits.TrailingZeros16(bit))
								break
							}
							bs ^= bit
							seen++
						}
						if _, _, err := trieCtx.BranchChild(p, nibble); err != nil {
							b.Fatal(err)
						}
					}
					idx++
				}
			})
		})
	}
}
