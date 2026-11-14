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

package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestCachesRepeatedUnwind(t *testing.T) {
	t.Parallel()
	for _, domain := range kv.StateDomains {
		t.Run(domain.String(), func(t *testing.T) {
			ctx := t.Context()
			db := newTestDb(t, 16)
			sc := newSmallStateCache()
			t.Cleanup(sc.Close)
			key := make([]byte, 20)
			a, b, c := []byte{1}, []byte{2}, []byte{3}
			switch domain {
			case kv.AccountsDomain:
				a, b, c = encAccount(1), encAccount(2), encAccount(3)
			case kv.StorageDomain:
				key = make([]byte, 52)
			case kv.CommitmentDomain:
				key = []byte{0x0a, 0x0c}
				a, b, c = []byte{0, 0, 0, 0, 1}, []byte{0, 0, 0, 0, 2}, []byte{0, 0, 0, 0, 3}
			}
			cached := func() bool {
				_, hit := sc.View(nil).Get(domain, key)
				return hit
			}
			if domain == kv.CommitmentDomain {
				require.NoError(t, db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
					branches := tx.AggTx().(commitment.BranchCacheProvider).BranchCache()
					require.NotNil(t, branches)
					cached = func() bool {
						_, _, hit := branches.Get(key)
						return hit
					}
					return nil
				}))
			}
			diffs := map[uint64][kv.DomainLen][]kv.DomainEntryDiff{}
			write := func(txNum uint64, value, previous []byte) {
				t.Helper()
				tx, err := db.BeginTemporalRw(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
				require.NoError(t, err)
				defer sd.Close()
				sd.BindStateCache(sc)
				changes := &changeset.StateChangeSet{}
				sd.SetChangesetAccumulator(changes)
				sd.SetTxNum(txNum)
				require.NoError(t, sd.DomainPut(domain, tx, key, value, txNum, previous))
				var diff [kv.DomainLen][]kv.DomainEntryDiff
				for id := range changes.Diffs {
					diff[id] = changeset.DeserializeDiffSet(changeset.SerializeDiffSet(changes.Diffs[id].GetDiffSet(), nil))
				}
				diffs[txNum] = diff
				require.NoError(t, sd.Commit(ctx, tx))
			}
			unwind := func(to uint64, beforeCommit func()) {
				t.Helper()
				tx, err := db.BeginTemporalRw(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
				require.NoError(t, err)
				defer sd.Close()
				sd.BindStateCache(sc)
				sd.SetTxNum(to - 1)
				diff := diffs[to]
				sd.Unwind(to, &diff)
				got, _, err := sd.AsGetter(tx).GetLatest(domain, key, kv.GetLatestOptions{})
				require.NoError(t, err)
				require.Equal(t, diff[domain][0].Value, got, "the staged view must serve the restored value")
				require.NoError(t, sd.Commit(ctx, tx, func(kv.RwTx) error {
					if beforeCommit != nil {
						beforeCommit()
					}
					return nil
				}))
			}
			read := func(want []byte) {
				t.Helper()
				tx, err := db.BeginTemporalRo(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
				require.NoError(t, err)
				defer sd.Close()
				sd.BindStateCache(sc)
				direct, _, err := tx.GetLatest(domain, key, kv.GetLatestOptions{})
				require.NoError(t, err)
				require.Equal(t, want, direct)
				got, _, err := sd.AsGetter(tx).GetLatest(domain, key, kv.GetLatestOptions{})
				require.NoError(t, err)
				require.Equal(t, direct, got, "cached reads must match the committed MDBX state")
			}
			write(2, a, nil)
			write(17, b, a)
			write(18, a, b)
			write(19, c, a)
			unwind(19, nil)
			read(a)
			if domain != kv.CommitmentDomain {
				t.Run("history", func(t *testing.T) {
					require.NoError(t, db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
						sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
						require.NoError(t, err)
						defer sd.Close()
						sd.BindStateCache(sc)
						sd.SetInMemHistoryReads(true)
						direct, _, err := tx.GetAsOf(domain, key, 18)
						require.NoError(t, err)
						require.Equal(t, b, direct)
						got, _, err := sd.AsOfGetter(tx).GetAsOf(domain, key, 18)
						require.NoError(t, err)
						require.Equal(t, direct, got, "the latest value must not replace an older historical value")
						return nil
					}))
				})
			}
			require.True(t, cached(), "the read between unwinds must fill the cache")
			// Readers can refill from the old committed state after staging the unwind.
			unwind(18, func() { read(a) })
			read(b)
		})
	}
}
